package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Storage defines the interface for bucket/object operations.
type Storage interface {
	BucketExists(bucket string) bool
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]BucketInfo, error)
	ListObjects(bucket, prefix string, maxKeys int) ([]ObjectInfo, error)
	PutObject(bucket, key string, reader io.Reader, contentType string) (*ObjectMetadata, error)
	GetObject(bucket, key string) (io.ReadCloser, *ObjectMetadata, error)
	HeadObject(bucket, key string) (*ObjectMetadata, error)
	DeleteObject(bucket, key string) error
	CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*ObjectMetadata, error)
}

type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

type FilesystemStorage struct {
	dataDir    string
	writeLocks sync.Map // map[string]*sync.Mutex for per-object write locks
}

type ObjectMetadata struct {
	Size         int64     `json:"size"`
	LastModified time.Time `json:"lastModified"`
	ETag         string    `json:"etag"`
	ContentType  string    `json:"contentType,omitempty"`
}

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

func NewFilesystemStorage(dataDir string) *FilesystemStorage {
	return &FilesystemStorage{dataDir: dataDir}
}

// Path validation to prevent directory traversal
func (fs *FilesystemStorage) validateBucketPath(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("invalid bucket name")
	}
	resolved := filepath.Join(fs.dataDir, bucket)
	absData, err := filepath.Abs(fs.dataDir)
	if err != nil {
		return err
	}
	absResolved, err := filepath.Abs(resolved)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(absResolved, absData+string(filepath.Separator)) {
		return fmt.Errorf("invalid bucket name")
	}
	return nil
}

func (fs *FilesystemStorage) validateObjectPath(bucket, key string) error {
	if err := fs.validateBucketPath(bucket); err != nil {
		return err
	}
	if key == "" || strings.Contains(key, "\x00") {
		return fmt.Errorf("invalid key")
	}
	resolved := filepath.Join(fs.dataDir, bucket, filepath.FromSlash(key))
	absBucket, err := filepath.Abs(filepath.Join(fs.dataDir, bucket))
	if err != nil {
		return err
	}
	absResolved, err := filepath.Abs(resolved)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(absResolved, absBucket+string(filepath.Separator)) {
		return fmt.Errorf("invalid key")
	}
	return nil
}

// computeFileETag computes an MD5 ETag by streaming the file content.
func (fs *FilesystemStorage) computeFileETag(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(h.Sum(nil))), nil
}

// generatePseudoETag generates a pseudo-ETag from file metadata without reading the file.
// Used as fallback when .metadata.json is missing.
func (fs *FilesystemStorage) generatePseudoETag(info os.FileInfo) string {
	data := fmt.Sprintf("%d-%d", info.Size(), info.ModTime().UnixNano())
	hash := md5.Sum([]byte(data))
	return fmt.Sprintf("\"%x\"", hash)
}

// Bucket operations
func (fs *FilesystemStorage) BucketExists(bucket string) bool {
	if err := fs.validateBucketPath(bucket); err != nil {
		return false
	}
	path := filepath.Join(fs.dataDir, bucket)
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func (fs *FilesystemStorage) CreateBucket(bucket string) error {
	if err := fs.validateBucketPath(bucket); err != nil {
		return err
	}
	path := filepath.Join(fs.dataDir, bucket)
	return os.MkdirAll(path, 0755)
}

func (fs *FilesystemStorage) DeleteBucket(bucket string) error {
	if err := fs.validateBucketPath(bucket); err != nil {
		return err
	}
	path := filepath.Join(fs.dataDir, bucket)

	// Check if directory is empty
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	if len(entries) > 0 {
		return fmt.Errorf("bucket not empty")
	}

	return os.Remove(path)
}

func (fs *FilesystemStorage) ListBuckets() ([]BucketInfo, error) {
	entries, err := os.ReadDir(fs.dataDir)
	if err != nil {
		return nil, err
	}

	var buckets []BucketInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		buckets = append(buckets, BucketInfo{
			Name:         entry.Name(),
			CreationDate: info.ModTime(),
		})
	}
	return buckets, nil
}

func (fs *FilesystemStorage) ListObjects(bucket, prefix string, maxKeys int) ([]ObjectInfo, error) {
	if err := fs.validateBucketPath(bucket); err != nil {
		return nil, err
	}
	bucketPath := filepath.Join(fs.dataDir, bucket)

	if !fs.BucketExists(bucket) {
		return nil, fmt.Errorf("bucket does not exist")
	}

	// Collect keys first without fetching metadata
	var keys []string

	err := filepath.WalkDir(bucketPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and metadata files
		if d.IsDir() || strings.HasSuffix(path, ".metadata.json") {
			return nil
		}

		// Get relative path from bucket
		relPath, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return err
		}

		// Convert to S3 key format (use forward slashes)
		key := filepath.ToSlash(relPath)

		// Apply prefix filter
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		keys = append(keys, key)
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort keys lexicographically (S3 compliance)
	sort.Strings(keys)

	// Apply maxKeys pagination
	if maxKeys > 0 && len(keys) > maxKeys {
		keys = keys[:maxKeys]
	}

	// Now fetch metadata only for the keys in the current page
	objects := make([]ObjectInfo, 0, len(keys))
	for _, key := range keys {
		objectPath := fs.objectPath(bucket, key)

		info, err := os.Stat(objectPath)
		if err != nil {
			// File was deleted between walk and now, skip it
			continue
		}

		// Try to load metadata
		etag := ""
		if meta, loadErr := fs.loadMetadata(bucket, key); loadErr == nil {
			etag = meta.ETag
		}
		if etag == "" {
			// Use pseudo-ETag instead of reading entire file
			etag = fs.generatePseudoETag(info)
		}

		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         etag,
		})
	}

	return objects, nil
}

// Object operations
func (fs *FilesystemStorage) PutObject(bucket, key string, reader io.Reader, contentType string) (*ObjectMetadata, error) {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return nil, err
	}
	objectPath := fs.objectPath(bucket, key)

	// Acquire per-object lock to prevent concurrent write races
	lockKey := objectPath
	lockValue, _ := fs.writeLocks.LoadOrStore(lockKey, &sync.Mutex{})
	mutex := lockValue.(*sync.Mutex)
	mutex.Lock()
	defer func() {
		mutex.Unlock()
		fs.writeLocks.Delete(lockKey)
	}()

	// Create parent directories with retry logic
	dir := filepath.Dir(objectPath)
	var mkdirErr error
	for attempt := 0; attempt < 3; attempt++ {
		mkdirErr = os.MkdirAll(dir, 0755)
		if mkdirErr == nil {
			break
		}
		if attempt < 2 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if mkdirErr != nil {
		return nil, mkdirErr
	}

	// Write to unique temporary file to avoid concurrent write races
	tempFile, err := os.CreateTemp(dir, ".geckos3-tmp-*")
	if err != nil {
		return nil, err
	}
	tempPath := tempFile.Name()

	// Stream data and calculate MD5
	hash := md5.New()
	multiWriter := io.MultiWriter(tempFile, hash)

	size, err := io.Copy(multiWriter, reader)
	if err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return nil, err
	}

	// Flush data to disk before rename for crash safety
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return nil, err
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Atomic rename for data file
	if err := os.Rename(tempPath, objectPath); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Create metadata
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	metadata := &ObjectMetadata{
		Size:         size,
		LastModified: time.Now().UTC(),
		ETag:         etag,
		ContentType:  contentType,
	}

	// Save metadata (also protected by the same lock)
	if err := fs.saveMetadata(bucket, key, metadata); err != nil {
		// Non-fatal - object is saved
		return metadata, nil
	}

	return metadata, nil
}

func (fs *FilesystemStorage) GetObject(bucket, key string) (io.ReadCloser, *ObjectMetadata, error) {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return nil, nil, err
	}
	objectPath := fs.objectPath(bucket, key)

	file, err := os.Open(objectPath)
	if err != nil {
		return nil, nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}

	// Try to load metadata
	metadata, err := fs.loadMetadata(bucket, key)
	if err != nil {
		// Use pseudo-ETag instead of reading entire file
		metadata = &ObjectMetadata{
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         fs.generatePseudoETag(info),
		}
	}

	return file, metadata, nil
}

func (fs *FilesystemStorage) HeadObject(bucket, key string) (*ObjectMetadata, error) {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return nil, err
	}
	objectPath := fs.objectPath(bucket, key)

	info, err := os.Stat(objectPath)
	if err != nil {
		return nil, err
	}

	// Try to load metadata
	metadata, err := fs.loadMetadata(bucket, key)
	if err != nil {
		// Use pseudo-ETag instead of reading entire file
		metadata = &ObjectMetadata{
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         fs.generatePseudoETag(info),
		}
	}

	return metadata, nil
}

func (fs *FilesystemStorage) DeleteObject(bucket, key string) error {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return err
	}
	objectPath := fs.objectPath(bucket, key)
	metadataPath := fs.metadataPath(bucket, key)

	// Remove object file
	if err := os.Remove(objectPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove metadata file (non-fatal)
	os.Remove(metadataPath)

	// Clean up empty parent directories up to the bucket root
	bucketPath := filepath.Join(fs.dataDir, bucket)
	dir := filepath.Dir(objectPath)
	for dir != bucketPath && dir != "." {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			break
		}
		os.Remove(dir)
		dir = filepath.Dir(dir)
	}

	return nil
}

func (fs *FilesystemStorage) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*ObjectMetadata, error) {
	// Validate paths
	if err := fs.validateObjectPath(srcBucket, srcKey); err != nil {
		return nil, err
	}
	if err := fs.validateObjectPath(dstBucket, dstKey); err != nil {
		return nil, err
	}

	// Open source
	reader, srcMeta, err := fs.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, fmt.Errorf("source object not found")
	}
	defer reader.Close()

	// Write to destination, preserving content type
	ct := srcMeta.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return fs.PutObject(dstBucket, dstKey, reader, ct)
}

// Helper functions
func (fs *FilesystemStorage) objectPath(bucket, key string) string {
	// Convert S3 key to filesystem path
	return filepath.Join(fs.dataDir, bucket, filepath.FromSlash(key))
}

func (fs *FilesystemStorage) metadataPath(bucket, key string) string {
	return fs.objectPath(bucket, key) + ".metadata.json"
}

func (fs *FilesystemStorage) saveMetadata(bucket, key string, metadata *ObjectMetadata) error {
	path := fs.metadataPath(bucket, key)

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	// Atomic write via temp file + rename
	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, ".metadata-tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}

func (fs *FilesystemStorage) loadMetadata(bucket, key string) (*ObjectMetadata, error) {
	path := fs.metadataPath(bucket, key)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var metadata ObjectMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}
