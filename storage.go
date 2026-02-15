package main

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// MaxScanLimit is the upper bound on objects collected during a ListObjects walk.
// Buckets exceeding this count will return an error rather than risk OOM.
const MaxScanLimit = 100000

// multipartStagingDir is the hidden directory used for multipart upload staging.
const multipartStagingDir = ".geckos3-multipart"

// lockStripes is the number of mutexes in the lock-striping array.
const lockStripes = 256

// Storage defines the interface for bucket/object operations.
type Storage interface {
	BucketExists(bucket string) bool
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]BucketInfo, error)
	ListObjects(bucket, prefix string, maxKeys int) ([]ObjectInfo, error)
	PutObject(bucket, key string, reader io.Reader, input *PutObjectInput) (*ObjectMetadata, error)
	GetObject(bucket, key string) (io.ReadCloser, *ObjectMetadata, error)
	HeadObject(bucket, key string) (*ObjectMetadata, error)
	DeleteObject(bucket, key string) error
	CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*ObjectMetadata, error)

	// Multipart upload operations
	CreateMultipartUpload(bucket, key, contentType string) (string, error)
	UploadPart(bucket, key, uploadID string, partNumber int, reader io.Reader) (string, error)
	CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) (*ObjectMetadata, error)
	AbortMultipartUpload(bucket, key, uploadID string) error
}

type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

// FilesystemStorage maps S3 operations to local filesystem operations.
// Lock striping with a fixed array of mutexes prevents concurrent write races
// without unbounded memory growth from per-key locks.
type FilesystemStorage struct {
	dataDir string
	stripes [lockStripes]sync.Mutex
}

type ObjectMetadata struct {
	Size               int64             `json:"size"`
	LastModified       time.Time         `json:"lastModified"`
	ETag               string            `json:"etag"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	CustomMetadata     map[string]string `json:"customMetadata,omitempty"`
}

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// PutObjectInput carries all headers for a PutObject call.
type PutObjectInput struct {
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	CacheControl       string
	CustomMetadata     map[string]string
}

// CompletedPart represents a single part in a CompleteMultipartUpload request.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

func NewFilesystemStorage(dataDir string) *FilesystemStorage {
	return &FilesystemStorage{dataDir: dataDir}
}

// stripe returns the mutex for a given key using FNV-1a hashing.
func (fs *FilesystemStorage) stripe(key string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &fs.stripes[h.Sum32()%lockStripes]
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

// ═══════════════════════════════════════════════════════════════════════════════
// Bucket Operations
// ═══════════════════════════════════════════════════════════════════════════════

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

	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// A bucket is empty if it contains nothing besides multipart staging
	for _, entry := range entries {
		if entry.Name() != multipartStagingDir {
			return fmt.Errorf("bucket not empty")
		}
	}

	return os.RemoveAll(path)
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

	// Collect keys as strings only, skipping metadata/staging files.
	// Enforces MaxScanLimit to prevent unbounded memory growth.
	var keys []string
	scanCount := 0

	err := filepath.WalkDir(bucketPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip the multipart staging directory entirely
		if d.IsDir() && d.Name() == multipartStagingDir {
			return filepath.SkipDir
		}

		// Skip directories and metadata sidecar files
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

		scanCount++
		if scanCount > MaxScanLimit {
			return fmt.Errorf("bucket exceeds scan limit of %d objects; listing aborted", MaxScanLimit)
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

	// Fetch metadata only for the keys in the current page
	objects := make([]ObjectInfo, 0, len(keys))
	for _, key := range keys {
		objectPath := fs.objectPath(bucket, key)

		info, err := os.Stat(objectPath)
		if err != nil {
			// File was deleted between walk and stat, skip it
			continue
		}

		etag := ""
		if meta, loadErr := fs.loadMetadata(bucket, key); loadErr == nil {
			etag = meta.ETag
		}
		if etag == "" {
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

// ═══════════════════════════════════════════════════════════════════════════════
// Object Operations
// ═══════════════════════════════════════════════════════════════════════════════

func (fs *FilesystemStorage) PutObject(bucket, key string, reader io.Reader, input *PutObjectInput) (*ObjectMetadata, error) {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return nil, err
	}
	objectPath := fs.objectPath(bucket, key)

	// Lock striping: hash the object path to select a stripe mutex.
	// This is race-free: the stripe array is fixed-size and never deallocated.
	mu := fs.stripe(objectPath)
	mu.Lock()
	defer mu.Unlock()

	// Create parent directories with retry to handle races with DeleteObject cleanup
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

	// Write to unique temporary file to avoid partial writes
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

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return nil, err
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Atomic rename
	if err := os.Rename(tempPath, objectPath); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Build metadata from input
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
	contentType := "application/octet-stream"
	var contentEncoding, contentDisposition, cacheControl string
	var customMeta map[string]string

	if input != nil {
		if input.ContentType != "" {
			contentType = input.ContentType
		}
		contentEncoding = input.ContentEncoding
		contentDisposition = input.ContentDisposition
		cacheControl = input.CacheControl
		customMeta = input.CustomMetadata
	}

	metadata := &ObjectMetadata{
		Size:               size,
		LastModified:       time.Now().UTC(),
		ETag:               etag,
		ContentType:        contentType,
		ContentEncoding:    contentEncoding,
		ContentDisposition: contentDisposition,
		CacheControl:       cacheControl,
		CustomMetadata:     customMeta,
	}

	if err := fs.saveMetadata(bucket, key, metadata); err != nil {
		// Non-fatal: object is saved, metadata is best-effort
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

	metadata, err := fs.loadMetadata(bucket, key)
	if err != nil {
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

	metadata, err := fs.loadMetadata(bucket, key)
	if err != nil {
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

	if err := os.Remove(objectPath); err != nil && !os.IsNotExist(err) {
		return err
	}

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
	if err := fs.validateObjectPath(srcBucket, srcKey); err != nil {
		return nil, err
	}
	if err := fs.validateObjectPath(dstBucket, dstKey); err != nil {
		return nil, err
	}

	reader, srcMeta, err := fs.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, fmt.Errorf("source object not found")
	}
	defer reader.Close()

	// Preserve all metadata from source
	input := &PutObjectInput{
		ContentType:        srcMeta.ContentType,
		ContentEncoding:    srcMeta.ContentEncoding,
		ContentDisposition: srcMeta.ContentDisposition,
		CacheControl:       srcMeta.CacheControl,
		CustomMetadata:     srcMeta.CustomMetadata,
	}
	if input.ContentType == "" {
		input.ContentType = "application/octet-stream"
	}
	return fs.PutObject(dstBucket, dstKey, reader, input)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multipart Upload Operations
// ═══════════════════════════════════════════════════════════════════════════════

// CreateMultipartUpload generates a unique upload ID and creates a staging directory.
func (fs *FilesystemStorage) CreateMultipartUpload(bucket, key, contentType string) (string, error) {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return "", err
	}
	if !fs.BucketExists(bucket) {
		return "", fmt.Errorf("bucket does not exist")
	}

	uploadID := generateUploadID()
	stagingDir := fs.multipartStagingPath(bucket, uploadID)

	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create multipart staging: %w", err)
	}

	// Persist the target key and content type in a manifest
	manifest := map[string]string{
		"key":         key,
		"contentType": contentType,
	}
	data, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(stagingDir, "manifest.json"), data, 0644); err != nil {
		os.RemoveAll(stagingDir)
		return "", err
	}

	return uploadID, nil
}

// UploadPart saves a single part to the staging directory and returns its ETag.
func (fs *FilesystemStorage) UploadPart(bucket, key, uploadID string, partNumber int, reader io.Reader) (string, error) {
	stagingDir := fs.multipartStagingPath(bucket, uploadID)
	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		return "", fmt.Errorf("upload ID not found")
	}

	partPath := filepath.Join(stagingDir, fmt.Sprintf("part-%05d.tmp", partNumber))

	tempFile, err := os.CreateTemp(stagingDir, ".part-tmp-*")
	if err != nil {
		return "", err
	}
	tempPath := tempFile.Name()

	hash := md5.New()
	multiWriter := io.MultiWriter(tempFile, hash)

	if _, err := io.Copy(multiWriter, reader); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return "", err
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return "", err
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return "", err
	}

	if err := os.Rename(tempPath, partPath); err != nil {
		os.Remove(tempPath)
		return "", err
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
	return etag, nil
}

// CompleteMultipartUpload concatenates parts in order, writes the final object, and cleans up.
func (fs *FilesystemStorage) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) (*ObjectMetadata, error) {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return nil, err
	}

	stagingDir := fs.multipartStagingPath(bucket, uploadID)
	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("upload ID not found")
	}

	objectPath := fs.objectPath(bucket, key)

	mu := fs.stripe(objectPath)
	mu.Lock()
	defer mu.Unlock()

	dir := filepath.Dir(objectPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	tempFile, err := os.CreateTemp(dir, ".geckos3-tmp-*")
	if err != nil {
		return nil, err
	}
	tempPath := tempFile.Name()

	// Concatenate parts in order while computing combined MD5
	hash := md5.New()
	multiWriter := io.MultiWriter(tempFile, hash)
	var totalSize int64

	for _, part := range parts {
		partPath := filepath.Join(stagingDir, fmt.Sprintf("part-%05d.tmp", part.PartNumber))
		partFile, err := os.Open(partPath)
		if err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return nil, fmt.Errorf("part %d not found", part.PartNumber)
		}
		n, err := io.Copy(multiWriter, partFile)
		partFile.Close()
		if err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return nil, fmt.Errorf("failed to copy part %d: %w", part.PartNumber, err)
		}
		totalSize += n
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return nil, err
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	if err := os.Rename(tempPath, objectPath); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Build S3-style multipart ETag: MD5-of-data + "-N"
	etag := fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(hash.Sum(nil)), len(parts))

	// Read manifest for content type
	contentType := "application/octet-stream"
	if manifestData, err := os.ReadFile(filepath.Join(stagingDir, "manifest.json")); err == nil {
		var manifest map[string]string
		if json.Unmarshal(manifestData, &manifest) == nil {
			if ct := manifest["contentType"]; ct != "" {
				contentType = ct
			}
		}
	}

	metadata := &ObjectMetadata{
		Size:         totalSize,
		LastModified: time.Now().UTC(),
		ETag:         etag,
		ContentType:  contentType,
	}

	fs.saveMetadata(bucket, key, metadata)
	os.RemoveAll(stagingDir)

	return metadata, nil
}

// AbortMultipartUpload removes the staging directory and all uploaded parts.
func (fs *FilesystemStorage) AbortMultipartUpload(bucket, key, uploadID string) error {
	stagingDir := fs.multipartStagingPath(bucket, uploadID)
	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		return fmt.Errorf("upload ID not found")
	}
	return os.RemoveAll(stagingDir)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper Functions
// ═══════════════════════════════════════════════════════════════════════════════

func (fs *FilesystemStorage) objectPath(bucket, key string) string {
	return filepath.Join(fs.dataDir, bucket, filepath.FromSlash(key))
}

func (fs *FilesystemStorage) metadataPath(bucket, key string) string {
	return fs.objectPath(bucket, key) + ".metadata.json"
}

func (fs *FilesystemStorage) multipartStagingPath(bucket, uploadID string) string {
	return filepath.Join(fs.dataDir, bucket, multipartStagingDir, uploadID)
}

func (fs *FilesystemStorage) saveMetadata(bucket, key string, metadata *ObjectMetadata) error {
	path := fs.metadataPath(bucket, key)

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

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

// generateUploadID creates a random hex ID for multipart uploads.
func generateUploadID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
