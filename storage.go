package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FilesystemStorage struct {
	dataDir string
}

type ObjectMetadata struct {
	Size         int64     `json:"size"`
	LastModified time.Time `json:"lastModified"`
	ETag         string    `json:"etag"`
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

func (fs *FilesystemStorage) ListObjects(bucket, prefix string, maxKeys int) ([]ObjectInfo, error) {
	if err := fs.validateBucketPath(bucket); err != nil {
		return nil, err
	}
	bucketPath := filepath.Join(fs.dataDir, bucket)

	if !fs.BucketExists(bucket) {
		return nil, fmt.Errorf("bucket does not exist")
	}

	var objects []ObjectInfo
	count := 0

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

		// Apply maxKeys limit
		if maxKeys > 0 && count >= maxKeys {
			return filepath.SkipAll
		}

		// Get object info
		info, err := d.Info()
		if err != nil {
			return err
		}

		// Try to load metadata
		etag := ""
		if meta, loadErr := fs.loadMetadata(bucket, key); loadErr == nil {
			etag = meta.ETag
		}
		if etag == "" {
			if computed, hashErr := fs.computeFileETag(path); hashErr == nil {
				etag = computed
			}
		}

		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         etag,
		})
		count++

		return nil
	})

	return objects, err
}

// Object operations
func (fs *FilesystemStorage) PutObject(bucket, key string, reader io.Reader) (*ObjectMetadata, error) {
	if err := fs.validateObjectPath(bucket, key); err != nil {
		return nil, err
	}
	objectPath := fs.objectPath(bucket, key)

	// Create parent directories
	dir := filepath.Dir(objectPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
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

	// Atomic rename
	if err := os.Rename(tempPath, objectPath); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Create metadata
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
	metadata := &ObjectMetadata{
		Size:         size,
		LastModified: time.Now().UTC(),
		ETag:         etag,
	}

	// Save metadata
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
		// Fallback: compute ETag from file content
		etag, hashErr := fs.computeFileETag(objectPath)
		if hashErr != nil {
			etag = ""
		}
		metadata = &ObjectMetadata{
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         etag,
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
		// Fallback: compute ETag from file content
		etag, hashErr := fs.computeFileETag(objectPath)
		if hashErr != nil {
			etag = ""
		}
		metadata = &ObjectMetadata{
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         etag,
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

	return os.WriteFile(path, data, 0644)
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
