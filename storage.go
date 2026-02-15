package main

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// tmpStagingDir is the hidden directory used for temporary file staging.
// Temp files are written here to avoid races with DeleteObject cleanup.
const tmpStagingDir = ".geckos3-tmp"

// lockStripes is the number of mutexes in the lock-striping array.
const lockStripes = 256

// ErrBadDigest is returned when the SHA256 hash of the uploaded content
// does not match the expected hash provided in the request.
var ErrBadDigest = errors.New("the Content-SHA256 you specified did not match what we received")

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
	CopyObject(srcBucket, srcKey, dstBucket, dstKey string, overrideMeta *PutObjectInput) (*ObjectMetadata, error)

	// Multipart upload operations
	CreateMultipartUpload(bucket, key, contentType string) (string, error)
	UploadPart(bucket, key, uploadID string, partNumber int, reader io.Reader, expectedSHA256 string) (string, error)
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
	dataDir        string
	stripes        [lockStripes]sync.Mutex
	enableFsync    bool // When true, fsync files and directories after writes
	enableMetadata bool // When true, persist metadata to .metadata.json sidecar files
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
	ExpectedSHA256     string // If set, verify content hash before committing
}

// CompletedPart represents a single part in a CompleteMultipartUpload request.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

func NewFilesystemStorage(dataDir string) *FilesystemStorage {
	return &FilesystemStorage{
		dataDir:        dataDir,
		enableMetadata: true,
	}
}

// SetFsync enables or disables per-object fsync. When disabled (default),
// writes rely on OS page cache and atomic rename for consistency, matching
// the behavior of MinIO and other high-performance object stores.
func (fs *FilesystemStorage) SetFsync(enabled bool) {
	fs.enableFsync = enabled
}

// SetMetadataEnabled controls whether metadata is persisted to .metadata.json files.
// When disabled, metadata is computed on-demand from file attributes for performance.
// Default: true (full S3 compatibility).
func (fs *FilesystemStorage) SetMetadataEnabled(enabled bool) {
	fs.enableMetadata = enabled
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

	// A bucket is empty if it contains nothing besides internal hidden directories
	// and common OS artifacts.
	hiddenEntries := map[string]bool{
		multipartStagingDir: true,
		tmpStagingDir:       true,
		".DS_Store":         true,
		"Thumbs.db":         true,
	}
	for _, entry := range entries {
		if !hiddenEntries[entry.Name()] {
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

		// Skip internal staging directories entirely
		if d.IsDir() && (d.Name() == multipartStagingDir || d.Name() == tmpStagingDir) {
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
	bucketPath := filepath.Join(fs.dataDir, bucket)

	// Stage temp files in a dedicated hidden directory to avoid races
	// with DeleteObject empty-directory cleanup.
	stagingDir := filepath.Join(bucketPath, tmpStagingDir)
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return nil, err
	}

	// Write to temp file OUTSIDE the stripe lock — network I/O must not
	// hold a mutex because clients may be slow or large uploads take time.
	tempFile, err := os.CreateTemp(stagingDir, ".put-*")
	if err != nil {
		return nil, err
	}
	tempPath := tempFile.Name()

	// Stream data and calculate MD5 (+ optional SHA256)
	md5Hash := md5.New()
	writers := []io.Writer{tempFile, md5Hash}

	var sha256Hasher io.Writer
	var sha256Sum func() []byte
	var expectedSHA string
	if input != nil && input.ExpectedSHA256 != "" {
		expectedSHA = input.ExpectedSHA256
		h := sha256.New()
		sha256Hasher = h
		sha256Sum = func() []byte { return h.Sum(nil) }
		writers = append(writers, h)
	}

	multiWriter := io.MultiWriter(writers...)
	size, err := io.Copy(multiWriter, reader)
	if err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return nil, err
	}

	if fs.enableFsync {
		if err := tempFile.Sync(); err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return nil, err
		}
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Verify SHA256 BEFORE committing — never overwrite valid data with
	// mismatched content.
	if sha256Hasher != nil {
		computed := hex.EncodeToString(sha256Sum())
		if computed != expectedSHA {
			os.Remove(tempPath)
			return nil, ErrBadDigest
		}
	}

	// Lock only for the directory creation + atomic rename.
	mu := fs.stripe(objectPath)
	mu.Lock()
	dir := filepath.Dir(objectPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		mu.Unlock()
		os.Remove(tempPath)
		return nil, err
	}
	if err := os.Rename(tempPath, objectPath); err != nil {
		mu.Unlock()
		os.Remove(tempPath)
		return nil, err
	}
	if fs.enableFsync {
		syncParentDir(objectPath)
	}
	mu.Unlock()

	// Build metadata from input
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(md5Hash.Sum(nil)))
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

	if fs.enableMetadata {
		if err := fs.saveMetadata(bucket, key, metadata); err != nil {
			// Non-fatal: object is saved, metadata is best-effort
			return metadata, nil
		}
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

func (fs *FilesystemStorage) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, overrideMeta *PutObjectInput) (*ObjectMetadata, error) {
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

	// If overrideMeta is provided (REPLACE directive), use it instead of source metadata.
	if overrideMeta != nil {
		if overrideMeta.ContentType == "" {
			overrideMeta.ContentType = "application/octet-stream"
		}
		return fs.PutObject(dstBucket, dstKey, reader, overrideMeta)
	}

	// Default: COPY directive — preserve all metadata from source
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
func (fs *FilesystemStorage) UploadPart(bucket, key, uploadID string, partNumber int, reader io.Reader, expectedSHA256 string) (string, error) {
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

	md5Hash := md5.New()
	writers := []io.Writer{tempFile, md5Hash}

	var sha256Sum func() []byte
	if expectedSHA256 != "" {
		h := sha256.New()
		sha256Sum = func() []byte { return h.Sum(nil) }
		writers = append(writers, h)
	}

	multiWriter := io.MultiWriter(writers...)

	if _, err := io.Copy(multiWriter, reader); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return "", err
	}

	if fs.enableFsync {
		if err := tempFile.Sync(); err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return "", err
		}
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return "", err
	}

	// Verify SHA256 before committing the part.
	if sha256Sum != nil {
		computed := hex.EncodeToString(sha256Sum())
		if computed != expectedSHA256 {
			os.Remove(tempPath)
			return "", ErrBadDigest
		}
	}

	if err := os.Rename(tempPath, partPath); err != nil {
		os.Remove(tempPath)
		return "", err
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(md5Hash.Sum(nil)))
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
	bucketPath := filepath.Join(fs.dataDir, bucket)

	// Stage temp file in the dedicated hidden directory.
	tmpDir := filepath.Join(bucketPath, tmpStagingDir)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, err
	}

	// Concatenate parts OUTSIDE the stripe lock — local disk I/O for
	// large multipart objects should never block other writers.
	tempFile, err := os.CreateTemp(tmpDir, ".complete-*")
	if err != nil {
		return nil, err
	}
	tempPath := tempFile.Name()

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

	if fs.enableFsync {
		if err := tempFile.Sync(); err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return nil, err
		}
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	// Lock only for directory creation + atomic rename.
	mu := fs.stripe(objectPath)
	mu.Lock()
	dir := filepath.Dir(objectPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		mu.Unlock()
		os.Remove(tempPath)
		return nil, err
	}
	if err := os.Rename(tempPath, objectPath); err != nil {
		mu.Unlock()
		os.Remove(tempPath)
		return nil, err
	}
	if fs.enableFsync {
		syncParentDir(objectPath)
	}
	mu.Unlock()

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

	if fs.enableMetadata {
		fs.saveMetadata(bucket, key, metadata)
	}
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

// syncParentDir opens the parent directory of path, calls Sync to flush the
// directory entry to durable storage, then closes it. Errors are intentionally
// ignored because some filesystems (e.g. Windows, certain FUSE mounts) do not
// support fsync on directories.
func syncParentDir(path string) {
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}
