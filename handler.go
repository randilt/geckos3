package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type S3Handler struct {
	storage Storage
	auth    Authenticator
}

// MaxClientsMiddleware limits concurrent in-flight HTTP operations using a
// buffered-channel semaphore to protect file descriptor limits.
func MaxClientsMiddleware(maxClients int) func(http.Handler) http.Handler {
	semaphore := make(chan struct{}, maxClients)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release
			next.ServeHTTP(w, r)
		})
	}
}

func NewS3Handler(storage Storage, auth Authenticator) *S3Handler {
	return &S3Handler{
		storage: storage,
		auth:    auth,
	}
}

func (h *S3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Health check endpoint (bypasses auth)
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		return
	}

	// Authenticate request
	if err := h.auth.Authenticate(r); err != nil {
		h.writeError(w, r, "AccessDenied", err.Error(), http.StatusForbidden)
		return
	}

	// Parse bucket and key from path
	bucket, key := h.parsePath(r.URL.Path)

	// Route based on method and path
	if bucket == "" {
		if r.Method == http.MethodGet {
			h.handleListBuckets(w, r)
		} else {
			h.writeError(w, r, "NotImplemented", "Service operation not supported", http.StatusNotImplemented)
		}
		return
	}

	if key == "" {
		h.handleBucketOperation(w, r, bucket)
	} else {
		h.handleObjectOperation(w, r, bucket, key)
	}
}

func (h *S3Handler) handleBucketOperation(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodPut:
		h.handleCreateBucket(w, r, bucket)
	case http.MethodDelete:
		h.handleDeleteBucket(w, r, bucket)
	case http.MethodHead:
		h.handleHeadBucket(w, r, bucket)
	case http.MethodPost:
		if r.URL.Query().Has("delete") {
			h.handleDeleteObjects(w, r, bucket)
		} else {
			h.writeError(w, r, "NotImplemented", "Operation not supported", http.StatusNotImplemented)
		}
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			h.handleListObjectsV2(w, r, bucket)
		} else {
			h.handleListObjectsV1(w, r, bucket)
		}
	default:
		h.writeError(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *S3Handler) handleObjectOperation(w http.ResponseWriter, r *http.Request, bucket, key string) {
	query := r.URL.Query()

	switch r.Method {
	case http.MethodPost:
		// POST /{bucket}/{key}?uploads → CreateMultipartUpload
		if query.Has("uploads") {
			h.handleCreateMultipartUpload(w, r, bucket, key)
			return
		}
		// POST /{bucket}/{key}?uploadId=X → CompleteMultipartUpload
		if query.Has("uploadId") {
			h.handleCompleteMultipartUpload(w, r, bucket, key)
			return
		}
		h.writeError(w, r, "NotImplemented", "Operation not supported", http.StatusNotImplemented)

	case http.MethodPut:
		// PUT /{bucket}/{key}?partNumber=N&uploadId=X → UploadPart
		if query.Has("partNumber") && query.Has("uploadId") {
			h.handleUploadPart(w, r, bucket, key)
			return
		}
		if copySource := r.Header.Get("x-amz-copy-source"); copySource != "" {
			h.handleCopyObject(w, r, bucket, key, copySource)
		} else {
			h.handlePutObject(w, r, bucket, key)
		}

	case http.MethodGet:
		h.handleGetObject(w, r, bucket, key)
	case http.MethodHead:
		h.handleHeadObject(w, r, bucket, key)

	case http.MethodDelete:
		// DELETE /{bucket}/{key}?uploadId=X → AbortMultipartUpload
		if query.Has("uploadId") {
			h.handleAbortMultipartUpload(w, r, bucket, key)
			return
		}
		h.handleDeleteObject(w, r, bucket, key)

	default:
		h.writeError(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Bucket Handlers
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !isValidBucketName(bucket) {
		h.writeError(w, r, "InvalidBucketName", "The specified bucket is not valid", http.StatusBadRequest)
		return
	}

	if h.storage.BucketExists(bucket) {
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := h.storage.CreateBucket(bucket); err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	if err := h.storage.DeleteBucket(bucket); err != nil {
		h.writeError(w, r, "BucketNotEmpty", "The bucket you tried to delete is not empty", http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	startAfter := r.URL.Query().Get("start-after")
	continuationToken := r.URL.Query().Get("continuation-token")
	maxKeys := 1000
	if mk := r.URL.Query().Get("max-keys"); mk != "" {
		if parsed, err := strconv.Atoi(mk); err == nil && parsed >= 0 {
			maxKeys = parsed
		}
	}
	if maxKeys > 1000 {
		maxKeys = 1000
	}

	startKey := startAfter
	if continuationToken != "" {
		if decoded, err := base64.StdEncoding.DecodeString(continuationToken); err == nil {
			startKey = string(decoded)
		}
	}

	objects, err := h.storage.ListObjects(bucket, prefix, 0)
	if err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	if startKey != "" {
		idx := sort.Search(len(objects), func(i int) bool {
			return objects[i].Key > startKey
		})
		objects = objects[idx:]
	}

	isTruncated := false
	var nextToken string
	var commonPrefixes []CommonPrefix

	if delimiter != "" {
		seenPrefixes := make(map[string]bool)
		var filteredObjects []ObjectInfo
		totalCount := 0
		lastKey := ""

		for _, obj := range objects {
			if maxKeys > 0 && totalCount >= maxKeys {
				isTruncated = true
				break
			}

			rest := strings.TrimPrefix(obj.Key, prefix)
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: cp})
					totalCount++
					lastKey = obj.Key
				}
			} else {
				filteredObjects = append(filteredObjects, obj)
				totalCount++
				lastKey = obj.Key
			}
		}

		if isTruncated && lastKey != "" {
			nextToken = base64.StdEncoding.EncodeToString([]byte(lastKey))
		}
		objects = filteredObjects
	} else {
		if maxKeys == 0 {
			objects = nil
		} else if len(objects) > maxKeys {
			isTruncated = true
			nextToken = base64.StdEncoding.EncodeToString([]byte(objects[maxKeys-1].Key))
			objects = objects[:maxKeys]
		}
	}

	keyCount := len(objects) + len(commonPrefixes)
	response := ListBucketResult{
		Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		MaxKeys:               maxKeys,
		IsTruncated:           isTruncated,
		KeyCount:              keyCount,
		Contents:              make([]Object, len(objects)),
		CommonPrefixes:        commonPrefixes,
		NextContinuationToken: nextToken,
		StartAfter:            startAfter,
		ContinuationToken:     continuationToken,
	}

	for i, obj := range objects {
		response.Contents[i] = Object{
			Key:          obj.Key,
			LastModified: obj.LastModified.Format(time.RFC3339),
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: "STANDARD",
		}
	}

	h.writeXML(w, http.StatusOK, response)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Object Handlers
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// Build PutObjectInput from request headers
	input := &PutObjectInput{
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		CacheControl:       r.Header.Get("Cache-Control"),
	}

	// Parse x-amz-meta-* custom metadata headers
	customMeta := make(map[string]string)
	for name, values := range r.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			metaKey := strings.TrimPrefix(lower, "x-amz-meta-")
			customMeta[metaKey] = values[0]
		}
	}
	if len(customMeta) > 0 {
		input.CustomMetadata = customMeta
	}

	// Pass SHA256 expectation to storage layer for atomic verification.
	// The storage layer will verify the hash before committing the file.
	expectedSHA := r.Header.Get("X-Amz-Content-Sha256")
	if expectedSHA != "" && expectedSHA != "UNSIGNED-PAYLOAD" && expectedSHA != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		input.ExpectedSHA256 = expectedSHA
	}

	// If the client is using AWS chunked transfer encoding, decode the
	// chunked framing so only raw object bytes reach the storage layer.
	var body io.Reader = r.Body
	if isAWSChunked(r) {
		body = newAWSChunkedReader(r.Body)
	}

	metadata, err := h.storage.PutObject(bucket, key, body, input)
	if err != nil {
		if errors.Is(err, ErrBadDigest) {
			h.writeError(w, r, "BadDigest", "The Content-SHA256 you specified did not match what we received", http.StatusBadRequest)
			return
		}
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", metadata.ETag)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	reader, metadata, err := h.storage.GetObject(bucket, key)
	if err != nil {
		h.writeError(w, r, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}
	defer reader.Close()

	// Set ETag
	if metadata.ETag != "" {
		w.Header().Set("ETag", metadata.ETag)
	}

	// Set Content-Type
	ct := metadata.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)

	// Emit stored standard headers
	if metadata.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", metadata.ContentEncoding)
	}
	if metadata.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", metadata.ContentDisposition)
	}
	if metadata.CacheControl != "" {
		w.Header().Set("Cache-Control", metadata.CacheControl)
	}

	// Emit custom x-amz-meta-* headers
	for k, v := range metadata.CustomMetadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}

	// Use http.ServeContent for automatic Range request support
	if rs, ok := reader.(io.ReadSeeker); ok {
		http.ServeContent(w, r, "", metadata.LastModified, rs)
		return
	}

	// Fallback for non-seekable readers
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	w.Header().Set("Last-Modified", metadata.LastModified.Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

func (h *S3Handler) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	metadata, err := h.storage.HeadObject(bucket, key)
	if err != nil {
		h.writeError(w, r, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}

	ct := metadata.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	w.Header().Set("Last-Modified", metadata.LastModified.Format(http.TimeFormat))
	w.Header().Set("ETag", metadata.ETag)
	w.Header().Set("Accept-Ranges", "bytes")

	// Emit stored standard headers
	if metadata.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", metadata.ContentEncoding)
	}
	if metadata.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", metadata.ContentDisposition)
	}
	if metadata.CacheControl != "" {
		w.Header().Set("Cache-Control", metadata.CacheControl)
	}

	// Emit custom x-amz-meta-* headers
	for k, v := range metadata.CustomMetadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := h.storage.DeleteObject(bucket, key); err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ═══════════════════════════════════════════════════════════════════════════════
// ListBuckets Handler
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.storage.ListBuckets()
	if err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	xmlBuckets := make([]XMLBucket, len(buckets))
	for i, b := range buckets {
		xmlBuckets[i] = XMLBucket{
			Name:         b.Name,
			CreationDate: b.CreationDate.Format(time.RFC3339),
		}
	}

	response := ListAllMyBucketsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: XMLBuckets{Bucket: xmlBuckets},
	}

	h.writeXML(w, http.StatusOK, response)
}

// ═══════════════════════════════════════════════════════════════════════════════
// ListObjectsV1 Handler
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) handleListObjectsV1(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")
	maxKeys := 1000
	if mk := r.URL.Query().Get("max-keys"); mk != "" {
		if parsed, err := strconv.Atoi(mk); err == nil && parsed >= 0 {
			maxKeys = parsed
		}
	}
	if maxKeys > 1000 {
		maxKeys = 1000
	}

	objects, err := h.storage.ListObjects(bucket, prefix, 0)
	if err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	if marker != "" {
		idx := sort.Search(len(objects), func(i int) bool {
			return objects[i].Key > marker
		})
		objects = objects[idx:]
	}

	isTruncated := false
	var nextMarker string
	var commonPrefixes []CommonPrefix

	if delimiter != "" {
		seenPrefixes := make(map[string]bool)
		var filteredObjects []ObjectInfo
		totalCount := 0

		for _, obj := range objects {
			if maxKeys > 0 && totalCount >= maxKeys {
				isTruncated = true
				break
			}

			rest := strings.TrimPrefix(obj.Key, prefix)
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: cp})
					totalCount++
					nextMarker = obj.Key
				}
			} else {
				filteredObjects = append(filteredObjects, obj)
				totalCount++
				nextMarker = obj.Key
			}
		}
		objects = filteredObjects
	} else {
		if maxKeys == 0 {
			objects = nil
		} else if len(objects) > maxKeys {
			isTruncated = true
			nextMarker = objects[maxKeys-1].Key
			objects = objects[:maxKeys]
		}
	}

	response := ListBucketResultV1{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		Marker:         marker,
		MaxKeys:        maxKeys,
		IsTruncated:    isTruncated,
		Contents:       make([]Object, len(objects)),
		CommonPrefixes: commonPrefixes,
	}
	if isTruncated {
		response.NextMarker = nextMarker
	}

	for i, obj := range objects {
		response.Contents[i] = Object{
			Key:          obj.Key,
			LastModified: obj.LastModified.Format(time.RFC3339),
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: "STANDARD",
		}
	}

	h.writeXML(w, http.StatusOK, response)
}

// ═══════════════════════════════════════════════════════════════════════════════
// CopyObject Handler
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) handleCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey, copySource string) {
	copySource = strings.TrimPrefix(copySource, "/")
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		h.writeError(w, r, "InvalidArgument", "Invalid x-amz-copy-source", http.StatusBadRequest)
		return
	}
	srcBucket := parts[0]
	srcKey := parts[1]

	if !h.storage.BucketExists(srcBucket) {
		h.writeError(w, r, "NoSuchBucket", "The source bucket does not exist", http.StatusNotFound)
		return
	}
	if !h.storage.BucketExists(dstBucket) {
		h.writeError(w, r, "NoSuchBucket", "The destination bucket does not exist", http.StatusNotFound)
		return
	}

	// Check metadata directive: REPLACE uses headers from this request.
	var overrideMeta *PutObjectInput
	if strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
		overrideMeta = &PutObjectInput{
			ContentType:        r.Header.Get("Content-Type"),
			ContentEncoding:    r.Header.Get("Content-Encoding"),
			ContentDisposition: r.Header.Get("Content-Disposition"),
			CacheControl:       r.Header.Get("Cache-Control"),
		}
		customMeta := make(map[string]string)
		for name, values := range r.Header {
			lower := strings.ToLower(name)
			if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
				metaKey := strings.TrimPrefix(lower, "x-amz-meta-")
				customMeta[metaKey] = values[0]
			}
		}
		if len(customMeta) > 0 {
			overrideMeta.CustomMetadata = customMeta
		}
	}

	metadata, err := h.storage.CopyObject(srcBucket, srcKey, dstBucket, dstKey, overrideMeta)
	if err != nil {
		h.writeError(w, r, "NoSuchKey", "The specified source key does not exist", http.StatusNotFound)
		return
	}

	response := CopyObjectResult{
		LastModified: metadata.LastModified.Format(time.RFC3339),
		ETag:         metadata.ETag,
	}

	h.writeXML(w, http.StatusOK, response)
}

// ═══════════════════════════════════════════════════════════════════════════════
// DeleteObjects (Batch) Handler
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1*1024*1024)) // 1MB limit
	if err != nil {
		h.writeError(w, r, "InternalError", "Failed to read request body", http.StatusInternalServerError)
		return
	}

	var deleteReq DeleteRequest
	if err := xml.Unmarshal(body, &deleteReq); err != nil {
		h.writeError(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	var deleted []DeletedObject
	var errors []DeleteError

	for _, obj := range deleteReq.Objects {
		if err := h.storage.DeleteObject(bucket, obj.Key); err != nil {
			errors = append(errors, DeleteError{
				Key:     obj.Key,
				Code:    "InternalError",
				Message: err.Error(),
			})
		} else {
			if !deleteReq.Quiet {
				deleted = append(deleted, DeletedObject{Key: obj.Key})
			}
		}
	}

	response := DeleteResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Deleted: deleted,
		Errors:  errors,
	}

	h.writeXML(w, http.StatusOK, response)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multipart Upload Handlers
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	uploadID, err := h.storage.CreateMultipartUpload(bucket, key, contentType)
	if err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	response := InitiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	h.writeXML(w, http.StatusOK, response)
}

func (h *S3Handler) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	query := r.URL.Query()
	uploadID := query.Get("uploadId")
	partNumStr := query.Get("partNumber")

	partNumber, err := strconv.Atoi(partNumStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		h.writeError(w, r, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		return
	}

	// Pass SHA256 expectation to storage layer for verification.
	var expectedSHA string
	sha := r.Header.Get("X-Amz-Content-Sha256")
	if sha != "" && sha != "UNSIGNED-PAYLOAD" && !strings.HasPrefix(sha, "STREAMING-") {
		expectedSHA = sha
	}

	// If the client is using AWS chunked transfer encoding, decode the
	// chunked framing so only raw object bytes reach the storage layer.
	var body io.Reader = r.Body
	if isAWSChunked(r) {
		body = newAWSChunkedReader(r.Body)
	}

	etag, err := h.storage.UploadPart(bucket, key, uploadID, partNumber, body, expectedSHA)
	if err != nil {
		if errors.Is(err, ErrBadDigest) {
			h.writeError(w, r, "BadDigest", "The Content-SHA256 you specified did not match what we received", http.StatusBadRequest)
			return
		}
		h.writeError(w, r, "NoSuchUpload", err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	body, err := io.ReadAll(io.LimitReader(r.Body, 1*1024*1024))
	if err != nil {
		h.writeError(w, r, "InternalError", "Failed to read request body", http.StatusInternalServerError)
		return
	}

	var completeReq CompleteMultipartUploadRequest
	if err := xml.Unmarshal(body, &completeReq); err != nil {
		h.writeError(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	// Convert XML parts to storage parts
	parts := make([]CompletedPart, len(completeReq.Parts))
	for i, p := range completeReq.Parts {
		parts[i] = CompletedPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}

	metadata, err := h.storage.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	response := CompleteMultipartUploadResultXML{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   metadata.ETag,
	}

	h.writeXML(w, http.StatusOK, response)
}

func (h *S3Handler) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	if err := h.storage.AbortMultipartUpload(bucket, key, uploadID); err != nil {
		h.writeError(w, r, "NoSuchUpload", err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper Functions
// ═══════════════════════════════════════════════════════════════════════════════

func (h *S3Handler) parsePath(path string) (bucket, key string) {
	path = strings.TrimPrefix(path, "/")

	if path == "" {
		return "", ""
	}

	parts := strings.SplitN(path, "/", 2)
	bucket = parts[0]

	if len(parts) > 1 {
		key = parts[1]
	}

	return bucket, key
}

func (h *S3Handler) writeError(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	ctx := context.WithValue(r.Context(), errorContextKey, fmt.Sprintf("%s: %s", code, message))
	*r = *r.WithContext(ctx)

	errorResponse := ErrorResponse{
		Code:    code,
		Message: message,
	}

	h.writeXML(w, status, errorResponse)
}

func (h *S3Handler) writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(v)
}

func isValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.') {
			return false
		}
	}
	if name[0] == '-' || name[0] == '.' || name[len(name)-1] == '-' || name[len(name)-1] == '.' {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	return true
}

// ═══════════════════════════════════════════════════════════════════════════════
// XML Response/Request Structures
// ═══════════════════════════════════════════════════════════════════════════════

type ListBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	KeyCount              int            `xml:"KeyCount"`
	Contents              []Object       `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name   `xml:"ListAllMyBucketsResult"`
	Xmlns   string     `xml:"xmlns,attr"`
	Buckets XMLBuckets `xml:"Buckets"`
}

type XMLBuckets struct {
	Bucket []XMLBucket `xml:"Bucket"`
}

type XMLBucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ListBucketResultV1 struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	Marker         string         `xml:"Marker"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []Object       `xml:"Contents"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type DeleteRequest struct {
	XMLName xml.Name            `xml:"Delete"`
	Quiet   bool                `xml:"Quiet"`
	Objects []DeleteObjectEntry `xml:"Object"`
}

type DeleteObjectEntry struct {
	Key string `xml:"Key"`
}

type DeleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr"`
	Deleted []DeletedObject `xml:"Deleted,omitempty"`
	Errors  []DeleteError   `xml:"Error,omitempty"`
}

type DeletedObject struct {
	Key string `xml:"Key"`
}

type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// Multipart upload XML types

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartUploadRequest struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []CompletedPartXML `xml:"Part"`
}

type CompletedPartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadResultXML struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

// ═══════════════════════════════════════════════════════════════════════════════
// AWS Chunked Transfer Encoding Decoder
// ═══════════════════════════════════════════════════════════════════════════════
//
// When an AWS SDK sends a PutObject with content-sha256 =
// STREAMING-AWS4-HMAC-SHA256-PAYLOAD, the HTTP body is wrapped in AWS's own
// chunked encoding (distinct from HTTP/1.1 Transfer-Encoding: chunked).
// Each chunk is framed as:
//
//     <hex-size>;chunk-signature=<sig>\r\n
//     <data>\r\n
//
// The final chunk has size 0.  We must strip this framing so the storage
// layer receives only the raw object bytes.

// isAWSChunked reports whether the request uses AWS chunked transfer encoding.
func isAWSChunked(r *http.Request) bool {
	sha := r.Header.Get("X-Amz-Content-Sha256")
	if strings.HasPrefix(sha, "STREAMING-") {
		return true
	}
	ce := r.Header.Get("Content-Encoding")
	return strings.Contains(ce, "aws-chunked")
}

// awsChunkedReader strips AWS chunked framing from an io.Reader, yielding
// only the raw object data.
type awsChunkedReader struct {
	scanner *bufio.Reader
	chunk   io.Reader // current chunk data (limited reader)
	done    bool
}

func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{
		scanner: bufio.NewReaderSize(r, 64*1024),
	}
}

func (a *awsChunkedReader) Read(p []byte) (int, error) {
	for {
		if a.done {
			return 0, io.EOF
		}

		// If we have an active chunk, drain it first.
		if a.chunk != nil {
			n, err := a.chunk.Read(p)
			if n > 0 {
				return n, nil
			}
			if err == io.EOF {
				// Consume the trailing \r\n after chunk data.
				a.chunk = nil
				var crlf [2]byte
				if _, err2 := io.ReadFull(a.scanner, crlf[:]); err2 != nil {
					return 0, err2
				}
				continue
			}
			return n, err
		}

		// Read the next chunk header line: <hex-size>;chunk-signature=<sig>\r\n
		line, err := a.scanner.ReadBytes('\n')
		if err != nil {
			if err == io.EOF && len(line) == 0 {
				a.done = true
				return 0, io.EOF
			}
			if err == io.EOF {
				// partial line at end — treat as done
				a.done = true
				return 0, io.EOF
			}
			return 0, err
		}

		// Trim \r\n
		line = bytes.TrimRight(line, "\r\n")

		// Extract hex size before the semicolon.
		semiIdx := bytes.IndexByte(line, ';')
		var hexSize []byte
		if semiIdx >= 0 {
			hexSize = line[:semiIdx]
		} else {
			hexSize = line
		}

		size, err := strconv.ParseInt(string(bytes.TrimSpace(hexSize)), 16, 64)
		if err != nil {
			return 0, fmt.Errorf("aws-chunked: invalid chunk size %q: %w", hexSize, err)
		}

		if size == 0 {
			a.done = true
			// Drain any trailing headers/CRLF (best effort).
			io.Copy(io.Discard, a.scanner)
			return 0, io.EOF
		}

		a.chunk = io.LimitReader(a.scanner, size)
	}
}
