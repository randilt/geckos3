package main

import (
	"context"
	"encoding/base64"
	"encoding/xml"
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

// MaxClientsMiddleware limits concurrent requests using a semaphore
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
		// Service-level operations
		if r.Method == http.MethodGet {
			h.handleListBuckets(w, r)
		} else {
			h.writeError(w, r, "NotImplemented", "Service operation not supported", http.StatusNotImplemented)
		}
		return
	}

	if key == "" {
		// Bucket operations
		h.handleBucketOperation(w, r, bucket)
	} else {
		// Object operations
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
		// POST /{bucket}?delete = DeleteObjects
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
	switch r.Method {
	case http.MethodPut:
		// CopyObject if x-amz-copy-source is present
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
		h.handleDeleteObject(w, r, bucket, key)
	default:
		h.writeError(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Bucket handlers
func (h *S3Handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !isValidBucketName(bucket) {
		h.writeError(w, r, "InvalidBucketName", "The specified bucket is not valid", http.StatusBadRequest)
		return
	}

	// S3-1: Return appropriate status if bucket already exists
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

	// Parse query parameters
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
	if maxKeys > 10000 {
		maxKeys = 10000
	}

	// Decode continuation token (base64-encoded last key)
	startKey := startAfter
	if continuationToken != "" {
		if decoded, err := base64.StdEncoding.DecodeString(continuationToken); err == nil {
			startKey = string(decoded)
		}
	}

	// Get all matching objects for correct pagination
	objects, err := h.storage.ListObjects(bucket, prefix, 0)
	if err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	// Sort by key (S3 returns lexicographically sorted results)
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	// Apply start-after or continuation-token
	if startKey != "" {
		idx := sort.Search(len(objects), func(i int) bool {
			return objects[i].Key > startKey
		})
		objects = objects[idx:]
	}

	// Apply delimiter grouping and maxKeys
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
		// No delimiter: simple maxKeys truncation
		if maxKeys == 0 {
			objects = nil
		} else if len(objects) > maxKeys {
			isTruncated = true
			nextToken = base64.StdEncoding.EncodeToString([]byte(objects[maxKeys-1].Key))
			objects = objects[:maxKeys]
		}
	}

	// Build XML response
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

// Object handlers
func (h *S3Handler) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	contentType := r.Header.Get("Content-Type")
	metadata, err := h.storage.PutObject(bucket, key, r.Body, contentType)
	if err != nil {
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

	// Set headers before ServeContent
	if metadata.ETag != "" {
		w.Header().Set("ETag", metadata.ETag)
	}
	ct := metadata.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)

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

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := h.storage.DeleteObject(bucket, key); err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListBuckets handler
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

// ListObjectsV1 handler
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
	if maxKeys > 10000 {
		maxKeys = 10000
	}

	objects, err := h.storage.ListObjects(bucket, prefix, 0)
	if err != nil {
		h.writeError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	// Apply marker
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

// CopyObject handler
func (h *S3Handler) handleCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey, copySource string) {
	// Parse source: /bucket/key or bucket/key
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

	metadata, err := h.storage.CopyObject(srcBucket, srcKey, dstBucket, dstKey)
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

// DeleteObjects (batch) handler
func (h *S3Handler) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// Parse request body
	body, err := io.ReadAll(io.LimitReader(r.Body, 1*1024*1024)) // 1MB limit (reduced from 10MB)
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

// Helper functions
func (h *S3Handler) parsePath(path string) (bucket, key string) {
	// Remove leading slash
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
	// Store error in request context for logging
	ctx := context.WithValue(r.Context(), errorContextKey, fmt.Sprintf("%s: %s", code, message))
	*r = *r.WithContext(ctx)

	errorResponse := ErrorResponse{
		Code:    code,
		Message: message,
	}

	h.writeXML(w, status, errorResponse)
}

// writeXML writes an XML response with the standard XML declaration.
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

// XML response structures
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

// ListBuckets XML types
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

// ListObjectsV1 XML type
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

// CopyObject XML type
type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// DeleteObjects XML types
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
