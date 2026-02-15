package main

import (
	"encoding/base64"
	"encoding/xml"
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
		h.writeError(w, "AccessDenied", err.Error(), http.StatusForbidden)
		return
	}

	// Parse bucket and key from path
	bucket, key := h.parsePath(r.URL.Path)

	// Route based on method and path
	if bucket == "" {
		// Service operations (not implemented)
		h.writeError(w, "NotImplemented", "Service operations not supported", http.StatusNotImplemented)
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
	case http.MethodGet:
		// Check if this is ListObjects
		if r.URL.Query().Get("list-type") == "2" {
			h.handleListObjectsV2(w, r, bucket)
		} else {
			h.writeError(w, "NotImplemented", "ListObjectsV1 not supported", http.StatusNotImplemented)
		}
	default:
		h.writeError(w, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *S3Handler) handleObjectOperation(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch r.Method {
	case http.MethodPut:
		h.handlePutObject(w, r, bucket, key)
	case http.MethodGet:
		h.handleGetObject(w, r, bucket, key)
	case http.MethodHead:
		h.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		h.handleDeleteObject(w, r, bucket, key)
	default:
		h.writeError(w, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Bucket handlers
func (h *S3Handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !isValidBucketName(bucket) {
		h.writeError(w, "InvalidBucketName", "The specified bucket is not valid", http.StatusBadRequest)
		return
	}

	// S3-1: Return appropriate status if bucket already exists
	if h.storage.BucketExists(bucket) {
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := h.storage.CreateBucket(bucket); err != nil {
		h.writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	if err := h.storage.DeleteBucket(bucket); err != nil {
		h.writeError(w, "BucketNotEmpty", "The bucket you tried to delete is not empty", http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	if !h.storage.BucketExists(bucket) {
		h.writeError(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
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
		h.writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
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
		h.writeError(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	contentType := r.Header.Get("Content-Type")
	metadata, err := h.storage.PutObject(bucket, key, r.Body, contentType)
	if err != nil {
		h.writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", metadata.ETag)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	reader, metadata, err := h.storage.GetObject(bucket, key)
	if err != nil {
		h.writeError(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
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
		h.writeError(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
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
		h.writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
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

func (h *S3Handler) writeError(w http.ResponseWriter, code, message string, status int) {
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
