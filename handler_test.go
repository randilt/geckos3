package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Test Helpers
// ═══════════════════════════════════════════════════════════════════════════════

func setupTestServer(t *testing.T) (*httptest.Server, *FilesystemStorage) {
	t.Helper()
	dir := t.TempDir()
	storage := NewFilesystemStorage(dir)
	handler := NewS3Handler(storage, &NoOpAuthenticator{})
	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })
	return server, storage
}

func mustDo(t *testing.T, method, url string, body io.Reader, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Health Check
// ═══════════════════════════════════════════════════════════════════════════════

func TestHealthEndpoint(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "GET", srv.URL+"/health", nil, nil)
	body := readBody(t, resp)

	if resp.StatusCode != 200 {
		t.Errorf("health status: %d", resp.StatusCode)
	}
	if body != "OK" {
		t.Errorf("health body: %q", body)
	}
}

func TestHealthEndpointPostNotAllowed(t *testing.T) {
	srv, _ := setupTestServer(t)

	// POST /health should not match health check, should be treated as bucket operation
	resp := mustDo(t, "POST", srv.URL+"/health", nil, nil)
	resp.Body.Close()
	// "health" is a valid bucket name, POST without ?delete is not implemented
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("POST /health should be 501, got %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Bucket Operations via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPCreateBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("create bucket: %d", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/mybucket" {
		t.Errorf("Location header: %q", loc)
	}
}

func TestHTTPCreateBucketIdempotent(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	resp := mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("idempotent create: %d", resp.StatusCode)
	}
}

func TestHTTPCreateBucketInvalidName(t *testing.T) {
	srv, _ := setupTestServer(t)

	cases := []string{
		"AB",                    // too short
		"UPPERCASE",             // no uppercase
		"-leading-dash",         // leading dash
		"trailing-dash-",        // trailing dash
		".leading-dot",          // leading dot
		"trailing-dot.",         // trailing dot
		"buck..et",              // double dot
		"buck et",               // space
		strings.Repeat("a", 64), // too long
	}
	for _, name := range cases {
		resp := mustDo(t, "PUT", srv.URL+"/"+name, nil, nil)
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("bucket %q: expected 400, got %d (body: %s)", name, resp.StatusCode, body)
		}
	}
}

func TestHTTPCreateBucketValidNames(t *testing.T) {
	srv, _ := setupTestServer(t)

	cases := []string{
		"abc",
		"my-bucket",
		"bucket.name",
		"a123",
		strings.Repeat("a", 63),
	}
	for _, name := range cases {
		resp := mustDo(t, "PUT", srv.URL+"/"+name, nil, nil)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("bucket %q: expected 200, got %d", name, resp.StatusCode)
		}
	}
}

func TestHTTPHeadBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "HEAD", srv.URL+"/mybucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("head bucket: %d", resp.StatusCode)
	}
}

func TestHTTPHeadBucketNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "HEAD", srv.URL+"/nonexistent", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("head missing bucket: %d", resp.StatusCode)
	}
}

func TestHTTPDeleteBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/delbucket", nil, nil).Body.Close()

	resp := mustDo(t, "DELETE", srv.URL+"/delbucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("delete bucket: %d", resp.StatusCode)
	}

	// Verify gone
	resp = mustDo(t, "HEAD", srv.URL+"/delbucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("deleted bucket should 404: %d", resp.StatusCode)
	}
}

func TestHTTPDeleteBucketNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "DELETE", srv.URL+"/nonexistent", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 404 {
		t.Errorf("delete missing bucket: %d", resp.StatusCode)
	}
	if !strings.Contains(body, "NoSuchBucket") {
		t.Errorf("error should be NoSuchBucket, got: %s", body)
	}
}

func TestHTTPDeleteBucketNotEmpty(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/fullbucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/fullbucket/obj.txt",
		strings.NewReader("data"), map[string]string{"Content-Type": "text/plain"}).Body.Close()

	resp := mustDo(t, "DELETE", srv.URL+"/fullbucket", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 409 {
		t.Errorf("delete non-empty bucket: %d", resp.StatusCode)
	}
	if !strings.Contains(body, "BucketNotEmpty") {
		t.Errorf("error should be BucketNotEmpty: %s", body)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Object Operations via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPPutGetObject(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// PUT object
	putResp := mustDo(t, "PUT", srv.URL+"/mybucket/hello.txt",
		strings.NewReader("Hello World"),
		map[string]string{"Content-Type": "text/plain"})
	putResp.Body.Close()
	if putResp.StatusCode != 200 {
		t.Fatalf("put object: %d", putResp.StatusCode)
	}
	etag := putResp.Header.Get("ETag")
	if etag == "" {
		t.Error("put should return ETag")
	}

	// GET object
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/hello.txt", nil, nil)
	body := readBody(t, getResp)
	if getResp.StatusCode != 200 {
		t.Errorf("get object: %d", getResp.StatusCode)
	}
	if body != "Hello World" {
		t.Errorf("body: %q", body)
	}
	if getResp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("content-type: %q", getResp.Header.Get("Content-Type"))
	}
	if getResp.Header.Get("ETag") != etag {
		t.Errorf("ETag mismatch on GET")
	}
}

func TestHTTPPutObjectToNonExistentBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "PUT", srv.URL+"/nonexistent/obj.txt",
		strings.NewReader("data"), nil)
	body := readBody(t, resp)
	if resp.StatusCode != 404 {
		t.Errorf("put to missing bucket: %d, body: %s", resp.StatusCode, body)
	}
}

func TestHTTPGetObjectNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket/missing.txt", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 404 {
		t.Errorf("get missing: %d", resp.StatusCode)
	}
	if !strings.Contains(body, "NoSuchKey") {
		t.Errorf("error should be NoSuchKey: %s", body)
	}
}

func TestHTTPHeadObject(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/file.txt",
		strings.NewReader("12345"), map[string]string{"Content-Type": "text/plain"}).Body.Close()

	resp := mustDo(t, "HEAD", srv.URL+"/mybucket/file.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("head object: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "5" {
		t.Errorf("content-length: %q", resp.Header.Get("Content-Length"))
	}
	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("content-type: %q", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("ETag") == "" {
		t.Error("head should return ETag")
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Errorf("accept-ranges: %q", resp.Header.Get("Accept-Ranges"))
	}
}

func TestHTTPHeadObjectNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "HEAD", srv.URL+"/mybucket/missing.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("head missing: %d", resp.StatusCode)
	}
}

func TestHTTPDeleteObject(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/del.txt",
		strings.NewReader("delete-me"), nil).Body.Close()

	resp := mustDo(t, "DELETE", srv.URL+"/mybucket/del.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("delete object: %d", resp.StatusCode)
	}

	// Verify gone
	resp = mustDo(t, "GET", srv.URL+"/mybucket/del.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("deleted object should 404: %d", resp.StatusCode)
	}
}

func TestHTTPDeleteObjectIdempotent(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Delete non-existent object should return 204
	resp := mustDo(t, "DELETE", srv.URL+"/mybucket/never-existed.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("delete non-existent: %d", resp.StatusCode)
	}
}

func TestHTTPPutObjectNestedKey(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/a/b/c/deep.txt",
		strings.NewReader("deep content"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket/a/b/c/deep.txt", nil, nil)
	body := readBody(t, resp)
	if body != "deep content" {
		t.Errorf("nested key: %q", body)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// ListBuckets via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPListBuckets(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/alpha", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/beta", nil, nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Errorf("list buckets: %d", resp.StatusCode)
	}

	// Parse XML
	var result ListAllMyBucketsResult
	if err := xml.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("xml parse: %v\nbody: %s", err, body)
	}
	if len(result.Buckets.Bucket) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(result.Buckets.Bucket))
	}
	names := map[string]bool{}
	for _, b := range result.Buckets.Bucket {
		names[b.Name] = true
	}
	if !names["alpha"] || !names["beta"] {
		t.Errorf("missing buckets: %v", names)
	}
}

func TestHTTPListBucketsEmpty(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "GET", srv.URL+"/", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Errorf("list buckets empty: %d", resp.StatusCode)
	}

	var result ListAllMyBucketsResult
	xml.Unmarshal([]byte(body), &result)
	if len(result.Buckets.Bucket) != 0 {
		t.Errorf("expected 0 buckets, got %d", len(result.Buckets.Bucket))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// ListObjectsV2 via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPListObjectsV2Basic(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/a.txt", strings.NewReader("aaa"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/b.txt", strings.NewReader("bbb"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("list v2: %d", resp.StatusCode)
	}

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if result.Name != "mybucket" {
		t.Errorf("Name: %q", result.Name)
	}
	if result.KeyCount != 2 {
		t.Errorf("KeyCount: %d", result.KeyCount)
	}
	if len(result.Contents) != 2 {
		t.Errorf("Contents len: %d", len(result.Contents))
	}
	if result.IsTruncated {
		t.Error("should not be truncated")
	}
}

func TestHTTPListObjectsV2Prefix(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/logs/app.log", strings.NewReader("log"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/logs/err.log", strings.NewReader("err"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/data/file.csv", strings.NewReader("csv"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&prefix=logs/", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if result.Prefix != "logs/" {
		t.Errorf("Prefix: %q", result.Prefix)
	}
	if len(result.Contents) != 2 {
		t.Errorf("expected 2 with prefix, got %d", len(result.Contents))
	}
}

func TestHTTPListObjectsV2Delimiter(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/logs/app.log", strings.NewReader("a"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/logs/err.log", strings.NewReader("b"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/data/file.csv", strings.NewReader("c"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/root.txt", strings.NewReader("d"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&delimiter=/", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if result.Delimiter != "/" {
		t.Errorf("Delimiter: %q", result.Delimiter)
	}
	// Should have root.txt as content and logs/ + data/ as common prefixes
	if len(result.Contents) != 1 {
		t.Errorf("Contents: expected 1, got %d", len(result.Contents))
	}
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("CommonPrefixes: expected 2, got %d", len(result.CommonPrefixes))
	}
	cpSet := map[string]bool{}
	for _, cp := range result.CommonPrefixes {
		cpSet[cp.Prefix] = true
	}
	if !cpSet["logs/"] || !cpSet["data/"] {
		t.Errorf("CommonPrefixes: %v", cpSet)
	}
}

func TestHTTPListObjectsV2MaxKeys(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	for i := 0; i < 5; i++ {
		key := "file" + string(rune('a'+i)) + ".txt"
		mustDo(t, "PUT", srv.URL+"/mybucket/"+key, strings.NewReader("x"), nil).Body.Close()
	}

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=2", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if !result.IsTruncated {
		t.Error("should be truncated")
	}
	if result.KeyCount != 2 {
		t.Errorf("KeyCount: %d", result.KeyCount)
	}
	if result.NextContinuationToken == "" {
		t.Error("should have continuation token")
	}
}

func TestHTTPListObjectsV2Pagination(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	for i := 0; i < 5; i++ {
		key := "file" + string(rune('a'+i)) + ".txt"
		mustDo(t, "PUT", srv.URL+"/mybucket/"+key, strings.NewReader("x"), nil).Body.Close()
	}

	// Page 1
	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=2", nil, nil)
	body := readBody(t, resp)
	var page1 ListBucketResult
	xml.Unmarshal([]byte(body), &page1)

	if !page1.IsTruncated {
		t.Fatal("page1 should be truncated")
	}
	token := page1.NextContinuationToken

	// Page 2
	resp = mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=2&continuation-token="+token, nil, nil)
	body = readBody(t, resp)
	var page2 ListBucketResult
	xml.Unmarshal([]byte(body), &page2)

	if page2.KeyCount != 2 {
		t.Errorf("page2 KeyCount: %d", page2.KeyCount)
	}

	// Page 3
	if page2.IsTruncated {
		resp = mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=2&continuation-token="+page2.NextContinuationToken, nil, nil)
		body = readBody(t, resp)
		var page3 ListBucketResult
		xml.Unmarshal([]byte(body), &page3)

		if page3.IsTruncated {
			t.Error("page3 should not be truncated")
		}
	}

	// All keys collected
	allKeys := len(page1.Contents) + len(page2.Contents)
	if allKeys < 4 {
		t.Errorf("should have collected at least 4 keys across pages, got %d", allKeys)
	}
}

func TestHTTPListObjectsV2StartAfter(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/aaa.txt", strings.NewReader("x"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/bbb.txt", strings.NewReader("x"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/ccc.txt", strings.NewReader("x"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&start-after=aaa.txt", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if len(result.Contents) != 2 {
		t.Errorf("expected 2 after start-after, got %d", len(result.Contents))
	}
	if result.StartAfter != "aaa.txt" {
		t.Errorf("StartAfter: %q", result.StartAfter)
	}
}

func TestHTTPListObjectsV2EmptyBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if result.KeyCount != 0 {
		t.Errorf("KeyCount: %d", result.KeyCount)
	}
	if len(result.Contents) != 0 {
		t.Errorf("Contents: %d", len(result.Contents))
	}
}

func TestHTTPListObjectsV2NonExistentBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "GET", srv.URL+"/nonexistent?list-type=2", nil, nil)
	body := readBody(t, resp)

	if resp.StatusCode != 404 {
		t.Errorf("list v2 missing bucket: %d", resp.StatusCode)
	}
	if !strings.Contains(body, "NoSuchBucket") {
		t.Errorf("expected NoSuchBucket: %s", body)
	}
}

func TestHTTPListObjectsV2MaxKeysZero(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/a.txt", strings.NewReader("x"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=0", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if result.KeyCount != 0 {
		t.Errorf("max-keys=0 should return 0, got %d", result.KeyCount)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// ListObjectsV1 via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPListObjectsV1Basic(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/a.txt", strings.NewReader("aaa"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/b.txt", strings.NewReader("bbb"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("list v1: %d", resp.StatusCode)
	}

	var result ListBucketResultV1
	xml.Unmarshal([]byte(body), &result)

	if result.Name != "mybucket" {
		t.Errorf("Name: %q", result.Name)
	}
	if len(result.Contents) != 2 {
		t.Errorf("Contents: %d", len(result.Contents))
	}
}

func TestHTTPListObjectsV1Marker(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/aaa.txt", strings.NewReader("x"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/bbb.txt", strings.NewReader("x"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/ccc.txt", strings.NewReader("x"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?marker=aaa.txt", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResultV1
	xml.Unmarshal([]byte(body), &result)

	if len(result.Contents) != 2 {
		t.Errorf("expected 2 after marker, got %d", len(result.Contents))
	}
	if result.Marker != "aaa.txt" {
		t.Errorf("Marker: %q", result.Marker)
	}
}

func TestHTTPListObjectsV1Delimiter(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/dir1/a.txt", strings.NewReader("x"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/dir2/b.txt", strings.NewReader("x"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/root.txt", strings.NewReader("x"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?delimiter=/", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResultV1
	xml.Unmarshal([]byte(body), &result)

	if len(result.Contents) != 1 {
		t.Errorf("Contents: %d (expected 1 root file)", len(result.Contents))
	}
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("CommonPrefixes: %d (expected 2 dirs)", len(result.CommonPrefixes))
	}
}

func TestHTTPListObjectsV1MaxKeysTruncation(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	for i := 0; i < 5; i++ {
		key := "f" + string(rune('a'+i)) + ".txt"
		mustDo(t, "PUT", srv.URL+"/mybucket/"+key, strings.NewReader("x"), nil).Body.Close()
	}

	resp := mustDo(t, "GET", srv.URL+"/mybucket?max-keys=2", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResultV1
	xml.Unmarshal([]byte(body), &result)

	if !result.IsTruncated {
		t.Error("should be truncated")
	}
	if len(result.Contents) != 2 {
		t.Errorf("Contents: %d", len(result.Contents))
	}
	if result.NextMarker == "" {
		t.Error("should have NextMarker")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// CopyObject via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPCopyObject(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/original.txt",
		strings.NewReader("copy content"), map[string]string{"Content-Type": "text/plain"}).Body.Close()

	// Copy
	resp := mustDo(t, "PUT", srv.URL+"/mybucket/copied.txt", nil,
		map[string]string{"x-amz-copy-source": "/mybucket/original.txt"})
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Errorf("copy: %d, body: %s", resp.StatusCode, body)
	}

	var copyResult CopyObjectResult
	xml.Unmarshal([]byte(body), &copyResult)
	if copyResult.ETag == "" {
		t.Error("CopyObjectResult should have ETag")
	}

	// Verify copy content
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/copied.txt", nil, nil)
	getBody := readBody(t, getResp)
	if getBody != "copy content" {
		t.Errorf("copy content: %q", getBody)
	}
}

func TestHTTPCopyObjectCrossBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/src-bucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/dst-bucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/src-bucket/file.txt",
		strings.NewReader("cross"), nil).Body.Close()

	resp := mustDo(t, "PUT", srv.URL+"/dst-bucket/file.txt", nil,
		map[string]string{"x-amz-copy-source": "/src-bucket/file.txt"})
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("cross-bucket copy: %d", resp.StatusCode)
	}

	getResp := mustDo(t, "GET", srv.URL+"/dst-bucket/file.txt", nil, nil)
	body := readBody(t, getResp)
	if body != "cross" {
		t.Errorf("cross-bucket content: %q", body)
	}
}

func TestHTTPCopyObjectMissingSource(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/dest.txt", nil,
		map[string]string{"x-amz-copy-source": "/mybucket/nonexistent.txt"})
	body := readBody(t, resp)
	if resp.StatusCode != 404 {
		t.Errorf("copy missing source: %d, body: %s", resp.StatusCode, body)
	}
}

func TestHTTPCopyObjectInvalidSource(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Source without key
	resp := mustDo(t, "PUT", srv.URL+"/mybucket/dest.txt", nil,
		map[string]string{"x-amz-copy-source": "/mybucket"})
	body := readBody(t, resp)
	if resp.StatusCode != 400 {
		t.Errorf("copy invalid source: %d, body: %s", resp.StatusCode, body)
	}
}

func TestHTTPCopyObjectSourceBucketNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/dest.txt", nil,
		map[string]string{"x-amz-copy-source": "/nonexistent/file.txt"})
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("copy from nonexistent bucket: %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// DeleteObjects (Batch) via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPDeleteObjects(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/a.txt", strings.NewReader("a"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/b.txt", strings.NewReader("b"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/c.txt", strings.NewReader("c"), nil).Body.Close()

	deleteXML := `<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>`
	resp := mustDo(t, "POST", srv.URL+"/mybucket?delete", strings.NewReader(deleteXML),
		map[string]string{"Content-Type": "application/xml"})
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("delete objects: %d, body: %s", resp.StatusCode, body)
	}

	var result DeleteResult
	xml.Unmarshal([]byte(body), &result)
	if len(result.Deleted) != 2 {
		t.Errorf("expected 2 deleted, got %d", len(result.Deleted))
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected 0 errors, got %d", len(result.Errors))
	}

	// c.txt should still exist
	resp = mustDo(t, "GET", srv.URL+"/mybucket/c.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Error("c.txt should still exist")
	}

	// a.txt should be gone
	resp = mustDo(t, "GET", srv.URL+"/mybucket/a.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Error("a.txt should be gone")
	}
}

func TestHTTPDeleteObjectsQuietMode(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/q1.txt", strings.NewReader("q"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/q2.txt", strings.NewReader("q"), nil).Body.Close()

	deleteXML := `<Delete><Quiet>true</Quiet><Object><Key>q1.txt</Key></Object><Object><Key>q2.txt</Key></Object></Delete>`
	resp := mustDo(t, "POST", srv.URL+"/mybucket?delete", strings.NewReader(deleteXML),
		map[string]string{"Content-Type": "application/xml"})
	body := readBody(t, resp)

	var result DeleteResult
	xml.Unmarshal([]byte(body), &result)

	// Quiet mode should not return deleted keys
	if len(result.Deleted) != 0 {
		t.Errorf("quiet mode: expected 0 deleted entries, got %d", len(result.Deleted))
	}
}

func TestHTTPDeleteObjectsMalformedXML(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "POST", srv.URL+"/mybucket?delete",
		strings.NewReader("not-xml"), map[string]string{"Content-Type": "application/xml"})
	body := readBody(t, resp)
	if resp.StatusCode != 400 {
		t.Errorf("malformed XML: %d", resp.StatusCode)
	}
	if !strings.Contains(body, "MalformedXML") {
		t.Errorf("expected MalformedXML: %s", body)
	}
}

func TestHTTPDeleteObjectsNonExistentBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	deleteXML := `<Delete><Object><Key>a.txt</Key></Object></Delete>`
	resp := mustDo(t, "POST", srv.URL+"/nonexistent?delete",
		strings.NewReader(deleteXML), nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("delete objects nonexistent bucket: %d", resp.StatusCode)
	}
}

func TestHTTPDeleteObjectsIncludesNonExistentKeys(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Delete a key that doesn't exist — S3 returns success
	deleteXML := `<Delete><Object><Key>phantom.txt</Key></Object></Delete>`
	resp := mustDo(t, "POST", srv.URL+"/mybucket?delete",
		strings.NewReader(deleteXML), nil)
	body := readBody(t, resp)

	var result DeleteResult
	xml.Unmarshal([]byte(body), &result)

	if len(result.Deleted) != 1 {
		t.Errorf("expected 1 deleted (even for non-existent), got %d", len(result.Deleted))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Method Not Allowed
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPMethodNotAllowedBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "PATCH", srv.URL+"/mybucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Errorf("PATCH on bucket: %d", resp.StatusCode)
	}
}

func TestHTTPMethodNotAllowedObject(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "PATCH", srv.URL+"/mybucket/obj.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Errorf("PATCH on object: %d", resp.StatusCode)
	}
}

func TestHTTPServiceLevelUnsupported(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "DELETE", srv.URL+"/", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 501 {
		t.Errorf("DELETE /: %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// XML Response Format
// ═══════════════════════════════════════════════════════════════════════════════

func TestXMLDeclarationPresent(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	body := readBody(t, resp)

	if !strings.HasPrefix(body, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>") {
		t.Errorf("missing XML declaration, body starts: %q", body[:min(60, len(body))])
	}
}

func TestErrorResponseXMLFormat(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "GET", srv.URL+"/nonexistent?list-type=2", nil, nil)
	body := readBody(t, resp)

	// Should have XML declaration
	if !strings.HasPrefix(body, "<?xml") {
		t.Error("error response should have XML declaration")
	}

	var errResp ErrorResponse
	// Strip the XML declaration for unmarshal
	xmlContent := body[strings.Index(body, "<Error"):]
	if err := xml.Unmarshal([]byte(xmlContent), &errResp); err != nil {
		t.Fatalf("xml parse error response: %v\nbody: %s", err, body)
	}
	if errResp.Code != "NoSuchBucket" {
		t.Errorf("error code: %q", errResp.Code)
	}
}

func TestContentTypeIsXML(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if ct != "application/xml" {
		t.Errorf("Content-Type: %q", ct)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// parsePath
// ═══════════════════════════════════════════════════════════════════════════════

func TestParsePath(t *testing.T) {
	h := &S3Handler{}

	cases := []struct {
		path   string
		bucket string
		key    string
	}{
		{"/", "", ""},
		{"/mybucket", "mybucket", ""},
		{"/mybucket/", "mybucket", ""},
		{"/mybucket/key.txt", "mybucket", "key.txt"},
		{"/mybucket/a/b/c.txt", "mybucket", "a/b/c.txt"},
		{"", "", ""},
	}

	for _, c := range cases {
		b, k := h.parsePath(c.path)
		if b != c.bucket || k != c.key {
			t.Errorf("parsePath(%q) = (%q, %q), want (%q, %q)", c.path, b, k, c.bucket, c.key)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// isValidBucketName
// ═══════════════════════════════════════════════════════════════════════════════

func TestIsValidBucketName(t *testing.T) {
	valid := []string{
		"abc", "my-bucket", "bucket.name", "123bucket", "a23",
		strings.Repeat("a", 63),
	}
	for _, n := range valid {
		if !isValidBucketName(n) {
			t.Errorf("should be valid: %q", n)
		}
	}

	invalid := []string{
		"ab",                    // too short
		"",                      // empty
		"AB",                    // uppercase
		"Abc",                   // uppercase
		"-leading",              // starts with dash
		"trailing-",             // ends with dash
		".leading",              // starts with dot
		"trailing.",             // ends with dot
		"has..double",           // double dots
		"has space",             // space
		"has_underscore",        // underscore
		strings.Repeat("a", 64), // too long
	}
	for _, n := range invalid {
		if isValidBucketName(n) {
			t.Errorf("should be invalid: %q", n)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Range Request Support
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPRangeRequest(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/range.txt",
		strings.NewReader("0123456789"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket/range.txt", nil,
		map[string]string{"Range": "bytes=0-4"})
	body := readBody(t, resp)

	if resp.StatusCode != 206 {
		t.Errorf("range: expected 206, got %d", resp.StatusCode)
	}
	if body != "01234" {
		t.Errorf("range body: %q", body)
	}
}

func TestHTTPRangeRequestMiddle(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/range.txt",
		strings.NewReader("0123456789"), nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket/range.txt", nil,
		map[string]string{"Range": "bytes=3-7"})
	body := readBody(t, resp)

	if resp.StatusCode != 206 {
		t.Errorf("range: expected 206, got %d", resp.StatusCode)
	}
	if body != "34567" {
		t.Errorf("range body: %q", body)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// POST without ?delete on bucket
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPPostBucketWithoutDelete(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "POST", srv.URL+"/mybucket",
		strings.NewReader("irrelevant"), nil)
	resp.Body.Close()
	if resp.StatusCode != 501 {
		t.Errorf("POST bucket without ?delete: %d", resp.StatusCode)
	}
}

// min helper for Go < 1.21 compat
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ═══════════════════════════════════════════════════════════════════════════════
// ListObjectsV2 – Object StorageClass and fields
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPListObjectsV2ObjectFields(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/mybucket/field-test.txt",
		strings.NewReader("hello"), map[string]string{"Content-Type": "text/plain"}).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if len(result.Contents) != 1 {
		t.Fatalf("expected 1 object, got %d", len(result.Contents))
	}

	obj := result.Contents[0]
	if obj.Key != "field-test.txt" {
		t.Errorf("Key: %q", obj.Key)
	}
	if obj.Size != 5 {
		t.Errorf("Size: %d", obj.Size)
	}
	if obj.ETag == "" {
		t.Error("ETag should be present in listing")
	}
	if obj.StorageClass != "STANDARD" {
		t.Errorf("StorageClass: %q", obj.StorageClass)
	}
	if obj.LastModified == "" {
		t.Error("LastModified should be present")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Empty body PUT
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPPutEmptyObject(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/empty.bin",
		bytes.NewReader(nil), nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("put empty: %d", resp.StatusCode)
	}

	headResp := mustDo(t, "HEAD", srv.URL+"/mybucket/empty.bin", nil, nil)
	headResp.Body.Close()
	if headResp.Header.Get("Content-Length") != "0" {
		t.Errorf("empty object length: %q", headResp.Header.Get("Content-Length"))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multipart Upload – Handler Layer
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPMultipartUploadBasic(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Create bucket
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Initiate multipart upload: POST /{bucket}/{key}?uploads
	resp := mustDo(t, "POST", srv.URL+"/mybucket/multi.txt?uploads", nil,
		map[string]string{"Content-Type": "text/plain"})
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("initiate multipart: %d", resp.StatusCode)
	}

	var initResult InitiateMultipartUploadResult
	if err := xml.Unmarshal([]byte(body), &initResult); err != nil {
		t.Fatalf("unmarshal initiate result: %v", err)
	}
	if initResult.UploadId == "" {
		t.Fatal("UploadId should not be empty")
	}
	if initResult.Bucket != "mybucket" {
		t.Errorf("Bucket: %q", initResult.Bucket)
	}
	if initResult.Key != "multi.txt" {
		t.Errorf("Key: %q", initResult.Key)
	}
	uploadID := initResult.UploadId

	// Upload part 1: PUT /{bucket}/{key}?partNumber=1&uploadId=X
	part1Resp := mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/multi.txt?partNumber=1&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("part-one-"), nil)
	part1Resp.Body.Close()
	if part1Resp.StatusCode != 200 {
		t.Fatalf("upload part 1: %d", part1Resp.StatusCode)
	}
	etag1 := part1Resp.Header.Get("ETag")
	if etag1 == "" {
		t.Fatal("part 1 ETag missing")
	}

	// Upload part 2
	part2Resp := mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/multi.txt?partNumber=2&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("part-two"), nil)
	part2Resp.Body.Close()
	if part2Resp.StatusCode != 200 {
		t.Fatalf("upload part 2: %d", part2Resp.StatusCode)
	}
	etag2 := part2Resp.Header.Get("ETag")

	// Complete multipart: POST /{bucket}/{key}?uploadId=X
	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag1, etag2)

	completeResp := mustDo(t, "POST",
		fmt.Sprintf("%s/mybucket/multi.txt?uploadId=%s", srv.URL, uploadID),
		strings.NewReader(completeXML), nil)
	completeBody := readBody(t, completeResp)
	if completeResp.StatusCode != 200 {
		t.Fatalf("complete multipart: %d, body: %s", completeResp.StatusCode, completeBody)
	}

	var completeResult CompleteMultipartUploadResultXML
	xml.Unmarshal([]byte(completeBody), &completeResult)
	if completeResult.Bucket != "mybucket" {
		t.Errorf("complete Bucket: %q", completeResult.Bucket)
	}
	if completeResult.Key != "multi.txt" {
		t.Errorf("complete Key: %q", completeResult.Key)
	}
	if completeResult.ETag == "" {
		t.Error("complete ETag should not be empty")
	}

	// Verify the assembled object content
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/multi.txt", nil, nil)
	content := readBody(t, getResp)
	if content != "part-one-part-two" {
		t.Errorf("assembled content: %q", content)
	}
}

func TestHTTPMultipartUploadAbort(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Initiate + upload a part
	resp := mustDo(t, "POST", srv.URL+"/mybucket/abort.txt?uploads", nil, nil)
	body := readBody(t, resp)
	var initResult InitiateMultipartUploadResult
	xml.Unmarshal([]byte(body), &initResult)
	uploadID := initResult.UploadId

	mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/abort.txt?partNumber=1&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("data"), nil).Body.Close()

	// Abort: DELETE /{bucket}/{key}?uploadId=X
	abortResp := mustDo(t, "DELETE",
		fmt.Sprintf("%s/mybucket/abort.txt?uploadId=%s", srv.URL, uploadID),
		nil, nil)
	abortResp.Body.Close()
	if abortResp.StatusCode != 204 {
		t.Errorf("abort: expected 204, got %d", abortResp.StatusCode)
	}

	// Object should not exist
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/abort.txt", nil, nil)
	getResp.Body.Close()
	if getResp.StatusCode != 404 {
		t.Errorf("after abort, GET should 404, got %d", getResp.StatusCode)
	}
}

func TestHTTPMultipartUploadInvalidPartNumber(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "POST", srv.URL+"/mybucket/file.txt?uploads", nil, nil)
	body := readBody(t, resp)
	var initResult InitiateMultipartUploadResult
	xml.Unmarshal([]byte(body), &initResult)
	uploadID := initResult.UploadId

	// Part number 0 is invalid
	badResp := mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/file.txt?partNumber=0&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("data"), nil)
	badResp.Body.Close()
	if badResp.StatusCode != 400 {
		t.Errorf("part 0: expected 400, got %d", badResp.StatusCode)
	}

	// Non-numeric part number
	badResp2 := mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/file.txt?partNumber=abc&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("data"), nil)
	badResp2.Body.Close()
	if badResp2.StatusCode != 400 {
		t.Errorf("non-numeric part: expected 400, got %d", badResp2.StatusCode)
	}
}

func TestHTTPMultipartUploadNonExistentBucket(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp := mustDo(t, "POST", srv.URL+"/ghostbucket/file.txt?uploads", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("initiate in non-existent bucket: expected 404, got %d", resp.StatusCode)
	}
}

func TestHTTPMultipartCompleteMalformedXML(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "POST", srv.URL+"/mybucket/file.txt?uploads", nil, nil)
	body := readBody(t, resp)
	var initResult InitiateMultipartUploadResult
	xml.Unmarshal([]byte(body), &initResult)
	uploadID := initResult.UploadId

	// Send malformed XML
	completeResp := mustDo(t, "POST",
		fmt.Sprintf("%s/mybucket/file.txt?uploadId=%s", srv.URL, uploadID),
		strings.NewReader("not xml at all <<<<"), nil)
	completeResp.Body.Close()
	if completeResp.StatusCode != 400 {
		t.Errorf("malformed XML: expected 400, got %d", completeResp.StatusCode)
	}
}

func TestHTTPMultipartAbortInvalidUploadID(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "DELETE",
		srv.URL+"/mybucket/file.txt?uploadId=nonexistent-id", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("abort invalid uploadId: expected 404, got %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Custom Metadata & Standard Headers – Handler Layer
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPCustomMetadataHeaders(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// PUT with x-amz-meta-* headers
	putResp := mustDo(t, "PUT", srv.URL+"/mybucket/meta.txt",
		strings.NewReader("metadata test"), map[string]string{
			"Content-Type":       "text/plain",
			"x-amz-meta-author":  "alice",
			"x-amz-meta-project": "geckos3",
		})
	putResp.Body.Close()
	if putResp.StatusCode != 200 {
		t.Fatalf("put with metadata: %d", putResp.StatusCode)
	}

	// GET should return the metadata headers
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/meta.txt", nil, nil)
	readBody(t, getResp)
	if getResp.Header.Get("x-amz-meta-author") != "alice" {
		t.Errorf("GET x-amz-meta-author: %q", getResp.Header.Get("x-amz-meta-author"))
	}
	if getResp.Header.Get("x-amz-meta-project") != "geckos3" {
		t.Errorf("GET x-amz-meta-project: %q", getResp.Header.Get("x-amz-meta-project"))
	}

	// HEAD should also return them
	headResp := mustDo(t, "HEAD", srv.URL+"/mybucket/meta.txt", nil, nil)
	headResp.Body.Close()
	if headResp.Header.Get("x-amz-meta-author") != "alice" {
		t.Errorf("HEAD x-amz-meta-author: %q", headResp.Header.Get("x-amz-meta-author"))
	}
	if headResp.Header.Get("x-amz-meta-project") != "geckos3" {
		t.Errorf("HEAD x-amz-meta-project: %q", headResp.Header.Get("x-amz-meta-project"))
	}
}

func TestHTTPStandardHeaders(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Use a client that does NOT auto-decompress Content-Encoding: gzip
	noDecompressClient := &http.Client{
		Transport: &http.Transport{DisableCompression: true},
	}
	doRaw := func(method, url string, body io.Reader, headers map[string]string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := noDecompressClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// PUT with Content-Encoding, Content-Disposition, Cache-Control
	putResp := doRaw("PUT", srv.URL+"/mybucket/compressed.js",
		strings.NewReader("var x=1;"), map[string]string{
			"Content-Type":        "application/javascript",
			"Content-Encoding":    "gzip",
			"Content-Disposition": "attachment; filename=\"app.js\"",
			"Cache-Control":       "public, max-age=31536000",
		})
	putResp.Body.Close()
	if putResp.StatusCode != 200 {
		t.Fatalf("put with headers: %d", putResp.StatusCode)
	}

	// GET should return them
	getResp := doRaw("GET", srv.URL+"/mybucket/compressed.js", nil, nil)
	io.Copy(io.Discard, getResp.Body)
	getResp.Body.Close()
	if getResp.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("GET Content-Encoding: %q", getResp.Header.Get("Content-Encoding"))
	}
	if getResp.Header.Get("Content-Disposition") != "attachment; filename=\"app.js\"" {
		t.Errorf("GET Content-Disposition: %q", getResp.Header.Get("Content-Disposition"))
	}
	if getResp.Header.Get("Cache-Control") != "public, max-age=31536000" {
		t.Errorf("GET Cache-Control: %q", getResp.Header.Get("Cache-Control"))
	}

	// HEAD should also return them
	headResp := doRaw("HEAD", srv.URL+"/mybucket/compressed.js", nil, nil)
	headResp.Body.Close()
	if headResp.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("HEAD Content-Encoding: %q", headResp.Header.Get("Content-Encoding"))
	}
	if headResp.Header.Get("Content-Disposition") != "attachment; filename=\"app.js\"" {
		t.Errorf("HEAD Content-Disposition: %q", headResp.Header.Get("Content-Disposition"))
	}
	if headResp.Header.Get("Cache-Control") != "public, max-age=31536000" {
		t.Errorf("HEAD Cache-Control: %q", headResp.Header.Get("Cache-Control"))
	}
}

func TestHTTPNoStandardHeadersWhenNotSet(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	mustDo(t, "PUT", srv.URL+"/mybucket/plain.txt",
		strings.NewReader("no special headers"), nil).Body.Close()

	headResp := mustDo(t, "HEAD", srv.URL+"/mybucket/plain.txt", nil, nil)
	headResp.Body.Close()

	// These should not be present when not set
	if headResp.Header.Get("Content-Encoding") != "" {
		t.Errorf("Content-Encoding should be absent: %q", headResp.Header.Get("Content-Encoding"))
	}
	if headResp.Header.Get("Content-Disposition") != "" {
		t.Errorf("Content-Disposition should be absent: %q", headResp.Header.Get("Content-Disposition"))
	}
	if headResp.Header.Get("Cache-Control") != "" {
		t.Errorf("Cache-Control should be absent: %q", headResp.Header.Get("Cache-Control"))
	}
}

func TestHTTPMetadataOverwriteReplacesAll(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// First upload with key1
	mustDo(t, "PUT", srv.URL+"/mybucket/evolve.txt",
		strings.NewReader("v1"), map[string]string{
			"x-amz-meta-key1": "val1",
			"Cache-Control":   "no-cache",
		}).Body.Close()

	// Overwrite with key2 (key1 should disappear)
	mustDo(t, "PUT", srv.URL+"/mybucket/evolve.txt",
		strings.NewReader("v2"), map[string]string{
			"x-amz-meta-key2": "val2",
			"Cache-Control":   "max-age=600",
		}).Body.Close()

	headResp := mustDo(t, "HEAD", srv.URL+"/mybucket/evolve.txt", nil, nil)
	headResp.Body.Close()

	if headResp.Header.Get("x-amz-meta-key2") != "val2" {
		t.Errorf("key2: %q", headResp.Header.Get("x-amz-meta-key2"))
	}
	if headResp.Header.Get("x-amz-meta-key1") != "" {
		t.Errorf("key1 should be gone after overwrite: %q", headResp.Header.Get("x-amz-meta-key1"))
	}
	if headResp.Header.Get("Cache-Control") != "max-age=600" {
		t.Errorf("Cache-Control: %q", headResp.Header.Get("Cache-Control"))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// SHA256 Payload Verification – Handler Layer
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPPayloadSHA256CorrectHash(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	payload := []byte("verified payload content")
	hash := sha256.Sum256(payload)
	hashHex := hex.EncodeToString(hash[:])

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/verified.txt",
		bytes.NewReader(payload), map[string]string{
			"X-Amz-Content-Sha256": hashHex,
		})
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("correct SHA256: expected 200, got %d", resp.StatusCode)
	}

	// Verify object exists and is correct
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/verified.txt", nil, nil)
	body := readBody(t, getResp)
	if body != "verified payload content" {
		t.Errorf("content: %q", body)
	}
}

func TestHTTPPayloadSHA256WrongHash(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	payload := []byte("actual content")
	// Send wrong hash
	wrongHash := hex.EncodeToString(sha256.New().Sum(nil)) // hash of empty

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/bad-hash.txt",
		bytes.NewReader(payload), map[string]string{
			"X-Amz-Content-Sha256": wrongHash,
		})
	body := readBody(t, resp)
	if resp.StatusCode != 400 {
		t.Errorf("wrong SHA256: expected 400, got %d, body: %s", resp.StatusCode, body)
	}

	// Verify object was cleaned up (should not exist)
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/bad-hash.txt", nil, nil)
	getResp.Body.Close()
	if getResp.StatusCode != 404 {
		t.Errorf("after bad digest, object should be gone, got %d", getResp.StatusCode)
	}
}

func TestHTTPPayloadSHA256UnsignedPayloadSkipsVerification(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/unsigned.txt",
		strings.NewReader("any content"), map[string]string{
			"X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
		})
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("UNSIGNED-PAYLOAD: expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPPayloadSHA256StreamingSkipsVerification(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/streaming.txt",
		strings.NewReader("streaming content"), map[string]string{
			"X-Amz-Content-Sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
		})
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("streaming signature: expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPPayloadSHA256EmptyBody(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// SHA256 of empty string
	hash := sha256.Sum256([]byte{})
	hashHex := hex.EncodeToString(hash[:])

	resp := mustDo(t, "PUT", srv.URL+"/mybucket/empty-verified.txt",
		bytes.NewReader(nil), map[string]string{
			"X-Amz-Content-Sha256": hashHex,
		})
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("empty body with correct SHA256: expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPPayloadNoSHA256HeaderSkipsVerification(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// No X-Amz-Content-Sha256 header at all — should succeed without verification
	resp := mustDo(t, "PUT", srv.URL+"/mybucket/no-sha.txt",
		strings.NewReader("no verification"), nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("no SHA256 header: expected 200, got %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multipart Upload – E2E combining multiple handler operations
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPMultipartUploadThenList(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Create and complete a multipart upload
	resp := mustDo(t, "POST", srv.URL+"/mybucket/listed.txt?uploads", nil, nil)
	body := readBody(t, resp)
	var initResult InitiateMultipartUploadResult
	xml.Unmarshal([]byte(body), &initResult)
	uploadID := initResult.UploadId

	partResp := mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/listed.txt?partNumber=1&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("listed-data"), nil)
	partResp.Body.Close()
	etag := partResp.Header.Get("ETag")

	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, etag)
	mustDo(t, "POST",
		fmt.Sprintf("%s/mybucket/listed.txt?uploadId=%s", srv.URL, uploadID),
		strings.NewReader(completeXML), nil).Body.Close()

	// List should include the object
	listResp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	listBody := readBody(t, listResp)
	var result ListBucketResult
	xml.Unmarshal([]byte(listBody), &result)

	found := false
	for _, obj := range result.Contents {
		if obj.Key == "listed.txt" {
			found = true
			if obj.Size != 11 {
				t.Errorf("size: %d", obj.Size)
			}
		}
	}
	if !found {
		t.Error("multipart object should appear in listing")
	}
}

func TestHTTPMultipartUploadThenDelete(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Complete a multipart upload
	resp := mustDo(t, "POST", srv.URL+"/mybucket/deletable.txt?uploads", nil, nil)
	body := readBody(t, resp)
	var initResult InitiateMultipartUploadResult
	xml.Unmarshal([]byte(body), &initResult)
	uploadID := initResult.UploadId

	partResp := mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/deletable.txt?partNumber=1&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("temp"), nil)
	partResp.Body.Close()
	etag := partResp.Header.Get("ETag")

	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, etag)
	mustDo(t, "POST",
		fmt.Sprintf("%s/mybucket/deletable.txt?uploadId=%s", srv.URL, uploadID),
		strings.NewReader(completeXML), nil).Body.Close()

	// Delete it
	delResp := mustDo(t, "DELETE", srv.URL+"/mybucket/deletable.txt", nil, nil)
	delResp.Body.Close()
	if delResp.StatusCode != 204 {
		t.Errorf("delete: %d", delResp.StatusCode)
	}

	// Verify gone
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/deletable.txt", nil, nil)
	getResp.Body.Close()
	if getResp.StatusCode != 404 {
		t.Errorf("after delete: expected 404, got %d", getResp.StatusCode)
	}
}

func TestHTTPPostObjectWithoutUploadsOrUploadId(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// POST to object without ?uploads or ?uploadId should 501
	resp := mustDo(t, "POST", srv.URL+"/mybucket/file.txt",
		strings.NewReader("irrelevant"), nil)
	resp.Body.Close()
	if resp.StatusCode != 501 {
		t.Errorf("POST without multipart params: expected 501, got %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 2: SHA256 Non-Destructive Verification – Handler Layer
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPSHA256WrongHashPreservesExistingObject(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Write a valid object first
	resp := mustDo(t, "PUT", srv.URL+"/mybucket/preserve.txt",
		strings.NewReader("original"), nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("initial put: %d", resp.StatusCode)
	}

	// Try to overwrite with wrong SHA256
	wrongHash := hex.EncodeToString(sha256.New().Sum(nil))
	resp2 := mustDo(t, "PUT", srv.URL+"/mybucket/preserve.txt",
		strings.NewReader("bad payload"), map[string]string{
			"X-Amz-Content-Sha256": wrongHash,
		})
	body2 := readBody(t, resp2)
	if resp2.StatusCode != 400 {
		t.Fatalf("bad digest: expected 400, got %d, body: %s", resp2.StatusCode, body2)
	}
	if !strings.Contains(body2, "BadDigest") {
		t.Errorf("expected BadDigest error code, got: %s", body2)
	}

	// Original object must survive
	getResp := mustDo(t, "GET", srv.URL+"/mybucket/preserve.txt", nil, nil)
	content := readBody(t, getResp)
	if content != "original" {
		t.Errorf("original content should survive bad-digest overwrite attempt: got %q", content)
	}
}

func TestHTTPSHA256BadDigestErrorFormat(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	wrongHash := hex.EncodeToString(sha256.New().Sum(nil))
	resp := mustDo(t, "PUT", srv.URL+"/mybucket/errfmt.txt",
		strings.NewReader("content"), map[string]string{
			"X-Amz-Content-Sha256": wrongHash,
		})
	body := readBody(t, resp)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	// Response should be valid XML with BadDigest error code
	var errResp ErrorResponse
	if err := xml.Unmarshal([]byte(body), &errResp); err != nil {
		t.Fatalf("response should be valid XML: %v", err)
	}
	if errResp.Code != "BadDigest" {
		t.Errorf("error code: expected BadDigest, got %q", errResp.Code)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 4: CORS Middleware
// ═══════════════════════════════════════════════════════════════════════════════

func setupCORSServer(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	storage := NewFilesystemStorage(dir)
	handler := NewS3Handler(storage, &NoOpAuthenticator{})
	// Wrap with CORS middleware just like main.go does
	corsHandler := CORSMiddleware(handler)
	server := httptest.NewServer(corsHandler)
	t.Cleanup(func() { server.Close() })
	return server
}

func TestCORSHeadersOnGET(t *testing.T) {
	srv := setupCORSServer(t)

	resp := mustDo(t, "GET", srv.URL+"/health", nil, nil)
	resp.Body.Close()

	if resp.Header.Get("Access-Control-Allow-Origin") == "" {
		t.Error("CORS: missing Access-Control-Allow-Origin")
	}
	if resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Error("CORS: missing Access-Control-Allow-Methods")
	}
	if resp.Header.Get("Access-Control-Allow-Headers") == "" {
		t.Error("CORS: missing Access-Control-Allow-Headers")
	}
	if resp.Header.Get("Access-Control-Expose-Headers") == "" {
		t.Error("CORS: missing Access-Control-Expose-Headers")
	}
}

func TestCORSPreflightOPTIONS(t *testing.T) {
	srv := setupCORSServer(t)

	req, _ := http.NewRequest("OPTIONS", srv.URL+"/mybucket/test.txt", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "PUT")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("OPTIONS preflight: expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Errorf("CORS origin: expected https://example.com, got %q",
			resp.Header.Get("Access-Control-Allow-Origin"))
	}
	if resp.Header.Get("Access-Control-Max-Age") != "3600" {
		t.Errorf("CORS max-age: %q", resp.Header.Get("Access-Control-Max-Age"))
	}
}

func TestCORSPreflightDoesNotReachHandler(t *testing.T) {
	srv := setupCORSServer(t)

	// OPTIONS on a non-existent bucket should still return 200,
	// proving it never reaches the S3 handler
	req, _ := http.NewRequest("OPTIONS", srv.URL+"/nonexistent/key.txt", nil)
	req.Header.Set("Origin", "https://test.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("OPTIONS should always return 200, got %d", resp.StatusCode)
	}
}

func TestCORSDefaultOriginWildcard(t *testing.T) {
	srv := setupCORSServer(t)

	// Request without Origin header should get *
	resp := mustDo(t, "GET", srv.URL+"/health", nil, nil)
	resp.Body.Close()

	origin := resp.Header.Get("Access-Control-Allow-Origin")
	if origin != "*" {
		t.Errorf("CORS origin without Origin header: expected *, got %q", origin)
	}
}

func TestCORSReflectsRequestOrigin(t *testing.T) {
	srv := setupCORSServer(t)

	req, _ := http.NewRequest("GET", srv.URL+"/health", nil)
	req.Header.Set("Origin", "https://my-app.example.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	origin := resp.Header.Get("Access-Control-Allow-Origin")
	if origin != "https://my-app.example.com" {
		t.Errorf("CORS should reflect Origin header: expected https://my-app.example.com, got %q", origin)
	}
}

func TestCORSHeadersOnPUT(t *testing.T) {
	srv := setupCORSServer(t)

	// Create bucket
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	req, _ := http.NewRequest("PUT", srv.URL+"/mybucket/obj.txt", strings.NewReader("data"))
	req.Header.Set("Origin", "https://app.dev")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("PUT: expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "https://app.dev" {
		t.Errorf("CORS on PUT: %q", resp.Header.Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSAllowedMethods(t *testing.T) {
	srv := setupCORSServer(t)

	resp := mustDo(t, "GET", srv.URL+"/health", nil, nil)
	resp.Body.Close()

	methods := resp.Header.Get("Access-Control-Allow-Methods")
	for _, m := range []string{"GET", "PUT", "POST", "DELETE", "HEAD", "OPTIONS"} {
		if !strings.Contains(methods, m) {
			t.Errorf("CORS allowed methods should include %s, got: %s", m, methods)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 6: MaxKeys Pagination Cap at 1000
// ═══════════════════════════════════════════════════════════════════════════════

func TestListObjectsV2MaxKeysCappedAt1000(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Request max-keys=5000 — should be capped to 1000
	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=5000", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)
	if result.MaxKeys != 1000 {
		t.Errorf("V2 MaxKeys should be capped at 1000, got %d", result.MaxKeys)
	}
}

func TestListObjectsV1MaxKeysCappedAt1000(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// V1 listing with max-keys=9999
	resp := mustDo(t, "GET", srv.URL+"/mybucket?max-keys=9999", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResultV1
	xml.Unmarshal([]byte(body), &result)
	if result.MaxKeys != 1000 {
		t.Errorf("V1 MaxKeys should be capped at 1000, got %d", result.MaxKeys)
	}
}

func TestListObjectsMaxKeysExact1000Allowed(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=1000", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)
	if result.MaxKeys != 1000 {
		t.Errorf("MaxKeys=1000 should pass through unchanged, got %d", result.MaxKeys)
	}
}

func TestListObjectsMaxKeysBelow1000PassesThrough(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2&max-keys=50", nil, nil)
	body := readBody(t, resp)

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)
	if result.MaxKeys != 50 {
		t.Errorf("MaxKeys=50 should pass through unchanged, got %d", result.MaxKeys)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 3: Temp Staging Dir in Handler E2E
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPPutDoesNotLeaveTmpInListing(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Upload several objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("obj-%d.txt", i)
		mustDo(t, "PUT", srv.URL+"/mybucket/"+key,
			strings.NewReader("data"), nil).Body.Close()
	}

	// List all objects — .geckos3-tmp must not appear
	resp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	body := readBody(t, resp)

	if strings.Contains(body, ".geckos3-tmp") {
		t.Error(".geckos3-tmp should never appear in object listings")
	}

	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)
	if result.KeyCount != 5 {
		t.Errorf("expected 5 objects, got %d", result.KeyCount)
	}
}

func TestHTTPMultipartCompleteTmpNotInListing(t *testing.T) {
	srv, _ := setupTestServer(t)
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Multipart upload
	resp := mustDo(t, "POST", srv.URL+"/mybucket/mp.txt?uploads", nil, nil)
	body := readBody(t, resp)
	var initResult InitiateMultipartUploadResult
	xml.Unmarshal([]byte(body), &initResult)
	uploadID := initResult.UploadId

	partResp := mustDo(t, "PUT",
		fmt.Sprintf("%s/mybucket/mp.txt?partNumber=1&uploadId=%s", srv.URL, uploadID),
		strings.NewReader("multipart content"), nil)
	etag := partResp.Header.Get("ETag")
	partResp.Body.Close()

	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, etag)
	mustDo(t, "POST",
		fmt.Sprintf("%s/mybucket/mp.txt?uploadId=%s", srv.URL, uploadID),
		strings.NewReader(completeXML), nil).Body.Close()

	// List — should only see mp.txt, no staging dirs
	listResp := mustDo(t, "GET", srv.URL+"/mybucket?list-type=2", nil, nil)
	listBody := readBody(t, listResp)

	if strings.Contains(listBody, ".geckos3-tmp") {
		t.Error(".geckos3-tmp should not appear in listing after multipart complete")
	}
	if strings.Contains(listBody, ".geckos3-multipart") {
		t.Error(".geckos3-multipart should not appear in listing")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 5: DeleteBucket with Artifacts via HTTP
// ═══════════════════════════════════════════════════════════════════════════════

func TestHTTPDeleteBucketWithArtifacts(t *testing.T) {
	srv, storage := setupTestServer(t)

	// Create bucket via API
	mustDo(t, "PUT", srv.URL+"/mybucket", nil, nil).Body.Close()

	// Manually place OS artifacts that would block old DeleteBucket
	os.WriteFile(filepath.Join(storage.dataDir, "mybucket", ".DS_Store"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(storage.dataDir, "mybucket", "Thumbs.db"), []byte("x"), 0644)

	// Delete should succeed
	resp := mustDo(t, "DELETE", srv.URL+"/mybucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("DeleteBucket with artifacts: expected 204, got %d", resp.StatusCode)
	}

	// Verify bucket is gone
	headResp := mustDo(t, "HEAD", srv.URL+"/mybucket", nil, nil)
	headResp.Body.Close()
	if headResp.StatusCode != 404 {
		t.Errorf("bucket should be gone: got %d", headResp.StatusCode)
	}
}
