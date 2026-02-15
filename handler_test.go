package main

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
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
