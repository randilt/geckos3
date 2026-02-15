package main

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════════
// End-to-End Workflows – simulates realistic SDK usage patterns
// ═══════════════════════════════════════════════════════════════════════════════

// TestE2EFullObjectLifecycle simulates: create bucket → put → head → get → delete → verify gone
func TestE2EFullObjectLifecycle(t *testing.T) {
	srv, _ := setupTestServer(t)

	// 1. Create bucket
	resp := mustDo(t, "PUT", srv.URL+"/lifecycle", nil, nil)
	resp.Body.Close()
	assertStatus(t, "create bucket", resp.StatusCode, 200)

	// 2. Put object
	resp = mustDo(t, "PUT", srv.URL+"/lifecycle/readme.md",
		strings.NewReader("# Hello\nThis is a test."),
		map[string]string{"Content-Type": "text/markdown"})
	etag := resp.Header.Get("ETag")
	resp.Body.Close()
	assertStatus(t, "put object", resp.StatusCode, 200)
	if etag == "" {
		t.Fatal("PUT should return ETag")
	}

	// 3. Head object
	resp = mustDo(t, "HEAD", srv.URL+"/lifecycle/readme.md", nil, nil)
	resp.Body.Close()
	assertStatus(t, "head object", resp.StatusCode, 200)
	if resp.Header.Get("ETag") != etag {
		t.Error("HEAD ETag should match PUT ETag")
	}
	if resp.Header.Get("Content-Type") != "text/markdown" {
		t.Errorf("HEAD Content-Type: %q", resp.Header.Get("Content-Type"))
	}

	// 4. Get object
	resp = mustDo(t, "GET", srv.URL+"/lifecycle/readme.md", nil, nil)
	body := readBody(t, resp)
	assertStatus(t, "get object", resp.StatusCode, 200)
	if body != "# Hello\nThis is a test." {
		t.Errorf("GET body: %q", body)
	}

	// 5. Delete object
	resp = mustDo(t, "DELETE", srv.URL+"/lifecycle/readme.md", nil, nil)
	resp.Body.Close()
	assertStatus(t, "delete object", resp.StatusCode, 204)

	// 6. Verify gone
	resp = mustDo(t, "GET", srv.URL+"/lifecycle/readme.md", nil, nil)
	resp.Body.Close()
	assertStatus(t, "verify deleted", resp.StatusCode, 404)

	// 7. Delete empty bucket
	resp = mustDo(t, "DELETE", srv.URL+"/lifecycle", nil, nil)
	resp.Body.Close()
	assertStatus(t, "delete bucket", resp.StatusCode, 204)

	// 8. Verify bucket gone
	resp = mustDo(t, "HEAD", srv.URL+"/lifecycle", nil, nil)
	resp.Body.Close()
	assertStatus(t, "verify bucket gone", resp.StatusCode, 404)
}

// TestE2EMultiObjectUploadAndList simulates uploading many objects and paginating through them
func TestE2EMultiObjectUploadAndList(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/multi", nil, nil).Body.Close()

	// Upload 7 objects
	keys := []string{
		"alpha.txt", "beta.txt", "gamma.txt", "delta.txt",
		"epsilon.txt", "zeta.txt", "eta.txt",
	}
	for _, key := range keys {
		mustDo(t, "PUT", srv.URL+"/multi/"+key,
			strings.NewReader("content-"+key), nil).Body.Close()
	}

	// List with max-keys=3 and paginate through
	var allKeys []string
	token := ""
	for {
		url := srv.URL + "/multi?list-type=2&max-keys=3"
		if token != "" {
			url += "&continuation-token=" + token
		}
		resp := mustDo(t, "GET", url, nil, nil)
		body := readBody(t, resp)

		var result ListBucketResult
		xml.Unmarshal([]byte(body), &result)

		for _, obj := range result.Contents {
			allKeys = append(allKeys, obj.Key)
		}

		if !result.IsTruncated {
			break
		}
		token = result.NextContinuationToken
	}

	if len(allKeys) != 7 {
		t.Errorf("pagination collected %d keys, want 7: %v", len(allKeys), allKeys)
	}
}

// TestE2ECopyAndVerify simulates copy → verify original unchanged → verify copy content
func TestE2ECopyAndVerify(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/copysrc", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/copydst", nil, nil).Body.Close()

	original := "The quick brown fox jumps over the lazy dog."
	mustDo(t, "PUT", srv.URL+"/copysrc/doc.txt",
		strings.NewReader(original),
		map[string]string{"Content-Type": "text/plain"}).Body.Close()

	// Copy cross-bucket
	resp := mustDo(t, "PUT", srv.URL+"/copydst/doc-copy.txt", nil,
		map[string]string{"x-amz-copy-source": "/copysrc/doc.txt"})
	resp.Body.Close()
	assertStatus(t, "copy", resp.StatusCode, 200)

	// Original still intact
	resp = mustDo(t, "GET", srv.URL+"/copysrc/doc.txt", nil, nil)
	body := readBody(t, resp)
	if body != original {
		t.Errorf("original altered: %q", body)
	}

	// Copy matches
	resp = mustDo(t, "GET", srv.URL+"/copydst/doc-copy.txt", nil, nil)
	body = readBody(t, resp)
	if body != original {
		t.Errorf("copy content: %q", body)
	}

	// Copy has correct content-type
	resp = mustDo(t, "HEAD", srv.URL+"/copydst/doc-copy.txt", nil, nil)
	resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("copy content-type: %q", resp.Header.Get("Content-Type"))
	}
}

// TestE2EBatchDelete simulates creating objects then batch-deleting them
func TestE2EBatchDelete(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/batch", nil, nil).Body.Close()

	// Create objects
	for _, key := range []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"} {
		mustDo(t, "PUT", srv.URL+"/batch/"+key,
			strings.NewReader("data"), nil).Body.Close()
	}

	// Batch delete a, c, e
	deleteXML := `<Delete>
		<Object><Key>a.txt</Key></Object>
		<Object><Key>c.txt</Key></Object>
		<Object><Key>e.txt</Key></Object>
	</Delete>`
	resp := mustDo(t, "POST", srv.URL+"/batch?delete",
		strings.NewReader(deleteXML), nil)
	body := readBody(t, resp)
	assertStatus(t, "batch delete", resp.StatusCode, 200)

	var result DeleteResult
	xml.Unmarshal([]byte(body), &result)
	if len(result.Deleted) != 3 {
		t.Errorf("deleted: %d", len(result.Deleted))
	}

	// b.txt and d.txt should survive
	for _, key := range []string{"b.txt", "d.txt"} {
		resp = mustDo(t, "GET", srv.URL+"/batch/"+key, nil, nil)
		resp.Body.Close()
		assertStatus(t, key+" survive", resp.StatusCode, 200)
	}

	// a, c, e should be gone
	for _, key := range []string{"a.txt", "c.txt", "e.txt"} {
		resp = mustDo(t, "GET", srv.URL+"/batch/"+key, nil, nil)
		resp.Body.Close()
		assertStatus(t, key+" gone", resp.StatusCode, 404)
	}
}

// TestE2EDirectoryLikeStructure simulates folder-like navigation with delimiter
func TestE2EDirectoryLikeStructure(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/dirs", nil, nil).Body.Close()

	files := map[string]string{
		"photos/2024/jan/img1.jpg": "img1",
		"photos/2024/jan/img2.jpg": "img2",
		"photos/2024/feb/img3.jpg": "img3",
		"photos/2023/dec/img4.jpg": "img4",
		"docs/readme.md":           "readme",
		"index.html":               "html",
	}
	for key, content := range files {
		mustDo(t, "PUT", srv.URL+"/dirs/"+key,
			strings.NewReader(content), nil).Body.Close()
	}

	// List root with delimiter /
	resp := mustDo(t, "GET", srv.URL+"/dirs?list-type=2&delimiter=/", nil, nil)
	body := readBody(t, resp)
	var root ListBucketResult
	xml.Unmarshal([]byte(body), &root)

	if len(root.Contents) != 1 { // index.html
		t.Errorf("root contents: %d", len(root.Contents))
	}
	if len(root.CommonPrefixes) != 2 { // photos/, docs/
		t.Errorf("root common prefixes: %d", len(root.CommonPrefixes))
	}

	// List photos/2024/ with delimiter /
	resp = mustDo(t, "GET", srv.URL+"/dirs?list-type=2&prefix=photos/2024/&delimiter=/", nil, nil)
	body = readBody(t, resp)
	var sub ListBucketResult
	xml.Unmarshal([]byte(body), &sub)

	if len(sub.Contents) != 0 {
		t.Errorf("photos/2024 should have 0 direct objects, got %d", len(sub.Contents))
	}
	if len(sub.CommonPrefixes) != 2 { // jan/, feb/
		t.Errorf("photos/2024 common prefixes: %d", len(sub.CommonPrefixes))
	}
	cpSet := map[string]bool{}
	for _, cp := range sub.CommonPrefixes {
		cpSet[cp.Prefix] = true
	}
	if !cpSet["photos/2024/jan/"] || !cpSet["photos/2024/feb/"] {
		t.Errorf("expected jan/ and feb/ prefixes: %v", cpSet)
	}

	// List photos/2024/jan/ — should return actual objects
	resp = mustDo(t, "GET", srv.URL+"/dirs?list-type=2&prefix=photos/2024/jan/", nil, nil)
	body = readBody(t, resp)
	var leaf ListBucketResult
	xml.Unmarshal([]byte(body), &leaf)

	if len(leaf.Contents) != 2 {
		t.Errorf("photos/2024/jan/ expected 2 objects, got %d", len(leaf.Contents))
	}
}

// TestE2EOverwritePreservesListing simulates overwriting and verifying the object count doesn't change
func TestE2EOverwritePreservesListing(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/overwrite", nil, nil).Body.Close()

	mustDo(t, "PUT", srv.URL+"/overwrite/file.txt",
		strings.NewReader("version1"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/overwrite/file.txt",
		strings.NewReader("version2-longer"), nil).Body.Close()

	// Only 1 object in listing
	resp := mustDo(t, "GET", srv.URL+"/overwrite?list-type=2", nil, nil)
	body := readBody(t, resp)
	var result ListBucketResult
	xml.Unmarshal([]byte(body), &result)

	if result.KeyCount != 1 {
		t.Errorf("overwrite should not duplicate in listing, got %d", result.KeyCount)
	}
	if len(result.Contents) != 1 {
		t.Fatal("expected 1 object in listing")
	}
	if result.Contents[0].Size != int64(len("version2-longer")) {
		t.Errorf("listing size should reflect overwrite: %d", result.Contents[0].Size)
	}
}

// TestE2EListV1Pagination simulates V1 pagination with markers
func TestE2EListV1Pagination(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/v1page", nil, nil).Body.Close()
	for _, key := range []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"} {
		mustDo(t, "PUT", srv.URL+"/v1page/"+key,
			strings.NewReader("x"), nil).Body.Close()
	}

	var allKeys []string
	marker := ""
	for {
		url := srv.URL + "/v1page?max-keys=2"
		if marker != "" {
			url += "&marker=" + marker
		}
		resp := mustDo(t, "GET", url, nil, nil)
		body := readBody(t, resp)

		var result ListBucketResultV1
		xml.Unmarshal([]byte(body), &result)

		for _, obj := range result.Contents {
			allKeys = append(allKeys, obj.Key)
		}

		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
		if marker == "" {
			break
		}
	}

	if len(allKeys) != 5 {
		t.Errorf("V1 pagination: collected %d, want 5: %v", len(allKeys), allKeys)
	}
}

// TestE2EMultipleBucketsWithSameKeys verifies bucket isolation
func TestE2EMultipleBucketsWithSameKeys(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/bucket-a", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/bucket-b", nil, nil).Body.Close()

	mustDo(t, "PUT", srv.URL+"/bucket-a/shared.txt",
		strings.NewReader("content-A"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/bucket-b/shared.txt",
		strings.NewReader("content-B"), nil).Body.Close()

	// Verify isolation
	resp := mustDo(t, "GET", srv.URL+"/bucket-a/shared.txt", nil, nil)
	if readBody(t, resp) != "content-A" {
		t.Error("bucket-a content should be content-A")
	}

	resp = mustDo(t, "GET", srv.URL+"/bucket-b/shared.txt", nil, nil)
	if readBody(t, resp) != "content-B" {
		t.Error("bucket-b content should be content-B")
	}

	// Delete from bucket-a, bucket-b unaffected
	mustDo(t, "DELETE", srv.URL+"/bucket-a/shared.txt", nil, nil).Body.Close()

	resp = mustDo(t, "GET", srv.URL+"/bucket-b/shared.txt", nil, nil)
	if readBody(t, resp) != "content-B" {
		t.Error("bucket-b should be unaffected by bucket-a delete")
	}

	resp = mustDo(t, "GET", srv.URL+"/bucket-a/shared.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Error("bucket-a shared.txt should be gone")
	}
}

// TestE2EDeleteBucketRequiresEmpty verifies the full cleanup flow
func TestE2EDeleteBucketRequiresEmpty(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/cleanup", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/cleanup/f1.txt", strings.NewReader("a"), nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/cleanup/f2.txt", strings.NewReader("b"), nil).Body.Close()

	// Can't delete non-empty
	resp := mustDo(t, "DELETE", srv.URL+"/cleanup", nil, nil)
	resp.Body.Close()
	assertStatus(t, "delete non-empty", resp.StatusCode, 409)

	// Delete all objects
	mustDo(t, "DELETE", srv.URL+"/cleanup/f1.txt", nil, nil).Body.Close()
	mustDo(t, "DELETE", srv.URL+"/cleanup/f2.txt", nil, nil).Body.Close()

	// Now delete should work
	resp = mustDo(t, "DELETE", srv.URL+"/cleanup", nil, nil)
	resp.Body.Close()
	assertStatus(t, "delete empty", resp.StatusCode, 204)
}

// TestE2EListBucketsAfterCreateDelete verifies ListBuckets reflects creates/deletes
func TestE2EListBucketsAfterCreateDelete(t *testing.T) {
	srv, _ := setupTestServer(t)

	mustDo(t, "PUT", srv.URL+"/first", nil, nil).Body.Close()
	mustDo(t, "PUT", srv.URL+"/second", nil, nil).Body.Close()

	assertBucketCount(t, srv, 2)

	mustDo(t, "DELETE", srv.URL+"/first", nil, nil).Body.Close()
	assertBucketCount(t, srv, 1)

	mustDo(t, "PUT", srv.URL+"/third", nil, nil).Body.Close()
	assertBucketCount(t, srv, 2)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Logging Middleware
// ═══════════════════════════════════════════════════════════════════════════════

func TestLoggingMiddlewareSetsRequestID(t *testing.T) {
	dir := t.TempDir()
	storage := NewFilesystemStorage(dir)
	handler := NewS3Handler(storage, &NoOpAuthenticator{})
	logged := LoggingMiddleware(handler)
	server := httptest.NewServer(logged)
	defer server.Close()

	resp, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	reqID := resp.Header.Get("x-amz-request-id")
	if reqID == "" {
		t.Error("missing x-amz-request-id header")
	}
	if !strings.HasPrefix(reqID, "geckos3-") {
		t.Errorf("request ID format: %q", reqID)
	}
}

func TestLoggingMiddlewareIncrements(t *testing.T) {
	dir := t.TempDir()
	storage := NewFilesystemStorage(dir)
	handler := NewS3Handler(storage, &NoOpAuthenticator{})
	logged := LoggingMiddleware(handler)
	server := httptest.NewServer(logged)
	defer server.Close()

	resp1, _ := http.Get(server.URL + "/health")
	id1 := resp1.Header.Get("x-amz-request-id")
	resp1.Body.Close()

	resp2, _ := http.Get(server.URL + "/health")
	id2 := resp2.Header.Get("x-amz-request-id")
	resp2.Body.Close()

	if id1 == id2 {
		t.Error("request IDs should be unique")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

func assertStatus(t *testing.T, step string, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("[%s] status %d, want %d", step, got, want)
	}
}

func assertBucketCount(t *testing.T, srv *httptest.Server, expected int) {
	t.Helper()
	resp := mustDo(t, "GET", srv.URL+"/", nil, nil)
	body := readBody(t, resp)
	var result ListAllMyBucketsResult
	xml.Unmarshal([]byte(body), &result)
	if len(result.Buckets.Bucket) != expected {
		t.Errorf("expected %d buckets, got %d", expected, len(result.Buckets.Bucket))
	}
}
