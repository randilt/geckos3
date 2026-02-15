package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Bucket Operations – Comprehensive
// ═══════════════════════════════════════════════════════════════════════════════

func TestCreateBucketIdempotent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	if err := s.CreateBucket("mybucket"); err != nil {
		t.Fatal(err)
	}
	// second call must not error
	if err := s.CreateBucket("mybucket"); err != nil {
		t.Fatalf("second CreateBucket should be idempotent: %v", err)
	}
	if !s.BucketExists("mybucket") {
		t.Fatal("bucket should still exist")
	}
}

func TestBucketExistsNonExistent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	if s.BucketExists("ghost") {
		t.Fatal("non-existent bucket should return false")
	}
}

func TestDeleteNonExistentBucket(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	if err := s.DeleteBucket("nope"); err == nil {
		t.Fatal("should fail to delete non-existent bucket")
	}
}

func TestDeleteNonEmptyBucketFails(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	s.CreateBucket("full")
	s.PutObject("full", "obj.txt", strings.NewReader("data"), "")

	if err := s.DeleteBucket("full"); err == nil {
		t.Fatal("should fail to delete non-empty bucket")
	}
}

func TestDeleteEmptyBucket(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	s.CreateBucket("empty")
	if err := s.DeleteBucket("empty"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if s.BucketExists("empty") {
		t.Fatal("bucket should not exist after deletion")
	}
}

func TestListBuckets(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	s.CreateBucket("alpha")
	s.CreateBucket("beta")
	s.CreateBucket("gamma")

	buckets, err := s.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(buckets))
	}
	names := map[string]bool{}
	for _, b := range buckets {
		names[b.Name] = true
		if b.CreationDate.IsZero() {
			t.Errorf("bucket %q has zero creation date", b.Name)
		}
	}
	for _, n := range []string{"alpha", "beta", "gamma"} {
		if !names[n] {
			t.Errorf("missing bucket %q", n)
		}
	}
}

func TestListBucketsEmpty(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	buckets, err := s.ListBuckets()
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 0 {
		t.Fatalf("expected 0 buckets, got %d", len(buckets))
	}
}

func TestListBucketsIgnoresFiles(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	// Create a regular file in the data dir (not a bucket)
	os.WriteFile(filepath.Join(s.dataDir, "stray-file.txt"), []byte("oops"), 0644)
	s.CreateBucket("realbucket")

	buckets, _ := s.ListBuckets()
	for _, b := range buckets {
		if b.Name == "stray-file.txt" {
			t.Error("regular file should not appear as a bucket")
		}
	}
	if len(buckets) != 1 || buckets[0].Name != "realbucket" {
		t.Errorf("expected only 'realbucket', got %v", buckets)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Object CRUD – Comprehensive
// ═══════════════════════════════════════════════════════════════════════════════

func TestPutGetRoundTrip(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	body := "hello, geckos3"
	meta, err := s.PutObject("b", "greet.txt", strings.NewReader(body), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != int64(len(body)) {
		t.Errorf("size: %d, want %d", meta.Size, len(body))
	}
	if meta.ETag == "" {
		t.Error("ETag should not be empty")
	}
	if meta.ContentType != "text/plain" {
		t.Errorf("content-type: %q, want %q", meta.ContentType, "text/plain")
	}

	reader, gMeta, err := s.GetObject("b", "greet.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != body {
		t.Errorf("body: %q, want %q", data, body)
	}
	if gMeta.ETag != meta.ETag {
		t.Errorf("ETag mismatch on get: %q vs %q", gMeta.ETag, meta.ETag)
	}
}

func TestPutObjectDefaultContentType(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	meta, _ := s.PutObject("b", "file.bin", strings.NewReader("binary"), "")
	if meta.ContentType != "application/octet-stream" {
		t.Errorf("default content-type: got %q", meta.ContentType)
	}
}

func TestPutObjectOverwrite(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "f.txt", strings.NewReader("version1"), "text/plain")
	s.PutObject("b", "f.txt", strings.NewReader("version2"), "text/plain")

	reader, _, _ := s.GetObject("b", "f.txt")
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "version2" {
		t.Errorf("overwrite: got %q", data)
	}
}

func TestPutObjectNestedKey(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.PutObject("b", "a/b/c/deep.txt", strings.NewReader("deep"), "")
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := s.GetObject("b", "a/b/c/deep.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "deep" {
		t.Errorf("nested: got %q", data)
	}
}

func TestPutObjectToNonExistentBucketCreatesIt(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	// At the storage layer, PutObject creates dirs implicitly.
	// Bucket existence is enforced at the HTTP handler level.
	_, err := s.PutObject("newbucket", "file.txt", strings.NewReader("data"), "")
	if err != nil {
		t.Fatalf("PutObject should create dirs implicitly: %v", err)
	}
	if !s.BucketExists("newbucket") {
		t.Fatal("bucket should exist after implicit creation")
	}
}

func TestPutObjectEmptyBody(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	meta, err := s.PutObject("b", "zero.txt", strings.NewReader(""), "")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 0 {
		t.Errorf("size: got %d, want 0", meta.Size)
	}

	reader, gMeta, _ := s.GetObject("b", "zero.txt")
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if len(data) != 0 {
		t.Errorf("body should be empty, got %d bytes", len(data))
	}
	if gMeta.ETag == "" {
		t.Error("ETag should be non-empty even for empty file")
	}
}

func TestGetObjectNotFound(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, _, err := s.GetObject("b", "missing.txt")
	if err == nil {
		t.Fatal("GetObject for missing key should fail")
	}
}

func TestGetObjectNonExistentBucket(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	_, _, err := s.GetObject("ghost", "file.txt")
	if err == nil {
		t.Fatal("GetObject from non-existent bucket should fail")
	}
}

func TestHeadObjectMatch(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	body := "head-test-content"
	putMeta, _ := s.PutObject("b", "h.txt", strings.NewReader(body), "application/json")

	hMeta, err := s.HeadObject("b", "h.txt")
	if err != nil {
		t.Fatal(err)
	}
	if hMeta.Size != putMeta.Size {
		t.Errorf("size: %d vs %d", hMeta.Size, putMeta.Size)
	}
	if hMeta.ETag != putMeta.ETag {
		t.Errorf("ETag: %q vs %q", hMeta.ETag, putMeta.ETag)
	}
	if hMeta.ContentType != "application/json" {
		t.Errorf("content-type: %q", hMeta.ContentType)
	}
}

func TestHeadObjectNotFound(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.HeadObject("b", "nope")
	if err == nil {
		t.Fatal("HeadObject for missing key should fail")
	}
}

func TestDeleteObject(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "del.txt", strings.NewReader("gone"), "")
	if err := s.DeleteObject("b", "del.txt"); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.GetObject("b", "del.txt")
	if err == nil {
		t.Fatal("object should be gone")
	}
}

func TestDeleteObjectCleansEmptyDirs(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "x/y/z/file.txt", strings.NewReader("deep"), "")
	s.DeleteObject("b", "x/y/z/file.txt")

	if _, err := os.Stat(filepath.Join(s.dataDir, "b", "x")); err == nil {
		t.Error("empty parent dirs should be cleaned up")
	}
}

func TestDeleteObjectMetadataCleaned(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "m.txt", strings.NewReader("meta"), "")
	metaPath := s.metadataPath("b", "m.txt")
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("metadata should exist before delete: %v", err)
	}

	s.DeleteObject("b", "m.txt")
	if _, err := os.Stat(metaPath); err == nil {
		t.Fatal("metadata should be removed after delete")
	}
}

func TestDeleteNonExistentObjectSilent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// S3 returns 204 for deleting non-existent keys
	if err := s.DeleteObject("b", "nope.txt"); err != nil {
		t.Fatalf("deleting non-existent object should not error: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Copy Object
// ═══════════════════════════════════════════════════════════════════════════════

func TestCopyObjectSameBucket(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	body := "copy-me"
	s.PutObject("b", "orig.txt", strings.NewReader(body), "text/plain")

	meta, err := s.CopyObject("b", "orig.txt", "b", "copied.txt")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ContentType != "text/plain" {
		t.Errorf("content-type not preserved: %q", meta.ContentType)
	}

	reader, _, _ := s.GetObject("b", "copied.txt")
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != body {
		t.Errorf("copy content: %q", data)
	}
}

func TestCopyObjectCrossBucket(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("src")
	s.CreateBucket("dst")

	s.PutObject("src", "file.txt", strings.NewReader("cross-bucket"), "application/json")
	meta, err := s.CopyObject("src", "file.txt", "dst", "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ContentType != "application/json" {
		t.Errorf("content-type not preserved: %q", meta.ContentType)
	}

	reader, _, _ := s.GetObject("dst", "file.txt")
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "cross-bucket" {
		t.Errorf("content: %q", data)
	}
}

func TestCopyObjectSourceNotFound(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.CopyObject("b", "nope.txt", "b", "dest.txt")
	if err == nil {
		t.Fatal("copy from missing source should fail")
	}
}

func TestCopyObjectToNested(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "flat.txt", strings.NewReader("nested-copy"), "")
	_, err := s.CopyObject("b", "flat.txt", "b", "deep/nested/copy.txt")
	if err != nil {
		t.Fatal(err)
	}
	reader, _, _ := s.GetObject("b", "deep/nested/copy.txt")
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "nested-copy" {
		t.Errorf("nested copy: %q", data)
	}
}

func TestCopyObjectOverwritesExisting(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "src.txt", strings.NewReader("source"), "text/plain")
	s.PutObject("b", "dst.txt", strings.NewReader("old-dest"), "text/html")

	_, err := s.CopyObject("b", "src.txt", "b", "dst.txt")
	if err != nil {
		t.Fatal(err)
	}
	reader, _, _ := s.GetObject("b", "dst.txt")
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "source" {
		t.Errorf("overwrite copy: %q", data)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// List Objects – Advanced
// ═══════════════════════════════════════════════════════════════════════════════

func TestListObjectsEmpty(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	objs, err := s.ListObjects("b", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 0 {
		t.Errorf("expected 0 objects, got %d", len(objs))
	}
}

func TestListObjectsWithPrefix(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "logs/app.log", strings.NewReader("a"), "")
	s.PutObject("b", "logs/err.log", strings.NewReader("b"), "")
	s.PutObject("b", "data/file.csv", strings.NewReader("c"), "")

	objs, _ := s.ListObjects("b", "logs/", 0)
	if len(objs) != 2 {
		t.Errorf("prefix: expected 2, got %d", len(objs))
	}
	for _, o := range objs {
		if !strings.HasPrefix(o.Key, "logs/") {
			t.Errorf("unexpected key: %s", o.Key)
		}
	}
}

func TestListObjectsMaxKeys(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	for i := 0; i < 10; i++ {
		key := "file" + string(rune('a'+i)) + ".txt"
		s.PutObject("b", key, strings.NewReader("x"), "")
	}

	objs, _ := s.ListObjects("b", "", 3)
	if len(objs) != 3 {
		t.Errorf("maxKeys 3: expected 3, got %d", len(objs))
	}
}

func TestListObjectsNonExistentBucket(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	_, err := s.ListObjects("ghost", "", 0)
	if err == nil {
		t.Fatal("ListObjects on non-existent bucket should fail")
	}
}

func TestListObjectsSkipsMetadataFiles(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "real.txt", strings.NewReader("data"), "")

	objs, _ := s.ListObjects("b", "", 0)
	for _, o := range objs {
		if strings.HasSuffix(o.Key, ".metadata.json") {
			t.Errorf("metadata file in listing: %s", o.Key)
		}
	}
	if len(objs) != 1 || objs[0].Key != "real.txt" {
		t.Errorf("expected ['real.txt'], got %v", objs)
	}
}

func TestListObjectsETagPresent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	putMeta, _ := s.PutObject("b", "x.txt", strings.NewReader("etag-check"), "")
	objs, _ := s.ListObjects("b", "", 0)
	if len(objs) != 1 {
		t.Fatal("expected 1 object")
	}
	if objs[0].ETag != putMeta.ETag {
		t.Errorf("ETag in listing: %q vs put: %q", objs[0].ETag, putMeta.ETag)
	}
}

func TestListObjectsUnlimited(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	n := 50
	for i := 0; i < n; i++ {
		key := "item" + string(rune('A'+i%26)) + string(rune('0'+i/26)) + ".dat"
		s.PutObject("b", key, strings.NewReader("x"), "")
	}

	objs, _ := s.ListObjects("b", "", 0)
	if len(objs) != n {
		t.Errorf("unlimited: expected %d, got %d", n, len(objs))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Path Traversal Security
// ═══════════════════════════════════════════════════════════════════════════════

func TestPathTraversalBucketCreation(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	attacks := []string{
		"../etc",
		"../../",
		"..",
		"foo/../../etc",
		"./../../etc/passwd",
	}
	for _, name := range attacks {
		if err := s.CreateBucket(name); err == nil {
			t.Errorf("should reject traversal bucket name %q", name)
		}
		if s.BucketExists(name) {
			t.Errorf("traversal bucket %q should not exist", name)
		}
	}
}

func TestPathTraversalObjectKey(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	attacks := []string{
		"../../../etc/passwd",
		"foo/../../outside",
		"../secret",
	}
	for _, key := range attacks {
		_, err := s.PutObject("b", key, strings.NewReader("evil"), "")
		if err == nil {
			t.Errorf("should reject traversal key %q", key)
		}
	}
}

func TestNullByteInKeyRejected(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.PutObject("b", "file\x00.txt", strings.NewReader("null"), "")
	if err == nil {
		t.Fatal("should reject key with null byte")
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.PutObject("b", "", strings.NewReader("empty"), "")
	if err == nil {
		t.Fatal("should reject empty key")
	}
}

func TestEmptyBucketNameRejected(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	if err := s.CreateBucket(""); err == nil {
		t.Fatal("should reject empty bucket name")
	}
}

func TestNullByteInBucketRejected(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	// Null byte in bucket name should not create a bucket
	if err := s.CreateBucket("buck\x00et"); err == nil {
		t.Fatal("should reject bucket with null byte")
	}
}

func TestPathTraversalGetObject(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Create a file outside the bucket
	outsidePath := filepath.Join(s.dataDir, "outside.txt")
	os.WriteFile(outsidePath, []byte("secret"), 0644)

	_, _, err := s.GetObject("b", "../../outside.txt")
	if err == nil {
		t.Fatal("should reject path traversal in GetObject")
	}
}

func TestPathTraversalDeleteObject(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	err := s.DeleteObject("b", "../../../etc/passwd")
	if err == nil {
		t.Fatal("should reject path traversal in DeleteObject")
	}
}

func TestPathTraversalHeadObject(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.HeadObject("b", "../../outside")
	if err == nil {
		t.Fatal("should reject path traversal in HeadObject")
	}
}

func TestPathTraversalCopyObject(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")
	s.PutObject("b", "legit.txt", strings.NewReader("ok"), "")

	_, err := s.CopyObject("b", "legit.txt", "b", "../../escape.txt")
	if err == nil {
		t.Fatal("should reject path traversal in CopyObject destination")
	}

	_, err = s.CopyObject("b", "../../passwd", "b", "dest.txt")
	if err == nil {
		t.Fatal("should reject path traversal in CopyObject source")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// ETag Correctness
// ═══════════════════════════════════════════════════════════════════════════════

func TestETagIsQuotedMD5(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	meta, _ := s.PutObject("b", "etag.txt", strings.NewReader("hello"), "")

	if !strings.HasPrefix(meta.ETag, "\"") || !strings.HasSuffix(meta.ETag, "\"") {
		t.Errorf("ETag should be quoted: %q", meta.ETag)
	}
	inner := strings.Trim(meta.ETag, "\"")
	if len(inner) != 32 {
		t.Errorf("ETag inner should be 32 hex chars, got %d: %q", len(inner), inner)
	}
}

func TestETagConsistentAcrossOperations(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	putMeta, _ := s.PutObject("b", "e.txt", strings.NewReader("consistent"), "")
	_, getMeta, _ := s.GetObject("b", "e.txt")
	headMeta, _ := s.HeadObject("b", "e.txt")
	objs, _ := s.ListObjects("b", "", 0)

	if getMeta.ETag != putMeta.ETag {
		t.Error("GetObject ETag mismatch")
	}
	if headMeta.ETag != putMeta.ETag {
		t.Error("HeadObject ETag mismatch")
	}
	if len(objs) == 0 || objs[0].ETag != putMeta.ETag {
		t.Error("ListObjects ETag mismatch")
	}
}

func TestETagDiffersForDifferentContent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	m1, _ := s.PutObject("b", "a.txt", strings.NewReader("aaa"), "")
	m2, _ := s.PutObject("b", "b.txt", strings.NewReader("bbb"), "")

	if m1.ETag == m2.ETag {
		t.Error("different content should produce different ETags")
	}
}

func TestETagSameForSameContent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	m1, _ := s.PutObject("b", "a.txt", strings.NewReader("same"), "")
	m2, _ := s.PutObject("b", "b.txt", strings.NewReader("same"), "")

	if m1.ETag != m2.ETag {
		t.Error("same content should produce same ETag")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Content-Type Preservation
// ═══════════════════════════════════════════════════════════════════════════════

func TestContentTypePreserved(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	types := []string{
		"text/plain",
		"application/json",
		"image/png",
		"application/pdf",
		"text/html; charset=utf-8",
	}
	for i, ct := range types {
		key := "file" + string(rune('A'+i)) + ".dat"
		s.PutObject("b", key, strings.NewReader("data"), ct)

		_, getMeta, _ := s.GetObject("b", key)
		if getMeta.ContentType != ct {
			t.Errorf("Get: %q, want %q", getMeta.ContentType, ct)
		}

		headMeta, _ := s.HeadObject("b", key)
		if headMeta.ContentType != ct {
			t.Errorf("Head: %q, want %q", headMeta.ContentType, ct)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Concurrent Access
// ═══════════════════════════════════════════════════════════════════════════════

func TestConcurrentPutsSameKey(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			data := bytes.Repeat([]byte{byte(n)}, 1024)
			s.PutObject("b", "race.txt", bytes.NewReader(data), "")
		}(i)
	}
	wg.Wait()

	reader, meta, err := s.GetObject("b", "race.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if int64(len(data)) != meta.Size {
		t.Errorf("size mismatch after concurrent writes: body=%d meta=%d", len(data), meta.Size)
	}
}

func TestConcurrentPutsDifferentKeys(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	n := 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := "file" + string(rune('A'+idx%26)) + string(rune('0'+idx/26)) + ".txt"
			s.PutObject("b", key, strings.NewReader("concurrent"), "")
		}(i)
	}
	wg.Wait()

	objs, _ := s.ListObjects("b", "", 0)
	if len(objs) != n {
		t.Errorf("expected %d objects, got %d", n, len(objs))
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "shared.txt", strings.NewReader("initial"), "")

	var wg sync.WaitGroup

	// Concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				reader, _, err := s.GetObject("b", "shared.txt")
				if err == nil {
					io.Copy(io.Discard, reader)
					reader.Close()
				}
			}
		}()
	}

	// Concurrent writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := bytes.Repeat([]byte{byte(n)}, 100)
				s.PutObject("b", "shared.txt", bytes.NewReader(data), "")
			}
		}(i)
	}

	wg.Wait()

	// Should still be readable
	reader, _, err := s.GetObject("b", "shared.txt")
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Large Object
// ═══════════════════════════════════════════════════════════════════════════════

func TestLargeObject(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large object test in short mode")
	}
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	size := 10 * 1024 * 1024 // 10MB
	data := bytes.Repeat([]byte("x"), size)

	meta, err := s.PutObject("b", "big.bin", bytes.NewReader(data), "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != int64(size) {
		t.Errorf("size: %d, want %d", meta.Size, size)
	}

	reader, _, err := s.GetObject("b", "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, _ := io.ReadAll(reader)
	if len(got) != size {
		t.Errorf("readback: %d, want %d", len(got), size)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Metadata Persistence
// ═══════════════════════════════════════════════════════════════════════════════

func TestMetadataFileCreated(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "m.txt", strings.NewReader("meta"), "")
	metaPath := s.metadataPath("b", "m.txt")
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("metadata sidecar should exist: %v", err)
	}
}

func TestMetadataSurvivesOverwrite(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "ow.txt", strings.NewReader("v1"), "text/plain")
	_, m1, _ := s.GetObject("b", "ow.txt")

	s.PutObject("b", "ow.txt", strings.NewReader("v2-changed"), "application/json")
	_, m2, _ := s.GetObject("b", "ow.txt")

	if m1.ETag == m2.ETag {
		t.Error("ETag should change after overwrite")
	}
	if m2.ContentType != "application/json" {
		t.Errorf("content-type after overwrite: %q", m2.ContentType)
	}
	if m2.Size != int64(len("v2-changed")) {
		t.Errorf("size after overwrite: %d", m2.Size)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Object Path Mapping
// ═══════════════════════════════════════════════════════════════════════════════

func TestObjectPathMapping(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	tests := []struct {
		bucket   string
		key      string
		expected string
	}{
		{"mybucket", "file.txt", filepath.Join(s.dataDir, "mybucket", "file.txt")},
		{"mybucket", "dir/file.txt", filepath.Join(s.dataDir, "mybucket", "dir", "file.txt")},
		{"mybucket", "a/b/c/file.txt", filepath.Join(s.dataDir, "mybucket", "a", "b", "c", "file.txt")},
	}

	for _, tt := range tests {
		result := s.objectPath(tt.bucket, tt.key)
		if result != tt.expected {
			t.Errorf("objectPath(%q, %q) = %q, expected %q", tt.bucket, tt.key, result, tt.expected)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

func BenchmarkPutObject(b *testing.B) {
	tempDir := b.TempDir()
	storage := NewFilesystemStorage(tempDir)
	storage.CreateBucket("benchmark")

	content := bytes.Repeat([]byte("a"), 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := filepath.Join("test", string(rune(i%26+97)), "file.txt")
		storage.PutObject("benchmark", key, bytes.NewReader(content), "")
	}
}

func BenchmarkGetObject(b *testing.B) {
	tempDir := b.TempDir()
	storage := NewFilesystemStorage(tempDir)
	storage.CreateBucket("benchmark")

	content := bytes.Repeat([]byte("a"), 1024) // 1KB
	storage.PutObject("benchmark", "test.txt", bytes.NewReader(content), "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _, _ := storage.GetObject("benchmark", "test.txt")
		io.Copy(io.Discard, reader)
		reader.Close()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

func setupTestStorage(t *testing.T) (*FilesystemStorage, func()) {
	t.Helper()
	dir := t.TempDir()
	s := NewFilesystemStorage(dir)
	return s, func() { os.RemoveAll(dir) }
}
