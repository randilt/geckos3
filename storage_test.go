package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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
	s.PutObject("full", "obj.txt", strings.NewReader("data"), nil)

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
	meta, err := s.PutObject("b", "greet.txt", strings.NewReader(body), &PutObjectInput{ContentType: "text/plain"})
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

	meta, _ := s.PutObject("b", "file.bin", strings.NewReader("binary"), nil)
	if meta.ContentType != "application/octet-stream" {
		t.Errorf("default content-type: got %q", meta.ContentType)
	}
}

func TestPutObjectOverwrite(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "f.txt", strings.NewReader("version1"), &PutObjectInput{ContentType: "text/plain"})
	s.PutObject("b", "f.txt", strings.NewReader("version2"), &PutObjectInput{ContentType: "text/plain"})

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

	_, err := s.PutObject("b", "a/b/c/deep.txt", strings.NewReader("deep"), nil)
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
	_, err := s.PutObject("newbucket", "file.txt", strings.NewReader("data"), nil)
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

	meta, err := s.PutObject("b", "zero.txt", strings.NewReader(""), nil)
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
	putMeta, _ := s.PutObject("b", "h.txt", strings.NewReader(body), &PutObjectInput{ContentType: "application/json"})

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

	s.PutObject("b", "del.txt", strings.NewReader("gone"), nil)
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

	s.PutObject("b", "x/y/z/file.txt", strings.NewReader("deep"), nil)
	s.DeleteObject("b", "x/y/z/file.txt")

	if _, err := os.Stat(filepath.Join(s.dataDir, "b", "x")); err == nil {
		t.Error("empty parent dirs should be cleaned up")
	}
}

func TestDeleteObjectMetadataCleaned(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "m.txt", strings.NewReader("meta"), nil)
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
	s.PutObject("b", "orig.txt", strings.NewReader(body), &PutObjectInput{ContentType: "text/plain"})

	meta, err := s.CopyObject("b", "orig.txt", "b", "copied.txt", nil)
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

	s.PutObject("src", "file.txt", strings.NewReader("cross-bucket"), &PutObjectInput{ContentType: "application/json"})
	meta, err := s.CopyObject("src", "file.txt", "dst", "file.txt", nil)
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

	_, err := s.CopyObject("b", "nope.txt", "b", "dest.txt", nil)
	if err == nil {
		t.Fatal("copy from missing source should fail")
	}
}

func TestCopyObjectToNested(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "flat.txt", strings.NewReader("nested-copy"), nil)
	_, err := s.CopyObject("b", "flat.txt", "b", "deep/nested/copy.txt", nil)
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

	s.PutObject("b", "src.txt", strings.NewReader("source"), &PutObjectInput{ContentType: "text/plain"})
	s.PutObject("b", "dst.txt", strings.NewReader("old-dest"), &PutObjectInput{ContentType: "text/html"})

	_, err := s.CopyObject("b", "src.txt", "b", "dst.txt", nil)
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

	s.PutObject("b", "logs/app.log", strings.NewReader("a"), nil)
	s.PutObject("b", "logs/err.log", strings.NewReader("b"), nil)
	s.PutObject("b", "data/file.csv", strings.NewReader("c"), nil)

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
		s.PutObject("b", key, strings.NewReader("x"), nil)
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

	s.PutObject("b", "real.txt", strings.NewReader("data"), nil)

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

	putMeta, _ := s.PutObject("b", "x.txt", strings.NewReader("etag-check"), nil)
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
		s.PutObject("b", key, strings.NewReader("x"), nil)
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
		_, err := s.PutObject("b", key, strings.NewReader("evil"), nil)
		if err == nil {
			t.Errorf("should reject traversal key %q", key)
		}
	}
}

func TestNullByteInKeyRejected(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.PutObject("b", "file\x00.txt", strings.NewReader("null"), nil)
	if err == nil {
		t.Fatal("should reject key with null byte")
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.PutObject("b", "", strings.NewReader("empty"), nil)
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
	s.PutObject("b", "legit.txt", strings.NewReader("ok"), nil)

	_, err := s.CopyObject("b", "legit.txt", "b", "../../escape.txt", nil)
	if err == nil {
		t.Fatal("should reject path traversal in CopyObject destination")
	}

	_, err = s.CopyObject("b", "../../passwd", "b", "dest.txt", nil)
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

	meta, _ := s.PutObject("b", "etag.txt", strings.NewReader("hello"), nil)

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

	putMeta, _ := s.PutObject("b", "e.txt", strings.NewReader("consistent"), nil)
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

	m1, _ := s.PutObject("b", "a.txt", strings.NewReader("aaa"), nil)
	m2, _ := s.PutObject("b", "b.txt", strings.NewReader("bbb"), nil)

	if m1.ETag == m2.ETag {
		t.Error("different content should produce different ETags")
	}
}

func TestETagSameForSameContent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	m1, _ := s.PutObject("b", "a.txt", strings.NewReader("same"), nil)
	m2, _ := s.PutObject("b", "b.txt", strings.NewReader("same"), nil)

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
		s.PutObject("b", key, strings.NewReader("data"), &PutObjectInput{ContentType: ct})

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
			s.PutObject("b", "race.txt", bytes.NewReader(data), nil)
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
			s.PutObject("b", key, strings.NewReader("concurrent"), nil)
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

	s.PutObject("b", "shared.txt", strings.NewReader("initial"), nil)

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
				s.PutObject("b", "shared.txt", bytes.NewReader(data), nil)
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

	meta, err := s.PutObject("b", "big.bin", bytes.NewReader(data), &PutObjectInput{ContentType: "application/octet-stream"})
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

	s.PutObject("b", "m.txt", strings.NewReader("meta"), nil)
	metaPath := s.metadataPath("b", "m.txt")
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("metadata sidecar should exist: %v", err)
	}
}

func TestMetadataSurvivesOverwrite(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "ow.txt", strings.NewReader("v1"), &PutObjectInput{ContentType: "text/plain"})
	_, m1, _ := s.GetObject("b", "ow.txt")

	s.PutObject("b", "ow.txt", strings.NewReader("v2-changed"), &PutObjectInput{ContentType: "application/json"})
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
// Multipart Upload – Storage Layer
// ═══════════════════════════════════════════════════════════════════════════════

func TestMultipartUploadBasic(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Create multipart upload
	uploadID, err := s.CreateMultipartUpload("b", "multipart.txt", "text/plain")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if uploadID == "" {
		t.Fatal("uploadID should not be empty")
	}

	// Upload two parts
	etag1, err := s.UploadPart("b", "multipart.txt", uploadID, 1, strings.NewReader("Hello, "), "")
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}
	if etag1 == "" {
		t.Fatal("part 1 etag should not be empty")
	}

	etag2, err := s.UploadPart("b", "multipart.txt", uploadID, 2, strings.NewReader("World!"), "")
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}

	// Complete
	parts := []CompletedPart{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	}
	meta, err := s.CompleteMultipartUpload("b", "multipart.txt", uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if meta.Size != 13 {
		t.Errorf("expected size 13, got %d", meta.Size)
	}
	if meta.ContentType != "text/plain" {
		t.Errorf("content type: %q", meta.ContentType)
	}
	// Multipart ETag should contain "-2" suffix
	if !strings.Contains(meta.ETag, "-2") {
		t.Errorf("multipart etag should contain '-2': %q", meta.ETag)
	}

	// Verify object is readable
	reader, getMeta, err := s.GetObject("b", "multipart.txt")
	if err != nil {
		t.Fatalf("GetObject after multipart: %v", err)
	}
	data, _ := io.ReadAll(reader)
	reader.Close()
	if string(data) != "Hello, World!" {
		t.Errorf("content: %q", string(data))
	}
	if getMeta.ContentType != "text/plain" {
		t.Errorf("GetObject content type: %q", getMeta.ContentType)
	}
}

func TestMultipartUploadBucketNotExist(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	_, err := s.CreateMultipartUpload("ghost", "file.txt", "text/plain")
	if err == nil {
		t.Fatal("should fail for non-existent bucket")
	}
}

func TestMultipartUploadInvalidUploadID(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.UploadPart("b", "file.txt", "invalid-upload-id", 1, strings.NewReader("data"), "")
	if err == nil {
		t.Fatal("UploadPart should fail with invalid uploadID")
	}
}

func TestMultipartUploadAbort(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "abort.txt", "text/plain")
	s.UploadPart("b", "abort.txt", uploadID, 1, strings.NewReader("data"), "")

	if err := s.AbortMultipartUpload("b", "abort.txt", uploadID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	// After abort, the upload should no longer exist
	_, err := s.UploadPart("b", "abort.txt", uploadID, 2, strings.NewReader("more"), "")
	if err == nil {
		t.Fatal("UploadPart should fail after abort")
	}

	// Object should NOT exist
	_, _, err = s.GetObject("b", "abort.txt")
	if err == nil {
		t.Fatal("object should not exist after abort")
	}
}

func TestMultipartUploadAbortInvalidID(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	err := s.AbortMultipartUpload("b", "file.txt", "nonexistent")
	if err == nil {
		t.Fatal("abort should fail with invalid uploadID")
	}
}

func TestMultipartCompleteMissingPart(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "missing.txt", "text/plain")
	s.UploadPart("b", "missing.txt", uploadID, 1, strings.NewReader("data"), "")

	// Complete with part 2 which was never uploaded
	parts := []CompletedPart{
		{PartNumber: 1, ETag: "\"x\""},
		{PartNumber: 2, ETag: "\"y\""},
	}
	_, err := s.CompleteMultipartUpload("b", "missing.txt", uploadID, parts)
	if err == nil {
		t.Fatal("should fail when part is missing")
	}
}

func TestMultipartCompleteInvalidUploadID(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	_, err := s.CompleteMultipartUpload("b", "file.txt", "bad-id", nil)
	if err == nil {
		t.Fatal("should fail with invalid uploadID")
	}
}

func TestMultipartUploadDefaultContentType(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "file.bin", "")
	etag, _ := s.UploadPart("b", "file.bin", uploadID, 1, strings.NewReader("binary"), "")
	meta, err := s.CompleteMultipartUpload("b", "file.bin", uploadID, []CompletedPart{
		{PartNumber: 1, ETag: etag},
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta.ContentType != "application/octet-stream" {
		t.Errorf("default content type: %q", meta.ContentType)
	}
}

func TestMultipartUploadSinglePart(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "single.txt", "text/plain")
	etag, _ := s.UploadPart("b", "single.txt", uploadID, 1, strings.NewReader("only-one-part"), "")

	meta, err := s.CompleteMultipartUpload("b", "single.txt", uploadID, []CompletedPart{
		{PartNumber: 1, ETag: etag},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(meta.ETag, "-1") {
		t.Errorf("single-part multipart etag should end with -1: %q", meta.ETag)
	}

	reader, _, _ := s.GetObject("b", "single.txt")
	data, _ := io.ReadAll(reader)
	reader.Close()
	if string(data) != "only-one-part" {
		t.Errorf("content: %q", string(data))
	}
}

func TestMultipartUploadLargePartCount(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "many-parts.txt", "text/plain")

	var parts []CompletedPart
	for i := 1; i <= 5; i++ {
		data := strings.Repeat(string(rune('a'+i-1)), 100)
		etag, err := s.UploadPart("b", "many-parts.txt", uploadID, i, strings.NewReader(data), "")
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i, err)
		}
		parts = append(parts, CompletedPart{PartNumber: i, ETag: etag})
	}

	meta, err := s.CompleteMultipartUpload("b", "many-parts.txt", uploadID, parts)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 500 {
		t.Errorf("total size: %d, want 500", meta.Size)
	}
	if !strings.Contains(meta.ETag, "-5") {
		t.Errorf("etag should contain -5: %q", meta.ETag)
	}
}

func TestMultipartUploadDoesNotAppearInListing(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Start a multipart upload but don't complete it
	uploadID, _ := s.CreateMultipartUpload("b", "pending.txt", "text/plain")
	s.UploadPart("b", "pending.txt", uploadID, 1, strings.NewReader("partial"), "")

	// Also put a normal object
	s.PutObject("b", "normal.txt", strings.NewReader("ok"), nil)

	objects, err := s.ListObjects("b", "", 0)
	if err != nil {
		t.Fatal(err)
	}

	for _, obj := range objects {
		if strings.Contains(obj.Key, multipartStagingDir) || strings.Contains(obj.Key, "pending") {
			t.Errorf("multipart staging should not appear in listing: %q", obj.Key)
		}
	}
	if len(objects) != 1 || objects[0].Key != "normal.txt" {
		t.Errorf("expected only normal.txt, got %v", objects)
	}
}

func TestMultipartOverwritesExistingObject(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Put a regular object first
	s.PutObject("b", "overwrite.txt", strings.NewReader("original"), nil)

	// Overwrite via multipart
	uploadID, _ := s.CreateMultipartUpload("b", "overwrite.txt", "text/plain")
	etag, _ := s.UploadPart("b", "overwrite.txt", uploadID, 1, strings.NewReader("replaced"), "")
	_, err := s.CompleteMultipartUpload("b", "overwrite.txt", uploadID, []CompletedPart{
		{PartNumber: 1, ETag: etag},
	})
	if err != nil {
		t.Fatal(err)
	}

	reader, _, _ := s.GetObject("b", "overwrite.txt")
	data, _ := io.ReadAll(reader)
	reader.Close()
	if string(data) != "replaced" {
		t.Errorf("content after overwrite: %q", string(data))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Custom Metadata & Standard Headers – Storage Layer
// ═══════════════════════════════════════════════════════════════════════════════

func TestPutObjectWithCustomMetadata(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	input := &PutObjectInput{
		ContentType:    "application/json",
		CustomMetadata: map[string]string{"author": "alice", "version": "1.0"},
	}
	_, err := s.PutObject("b", "meta.json", strings.NewReader("{}"), input)
	if err != nil {
		t.Fatal(err)
	}

	_, meta, err := s.GetObject("b", "meta.json")
	if err != nil {
		t.Fatal(err)
	}
	if meta.CustomMetadata["author"] != "alice" {
		t.Errorf("author: %q", meta.CustomMetadata["author"])
	}
	if meta.CustomMetadata["version"] != "1.0" {
		t.Errorf("version: %q", meta.CustomMetadata["version"])
	}
}

func TestPutObjectWithStandardHeaders(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	input := &PutObjectInput{
		ContentType:        "text/css",
		ContentEncoding:    "gzip",
		ContentDisposition: "attachment; filename=\"style.css\"",
		CacheControl:       "max-age=3600",
	}
	_, err := s.PutObject("b", "style.css", strings.NewReader("body{}"), input)
	if err != nil {
		t.Fatal(err)
	}

	meta, err := s.HeadObject("b", "style.css")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ContentEncoding != "gzip" {
		t.Errorf("ContentEncoding: %q", meta.ContentEncoding)
	}
	if meta.ContentDisposition != "attachment; filename=\"style.css\"" {
		t.Errorf("ContentDisposition: %q", meta.ContentDisposition)
	}
	if meta.CacheControl != "max-age=3600" {
		t.Errorf("CacheControl: %q", meta.CacheControl)
	}
	if meta.ContentType != "text/css" {
		t.Errorf("ContentType: %q", meta.ContentType)
	}
}

func TestPutObjectNilInputDefaults(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	meta, err := s.PutObject("b", "default.bin", strings.NewReader("data"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ContentType != "application/octet-stream" {
		t.Errorf("nil input should default to octet-stream: %q", meta.ContentType)
	}
	if meta.ContentEncoding != "" {
		t.Errorf("nil input ContentEncoding should be empty: %q", meta.ContentEncoding)
	}
}

func TestOverwritePreservesNewMetadata(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// First write with metadata
	input1 := &PutObjectInput{
		ContentType:    "text/plain",
		CacheControl:   "no-cache",
		CustomMetadata: map[string]string{"key1": "val1"},
	}
	s.PutObject("b", "file.txt", strings.NewReader("v1"), input1)

	// Overwrite with different metadata
	input2 := &PutObjectInput{
		ContentType:    "application/json",
		CacheControl:   "max-age=600",
		CustomMetadata: map[string]string{"key2": "val2"},
	}
	s.PutObject("b", "file.txt", strings.NewReader("v2"), input2)

	meta, _ := s.HeadObject("b", "file.txt")
	if meta.ContentType != "application/json" {
		t.Errorf("ContentType: %q", meta.ContentType)
	}
	if meta.CacheControl != "max-age=600" {
		t.Errorf("CacheControl: %q", meta.CacheControl)
	}
	if meta.CustomMetadata["key2"] != "val2" {
		t.Errorf("new key2: %q", meta.CustomMetadata["key2"])
	}
	// Old metadata should be gone
	if _, ok := meta.CustomMetadata["key1"]; ok {
		t.Error("old key1 should not survive overwrite")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lock Striping – Verify Deterministic Stripe Selection
// ═══════════════════════════════════════════════════════════════════════════════

func TestLockStripeConsistency(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	path := s.objectPath("bucket", "key.txt")
	mu1 := s.stripe(path)
	mu2 := s.stripe(path)

	// Same path must always map to the same stripe
	if mu1 != mu2 {
		t.Error("stripe() should return same mutex for same path")
	}
}

func TestLockStripeDifferentKeys(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	// Different paths may or may not hit different stripes,
	// but the call should never panic.
	for i := 0; i < 1000; i++ {
		path := s.objectPath("bucket", strings.Repeat("x", i))
		mu := s.stripe(path)
		if mu == nil {
			t.Fatal("stripe returned nil")
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 2: SHA256 Verification in Storage Layer
// ═══════════════════════════════════════════════════════════════════════════════

func TestPutObjectExpectedSHA256Correct(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	payload := []byte("verified content")
	hash := sha256.Sum256(payload)
	hashHex := hex.EncodeToString(hash[:])

	meta, err := s.PutObject("b", "verified.txt", bytes.NewReader(payload), &PutObjectInput{
		ExpectedSHA256: hashHex,
	})
	if err != nil {
		t.Fatalf("correct SHA256 should succeed: %v", err)
	}
	if meta.Size != int64(len(payload)) {
		t.Errorf("size: got %d, want %d", meta.Size, len(payload))
	}

	// Verify content was written
	r, _, err := s.GetObject("b", "verified.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "verified content" {
		t.Errorf("content mismatch: %q", data)
	}
}

func TestPutObjectExpectedSHA256Wrong(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	payload := []byte("actual content")
	wrongHash := hex.EncodeToString(sha256.New().Sum(nil)) // hash of empty

	_, err := s.PutObject("b", "bad.txt", bytes.NewReader(payload), &PutObjectInput{
		ExpectedSHA256: wrongHash,
	})
	if err != ErrBadDigest {
		t.Fatalf("wrong SHA256 should return ErrBadDigest, got: %v", err)
	}

	// Object must NOT exist — never committed
	_, _, getErr := s.GetObject("b", "bad.txt")
	if getErr == nil {
		t.Error("object should not exist after bad digest")
	}
}

func TestPutObjectSHA256DoesNotOverwriteExisting(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Write a valid object first
	s.PutObject("b", "keep.txt", strings.NewReader("original"), nil)

	// Attempt to overwrite with wrong SHA256
	wrongHash := hex.EncodeToString(sha256.New().Sum(nil))
	_, err := s.PutObject("b", "keep.txt", strings.NewReader("replacement"), &PutObjectInput{
		ExpectedSHA256: wrongHash,
	})
	if err != ErrBadDigest {
		t.Fatalf("expected ErrBadDigest, got: %v", err)
	}

	// Original object must survive untouched
	r, _, _ := s.GetObject("b", "keep.txt")
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "original" {
		t.Errorf("original content should survive bad-digest attempt: got %q", data)
	}
}

func TestPutObjectEmptySHA256FieldSkipsVerification(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// ExpectedSHA256 is empty — no verification
	meta, err := s.PutObject("b", "nocheck.txt", strings.NewReader("anything"), &PutObjectInput{
		ExpectedSHA256: "",
	})
	if err != nil {
		t.Fatalf("empty ExpectedSHA256 should skip verification: %v", err)
	}
	if meta.Size != 8 {
		t.Errorf("size: %d", meta.Size)
	}
}

func TestPutObjectSHA256EmptyBodyCorrect(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	hash := sha256.Sum256([]byte{})
	hashHex := hex.EncodeToString(hash[:])

	meta, err := s.PutObject("b", "empty.txt", bytes.NewReader(nil), &PutObjectInput{
		ExpectedSHA256: hashHex,
	})
	if err != nil {
		t.Fatalf("empty body with correct SHA256 should succeed: %v", err)
	}
	if meta.Size != 0 {
		t.Errorf("size: %d", meta.Size)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 3: Temp File Staging Directory (.geckos3-tmp)
// ═══════════════════════════════════════════════════════════════════════════════

func TestTmpStagingDirCreatedOnPut(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "file.txt", strings.NewReader("data"), nil)

	// The staging dir should exist after a PutObject
	stagingPath := filepath.Join(s.dataDir, "b", tmpStagingDir)
	info, err := os.Stat(stagingPath)
	if err != nil {
		t.Fatalf(".geckos3-tmp should be created: %v", err)
	}
	if !info.IsDir() {
		t.Error(".geckos3-tmp should be a directory")
	}
}

func TestTmpStagingDirNotInListing(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "file.txt", strings.NewReader("data"), nil)

	objects, err := s.ListObjects("b", "", 0)
	if err != nil {
		t.Fatal(err)
	}

	for _, obj := range objects {
		if strings.Contains(obj.Key, tmpStagingDir) {
			t.Errorf(".geckos3-tmp should not appear in listing, found: %s", obj.Key)
		}
	}
	if len(objects) != 1 || objects[0].Key != "file.txt" {
		t.Errorf("listing should have exactly [file.txt], got: %v", objects)
	}
}

func TestTmpStagingDirNoLeftoverAfterPut(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Write several objects
	for i := 0; i < 10; i++ {
		s.PutObject("b", strings.Repeat("x", i+1)+".txt", strings.NewReader("data"), nil)
	}

	// Staging dir should have no leftover temp files
	stagingPath := filepath.Join(s.dataDir, "b", tmpStagingDir)
	entries, err := os.ReadDir(stagingPath)
	if err != nil {
		t.Fatalf("cannot read staging dir: %v", err)
	}
	if len(entries) > 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("staging dir should be empty after successful puts, found: %v", names)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 5: DeleteBucket Ignores OS Artifacts
// ═══════════════════════════════════════════════════════════════════════════════

func TestDeleteBucketWithDSStore(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Create .DS_Store artifact
	dsStorePath := filepath.Join(s.dataDir, "b", ".DS_Store")
	os.WriteFile(dsStorePath, []byte("Bud1\x00"), 0644)

	// DeleteBucket should succeed despite .DS_Store
	if err := s.DeleteBucket("b"); err != nil {
		t.Fatalf("DeleteBucket with .DS_Store should succeed: %v", err)
	}
	if s.BucketExists("b") {
		t.Error("bucket should be gone after delete")
	}
}

func TestDeleteBucketWithThumbsDb(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	thumbsPath := filepath.Join(s.dataDir, "b", "Thumbs.db")
	os.WriteFile(thumbsPath, []byte("thumb"), 0644)

	if err := s.DeleteBucket("b"); err != nil {
		t.Fatalf("DeleteBucket with Thumbs.db should succeed: %v", err)
	}
}

func TestDeleteBucketWithStagingDirs(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Create both staging directories
	os.MkdirAll(filepath.Join(s.dataDir, "b", multipartStagingDir), 0755)
	os.MkdirAll(filepath.Join(s.dataDir, "b", tmpStagingDir), 0755)

	if err := s.DeleteBucket("b"); err != nil {
		t.Fatalf("DeleteBucket with staging dirs should succeed: %v", err)
	}
}

func TestDeleteBucketWithAllArtifactsCombined(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Create all possible hidden entries at once
	os.MkdirAll(filepath.Join(s.dataDir, "b", multipartStagingDir), 0755)
	os.MkdirAll(filepath.Join(s.dataDir, "b", tmpStagingDir), 0755)
	os.WriteFile(filepath.Join(s.dataDir, "b", ".DS_Store"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(s.dataDir, "b", "Thumbs.db"), []byte("x"), 0644)

	if err := s.DeleteBucket("b"); err != nil {
		t.Fatalf("DeleteBucket with all artifacts should succeed: %v", err)
	}
}

func TestDeleteBucketStillFailsWithRealObjects(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Add artifacts AND a real object
	os.WriteFile(filepath.Join(s.dataDir, "b", ".DS_Store"), []byte("x"), 0644)
	s.PutObject("b", "real.txt", strings.NewReader("real"), nil)

	if err := s.DeleteBucket("b"); err == nil {
		t.Fatal("DeleteBucket should fail when real objects exist even with artifacts")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 1: I/O Outside Stripe Lock – Concurrent Stress
// ═══════════════════════════════════════════════════════════════════════════════

func TestConcurrentPutsDifferentBucketsSameStripe(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	// Create multiple buckets and hammer concurrent writes to verify
	// that I/O outside the lock doesn't cause deadlocks
	for i := 0; i < 4; i++ {
		s.CreateBucket(strings.Repeat("b", i+1))
	}

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			bucket := strings.Repeat("b", (n%4)+1)
			key := strings.Repeat("k", (n%10)+1) + ".txt"
			content := strings.Repeat("data", n+1)
			_, err := s.PutObject(bucket, key, strings.NewReader(content), nil)
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent put failed: %v", err)
	}
}

func TestConcurrentPutAndDeleteNoDeadlock(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Write and delete the same key concurrently — the lock-outside-IO
	// change must not deadlock or panic
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.PutObject("b", "race.txt", strings.NewReader("write"), nil)
		}()
		go func() {
			defer wg.Done()
			s.DeleteObject("b", "race.txt")
		}()
	}
	wg.Wait()
	// If we get here without deadlock or panic, the test passes
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
		storage.PutObject("benchmark", key, bytes.NewReader(content), nil)
	}
}

func BenchmarkGetObject(b *testing.B) {
	tempDir := b.TempDir()
	storage := NewFilesystemStorage(tempDir)
	storage.CreateBucket("benchmark")

	content := bytes.Repeat([]byte("a"), 1024) // 1KB
	storage.PutObject("benchmark", "test.txt", bytes.NewReader(content), nil)

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

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 1: UploadPart SHA256 Verification
// ═══════════════════════════════════════════════════════════════════════════════

func TestUploadPartSHA256Match(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "sha.txt", "text/plain")

	data := []byte("part-data-for-sha")
	h := sha256.Sum256(data)
	expected := hex.EncodeToString(h[:])

	etag, err := s.UploadPart("b", "sha.txt", uploadID, 1, bytes.NewReader(data), expected)
	if err != nil {
		t.Fatalf("UploadPart with valid SHA256: %v", err)
	}
	if etag == "" {
		t.Fatal("etag should not be empty")
	}
}

func TestUploadPartSHA256Mismatch(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "sha.txt", "text/plain")

	data := []byte("real-data")
	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"

	_, err := s.UploadPart("b", "sha.txt", uploadID, 1, bytes.NewReader(data), wrongHash)
	if err == nil {
		t.Fatal("should fail with mismatched SHA256")
	}
	if err.Error() != ErrBadDigest.Error() {
		t.Errorf("expected ErrBadDigest, got: %v", err)
	}
}

func TestUploadPartSHA256EmptySkipsCheck(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	uploadID, _ := s.CreateMultipartUpload("b", "sha.txt", "text/plain")

	// Empty expectedSHA256 should skip verification
	etag, err := s.UploadPart("b", "sha.txt", uploadID, 1, bytes.NewReader([]byte("data")), "")
	if err != nil {
		t.Fatalf("UploadPart with empty SHA256: %v", err)
	}
	if etag == "" {
		t.Fatal("etag should not be empty")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 2: Multipart Upload Garbage Collection
// ═══════════════════════════════════════════════════════════════════════════════

func TestCleanAbandonedUploads(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Create a multipart upload and stage a part
	uploadID, _ := s.CreateMultipartUpload("b", "abandoned.txt", "text/plain")
	s.UploadPart("b", "abandoned.txt", uploadID, 1, strings.NewReader("data"), "")

	stagingDir := s.multipartStagingPath("b", uploadID)

	// Backdate the staging directory to 25 hours ago
	old := time.Now().Add(-25 * time.Hour)
	os.Chtimes(stagingDir, old, old)

	// Run GC with 24h max age
	cleanAbandonedUploads(s.dataDir, 24*time.Hour)

	// Staging dir should be gone
	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Fatal("abandoned staging dir should have been removed")
	}
}

func TestCleanAbandonedUploadsKeepsRecent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// Create a recent multipart upload
	uploadID, _ := s.CreateMultipartUpload("b", "recent.txt", "text/plain")
	s.UploadPart("b", "recent.txt", uploadID, 1, strings.NewReader("data"), "")

	stagingDir := s.multipartStagingPath("b", uploadID)

	// Run GC — this upload is fresh, should NOT be removed
	cleanAbandonedUploads(s.dataDir, 24*time.Hour)

	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		t.Fatal("recent staging dir should NOT have been removed")
	}
}

func TestCleanAbandonedUploadsNoBuckets(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	// No buckets — should not panic or error
	cleanAbandonedUploads(s.dataDir, 24*time.Hour)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 3: Directory Fsync on Rename (syncParentDir)
// ═══════════════════════════════════════════════════════════════════════════════

func TestSyncParentDirDoesNotPanic(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	// PutObject internally calls syncParentDir — just verify no panic
	_, err := s.PutObject("b", "sync-test.txt", strings.NewReader("data"), nil)
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// CompleteMultipartUpload also calls syncParentDir
	uploadID, _ := s.CreateMultipartUpload("b", "sync-multi.txt", "text/plain")
	etag, _ := s.UploadPart("b", "sync-multi.txt", uploadID, 1, strings.NewReader("data"), "")
	_, err = s.CompleteMultipartUpload("b", "sync-multi.txt", uploadID, []CompletedPart{
		{PartNumber: 1, ETag: etag},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
}

func TestSyncParentDirNonExistentPath(t *testing.T) {
	// Should not panic on invalid path
	syncParentDir("/nonexistent/path/file.txt")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fix 5: CopyObject Metadata Directive
// ═══════════════════════════════════════════════════════════════════════════════

func TestCopyObjectDefaultCopiesMetadata(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "src.txt", strings.NewReader("data"), &PutObjectInput{
		ContentType:    "text/html",
		CacheControl:   "max-age=3600",
		CustomMetadata: map[string]string{"author": "alice"},
	})

	// COPY directive (nil override) should preserve source metadata
	meta, err := s.CopyObject("b", "src.txt", "b", "dst.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ContentType != "text/html" {
		t.Errorf("content type: %q, want text/html", meta.ContentType)
	}
	if meta.CacheControl != "max-age=3600" {
		t.Errorf("cache-control: %q", meta.CacheControl)
	}

	dstMeta, _ := s.HeadObject("b", "dst.txt")
	if dstMeta.CustomMetadata["author"] != "alice" {
		t.Errorf("custom metadata missing: %v", dstMeta.CustomMetadata)
	}
}

func TestCopyObjectReplaceMetadata(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "src.txt", strings.NewReader("data"), &PutObjectInput{
		ContentType:    "text/html",
		CacheControl:   "max-age=3600",
		CustomMetadata: map[string]string{"author": "alice"},
	})

	// REPLACE directive — provide new metadata
	overrideMeta := &PutObjectInput{
		ContentType:    "application/json",
		CacheControl:   "no-cache",
		CustomMetadata: map[string]string{"version": "2"},
	}
	meta, err := s.CopyObject("b", "src.txt", "b", "dst.txt", overrideMeta)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ContentType != "application/json" {
		t.Errorf("content type: %q, want application/json", meta.ContentType)
	}
	if meta.CacheControl != "no-cache" {
		t.Errorf("cache-control: %q", meta.CacheControl)
	}

	dstMeta, _ := s.HeadObject("b", "dst.txt")
	if dstMeta.CustomMetadata["version"] != "2" {
		t.Errorf("custom metadata: %v", dstMeta.CustomMetadata)
	}
	if _, ok := dstMeta.CustomMetadata["author"]; ok {
		t.Error("source metadata should not be present with REPLACE directive")
	}
}

func TestCopyObjectReplaceEmptyContentType(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()
	s.CreateBucket("b")

	s.PutObject("b", "src.txt", strings.NewReader("data"), &PutObjectInput{
		ContentType: "text/html",
	})

	// REPLACE with empty ContentType should default to application/octet-stream
	overrideMeta := &PutObjectInput{}
	meta, _ := s.CopyObject("b", "src.txt", "b", "dst.txt", overrideMeta)
	if meta.ContentType != "application/octet-stream" {
		t.Errorf("content type: %q, want application/octet-stream", meta.ContentType)
	}
}
