package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemStorage(t *testing.T) {
	// Create temporary directory for tests
	tempDir := t.TempDir()
	storage := NewFilesystemStorage(tempDir)

	t.Run("CreateBucket", func(t *testing.T) {
		err := storage.CreateBucket("testbucket")
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		if !storage.BucketExists("testbucket") {
			t.Fatal("Bucket does not exist after creation")
		}
	})

	t.Run("PutObject", func(t *testing.T) {
		content := []byte("Hello, S3!")
		reader := bytes.NewReader(content)

		metadata, err := storage.PutObject("testbucket", "test.txt", reader)
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		if metadata.Size != int64(len(content)) {
			t.Errorf("Expected size %d, got %d", len(content), metadata.Size)
		}

		if metadata.ETag == "" {
			t.Error("ETag is empty")
		}
	})

	t.Run("GetObject", func(t *testing.T) {
		reader, metadata, err := storage.GetObject("testbucket", "test.txt")
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer reader.Close()

		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("Failed to read object: %v", err)
		}

		expected := "Hello, S3!"
		if string(content) != expected {
			t.Errorf("Expected content %q, got %q", expected, string(content))
		}

		if metadata.Size != int64(len(expected)) {
			t.Errorf("Expected size %d, got %d", len(expected), metadata.Size)
		}
	})

	t.Run("HeadObject", func(t *testing.T) {
		metadata, err := storage.HeadObject("testbucket", "test.txt")
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		if metadata.Size != 10 {
			t.Errorf("Expected size 10, got %d", metadata.Size)
		}
	})

	t.Run("ListObjects", func(t *testing.T) {
		// Add more objects
		storage.PutObject("testbucket", "dir1/file1.txt", bytes.NewReader([]byte("file1")))
		storage.PutObject("testbucket", "dir1/file2.txt", bytes.NewReader([]byte("file2")))
		storage.PutObject("testbucket", "dir2/file3.txt", bytes.NewReader([]byte("file3")))

		objects, err := storage.ListObjects("testbucket", "", 100)
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		if len(objects) < 4 {
			t.Errorf("Expected at least 4 objects, got %d", len(objects))
		}

		// Test prefix filtering
		objects, err = storage.ListObjects("testbucket", "dir1/", 100)
		if err != nil {
			t.Fatalf("ListObjects with prefix failed: %v", err)
		}

		if len(objects) != 2 {
			t.Errorf("Expected 2 objects with prefix 'dir1/', got %d", len(objects))
		}
	})

	t.Run("DeleteObject", func(t *testing.T) {
		err := storage.DeleteObject("testbucket", "test.txt")
		if err != nil {
			t.Fatalf("DeleteObject failed: %v", err)
		}

		_, _, err = storage.GetObject("testbucket", "test.txt")
		if err == nil {
			t.Error("Expected error when getting deleted object")
		}
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		// Create and delete empty bucket
		storage.CreateBucket("emptybucket")
		err := storage.DeleteBucket("emptybucket")
		if err != nil {
			t.Fatalf("DeleteBucket failed: %v", err)
		}

		if storage.BucketExists("emptybucket") {
			t.Error("Bucket still exists after deletion")
		}

		// Try to delete non-empty bucket
		err = storage.DeleteBucket("testbucket")
		if err == nil {
			t.Error("Expected error when deleting non-empty bucket")
		}
	})
}

func TestObjectPathMapping(t *testing.T) {
	tempDir := t.TempDir()
	storage := NewFilesystemStorage(tempDir)

	tests := []struct {
		bucket   string
		key      string
		expected string
	}{
		{"mybucket", "file.txt", filepath.Join(tempDir, "mybucket", "file.txt")},
		{"mybucket", "dir/file.txt", filepath.Join(tempDir, "mybucket", "dir", "file.txt")},
		{"mybucket", "a/b/c/file.txt", filepath.Join(tempDir, "mybucket", "a", "b", "c", "file.txt")},
	}

	for _, tt := range tests {
		result := storage.objectPath(tt.bucket, tt.key)
		if result != tt.expected {
			t.Errorf("objectPath(%q, %q) = %q, expected %q", tt.bucket, tt.key, result, tt.expected)
		}
	}
}

func TestMetadataPersistence(t *testing.T) {
	tempDir := t.TempDir()
	storage := NewFilesystemStorage(tempDir)

	storage.CreateBucket("testbucket")

	// Put object
	content := []byte("test content")
	metadata1, err := storage.PutObject("testbucket", "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify metadata file exists
	metadataPath := storage.metadataPath("testbucket", "test.txt")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Error("Metadata file was not created")
	}

	// Get object and verify metadata matches
	_, metadata2, err := storage.GetObject("testbucket", "test.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	if metadata1.ETag != metadata2.ETag {
		t.Errorf("ETag mismatch: %s != %s", metadata1.ETag, metadata2.ETag)
	}

	if metadata1.Size != metadata2.Size {
		t.Errorf("Size mismatch: %d != %d", metadata1.Size, metadata2.Size)
	}
}

func TestAuthenticatorNoOp(t *testing.T) {
	auth := &NoOpAuthenticator{}
	// Should always return nil
	if err := auth.Authenticate(nil); err != nil {
		t.Errorf("NoOpAuthenticator should return nil, got: %v", err)
	}
}

func BenchmarkPutObject(b *testing.B) {
	tempDir := b.TempDir()
	storage := NewFilesystemStorage(tempDir)
	storage.CreateBucket("benchmark")

	content := bytes.Repeat([]byte("a"), 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := filepath.Join("test", string(rune(i%26+97)), "file.txt")
		storage.PutObject("benchmark", key, bytes.NewReader(content))
	}
}

func BenchmarkGetObject(b *testing.B) {
	tempDir := b.TempDir()
	storage := NewFilesystemStorage(tempDir)
	storage.CreateBucket("benchmark")

	content := bytes.Repeat([]byte("a"), 1024) // 1KB
	storage.PutObject("benchmark", "test.txt", bytes.NewReader(content))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _, _ := storage.GetObject("benchmark", "test.txt")
		io.Copy(io.Discard, reader)
		reader.Close()
	}
}
