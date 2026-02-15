package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════════
// NoOp Authenticator
// ═══════════════════════════════════════════════════════════════════════════════

func TestNoOpAuthenticatorNilRequest(t *testing.T) {
	auth := &NoOpAuthenticator{}
	if err := auth.Authenticate(nil); err == nil {
		t.Fatal("NoOp should error on nil request")
	}
}

func TestNoOpAuthenticatorValidRequest(t *testing.T) {
	auth := &NoOpAuthenticator{}
	req := httptest.NewRequest("GET", "/", nil)
	if err := auth.Authenticate(req); err != nil {
		t.Fatalf("NoOp should pass valid requests: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// SigV4 Authenticator – Header Auth
// ═══════════════════════════════════════════════════════════════════════════════

func sigV4TestHelper(accessKey, secretKey, method, path string) *http.Request {
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	region := "us-east-1"
	service := "s3"

	req := httptest.NewRequest(method, path, nil)
	req.Host = "localhost:9000"
	req.Header.Set("Host", "localhost:9000")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	// Build canonical request
	canonicalURI := canonicalURI(req.URL.Path)
	canonicalQueryString := ""
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:%s\n",
		req.Host, amzDate)
	hashedPayload := "UNSIGNED-PAYLOAD"

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method, canonicalURI, canonicalQueryString, canonicalHeaders, signedHeaders, hashedPayload)

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	hashedCanonical := sha256Hex(canonicalRequest)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, credentialScope, hashedCanonical)

	// Calculate signature
	kDate := hmacSHA256Sign([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256Sign(kDate, []byte(region))
	kService := hmacSHA256Sign(kRegion, []byte(service))
	kSigning := hmacSHA256Sign(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256Sign(kSigning, []byte(stringToSign)))

	credential := fmt.Sprintf("%s/%s", accessKey, credentialScope)
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s, SignedHeaders=%s, Signature=%s",
		credential, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)

	return req
}

func hmacSHA256Sign(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func TestSigV4ValidSignature(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")
	req := sigV4TestHelper("testkey", "testsecret", "GET", "/mybucket")

	if err := auth.Authenticate(req); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestSigV4WrongAccessKey(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")
	req := sigV4TestHelper("wrongkey", "testsecret", "GET", "/mybucket")

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("wrong access key should fail")
	}
	if !strings.Contains(err.Error(), "Access Key Id") {
		t.Errorf("error message: %v", err)
	}
}

func TestSigV4WrongSecretKey(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")
	req := sigV4TestHelper("testkey", "wrongsecret", "GET", "/mybucket")

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("wrong secret key should fail")
	}
	if !strings.Contains(err.Error(), "signature") {
		t.Errorf("error message: %v", err)
	}
}

func TestSigV4MissingAuthHeader(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")
	req := httptest.NewRequest("GET", "/mybucket", nil)

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("missing auth should fail")
	}
	if !strings.Contains(err.Error(), "missing authorization") {
		t.Errorf("error message: %v", err)
	}
}

func TestSigV4ExpiredTimestamp(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")

	// Create a request with a timestamp 20 minutes in the past
	now := time.Now().UTC().Add(-20 * time.Minute)
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	region := "us-east-1"
	service := "s3"

	req := httptest.NewRequest("GET", "/mybucket", nil)
	req.Host = "localhost:9000"
	req.Header.Set("Host", "localhost:9000")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:%s\n",
		req.Host, amzDate)

	canonicalRequest := fmt.Sprintf("GET\n/mybucket\n\n%s\n%s\nUNSIGNED-PAYLOAD",
		canonicalHeaders, signedHeaders)
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate, credentialScope, sha256Hex(canonicalRequest))

	kDate := hmacSHA256Sign([]byte("AWS4testsecret"), []byte(dateStamp))
	kRegion := hmacSHA256Sign(kDate, []byte(region))
	kService := hmacSHA256Sign(kRegion, []byte(service))
	kSigning := hmacSHA256Sign(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256Sign(kSigning, []byte(stringToSign)))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=testkey/%s, SignedHeaders=%s, Signature=%s",
		credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("expired timestamp should fail")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Errorf("error message: %v", err)
	}
}

func TestSigV4UnsupportedScheme(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")
	req := httptest.NewRequest("GET", "/mybucket", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("unsupported scheme should fail")
	}
}

func TestSigV4DifferentMethods(t *testing.T) {
	auth := NewSigV4Authenticator("mykey", "mysecret")

	methods := []string{"GET", "PUT", "DELETE", "HEAD"}
	for _, method := range methods {
		req := sigV4TestHelper("mykey", "mysecret", method, "/mybucket")
		if err := auth.Authenticate(req); err != nil {
			t.Errorf("%s auth failed: %v", method, err)
		}
	}
}

func TestSigV4NestedPath(t *testing.T) {
	auth := NewSigV4Authenticator("mykey", "mysecret")
	req := sigV4TestHelper("mykey", "mysecret", "GET", "/mybucket/path/to/object.txt")

	if err := auth.Authenticate(req); err != nil {
		t.Fatalf("nested path auth failed: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// SigV4 Authenticator – Presigned URL Auth
// ═══════════════════════════════════════════════════════════════════════════════

func TestSigV4PresignedValid(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")

	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	region := "us-east-1"
	service := "s3"
	expires := "3600"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	credential := fmt.Sprintf("testkey/%s", credentialScope)
	signedHeaders := "host"

	// Build query string without signature
	qsWithoutSig := fmt.Sprintf("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%s&X-Amz-SignedHeaders=%s",
		uriEncode(credential), amzDate, expires, signedHeaders)

	path := "/mybucket/file.txt"

	req := httptest.NewRequest("GET", path+"?"+qsWithoutSig, nil)
	req.Host = "localhost:9000"
	req.Header.Set("Host", "localhost:9000")

	// Build canonical request for presigned
	canonURL := canonicalURI(path)
	canonHeaders := fmt.Sprintf("host:%s\n", req.Host)

	canonicalRequest := fmt.Sprintf("GET\n%s\n%s\n%s\n%s\nUNSIGNED-PAYLOAD",
		canonURL, qsWithoutSig, canonHeaders, signedHeaders)

	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate, credentialScope, sha256Hex(canonicalRequest))

	kDate := hmacSHA256Sign([]byte("AWS4testsecret"), []byte(dateStamp))
	kRegion := hmacSHA256Sign(kDate, []byte(region))
	kService := hmacSHA256Sign(kRegion, []byte(service))
	kSigning := hmacSHA256Sign(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256Sign(kSigning, []byte(stringToSign)))

	// Rebuild request with signature
	fullQS := qsWithoutSig + "&X-Amz-Signature=" + signature
	req = httptest.NewRequest("GET", path+"?"+fullQS, nil)
	req.Host = "localhost:9000"
	req.Header.Set("Host", "localhost:9000")

	if err := auth.Authenticate(req); err != nil {
		t.Fatalf("valid presigned URL rejected: %v", err)
	}
}

func TestSigV4PresignedExpired(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")

	// Use time 2 hours in the past with 1-second expiry
	then := time.Now().UTC().Add(-2 * time.Hour)
	dateStamp := then.Format("20060102")
	amzDate := then.Format("20060102T150405Z")
	region := "us-east-1"
	service := "s3"
	expires := "1" // 1 second
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	credential := fmt.Sprintf("testkey/%s", credentialScope)
	signedHeaders := "host"

	qsWithoutSig := fmt.Sprintf("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%s&X-Amz-SignedHeaders=%s",
		uriEncode(credential), amzDate, expires, signedHeaders)

	path := "/mybucket/file.txt"
	req := httptest.NewRequest("GET", path+"?"+qsWithoutSig+"&X-Amz-Signature=fakesig", nil)
	req.Host = "localhost:9000"
	req.Header.Set("Host", "localhost:9000")

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("expired presigned URL should fail")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("error: %v", err)
	}
}

func TestSigV4PresignedWrongKey(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")

	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credentialScope := fmt.Sprintf("%s/us-east-1/s3/aws4_request", dateStamp)
	credential := fmt.Sprintf("wrongkey/%s", credentialScope)

	qs := fmt.Sprintf("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=fakesig",
		uriEncode(credential), amzDate)

	req := httptest.NewRequest("GET", "/mybucket/file.txt?"+qs, nil)
	req.Host = "localhost:9000"

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("wrong access key should fail")
	}
}

func TestSigV4PresignedBadAlgorithm(t *testing.T) {
	auth := NewSigV4Authenticator("testkey", "testsecret")

	qs := "X-Amz-Algorithm=AWS4-HMAC-SHA512&X-Amz-Credential=testkey/20240101/us-east-1/s3/aws4_request&X-Amz-Date=20240101T000000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=fakesig"
	req := httptest.NewRequest("GET", "/mybucket/file.txt?"+qs, nil)
	req.Host = "localhost:9000"

	err := auth.Authenticate(req)
	if err == nil {
		t.Fatal("unsupported algorithm should fail")
	}
	if !strings.Contains(err.Error(), "unsupported algorithm") {
		t.Errorf("error: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Auth Integration with Handler
// ═══════════════════════════════════════════════════════════════════════════════

func TestAuthDeniedReturns403(t *testing.T) {
	dir := t.TempDir()
	storage := NewFilesystemStorage(dir)
	auth := NewSigV4Authenticator("testkey", "testsecret")
	handler := NewS3Handler(storage, auth)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Request without auth headers
	resp := mustDo(t, "PUT", server.URL+"/mybucket", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 403 {
		t.Errorf("expected 403, got %d (body: %s)", resp.StatusCode, body)
	}
	if !strings.Contains(body, "AccessDenied") {
		t.Errorf("expected AccessDenied: %s", body)
	}
}

func TestHealthBypassesAuth(t *testing.T) {
	dir := t.TempDir()
	storage := NewFilesystemStorage(dir)
	auth := NewSigV4Authenticator("testkey", "testsecret")
	handler := NewS3Handler(storage, auth)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Health check should work without auth
	resp := mustDo(t, "GET", server.URL+"/health", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != 200 || body != "OK" {
		t.Errorf("health check failed with auth enabled: %d %s", resp.StatusCode, body)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// URI Encoding Helpers
// ═══════════════════════════════════════════════════════════════════════════════

func TestUriEncode(t *testing.T) {
	cases := []struct{ in, out string }{
		{"hello", "hello"},
		{"hello world", "hello%20world"},
		{"a/b", "a%2Fb"},
		{"test@file", "test%40file"},
		{"100%", "100%25"},
	}
	for _, c := range cases {
		got := uriEncode(c.in)
		if got != c.out {
			t.Errorf("uriEncode(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestCanonicalURI(t *testing.T) {
	cases := []struct{ in, out string }{
		{"", "/"},
		{"/", "/"},
		{"/mybucket", "/mybucket"},
		{"/mybucket/key with spaces", "/mybucket/key%20with%20spaces"},
	}
	for _, c := range cases {
		got := canonicalURI(c.in)
		if got != c.out {
			t.Errorf("canonicalURI(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestCanonicalHeaderValue(t *testing.T) {
	cases := []struct{ in, out string }{
		{"simple", "simple"},
		{"  leading", "leading"},
		{"trailing  ", "trailing"},
		{"  multiple   spaces   between  ", "multiple spaces between"},
		{"\ttab\there\t", "tab here"},
	}
	for _, c := range cases {
		got := canonicalHeaderValue(c.in)
		if got != c.out {
			t.Errorf("canonicalHeaderValue(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}
