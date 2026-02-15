package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Authenticator interface {
	Authenticate(r *http.Request) error
}

type SigV4Authenticator struct {
	accessKey string
	secretKey string
}

type NoOpAuthenticator struct{}

func NewSigV4Authenticator(accessKey, secretKey string) *SigV4Authenticator {
	return &SigV4Authenticator{
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

func (a *NoOpAuthenticator) Authenticate(r *http.Request) error {
	return nil
}

func (a *SigV4Authenticator) Authenticate(r *http.Request) error {
	// Check for presigned URL
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return a.authenticatePresigned(r)
	}

	// Check for Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return fmt.Errorf("missing authorization")
	}

	return a.authenticateHeader(r, authHeader)
}

func (a *SigV4Authenticator) authenticatePresigned(r *http.Request) error {
	query := r.URL.Query()
	
	algorithm := query.Get("X-Amz-Algorithm")
	credential := query.Get("X-Amz-Credential")
	signedHeaders := query.Get("X-Amz-SignedHeaders")
	signature := query.Get("X-Amz-Signature")
	date := query.Get("X-Amz-Date")
	expires := query.Get("X-Amz-Expires")

	if algorithm != "AWS4-HMAC-SHA256" {
		return fmt.Errorf("unsupported algorithm")
	}

	// Parse credential
	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 || credParts[0] != a.accessKey {
		return fmt.Errorf("invalid credentials")
	}

	dateStamp := credParts[1]
	region := credParts[2]
	service := credParts[3]

	// Check expiration
	if expires != "" {
		reqTime, err := time.Parse("20060102T150405Z", date)
		if err != nil {
			return fmt.Errorf("invalid date format")
		}
		if time.Since(reqTime) > time.Hour*24 {
			return fmt.Errorf("request expired")
		}
	}

	// Calculate expected signature
	canonicalRequest := a.buildCanonicalRequestPresigned(r, signedHeaders)
	stringToSign := a.buildStringToSign(date, dateStamp, region, service, canonicalRequest)
	expectedSignature := a.calculateSignature(a.secretKey, dateStamp, region, service, stringToSign)

	if signature != expectedSignature {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

func (a *SigV4Authenticator) authenticateHeader(r *http.Request, authHeader string) error {
	// Parse Authorization header
	// Format: AWS4-HMAC-SHA256 Credential=ACCESS/DATE/REGION/SERVICE/aws4_request, SignedHeaders=..., Signature=...
	
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return fmt.Errorf("unsupported authorization scheme")
	}

	parts := strings.Split(authHeader[17:], ", ")
	authMap := make(map[string]string)
	
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			authMap[kv[0]] = kv[1]
		}
	}

	credential := authMap["Credential"]
	signedHeaders := authMap["SignedHeaders"]
	signature := authMap["Signature"]

	// Parse credential
	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 || credParts[0] != a.accessKey {
		return fmt.Errorf("invalid credentials")
	}

	dateStamp := credParts[1]
	region := credParts[2]
	service := credParts[3]

	// Get date from headers
	date := r.Header.Get("X-Amz-Date")
	if date == "" {
		date = r.Header.Get("Date")
	}

	// Calculate expected signature
	canonicalRequest := a.buildCanonicalRequest(r, signedHeaders)
	stringToSign := a.buildStringToSign(date, dateStamp, region, service, canonicalRequest)
	expectedSignature := a.calculateSignature(a.secretKey, dateStamp, region, service, stringToSign)

	if signature != expectedSignature {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

func (a *SigV4Authenticator) buildCanonicalRequest(r *http.Request, signedHeaders string) string {
	// HTTPMethod + '\n' + CanonicalURI + '\n' + CanonicalQueryString + '\n' + CanonicalHeaders + '\n' + SignedHeaders + '\n' + HashedPayload

	method := r.Method
	uri := r.URL.Path
	if uri == "" {
		uri = "/"
	}

	// Canonical query string
	queryString := a.buildCanonicalQueryString(r.URL.Query(), false)

	// Canonical headers
	headers := strings.Split(signedHeaders, ";")
	var canonicalHeaders strings.Builder
	for _, h := range headers {
		value := r.Header.Get(h)
		if value == "" && strings.ToLower(h) == "host" {
			value = r.Host
		}
		canonicalHeaders.WriteString(strings.ToLower(h))
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(strings.TrimSpace(value))
		canonicalHeaders.WriteString("\n")
	}

	// Hashed payload
	hashedPayload := r.Header.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		hashedPayload = "UNSIGNED-PAYLOAD"
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method, uri, queryString, canonicalHeaders.String(), signedHeaders, hashedPayload)
}

func (a *SigV4Authenticator) buildCanonicalRequestPresigned(r *http.Request, signedHeaders string) string {
	method := r.Method
	uri := r.URL.Path
	if uri == "" {
		uri = "/"
	}

	// Canonical query string (exclude signature)
	queryString := a.buildCanonicalQueryString(r.URL.Query(), true)

	// Canonical headers
	headers := strings.Split(signedHeaders, ";")
	var canonicalHeaders strings.Builder
	for _, h := range headers {
		value := r.Header.Get(h)
		if value == "" && h == "host" {
			value = r.Host
		}
		canonicalHeaders.WriteString(strings.ToLower(h))
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(strings.TrimSpace(value))
		canonicalHeaders.WriteString("\n")
	}

	hashedPayload := "UNSIGNED-PAYLOAD"

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method, uri, queryString, canonicalHeaders.String(), signedHeaders, hashedPayload)
}

func (a *SigV4Authenticator) buildCanonicalQueryString(query url.Values, excludeSignature bool) string {
	keys := make([]string, 0, len(query))
	for k := range query {
		if excludeSignature && k == "X-Amz-Signature" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		for _, v := range query[k] {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}

	return strings.Join(parts, "&")
}

func (a *SigV4Authenticator) buildStringToSign(date, dateStamp, region, service, canonicalRequest string) string {
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	hashedCanonicalRequest := sha256Hash(canonicalRequest)

	return fmt.Sprintf("%s\n%s\n%s\n%s",
		algorithm, date, credentialScope, hashedCanonicalRequest)
}

func (a *SigV4Authenticator) calculateSignature(secretKey, dateStamp, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hmacSHA256(kSigning, []byte(stringToSign))

	return hex.EncodeToString(signature)
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hash(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

// uriEncode encodes a string per AWS SigV4 rules (spaces as %20, not +).
func uriEncode(s string) string {
	encoded := url.QueryEscape(s)
	return strings.ReplaceAll(encoded, "+", "%20")
}