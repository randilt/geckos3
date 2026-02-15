package main

import "net/http"

// CORSMiddleware adds permissive CORS headers to every response and handles
// OPTIONS preflight requests. This allows browser-based S3 clients (e.g.
// presigned URL uploads, JavaScript SDKs) to interact with geckos3 directly.
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}

		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers",
			"Authorization, Content-Type, Content-Length, X-Amz-Content-Sha256, "+
				"X-Amz-Date, X-Amz-Security-Token, X-Amz-User-Agent, "+
				"x-amz-acl, x-amz-meta-*")
		w.Header().Set("Access-Control-Expose-Headers",
			"ETag, x-amz-request-id, x-amz-meta-*")
		w.Header().Set("Access-Control-Max-Age", "3600")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
