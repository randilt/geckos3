package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var requestCounter atomic.Int64

type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.written += int64(n)
	return n, err
}

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	RequestID string `json:"request_id"`
	Method    string `json:"method"`
	URI       string `json:"uri"`
	Status    int    `json:"status"`
	Duration  int64  `json:"duration_ms"`
	Bytes     int64  `json:"bytes,omitempty"`
	ClientIP  string `json:"client_ip"`
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Generate request ID
		reqID := fmt.Sprintf("geckos3-%d", requestCounter.Add(1))

		// Set request ID header on response
		w.Header().Set("x-amz-request-id", reqID)

		// Wrap response writer
		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		// Call next handler
		next.ServeHTTP(rw, r)

		// Log request
		duration := time.Since(start).Milliseconds()

		entry := LogEntry{
			Timestamp: start.UTC().Format(time.RFC3339),
			RequestID: reqID,
			Method:    r.Method,
			URI:       r.RequestURI,
			Status:    rw.statusCode,
			Duration:  duration,
			Bytes:     rw.written,
			ClientIP:  r.RemoteAddr,
		}

		// Write JSON log line to stdout
		data, _ := json.Marshal(entry)
		fmt.Fprintln(os.Stdout, string(data))
	})
}
