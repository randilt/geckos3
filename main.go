package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Config struct {
	DataDir     string
	ListenAddr  string
	AccessKey   string
	SecretKey   string
	AuthEnabled bool
}

func main() {
	config := &Config{}

	flag.StringVar(&config.DataDir, "data-dir", getEnv("S3LITE_DATA_DIR", "./data"), "Root directory for buckets")
	flag.StringVar(&config.ListenAddr, "listen", getEnv("S3LITE_LISTEN", ":9000"), "HTTP server address")
	flag.StringVar(&config.AccessKey, "access-key", getEnv("S3LITE_ACCESS_KEY", "minioadmin"), "AWS access key")
	flag.StringVar(&config.SecretKey, "secret-key", getEnv("S3LITE_SECRET_KEY", "minioadmin"), "AWS secret key")
	flag.BoolVar(&config.AuthEnabled, "auth", getEnv("S3LITE_AUTH_ENABLED", "true") == "true", "Enable authentication")
	flag.Parse()

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize storage layer
	storage := NewFilesystemStorage(config.DataDir)

	// Initialize auth layer
	var auth Authenticator
	if config.AuthEnabled {
		auth = NewSigV4Authenticator(config.AccessKey, config.SecretKey)
		if config.AccessKey == "minioadmin" || config.SecretKey == "minioadmin" {
			log.Println("WARNING: Using default credentials. Set S3LITE_ACCESS_KEY and S3LITE_SECRET_KEY for production use.")
		}
	} else {
		auth = &NoOpAuthenticator{}
		log.Println("WARNING: Authentication is disabled. All requests will be accepted.")
	}

	// Initialize handler
	handler := NewS3Handler(storage, auth)

	// Wrap with logging middleware
	loggedHandler := LoggingMiddleware(handler)

	server := &http.Server{
		Addr:              config.ListenAddr,
		Handler:           loggedHandler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Start server in goroutine for graceful shutdown support
	go func() {
		log.Printf("Starting S3-compatible server on %s (data-dir=%s, auth=%v)",
			config.ListenAddr, config.DataDir, config.AuthEnabled)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced shutdown: %v", err)
	}
	log.Println("Server stopped")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
