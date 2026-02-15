package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type Config struct {
	DataDir      string
	ListenAddr   string
	AccessKey    string
	SecretKey    string
	AuthEnabled  bool
	FsyncEnabled bool
}

func main() {
	var showVersion bool
	config := &Config{}

	flag.BoolVar(&showVersion, "version", false, "Show version information")
	flag.StringVar(&config.DataDir, "data-dir", getEnv("GECKOS3_DATA_DIR", "./data"), "Root directory for buckets")
	flag.StringVar(&config.ListenAddr, "listen", getEnv("GECKOS3_LISTEN", ":9000"), "HTTP server address")
	flag.StringVar(&config.AccessKey, "access-key", getEnv("GECKOS3_ACCESS_KEY", "geckoadmin"), "AWS access key")
	flag.StringVar(&config.SecretKey, "secret-key", getEnv("GECKOS3_SECRET_KEY", "geckoadmin"), "AWS secret key")
	flag.BoolVar(&config.AuthEnabled, "auth", parseBoolEnv("GECKOS3_AUTH_ENABLED", true), "Enable authentication")
	flag.BoolVar(&config.FsyncEnabled, "fsync", parseBoolEnv("GECKOS3_FSYNC", false), "Fsync files and directories after writes (slower, stronger durability)")
	flag.Parse()

	if showVersion {
		fmt.Printf("geckos3 %s\n", version)
		fmt.Printf("  commit: %s\n", commit)
		fmt.Printf("  built:  %s\n", date)
		os.Exit(0)
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize storage layer
	storage := NewFilesystemStorage(config.DataDir)
	if config.FsyncEnabled {
		storage.SetFsync(true)
		log.Println("Fsync enabled: per-object durability mode (slower writes)")
	}

	// Initialize auth layer
	var auth Authenticator
	if config.AuthEnabled {
		auth = NewSigV4Authenticator(config.AccessKey, config.SecretKey)
		if config.AccessKey == "geckoadmin" || config.SecretKey == "geckoadmin" {
			log.Println("WARNING: Using default credentials. Set GECKOS3_ACCESS_KEY and GECKOS3_SECRET_KEY for production use.")
		}
	} else {
		auth = &NoOpAuthenticator{}
		log.Println("WARNING: Authentication is disabled. All requests will be accepted.")
	}

	// Initialize handler
	handler := NewS3Handler(storage, auth)

	// Wrap with CORS, logging middleware and concurrency limit
	loggedHandler := CORSMiddleware(LoggingMiddleware(MaxClientsMiddleware(1024)(handler)))

	// Start background garbage collection for abandoned multipart uploads.
	startMultipartGC(config.DataDir, 1*time.Hour, 24*time.Hour)

	server := &http.Server{
		Addr:              config.ListenAddr,
		Handler:           loggedHandler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       6 * time.Hour,
		WriteTimeout:      6 * time.Hour,
		IdleTimeout:       120 * time.Second,
	}

	// Start server in goroutine for graceful shutdown support
	go func() {
		log.Printf("Starting geckos3 %s on %s (data-dir=%s, auth=%v)",
			version, config.ListenAddr, config.DataDir, config.AuthEnabled)
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

// parseBoolEnv reads an environment variable and parses it with strconv.ParseBool.
// Returns defaultVal if the variable is empty or unparseable.
func parseBoolEnv(key string, defaultVal bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return defaultVal
	}
	return b
}

// startMultipartGC launches a background goroutine that periodically removes
// abandoned multipart upload staging directories older than maxAge.
func startMultipartGC(dataDir string, interval, maxAge time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			cleanAbandonedUploads(dataDir, maxAge)
		}
	}()
}

func cleanAbandonedUploads(dataDir string, maxAge time.Duration) {
	buckets, err := os.ReadDir(dataDir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-maxAge)
	for _, b := range buckets {
		if !b.IsDir() {
			continue
		}
		mpDir := filepath.Join(dataDir, b.Name(), multipartStagingDir)
		uploads, err := os.ReadDir(mpDir)
		if err != nil {
			continue
		}
		for _, u := range uploads {
			info, err := u.Info()
			if err != nil {
				continue
			}
			if info.ModTime().Before(cutoff) {
				os.RemoveAll(filepath.Join(mpDir, u.Name()))
			}
		}
	}
}
