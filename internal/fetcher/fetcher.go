// Package fetcher provides an HTTP client with retry and timeout support.
package fetcher

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"
)

// maxResponseBytes caps the response body size to 50 MiB, preventing OOM
// if the target server returns an unexpectedly large or infinite response.
const maxResponseBytes = 50 << 20 // 50 MiB

// Fetcher wraps an HTTP client with configurable retry/backoff logic.
type Fetcher struct {
	client  *http.Client
	retries int
}

// New creates a Fetcher with the given timeout and max retry count.
func New(timeout time.Duration, retries int) *Fetcher {
	return &Fetcher{
		client:  &http.Client{Timeout: timeout},
		retries: retries,
	}
}

// Fetch GETs the given URL with exponential backoff retries.
// The caller is responsible for closing the returned ReadCloser.
func (f *Fetcher) Fetch(url string) (io.ReadCloser, error) {
	var lastErr error

	for attempt := 0; attempt <= f.retries; attempt++ {
		if attempt > 0 {
			wait := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			slog.Warn("fetch retry", "attempt", attempt, "of", f.retries, "wait", wait, "err", lastErr)
			time.Sleep(wait)
		}

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		// Set a real browser User-Agent so sites like Wikipedia don't block us.
		req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; html-kafka-pipeline/1.0)")
		req.Header.Set("Accept", "text/html,application/xhtml+xml")

		resp, err := f.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d %s", resp.StatusCode, resp.Status)
			continue
		}
		// Wrap in a limitReadCloser so the response is size-capped AND
		// the underlying TCP connection is still released when the caller closes it.
		return &limitReadCloser{
			Reader: io.LimitReader(resp.Body, maxResponseBytes),
			Closer: resp.Body,
		}, nil
	}

	return nil, fmt.Errorf("fetch failed after %d attempts: %w", f.retries+1, lastErr)
}

// limitReadCloser pairs a size-limited Reader with the original body's Closer
// so that closing the returned value properly releases the HTTP connection.
type limitReadCloser struct {
	io.Reader
	io.Closer
}
