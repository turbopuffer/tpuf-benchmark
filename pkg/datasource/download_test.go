package datasource

import (
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestDownloader(t *testing.T) {
	// Spin up a local HTTP server that serves a small JSON document at
	// /doc/<key>.
	type doc struct {
		Key   string `json:"key"`
		Value int    `json:"value"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/doc/"):]
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(doc{Key: key, Value: len(key)})
	}))
	defer srv.Close()

	const numDocs = 100
	cacheDir := t.TempDir()
	dl := newDownloader(Config{CacheDir: cacheDir})

	// Build an iterator of (key, url) pairs.
	var fileURLs iter.Seq2[string, string] = func(yield func(string, string) bool) {
		for i := range numDocs {
			key := fmt.Sprintf("doc-%03d", i)
			if !yield(key, srv.URL+"/doc/"+key) {
				return
			}
		}
	}

	// Download and verify all documents.
	var count int
	for res := range dl.Download(t.Context(), fileURLs, 1, Hooks{}) {
		if res.Err != nil {
			t.Fatalf("download %s: %v", res.Key, res.Err)
		}
		count++
		data, err := os.ReadFile(res.LocalPath)
		if err != nil {
			t.Fatalf("read %s: %v", res.LocalPath, err)
		}
		var d doc
		if err := json.Unmarshal(data, &d); err != nil {
			t.Fatalf("unmarshal %s: %v", res.Key, err)
		}
		if d.Key != res.Key {
			t.Errorf("expected key %s, got %s", res.Key, d.Key)
		}
		if d.Value != len(res.Key) {
			t.Errorf("expected value %d for key %s, got %d", len(res.Key), res.Key, d.Value)
		}
	}
	if count != numDocs {
		t.Fatalf("expected %d results, got %d", numDocs, count)
	}
}
