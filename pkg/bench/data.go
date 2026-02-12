package bench

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/turbopuffer/tpuf-benchmark/pkg/template"
)

// Source is an interface for a data source that can generate T values.
type Source[T any] interface {
	Next(context.Context) (T, error)
}

// Generator is a trivial implementation of a Source that generates T values
// based on some generator function.
type Generator[T any] struct {
	genFn func(context.Context) (T, error)
}

func (g *Generator[T]) Next(ctx context.Context) (T, error) {
	return g.genFn(ctx)
}

// RandomVectorSource returns a Source that generates random vectors of
// float32 values, with the given number of dimensions.
func RandomVectorSource(dims int) Source[[]float32] {
	gfn := func(_ context.Context) ([]float32, error) {
		vec := make([]float32, dims)
		for i := 0; i < dims; i++ {
			vec[i] = rand.Float32()
		}
		return vec, nil
	}
	return &Generator[[]float32]{genFn: gfn}
}

// fixedDimVectorSource consumes from a template.VectorSource, producing vectors
// of a fixed dimension. It implements the Source[[]float32] interface.
type fixedDimVectorSource struct {
	vecSrc template.VectorSource
	dims   int
}

// Assert that *fixedDimVectorSource implements the Source[[]float32] interface.
var _ Source[[]float32] = (*fixedDimVectorSource)(nil)

// Next implements the Source[[]float32] interface.
func (a *fixedDimVectorSource) Next(context.Context) ([]float32, error) {
	return a.vecSrc.Vector(a.dims)
}

type MSMarcoSource struct {
	queriesJSONL *bufio.Scanner
	corpusJSONL  *bufio.Scanner
}

func (msm *MSMarcoSource) NextQuery(ctx context.Context) (string, error) {
	if msm.queriesJSONL == nil {
		path, err := msm.fetchAndDecompressFile(ctx, "queries.jsonl.gz")
		if err != nil {
			return "", fmt.Errorf("fetching queries file: %w", err)
		}
		f, err := os.Open(path)
		if err != nil {
			return "", fmt.Errorf("opening queries file: %w", err)
		}
		msm.queriesJSONL = bufio.NewScanner(f)
	}

	// Read a line of JSONL from the file, and extract the `query` field.
	var query struct {
		Text string `json:"text"`
	}
	if !msm.queriesJSONL.Scan() {
		return "", fmt.Errorf("no more queries")
	}
	if err := json.Unmarshal(msm.queriesJSONL.Bytes(), &query); err != nil {
		return "", fmt.Errorf("decoding query: %w", err)
	}
	return cleanText(query.Text), nil
}

func (msm *MSMarcoSource) NextDocument(ctx context.Context) (string, error) {
	if msm.corpusJSONL == nil {
		path, err := msm.fetchAndDecompressFile(ctx, "corpus.jsonl.gz")
		if err != nil {
			return "", fmt.Errorf("fetching corpus file: %w", err)
		}
		f, err := os.Open(path)
		if err != nil {
			return "", fmt.Errorf("opening corpus file: %w", err)
		}
		msm.corpusJSONL = bufio.NewScanner(f)
	}

	// Read a line of JSONL from the file, and extract the `text` field.
	var doc struct {
		Text string `json:"text"`
	}
	if !msm.corpusJSONL.Scan() {
		return "", fmt.Errorf("no more documents")
	}
	if err := json.Unmarshal(msm.corpusJSONL.Bytes(), &doc); err != nil {
		return "", fmt.Errorf("decoding document: %w", err)
	}

	return cleanText(doc.Text), nil
}

func cleanText(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)    // Double up backslashes
	s = strings.ReplaceAll(s, "\u0000", "") // Remove null bytes
	s = strings.ReplaceAll(s, "\r", `\r`)   // Escape carriage returns
	s = strings.ReplaceAll(s, "\n", `\n`)   // Escape newlines
	s = strings.ReplaceAll(s, "\t", `\t`)   // Escape tabs
	s = strings.ReplaceAll(s, `"`, `\"`)    // Escape quotes
	return s
}

// Returns the path to the `.jsonl` file on disk.
func (msm *MSMarcoSource) fetchAndDecompressFile(ctx context.Context, name string) (string, error) {
	dst := filepath.Join(datasetCacheDir(), "tpuf-benchmark", "msmarco", name)
	if _, err := os.Stat(dst); err == nil {
		return dst, nil
	}

	url := fmt.Sprintf(
		"https://huggingface.co/datasets/BeIR/msmarco/resolve/main/%s?download=true",
		name,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("new request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching dataset file: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetching dataset file, got status %d", resp.StatusCode)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", fmt.Errorf("creating gzip reader: %w", err)
	}
	defer gz.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return "", fmt.Errorf("creating cache directory: %w", err)
	}

	f, err := os.Create(dst)
	if err != nil {
		return "", fmt.Errorf("creating cache file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, gz); err != nil {
		return "", fmt.Errorf("writing cache file: %w", err)
	}

	return dst, nil
}

func datasetCacheDir() string {
	dir := os.Getenv("DATASET_CACHE_DIR")
	if dir != "" {
		return dir
	}
	return os.TempDir()
}
