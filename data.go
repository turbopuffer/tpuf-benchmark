package main

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

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
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

// CohereVectorSource is a Source that generates vectors from the Cohere
// HuggingFace embeddings dataset. We use this to generate more realistic
// vectors for our benchmarks.
type CohereVectorSource struct {
	nextIdx int
	entries [][]float32
}

func NewCohereVectorSource() *CohereVectorSource {
	return &CohereVectorSource{}
}

func (csd *CohereVectorSource) Next(ctx context.Context) ([]float32, error) {
	for len(csd.entries) == 0 {
		if err := csd.loadNextFile(ctx); err != nil {
			return nil, fmt.Errorf("loading next file: %w", err)
		}
	}
	l := len(csd.entries) - 1
	e := csd.entries[l]
	csd.entries = csd.entries[:l]
	return e, nil
}

const cohereWikipediaEmbeddingFileCount = 415

func (cds *CohereVectorSource) loadNextFile(ctx context.Context) error {
	idx := cds.nextIdx
	if idx >= cohereWikipediaEmbeddingFileCount {
		return fmt.Errorf("no more files to load")
	}

	fileName := cohereEmbeddingFileName(idx)
	contents, err := cds.fetchDatasetFile(ctx, fileName)
	if err != nil {
		return fmt.Errorf("fetching dataset file: %w", err)
	}

	bf := buffer.NewBufferFileFromBytesNoAlloc(contents)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return fmt.Errorf("creating parquet reader: %w", err)
	}
	n := pr.GetNumRows()

	embeddings, _, _, err := pr.ReadColumnByIndex(
		4,
		n*1024,
	)
	if err != nil {
		return fmt.Errorf("reading embeddings: %w", err)
	}

	for i := int64(0); i < n; i++ {
		vector := make([]float32, 0, 1024)
		for j := int64(0); j < 1024; j++ {
			vector = append(vector, embeddings[i*1024+j].(float32))
		}
		cds.entries = append(cds.entries, vector)
	}

	cds.nextIdx++
	return nil
}

// cohereEmbeddingFileName generates the filename for a Cohere embedding file
// based on its index. Files are named as 4-digit zero-padded numbers, e.g.,
// "0000.parquet", "0001.parquet", etc.
func cohereEmbeddingFileName(idx int) string {
	return fmt.Sprintf("%04d.parquet", idx)
}

func (cds *CohereVectorSource) fetchDatasetFile(
	ctx context.Context,
	fileName string,
) ([]byte, error) {
	cacheFileName := filepath.Join(
		datasetCacheDir(),
		"tpuf-benchmark",
		"wikipedia-2023-11-embed-multilingual-v3-en",
		fileName,
	)
	if _, err := os.Stat(cacheFileName); err == nil {
		contents, err := os.ReadFile(cacheFileName)
		if err == nil {
			return contents, nil
		}
	}

	url := fmt.Sprintf(
		"https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3/resolve/main/en/%s?download=true",
		fileName,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching dataset file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("fetching dataset file, got status %d: %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(cacheFileName), 0755); err != nil {
		return nil, fmt.Errorf("creating cache directory: %w", err)
	}

	if err := os.WriteFile(cacheFileName, body, 0644); err != nil {
		return nil, fmt.Errorf("writing cache file: %w", err)
	}

	return body, nil
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
