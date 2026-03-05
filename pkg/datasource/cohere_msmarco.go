package datasource

import (
	"context"
	"fmt"
	"iter"
	"net/url"
	"path"
	"sync"
	"sync/atomic"
	"text/template"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

func CohereMSMarco(ctx context.Context, cfg Config) Source {
	cfg.ParseConcurrency = max(1, cfg.ParseConcurrency)
	return &cohereMSMarcoEmbeddings{dd: newDownloader(cfg)}
}

type cohereMSMarcoEmbeddings struct {
	dd *downloader
}

var _ Source = (*cohereMSMarcoEmbeddings)(nil)

func (c *cohereMSMarcoEmbeddings) FuncMap(ctx context.Context) template.FuncMap {
	nextVec := lazyPull2(func() iter.Seq2[[]float32, error] {
		return parsingAndDownloadingIterator(ctx, c.dd,
			cohereMSMarcoPassageURLs(), parseCohereMSMarcoVectors)
	})
	nextText := lazyPull2(func() iter.Seq2[string, error] {
		return parsingAndDownloadingIterator(ctx, c.dd,
			cohereMSMarcoPassageURLs(), parseCohereMSMarcoTexts)
	})
	// Queries are loaded lazily into a slice on first use, then cycled
	// through indefinitely using an atomic index.
	var (
		queriesOnce sync.Once
		queries     []string
		queriesErr  error
		queryIdx    atomic.Uint64
	)
	loadQueries := func() {
		const queryURL = "https://huggingface.co/datasets/Cohere/msmarco-v2.1-embed-english-v3/resolve/main/queries_parquet/queries.parquet?download=true"
		seq := parsingAndDownloadingIterator(ctx, c.dd,
			singletonSeq2("queries.parquet", queryURL), parseCohereMSMarcoQueries)
		for q, err := range seq {
			if err != nil {
				queriesErr = err
				return
			}
			queries = append(queries, q)
		}
		if len(queries) == 0 {
			queriesErr = fmt.Errorf("no queries found in dataset")
		}
	}

	return template.FuncMap{
		"text": func() string {
			text, err, ok := nextText()
			if !ok {
				panic("text source exhausted")
			} else if err != nil {
				panic(err)
			}
			return text
		},
		"vector": func(dims int) string {
			vec, err, ok := nextVec()
			if !ok {
				panic("vector source exhausted")
			} else if err != nil {
				panic(err)
			}
			return vectorToString(vec, dims)
		},
		"full_text_query": func() string {
			queriesOnce.Do(loadQueries)
			if queriesErr != nil {
				panic(queriesErr)
			}
			idx := queryIdx.Add(1) - 1
			return queries[idx%uint64(len(queries))]
		},
	}
}

// cohereMSMarcoPassageURLs returns a sequence of (name, url) pairs for the
// Cohere MSMarco v2.1 passage embeddings.
func cohereMSMarcoPassageURLs() iter.Seq2[string, string] {
	const fileCount = 60

	urlBase := url.URL{
		Scheme:   "https",
		Host:     "huggingface.co",
		Path:     "/datasets/Cohere/msmarco-v2.1-embed-english-v3/resolve/main/passages_parquet/",
		RawQuery: "download=true",
	}

	return func(yield func(string, string) bool) {
		for i := range fileCount {
			name := fmt.Sprintf("msmarco_v2.1_doc_segmented_%02d.parquet", i)
			u := urlBase
			u.Path = path.Join(u.Path, name)
			if !yield(name, u.String()) {
				return
			}
		}
	}
}

// parseCohereMSMarcoVectors parses the embedding column from an MSMarco v2.1
// passages parquet file. The "emb" column (index 7) contains 1024-dimensional
// float32 embeddings stored as a repeated/sequence field.
func parseCohereMSMarcoVectors(mmapped *MemoryMappedFile) (iter.Seq[[]float32], error) {
	const column int64 = 7
	const dims int64 = 1024

	bf := buffer.NewBufferFileFromBytesNoAlloc(mmapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	vectors, _, _, err := pr.ReadColumnByIndex(column, n*dims)
	if err != nil {
		return nil, fmt.Errorf("failed to read column %d: %w", column, err)
	}
	return func(yield func([]float32) bool) {
		for i := range n {
			vector := make([]float32, dims)
			for j := range dims {
				vector[j] = vectors[i*dims+j].(float32)
			}
			if !yield(vector) {
				break
			}
		}
	}, nil
}

// parseCohereMSMarcoTexts parses the "segment" column (index 4) from an
// MSMarco v2.1 passages parquet file, yielding the text of each passage segment.
func parseCohereMSMarcoTexts(mmapped *MemoryMappedFile) (iter.Seq[string], error) {
	const column int64 = 4

	bf := buffer.NewBufferFileFromBytesNoAlloc(mmapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	texts, _, _, err := pr.ReadColumnByIndex(column, n)
	if err != nil {
		return nil, fmt.Errorf("failed to read column %d: %w", column, err)
	}
	return func(yield func(string) bool) {
		for i := range n {
			if !yield(texts[i].(string)) {
				break
			}
		}
	}, nil
}

func parseCohereMSMarcoQueries(mmapped *MemoryMappedFile) (iter.Seq[string], error) {
	const column int64 = 1

	bf := buffer.NewBufferFileFromBytesNoAlloc(mmapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	queries, _, _, err := pr.ReadColumnByIndex(column, n)
	if err != nil {
		return nil, fmt.Errorf("failed to read column %d: %w", column, err)
	}
	return func(yield func(string) bool) {
		for i := range n {
			if !yield(queries[i].(string)) {
				break
			}
		}
	}, nil
}

func singletonSeq2[T, U any](x T, y U) iter.Seq2[T, U] {
	return func(yield func(T, U) bool) {
		if !yield(x, y) {
			return
		}
	}
}
