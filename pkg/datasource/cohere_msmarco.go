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

// cohereMSMarcoQuery pairs an MSMarco query string with its embedding so hybrid
// templates can use both in a single multi-query request.
type cohereMSMarcoQuery struct {
	text string
	vec  []float32
}

func (q cohereMSMarcoQuery) Text() string {
	return q.text
}

func (q cohereMSMarcoQuery) Vector(dims int) string {
	return vectorToString(q.vec, dims)
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
		queries     []cohereMSMarcoQuery
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
		"query": func() cohereMSMarcoQuery {
			queriesOnce.Do(loadQueries)
			if queriesErr != nil {
				panic(queriesErr)
			}
			idx := queryIdx.Add(1) - 1
			return queries[idx%uint64(len(queries))]
		},
		"full_text_query": func() string {
			queriesOnce.Do(loadQueries)
			if queriesErr != nil {
				panic(queriesErr)
			}
			idx := queryIdx.Add(1) - 1
			return queries[idx%uint64(len(queries))].text
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
	// Read in chunks so each chunk's []interface{} allocation can be GC'd after
	// it's yielded through, rather than holding the whole file in memory.
	const chunkRows int64 = 1024

	bf := buffer.NewBufferFileFromBytesNoAlloc(mmapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	return func(yield func([]float32) bool) {
		for rem := n; rem > 0; {
			batch := min(chunkRows, rem)
			chunk, _, _, err := pr.ReadColumnByIndex(column, batch)
			if err != nil {
				panic(fmt.Errorf("reading parquet column %d: %w", column, err))
			}
			if int64(len(chunk)) != batch*dims {
				panic(fmt.Errorf("reading parquet column %d: expected %d values, got %d (%d remaining)", column, batch*dims, len(chunk), rem))
			}
			for i := range batch {
				vector := make([]float32, dims)
				for j := range dims {
					vector[j] = chunk[i*dims+j].(float32)
				}
				if !yield(vector) {
					return
				}
			}
			rem -= batch
			// chunk goes out of scope here; GC can reclaim it before the next
			// batch.
		}
	}, nil
}

// parseCohereMSMarcoTexts parses the "segment" column (index 4) from an
// MSMarco v2.1 passages parquet file, yielding the text of each passage segment.
func parseCohereMSMarcoTexts(mmapped *MemoryMappedFile) (iter.Seq[string], error) {
	const column int64 = 4
	const chunkRows int64 = 1024

	bf := buffer.NewBufferFileFromBytesNoAlloc(mmapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	return func(yield func(string) bool) {
		for rem := n; rem > 0; {
			batch := min(chunkRows, rem)
			chunk, _, _, err := pr.ReadColumnByIndex(column, batch)
			if err != nil {
				panic(fmt.Errorf("reading parquet column %d: %w", column, err))
			}
			if int64(len(chunk)) != batch {
				panic(fmt.Errorf("reading parquet column %d: expected %d rows, got %d (%d remaining)", column, batch, len(chunk), rem))
			}
			for i := range batch {
				if !yield(chunk[i].(string)) {
					return
				}
			}
			rem -= batch
		}
	}, nil
}

func parseCohereMSMarcoQueries(mmapped *MemoryMappedFile) (iter.Seq[cohereMSMarcoQuery], error) {
	const textColumn int64 = 1
	const embColumn int64 = 3
	const dims int64 = 1024
	const chunkRows int64 = 1024

	bf := buffer.NewBufferFileFromBytesNoAlloc(mmapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	return func(yield func(cohereMSMarcoQuery) bool) {
		for rem := n; rem > 0; {
			batch := min(chunkRows, rem)
			texts, _, _, err := pr.ReadColumnByIndex(textColumn, batch)
			if err != nil {
				panic(fmt.Errorf("reading parquet column %d: %w", textColumn, err))
			}
			embeddings, _, _, err := pr.ReadColumnByIndex(embColumn, batch)
			if err != nil {
				panic(fmt.Errorf("reading parquet column %d: %w", embColumn, err))
			}
			if int64(len(texts)) != batch {
				panic(fmt.Errorf("reading parquet column %d: expected %d rows, got %d (%d remaining)", textColumn, batch, len(texts), rem))
			}
			if int64(len(embeddings)) != batch*dims {
				panic(fmt.Errorf("reading parquet column %d: expected %d values, got %d (%d remaining)", embColumn, batch*dims, len(embeddings), rem))
			}
			for i := range batch {
				vec := make([]float32, dims)
				for j := range dims {
					vec[j] = embeddings[i*dims+j].(float32)
				}
				if !yield(cohereMSMarcoQuery{
					text: texts[i].(string),
					vec:  vec,
				}) {
					return
				}
			}
			rem -= batch
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
