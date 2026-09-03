package datasource

import (
	"context"
	"fmt"
	"iter"
	"net/url"
	"path"
	"text/template"
)

func CohereWikipediaEmbeddings(ctx context.Context, cfg Config) Source {
	cfg.ParseConcurrency = max(1, cfg.ParseConcurrency)
	return &cohereWikipediaEmbeddings{dd: newDownloader(cfg)}
}

type cohereWikipediaEmbeddings struct {
	dd *downloader
}

var _ Source = (*cohereWikipediaEmbeddings)(nil)

func (c *cohereWikipediaEmbeddings) FuncMap(ctx context.Context) (template.FuncMap, func()) {
	// Use lazyPull2 so the download+parse pipeline is not started until the
	// first call to the template function. This keeps Init cheap and avoids
	// eagerly loading data that may not be needed (e.g. during sanity checks).
	nextVec, stopVec := lazyPull2(func() iter.Seq2[[]float32, error] {
		return parsingAndDownloadingIterator(ctx, c.dd,
			cohereWikipediaEmbeddingURLs(), parseCohereWikipediaEmbeddingsVectors)
	})
	nextText, stopText := lazyPull2(func() iter.Seq2[string, error] {
		return parsingAndDownloadingIterator(ctx, c.dd,
			cohereWikipediaEmbeddingURLs(), parseCohereWikipediaEmbeddingsTexts)
	})

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
	}, func() { stopVec(); stopText() }
}

type cohereLanguageDataset struct {
	language string
	count    int
}

var cohereLanguageDatasets = []cohereLanguageDataset{
	{language: "en", count: 415}, // 41,488,110 docs
	{language: "de", count: 208}, // 20,772,081 docs
	{language: "fr", count: 179}, // 17,813,768 docs
	{language: "ru", count: 138}, // 13,734,543 docs
	{language: "es", count: 130}, // 12,905,284 docs
	{language: "it", count: 105}, // 10,462,162 docs
	{language: "ceb", count: 99}, // 9,818,657 docs
}

// cohereWikipediaEmbeddingURLs returns a sequence of (name, url) pairs for the
// Cohere Wikipedia embeddings.
func cohereWikipediaEmbeddingURLs() iter.Seq2[string, string] {

	// embeddingFileName generates the filename for a Cohere embedding file
	// based on its index. Files are named as 4-digit zero-padded numbers, e.g.,
	// "0000.parquet", "0001.parquet", etc.
	embeddingFileName := func(idx int) string {
		return fmt.Sprintf("%04d.parquet", idx)
	}
	cohereURLBase := url.URL{
		Scheme:   "https",
		Host:     "huggingface.co",
		Path:     "/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3/resolve/main/",
		RawQuery: "download=true",
	}

	return func(yield func(string, string) bool) {
		for _, dataset := range cohereLanguageDatasets {
			for i := range dataset.count {
				u := cohereURLBase
				name := embeddingFileName(i)
				u.Path = path.Join(u.Path, dataset.language, name)
				if !yield(name, u.String()) {
					return
				}
			}
		}
	}
}

func parseCohereWikipediaEmbeddingsVectors(mmapped *MemoryMappedFile) (iter.Seq[[]float32], error) {
	const column = 4
	const dims = 1024

	f, err := openParquetFile(mmapped)
	if err != nil {
		return nil, err
	}
	return f.float32Vectors(column, dims)
}

func parseCohereWikipediaEmbeddingsTexts(mmapped *MemoryMappedFile) (iter.Seq[string], error) {
	const column = 3

	f, err := openParquetFile(mmapped)
	if err != nil {
		return nil, err
	}
	return f.strings(column)
}
