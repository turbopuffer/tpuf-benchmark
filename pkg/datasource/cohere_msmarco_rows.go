package datasource

import (
	"context"
	"fmt"
	"iter"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

type MSMarcoRow struct {
	Vector []float32
	Text   string
}

// CohereMSMarcoRows streams passage rows in dataset order.
func CohereMSMarcoRows(ctx context.Context, cfg Config) iter.Seq2[MSMarcoRow, error] {
	cfg.ParseConcurrency = 1
	return parsingAndDownloadingIterator(ctx, newDownloader(cfg), cohereMSMarcoPassageURLs(), parseCohereMSMarcoRows)
}

type MSMarcoQueryRow struct {
	Text   string
	Vector []float32
}

// CohereMSMarcoQueryRows loads all MSMarco queries.
func CohereMSMarcoQueryRows(ctx context.Context, cfg Config) ([]MSMarcoQueryRow, error) {
	cfg.ParseConcurrency = max(1, cfg.ParseConcurrency)
	const queryURL = "https://huggingface.co/datasets/Cohere/msmarco-v2.1-embed-english-v3/resolve/main/queries_parquet/queries.parquet?download=true"
	seq := parsingAndDownloadingIterator(ctx, newDownloader(cfg), singletonSeq2("queries.parquet", queryURL), parseCohereMSMarcoQueryRows)
	var rows []MSMarcoQueryRow
	for row, err := range seq {
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no queries found in dataset")
	}
	return rows, nil
}

func parseCohereMSMarcoRows(mmapped *MemoryMappedFile) (iter.Seq[MSMarcoRow], error) {
	const textColumn int64 = 4
	const embColumn int64 = 7
	const dims int64 = 1024
	const chunkRows int64 = 1024

	bf := buffer.NewBufferFileFromBytesNoAlloc(mmapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	return func(yield func(MSMarcoRow) bool) {
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
				vector := make([]float32, dims)
				for j := range dims {
					vector[j] = embeddings[i*dims+j].(float32)
				}
				if !yield(MSMarcoRow{Vector: vector, Text: texts[i].(string)}) {
					return
				}
			}
			rem -= batch
		}
	}, nil
}

func parseCohereMSMarcoQueryRows(mmapped *MemoryMappedFile) (iter.Seq[MSMarcoQueryRow], error) {
	seq, err := parseCohereMSMarcoQueries(mmapped)
	if err != nil {
		return nil, err
	}
	return func(yield func(MSMarcoQueryRow) bool) {
		for row := range seq {
			if !yield(MSMarcoQueryRow{Text: row.text, Vector: row.vec}) {
				return
			}
		}
	}, nil
}
