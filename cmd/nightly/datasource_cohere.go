package main

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/turbopuffer/tpuf-benchmark/pkg/template"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

// CohereWikipediaEmbeddings provides template data from Cohere's Wikipedia embeddings.
// See: https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3
type CohereWikipediaEmbeddings struct {
	logger    *slog.Logger
	lock      sync.Mutex              // protects downloads
	downloads map[string][]chan error // current in progress downloads
}

var _ template.Datasource = (*CohereWikipediaEmbeddings)(nil)

// NewCohereWikipediaEmbeddings creates a new instance of CohereWikipediaEmbeddings.
func NewCohereWikipediaEmbeddings(logger *slog.Logger) *CohereWikipediaEmbeddings {
	return &CohereWikipediaEmbeddings{
		logger:    logger,
		downloads: make(map[string][]chan error),
	}
}

func (c *CohereWikipediaEmbeddings) NewIDSource() template.IDSource {
	return &template.MonotonicIDSource{}
}

func (c *CohereWikipediaEmbeddings) NewVectorSource() template.VectorSource {
	return &cohereVectorSource{
		orig:     c,
		nextFile: 0,
		vectors:  nil,
	}
}

func (c *CohereWikipediaEmbeddings) NewTextSource() template.TextSource {
	return &cohereTextSource{
		rng:      rand.New(rand.NewPCG(42, 69)),
		orig:     c,
		nextFile: 0,
		texts:    nil,
	}
}

func (c *CohereWikipediaEmbeddings) filePath(fileName string) string {
	return filepath.Join("/tmp", fileName)
}

func (c *CohereWikipediaEmbeddings) downloadFile(ctx context.Context, fileName string) error {
	var wait chan error
	c.lock.Lock()
	if _, exists := c.downloads[fileName]; !exists {
		c.downloads[fileName] = []chan error{} // We're responsible for downloading
	} else {
		wait = make(chan error) // We're waiting for an existing download
		c.downloads[fileName] = append(c.downloads[fileName], wait)
	}
	c.lock.Unlock()

	if wait != nil {
		return <-wait
	}

	download := func() error {
		start := time.Now()
		c.logger.Info("downloading file", slog.String("file", fileName))

		url := fmt.Sprintf(
			"https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3/resolve/main/en/%s?download=true",
			fileName,
		)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request for %s: %w", url, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to download %s: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to download %s: status code %d", url, resp.StatusCode)
		}

		fp := c.filePath(fileName)
		f, err := os.Create(fp)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", fp, err)
		}

		if _, err := f.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("failed to write to file %s: %w", fp, err)
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close file %s: %w", fp, err)
		}

		c.logger.Info(
			"download complete",
			slog.String("file", fileName),
			slog.Duration("took", time.Since(start)),
		)

		return nil
	}

	err := download()

	c.lock.Lock()
	chans := c.downloads[fileName]
	delete(c.downloads, fileName) // Download complete, remove from map
	for _, ch := range chans {
		select {
		case ch <- err:
		default:
		}
	}
	c.lock.Unlock()

	return err
}

type cohereVectorSource struct {
	orig     *CohereWikipediaEmbeddings
	nextFile int
	vectors  []func() ([]float32, bool)
}

func (cvs *cohereVectorSource) Vector(dims int) ([]float32, error) {
	for {
		if len(cvs.vectors) == 0 {
			if err := cvs.loadNextFile(context.Background()); err != nil {
				return nil, fmt.Errorf("failed to load next file: %w", err)
			}
			continue
		}
		vector, ok := cvs.vectors[0]()
		if !ok {
			cvs.vectors = cvs.vectors[1:]
			continue
		}
		return template.TruncateOrExpandVector(vector, dims), nil
	}
}

const cohereWikipediaEmbeddingFileCount = 415

func (cvs *cohereVectorSource) loadNextFile(ctx context.Context) error {
	fileIdx := cvs.nextFile
	if fileIdx >= cohereWikipediaEmbeddingFileCount {
		return fmt.Errorf("no more files")
	}
	cvs.nextFile++

	var (
		fname = cohereEmbeddingFileName(fileIdx)
		fp    = cvs.orig.filePath(fname)
	)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		if err := cvs.orig.downloadFile(ctx, fname); err != nil {
			return fmt.Errorf("failed to download file %s: %w", fname, err)
		}
	}

	mmapped, err := template.MemoryMapFile(fp)
	if err != nil {
		return fmt.Errorf("failed to memory map file %s: %w", fp, err)
	}

	vectorSeq, err := readVectorColumn(mmapped, 4, 1024)
	if err != nil {
		return fmt.Errorf("failed to read vectors from file %s: %w", fp, err)
	}
	pull, _ := iter.Pull(vectorSeq)
	cvs.vectors = append(cvs.vectors, pull)

	return nil
}

func readVectorColumn(memoryMapped *template.MemoryMappedFile, column, dims int64) (iter.Seq[[]float32], error) {
	bf := buffer.NewBufferFileFromBytesNoAlloc(memoryMapped.Data)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		memoryMapped.Unmap()
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	vectors, _, _, err := pr.ReadColumnByIndex(column, n*dims)
	if err != nil {
		memoryMapped.Unmap()
		return nil, fmt.Errorf("failed to read column %d: %w", column, err)
	}
	return func(yield func([]float32) bool) {
		defer memoryMapped.Unmap() // Unmap the memory once we're finished yielding.
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

type cohereTextSource struct {
	orig     *CohereWikipediaEmbeddings
	rng      *rand.Rand
	nextFile int
	texts    []func() (string, bool)
}

func (cts *cohereTextSource) Document() (string, error) {
	text, err := cts.getText()
	return template.CleanText(text), err
}

func (cts *cohereTextSource) getText() (string, error) {
	for {
		if len(cts.texts) == 0 {
			if err := cts.loadNextFile(context.Background()); err != nil {
				return "", fmt.Errorf("failed to load next file: %w", err)
			}
			continue
		}
		text, ok := cts.texts[0]()
		if !ok {
			cts.texts = cts.texts[1:]
			continue
		}
		return text, nil
	}
}

func (cts *cohereTextSource) loadNextFile(ctx context.Context) error {
	fileIdx := cts.nextFile
	if fileIdx >= cohereWikipediaEmbeddingFileCount {
		return fmt.Errorf("no more files")
	}
	cts.nextFile++

	var (
		fname = cohereEmbeddingFileName(fileIdx)
		fp    = cts.orig.filePath(fname)
	)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		if err := cts.orig.downloadFile(ctx, fname); err != nil {
			return fmt.Errorf("failed to download file %s: %w", fname, err)
		}
	}

	mmapped, err := template.MemoryMapFile(fp)
	if err != nil {
		return fmt.Errorf("failed to memory map file %s: %w", fp, err)
	}
	textSeq, err := readTextColumn(mmapped.Data, 3)
	if err != nil {
		return fmt.Errorf("failed to read texts from file %s: %w", fp, err)
	}
	pull, _ := iter.Pull(textSeq)
	cts.texts = append(cts.texts, pull)

	return nil
}

// cohereEmbeddingFileName generates the filename for a Cohere embedding file
// based on its index. Files are named as 4-digit zero-padded numbers, e.g.,
// "0000.parquet", "0001.parquet", etc.
func cohereEmbeddingFileName(idx int) string {
	return fmt.Sprintf("%04d.parquet", idx)
}

func readTextColumn(fileContent []byte, column int64) (iter.Seq[string], error) {
	bf := buffer.NewBufferFileFromBytesNoAlloc(fileContent)
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
