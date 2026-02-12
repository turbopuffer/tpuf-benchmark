package bench

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
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
	logger *slog.Logger
	mu     struct {
		sync.Mutex
		// downloads holds inflight downloads. Every inflight download has an
		// entry within the map. The map value is a slice of channels, one for
		// every goroutine waiting on the download. When the download completes,
		// the goroutine that performed the download closes all the waiting
		// channels and clears the map entry.
		downloads map[string][]chan error
	}
}

var _ template.Datasource = (*CohereWikipediaEmbeddings)(nil)

// NewCohereWikipediaEmbeddings creates a new instance of CohereWikipediaEmbeddings.
func NewCohereWikipediaEmbeddings(logger *slog.Logger) *CohereWikipediaEmbeddings {
	cwe := &CohereWikipediaEmbeddings{logger: logger}
	cwe.mu.downloads = make(map[string][]chan error)
	return cwe
}

func (c *CohereWikipediaEmbeddings) NewIDSource() template.IDSource {
	return &template.MonotonicIDSource{}
}

func (c *CohereWikipediaEmbeddings) NewVectorSource() template.VectorSource {
	next, stop := iter.Pull2(readAllCohereFiles(context.Background(), c,
		cohereVectorColumnReader(4, 1024)))
	return &cohereVectorSource{next: next, stop: stop, dims: 1024}
}

func (c *CohereWikipediaEmbeddings) NewTextSource() template.TextSource {
	next, stop := iter.Pull2(readAllCohereFiles(context.Background(), c,
		cohereTextColumnReader(3)))
	return &cohereTextSource{next: next, stop: stop}
}

func (c *CohereWikipediaEmbeddings) filePath(fileName string) string {
	return filepath.Join(
		datasetCacheDir(),
		"tpuf-benchmark",
		"wikipedia-2023-11-embed-multilingual-v3-en",
		fileName,
	)
}

func (c *CohereWikipediaEmbeddings) downloadFile(ctx context.Context, fileName string) error {
	// Download the file (or wait for the inflight download to complete).
	if wait := func() chan error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, exists := c.mu.downloads[fileName]; exists {
			// We're waiting for an existing download.
			ch := make(chan error)
			c.mu.downloads[fileName] = append(c.mu.downloads[fileName], ch)
			return ch
		}
		// We're responsible for downloading.
		c.mu.downloads[fileName] = []chan error{}
		return nil
	}(); wait != nil {
		return <-wait
	}

	err := func() error {
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
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return fmt.Errorf("failed to create cache directory for %s: %w", fp, err)
		}
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
	}()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Download complete, notify any waiters and remove the entry.
	chans := c.mu.downloads[fileName]
	delete(c.mu.downloads, fileName)
	for _, ch := range chans {
		select {
		case ch <- err:
		default:
		}
	}
	return err
}

// readAllCohereFiles returns a sequence of items produced by visiting all the
// cohere embedding files in order, extracting items from each file using the
// provided iterFromFile function.
func readAllCohereFiles[T any](
	ctx context.Context,
	orig *CohereWikipediaEmbeddings,
	iterFromFile func(*template.MemoryMappedFile) (iter.Seq[T], error),
) iter.Seq2[T, error] {
	const cohereWikipediaEmbeddingFileCount = 415

	// cohereEmbeddingFileName generates the filename for a Cohere embedding
	// file based on its index. Files are named as 4-digit zero-padded numbers,
	// e.g., "0000.parquet", "0001.parquet", etc.
	cohereEmbeddingFileName := func(idx int) string {
		return fmt.Sprintf("%04d.parquet", idx)
	}

	var zero T
	return func(yield func(T, error) bool) {
		for fi := range cohereWikipediaEmbeddingFileCount {
			fname := cohereEmbeddingFileName(fi)
			fp := orig.filePath(fname)
			if _, err := os.Stat(fp); os.IsNotExist(err) {
				if err := orig.downloadFile(ctx, fname); err != nil {
					yield(zero, fmt.Errorf("failed to download file %s: %w", fname, err))
					return
				}
			}

			mmapped, err := template.MemoryMapFile(fp)
			if err != nil {
				// Unable to unnmap the file. We want to propagate the error to
				// the caller.
				yield(zero, err)
				// And then stop the outer sequence.
				return
			}
			iterFromFileSeq, err := iterFromFile(mmapped)
			if err != nil {
				// Unable to read the file and construct an iterator over the column.
				mmapped.Unmap()
				yield(zero, err)
				// And then stop the outer sequence.
				return
			}
			for v := range iterFromFileSeq {
				if !yield(v, nil) {
					mmapped.Unmap()
					return
				}
			}
			mmapped.Unmap()
		}
	}
}

func cohereVectorColumnReader(column, dims int64) func(*template.MemoryMappedFile) (iter.Seq[[]float32], error) {
	return func(mmapped *template.MemoryMappedFile) (iter.Seq[[]float32], error) {
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
}

type cohereVectorSource struct {
	dims int
	// next is a function that yields the next item. It's obtained through
	// iter.Pull over a sequence.
	next func() ([]float32, error, bool)
	stop func()
}

// Assert that *cohereVectorSource implements template.VectorSource.
var _ template.VectorSource = (*cohereVectorSource)(nil)

func (cvs *cohereVectorSource) Vector(dims int) ([]float32, error) {
	vector, err, ok := cvs.next()
	if !ok {
		return nil, fmt.Errorf("exhausted vectors")
	} else if err != nil {
		return nil, err
	}
	return template.TruncateOrExpandVector(vector, dims), nil
}

func (cvs *cohereVectorSource) Done() {
	cvs.stop()
}

func cohereTextColumnReader(column int64) func(*template.MemoryMappedFile) (iter.Seq[string], error) {
	return func(mmapped *template.MemoryMappedFile) (iter.Seq[string], error) {
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
}

type cohereTextSource struct {
	// next is a function that yields the next item. It's obtained through
	// iter.Pull over a sequence.
	next func() (string, error, bool)
	stop func()
}

// Assert that *cohereTextSource implements template.TextSource.
var _ template.TextSource = (*cohereTextSource)(nil)

func (cts *cohereTextSource) Document() (string, error) {
	text, err, ok := cts.next()
	if !ok {
		return "", fmt.Errorf("exhausted texts")
	}
	return template.CleanText(text), err
}

func (cts *cohereTextSource) Done() {
	cts.stop()
}
