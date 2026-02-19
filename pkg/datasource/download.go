package datasource

import (
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

func parsingAndDownloadingIterator[T any](
	ctx context.Context,
	dd *downloader,
	fileURLs iter.Seq2[string, string],
	iterFromFile func(*MemoryMappedFile) (iter.Seq[T], error),
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		downloadedFiles := dd.Download(ctx, fileURLs, dd.cfg.ParseConcurrency, dd.cfg.Hooks)
		values := parseDownloadedFiles(ctx, downloadedFiles, iterFromFile, dd.cfg.ParseConcurrency)
		for v, err := range values {
			if !yield(v, err) {
				return
			}
		}
	}
}

type DownloadResult struct {
	Key       string
	LocalPath string
	SourceURL string
	Err       error
}

func newDownloader(cfg Config) *downloader {
	d := &downloader{cfg: cfg}
	d.mu.downloads = make(map[string][]chan error)
	return d
}

// A downloader downloads files to a local cache directory. It deduplicates
// concurrent inflight requests for the same file.
type downloader struct {
	cfg Config
	mu  struct {
		sync.Mutex
		// downloads holds inflight downloads. Every inflight download has an
		// entry within the map. The map value is a slice of channels, one for
		// every goroutine waiting on the download. When the download completes,
		// the goroutine that performed the download closes all the waiting
		// channels and clears the map entry.
		downloads map[string][]chan error
	}
}

// Download consumes an iterator of (key, url) pairs and downloads the URLs to a
// local cache directory, keyed by the key. The keys in the input iterator must
// be unique.
//
// Download launches a background goroutine that performs the downloads and sends
// results over the provided channel. The channel is closed when all URLs have
// been consumed or the context is cancelled. The caller must cancel the context
// to stop the background goroutine if it does not drain the channel.
func (d *downloader) Download(
	ctx context.Context,
	fileURLs iter.Seq2[string, string],
	concurrency int,
	hooks Hooks,
) <-chan DownloadResult {
	ch := make(chan DownloadResult, concurrency)
	go func() {
		defer close(ch)
		for key, url := range fileURLs {
			res := DownloadResult{Key: key, SourceURL: url}
			fp, err := d.maybeDownloadFile(ctx, key, url, hooks)
			if err != nil {
				res.Err = err
			} else {
				res.LocalPath = fp
			}
			select {
			case ch <- res:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

func (d *downloader) maybeDownloadFile(ctx context.Context, key, url string, hooks Hooks) (string, error) {
	fp := filepath.Join(d.cfg.CacheDir, key)
	if _, err := os.Stat(fp); err == nil {
		if hooks.OnLoadCachedFile != nil {
			hooks.OnLoadCachedFile(url)
		}
		return fp, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	// Not yet downloaded or the download is in-flight.
	if err := d.downloadFile(ctx, fp, url, hooks.OnDownload); err != nil {
		return "", err
	}
	return fp, nil
}

func (d *downloader) downloadFile(ctx context.Context, fp, url string, onDownload OnDownload) error {
	// Download the file (or wait for the inflight download to complete).
	// Track inflight downloads by URL (not key) so that two datasources
	// that happen to use the same key for different URLs are not
	// incorrectly deduplicated.
	if wait := func() chan error {
		d.mu.Lock()
		defer d.mu.Unlock()
		if _, exists := d.mu.downloads[url]; exists {
			// We're waiting for an existing download.
			ch := make(chan error)
			d.mu.downloads[url] = append(d.mu.downloads[url], ch)
			return ch
		}
		// We're responsible for downloading.
		d.mu.downloads[url] = []chan error{}
		return nil
	}(); wait != nil {
		return <-wait
	}

	err := func() error {
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

		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return fmt.Errorf("failed to create cache directory for %s: %w", fp, err)
		}

		var body io.Reader = resp.Body
		if onDownload != nil && resp.ContentLength > 0 {
			onDownload(url, 0, resp.ContentLength)
			body = &downloadProgressReader{resp: resp, sourceURL: url, onDownload: onDownload}
		}

		// Write to a temporary file and atomically rename on success,
		// so that interrupted downloads don't leave truncated files in
		// the cache that would be treated as valid on the next run.
		tmp := fp + ".tmp"
		f, err := os.Create(tmp)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", tmp, err)
		}
		if _, err := io.Copy(f, body); err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("failed to write to file %s: %w", tmp, err)
		}
		if err := f.Close(); err != nil {
			os.Remove(tmp)
			return fmt.Errorf("failed to close file %s: %w", tmp, err)
		}
		if err := os.Rename(tmp, fp); err != nil {
			os.Remove(tmp)
			return fmt.Errorf("failed to rename %s to %s: %w", tmp, fp, err)
		}
		return nil
	}()

	d.mu.Lock()
	defer d.mu.Unlock()
	// Download complete, notify any waiters and remove the entry.
	chans := d.mu.downloads[url]
	delete(d.mu.downloads, url)
	for _, ch := range chans {
		select {
		case ch <- err:
		default:
		}
	}
	return err
}

// downloadProgressReader wraps an io.Reader and reports cumulative bytes read
// to an OnDownload callback.
type downloadProgressReader struct {
	resp       *http.Response
	sourceURL  string
	onDownload OnDownload
	received   int64
}

func (r *downloadProgressReader) Read(p []byte) (int, error) {
	n, err := r.resp.Body.Read(p)
	if n > 0 {
		r.received += int64(n)
		r.onDownload(r.sourceURL, r.received, r.resp.ContentLength)
	}
	return n, err
}

// parsedItem is an item produced by a parse worker, sent over the output
// channel in the concurrent parsing path.
type parsedItem[T any] struct {
	value T
	err   error
}

// parseDownloadedFile parses a single downloaded file, returning a sequence of
// items within.
func parseDownloadedFile[T any](
	res DownloadResult,
	iterFromFile func(*MemoryMappedFile) (iter.Seq[T], error),
) (iter.Seq[T], error) {
	if res.Err != nil {
		return nil, res.Err
	}
	mmapped, err := MemoryMapFile(res.LocalPath)
	if err != nil {
		return nil, err
	}
	iterFromFileSeq, err := iterFromFile(mmapped)
	if err != nil {
		mmapped.Unmap()
		return nil, err
	}
	// Wrap the iterator with a defer to unmap the file when iteration
	// completes.
	return func(yield func(T) bool) {
		defer mmapped.Unmap()
		for v := range iterFromFileSeq {
			if !yield(v) {
				return
			}
		}
	}, nil
}

// parseDownloadedFiles returns a sequence of items produced by visiting all the
// downloaded files, extracting items from each file using the provided parse
// function. The concurrency parameter controls how many files are parsed
// simultaneously. A concurrency of 1 or less uses a simple serial
// implementation.
func parseDownloadedFiles[T any](
	ctx context.Context,
	downloadedFiles <-chan DownloadResult,
	iterFromFile func(*MemoryMappedFile) (iter.Seq[T], error),
	concurrency int,
) iter.Seq2[T, error] {
	var zero T
	return func(yield func(T, error) bool) {
		ctx, cancel := context.WithCancel(ctx)

		outCh := make(chan parsedItem[T], 4096)

		var wg sync.WaitGroup
		for range concurrency {
			wg.Go(func() {
				for {
					select {
					case <-ctx.Done():
						return
					case res, ok := <-downloadedFiles:
						if !ok {
							return
						}
						values, err := parseDownloadedFile(res, iterFromFile)
						if err != nil {
							outCh <- parsedItem[T]{err: err}
							return
						}
						for v := range values {
							outCh <- parsedItem[T]{value: v}
						}
					}
				}
			})
		}

		// Close outCh after all workers finish.
		go func() {
			wg.Wait()
			close(outCh)
		}()

		// On clean up, cancel context to signal all goroutines to stop, then
		// drain outCh to unblock any workers stuck sending so they can finish
		// and unmap their files.
		defer func() {
			cancel()
			for range outCh {
			}
		}()

		// Consume parsed items and yield to caller.
		for item := range outCh {
			if item.err != nil {
				yield(zero, item.err)
				return
			}
			if !yield(item.value, nil) {
				return
			}
		}
	}
}

// MemoryMappedFile wraps a byte slice that's been memory-mapped to a file.
// Configured with a runtime.AddCleanup function, will be automatically cleaned up
// by the runtime. No need to manually unmap the memory.
type MemoryMappedFile struct {
	Data []byte
}

// MemoryMapFile maps a file into memory and returns a MemoryMappedFile.
// Caller must call Unmap() when finished.
func MemoryMapFile(fp string) (*MemoryMappedFile, error) {
	f, err := os.Open(fp)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fp, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %s: %w", fp, err)
	}

	size := info.Size()
	if size == 0 {
		return &MemoryMappedFile{Data: make([]byte, 0)}, nil
	} else if size < 0 {
		return nil, fmt.Errorf("file %s has negative size %d", fp, size)
	} else if size != int64(int(size)) {
		return nil, fmt.Errorf("file %s has size %d which is too large", fp, size)
	}

	conn, err := f.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("failed to get syscall connection for file %s: %w", fp, err)
	}

	var data []byte
	if err := conn.Control(func(fd uintptr) {
		data, err = syscall.Mmap(int(fd), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	}); err != nil {
		return nil, fmt.Errorf("failed to mmap file %s: %w", fp, err)
	}
	return &MemoryMappedFile{Data: data}, nil
}

// Unmap unmaps the memory-mapped file.
// Must be called by callers before being dropped.
func (mmf *MemoryMappedFile) Unmap() {
	syscall.Munmap(mmf.Data)
}
