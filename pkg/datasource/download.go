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
	"sync/atomic"
	"syscall"

	"golang.org/x/sync/errgroup"
)

const (
	defaultParallelDownloadThreshold = 512 << 20 // 512 MiB
	defaultParallelDownloadChunkSize = 512 << 20 // 512 MiB
	parallelDownloadWorkers          = 8
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
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return fmt.Errorf("failed to create cache directory for %s: %w", fp, err)
		}
		threshold := d.cfg.ParallelDownloadThreshold
		if threshold == 0 {
			threshold = defaultParallelDownloadThreshold
		}
		chunkSize := d.cfg.ParallelDownloadChunkSize
		if chunkSize == 0 {
			chunkSize = defaultParallelDownloadChunkSize
		}
		// Issue a HEAD request first. If the server advertises Accept-Ranges:
		// bytes and the file is large enough, download in parallel chunks.
		size, acceptsRanges, err := headRequest(ctx, url)
		if err == nil && acceptsRanges && size > threshold {
			return downloadFileParallel(ctx, fp, url, size, chunkSize, onDownload)
		}
		return downloadFileSequential(ctx, fp, url, onDownload)
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

// RangeRequest is a request to download a specific byte range of a URL as its
// own independent cache file.
type RangeRequest struct {
	URL   string
	Start int64
	End   int64 // inclusive
}

// DownloadRanged is like Download but downloads byte-range sub-requests
// concurrently, storing each range as a separate cache file. Unlike Download,
// it fans out to concurrency workers that issue requests in parallel.
func (d *downloader) DownloadRanged(
	ctx context.Context,
	requests iter.Seq2[string, RangeRequest],
	concurrency int,
	hooks Hooks,
) <-chan DownloadResult {
	type item struct {
		key string
		req RangeRequest
	}
	reqCh := make(chan item, concurrency)
	go func() {
		defer close(reqCh)
		for key, req := range requests {
			select {
			case reqCh <- item{key, req}:
			case <-ctx.Done():
				return
			}
		}
	}()

	ch := make(chan DownloadResult, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range reqCh {
				res := DownloadResult{Key: item.key, SourceURL: item.req.URL}
				fp, err := d.maybeDownloadRangedFile(ctx, item.key, item.req, hooks)
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
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func (d *downloader) maybeDownloadRangedFile(ctx context.Context, key string, req RangeRequest, hooks Hooks) (string, error) {
	fp := filepath.Join(d.cfg.CacheDir, key)
	if _, err := os.Stat(fp); err == nil {
		if hooks.OnLoadCachedFile != nil {
			hooks.OnLoadCachedFile(req.URL)
		}
		return fp, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err := d.downloadRangedFile(ctx, fp, req, hooks.OnDownload); err != nil {
		return "", err
	}
	return fp, nil
}

func (d *downloader) downloadRangedFile(ctx context.Context, fp string, req RangeRequest, onDownload OnDownload) error {
	// Deduplicate concurrent downloads of the same range.
	dedupKey := fmt.Sprintf("%s#%d-%d", req.URL, req.Start, req.End)
	if wait := func() chan error {
		d.mu.Lock()
		defer d.mu.Unlock()
		if _, exists := d.mu.downloads[dedupKey]; exists {
			ch := make(chan error)
			d.mu.downloads[dedupKey] = append(d.mu.downloads[dedupKey], ch)
			return ch
		}
		d.mu.downloads[dedupKey] = []chan error{}
		return nil
	}(); wait != nil {
		return <-wait
	}

	err := func() error {
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return fmt.Errorf("failed to create cache directory for %s: %w", fp, err)
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request for %s: %w", req.URL, err)
		}
		httpReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", req.Start, req.End))

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			return fmt.Errorf("failed to download %s bytes=%d-%d: %w", req.URL, req.Start, req.End, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPartialContent {
			return fmt.Errorf("downloading %s bytes=%d-%d: expected 206, got %d", req.URL, req.Start, req.End, resp.StatusCode)
		}

		var body io.Reader = resp.Body
		if onDownload != nil {
			chunkSize := req.End - req.Start + 1
			onDownload(req.URL, 0, chunkSize)
			body = &downloadProgressReader{resp: resp, sourceURL: req.URL, onDownload: onDownload}
		}

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
	chans := d.mu.downloads[dedupKey]
	delete(d.mu.downloads, dedupKey)
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

// headRequest issues a HEAD request and returns the content length and whether
// the server supports byte-range requests.
func headRequest(ctx context.Context, url string) (size int64, acceptsRanges bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("HEAD %s returned status %d", url, resp.StatusCode)
	}
	return resp.ContentLength, resp.Header.Get("Accept-Ranges") == "bytes", nil
}

// downloadFileSequential downloads url to fp+".tmp" using a single GET request,
// then renames the tmp file to fp on success.
func downloadFileSequential(ctx context.Context, fp, url string, onDownload OnDownload) error {
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

	var body io.Reader = resp.Body
	if onDownload != nil && resp.ContentLength > 0 {
		onDownload(url, 0, resp.ContentLength)
		body = &downloadProgressReader{resp: resp, sourceURL: url, onDownload: onDownload}
	}

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
}

// downloadFileParallel downloads url to fp+".tmp" using parallel Range requests,
// then renames the tmp file to fp on success. size must be the exact byte length
// of the remote file as reported by the HEAD response.
func downloadFileParallel(ctx context.Context, fp, url string, size, chunkSize int64, onDownload OnDownload) error {
	tmp := fp + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", tmp, err)
	}
	// Pre-allocate the full file so concurrent WriteAt calls land in bounds.
	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("failed to pre-allocate %s: %w", tmp, err)
	}

	// Build the chunk list.
	type chunk struct{ start, end int64 }
	var chunks []chunk
	for off := int64(0); off < size; off += chunkSize {
		end := off + chunkSize - 1
		if end >= size {
			end = size - 1
		}
		chunks = append(chunks, chunk{off, end})
	}

	if onDownload != nil {
		onDownload(url, 0, size)
	}

	var (
		downloaded atomic.Int64
		chunkCh    = make(chan chunk, len(chunks))
	)
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	g, ctx := errgroup.WithContext(ctx)
	for range min(parallelDownloadWorkers, len(chunks)) {
		g.Go(func() error {
			for c := range chunkCh {
				if err := downloadChunk(ctx, f, url, c.start, c.end, size, &downloaded, onDownload); err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	if err := os.Rename(tmp, fp); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("failed to rename %s to %s: %w", tmp, fp, err)
	}
	return nil
}

// downloadChunk fetches bytes [start, end] of url and writes them directly into
// f at offset start via WriteAt. Progress is reported atomically via downloaded.
func downloadChunk(
	ctx context.Context,
	f *os.File,
	url string,
	start, end, totalSize int64,
	downloaded *atomic.Int64,
	onDownload OnDownload,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetching bytes=%d-%d of %s: %w", start, end, url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("fetching bytes=%d-%d of %s: expected 206, got %d", start, end, url, resp.StatusCode)
	}

	buf := make([]byte, 128<<10) // 128 KiB read buffer
	offset := start
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], offset); werr != nil {
				return fmt.Errorf("writing to %s at offset %d: %w", f.Name(), offset, werr)
			}
			offset += int64(n)
			if onDownload != nil {
				onDownload(url, downloaded.Add(int64(n)), totalSize)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading bytes=%d-%d of %s: %w", start, end, url, err)
		}
	}
	return nil
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
// Caller must call Unmap() when finished.
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
