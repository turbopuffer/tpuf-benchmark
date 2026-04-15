package datasource

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"strconv"
	"strings"
	"text/template"
)

const deep1BURL = "https://turbopuffer-fixtures.storage.googleapis.com/ci/datasets/big-ann-deep/base.1B.fbin"

// Deep1B returns a datasource backed by the Yandex Deep1B dataset — 1 billion
// 96-dimensional float32 vectors stored in a single .fbin file.
//
// Chunks are downloaded in parallel and stored as separate cache files.
func Deep1B(ctx context.Context, cfg Config) Source {
	return &deep1BSource{dd: newDownloader(cfg)}
}

type deep1BSource struct {
	dd *downloader
}

var _ Source = (*deep1BSource)(nil)

func (d *deep1BSource) FuncMap(ctx context.Context) template.FuncMap {
	nextVec := lazyPull2(func() iter.Seq2[[]float32, error] {
		return deep1BVectorIterator(ctx, d.dd)
	})

	return template.FuncMap{
		"vector": func(dims int) string {
			vec, err, ok := nextVec()
			if !ok {
				panic("deep1B vector source exhausted")
			} else if err != nil {
				panic(err)
			}
			return vectorToString(vec, dims)
		},
	}
}

type deep1BMeta struct {
	dims       int64
	numVectors int64
}

// vectorRawFloat32s decodes one .fbin vector: len(b) must be dims * 4.
func vectorRawFloat32s(b []byte, dims int64) []float32 {
	vec := make([]float32, dims)
	for j := range dims {
		vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(b[int(j)*4:]))
	}
	return vec
}

// deep1BVectorIterator issues a HEAD request to determine file size, then
// downloads all chunks in parallel via DownloadRanged. A single goroutine
// processes chunk files in order (stripping the 8-byte .fbin header from chunk
// 0) and maintaining a carry buffer for vectors that straddle chunk boundaries.
func deep1BVectorIterator(ctx context.Context, dd *downloader) iter.Seq2[[]float32, error] {
	return func(yield func([]float32, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		size, acceptsRanges, err := headRequest(ctx, deep1BURL)
		if err != nil {
			yield(nil, fmt.Errorf("deep1B: HEAD request: %w", err))
			return
		}
		if !acceptsRanges {
			yield(nil, fmt.Errorf("deep1B: server does not support range requests"))
			return
		}

		chunkSize := dd.cfg.ParallelDownloadChunkSize
		if chunkSize == 0 {
			chunkSize = defaultParallelDownloadChunkSize
		}

		downloaded := dd.DownloadRanged(ctx,
			deep1BChunkRequests(size, chunkSize),
			parallelDownloadWorkers,
			dd.cfg.Hooks,
		)

		var (
			meta    *deep1BMeta
			vecBuf  []byte // single-vector accumulation buffer
			filled  int    // bytes written into vecBuf so far
			nextIdx int
			buffer  = make(map[int]DownloadResult)
		)

		// processChunk yields vectors.
		//
		// The first chunk requires special handling (extracting and stripping
		// the file's header, populating meta). Bytes are copied into vecBuf;
		// each time it fills to vectorBytes a vector is decoded and yielded.
		// Partial vectors at chunk boundaries survive in vecBuf[:filled].
		processChunk := func(idx int, res DownloadResult) bool {
			if res.Err != nil {
				yield(nil, res.Err)
				return false
			}
			mmapped, err := MemoryMapFile(res.LocalPath)
			if err != nil {
				yield(nil, fmt.Errorf("deep1B: mmap chunk %d: %w", idx, err))
				return false
			}
			defer mmapped.Unmap()

			data := mmapped.Data

			// Parse the preamble from chunk 0 and advance data past it.
			if idx == 0 {
				if len(data) < 8 {
					yield(nil, fmt.Errorf("deep1B: chunk 0 too small (%d bytes)", len(data)))
					return false
				}
				meta = &deep1BMeta{
					numVectors: int64(binary.LittleEndian.Uint32(data[0:4])),
					dims:       int64(binary.LittleEndian.Uint32(data[4:8])),
				}
				data = data[8:]
				vecBuf = make([]byte, int(meta.dims)*4)
			}

			vectorBytes := len(vecBuf)
			for len(data) > 0 {
				n := copy(vecBuf[filled:], data)
				filled += n
				data = data[n:]
				if filled == vectorBytes {
					vec := vectorRawFloat32s(vecBuf, meta.dims)
					if !yield(vec, nil) {
						return false
					}
					filled = 0
				}
			}
			return true
		}

		// Consume downloaded results, buffering out-of-order arrivals so the
		// parse worker always sees chunks in sequence. This is required to
		// ensure that vectors are composed correctly at chunk boundaries.
		for res := range downloaded {
			idx := deep1BChunkIndex(res.Key)
			if idx != nextIdx {
				buffer[idx] = res
				continue
			}
			if !processChunk(idx, res) {
				return
			}
			nextIdx++
			for {
				buffered, ok := buffer[nextIdx]
				if !ok {
					break
				}
				delete(buffer, nextIdx)
				if !processChunk(nextIdx, buffered) {
					return
				}
				nextIdx++
			}
		}
	}
}

// deep1BChunkRequests returns an iterator of (key, RangeRequest) pairs that
// cover totalSize bytes in chunkSize-byte segments.
func deep1BChunkRequests(totalSize, chunkSize int64) iter.Seq2[string, RangeRequest] {
	return func(yield func(string, RangeRequest) bool) {
		for i := int64(0); ; i++ {
			start := i * chunkSize
			if start >= totalSize {
				return
			}
			key := fmt.Sprintf("deep1b/base.1B.fbin.chunk-%04d", i)
			req := RangeRequest{
				URL:   deep1BURL,
				Start: start,
				End:   min(start+chunkSize-1, totalSize-1),
			}
			if !yield(key, req) {
				return
			}
		}
	}
}

// deep1BChunkIndex extracts the chunk index from a key of the form
// "deep1b/base.1B.fbin.chunk-NNNN".
func deep1BChunkIndex(key string) int {
	_, suffix, ok := strings.Cut(key, ".chunk-")
	if !ok {
		panic(fmt.Errorf("deep1B: malformed cache file key %q", key))
	}
	idx, err := strconv.Atoi(suffix)
	if err != nil {
		panic(fmt.Errorf("deep1B: invalid chunk index in key %q: %w", key, err))
	}
	return idx
}
