package datasource

import (
	"context"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
)

// Source is a datasource that can yield IDs, vectors, and/or texts.
type Source interface {
	FuncMap(ctx context.Context) template.FuncMap
}

// Kind references a datasource for a workload.
type Kind string

const (
	DatasourceRandom                    Kind = "random"
	DatasourceCohereWikipediaEmbeddings Kind = "CohereWikipediaEmbeddings"
	DatasourceCohereMSMarco             Kind = "CohereMSMarco"
	DatasourceDeep1B                    Kind = "Deep1B"
)

// Valid returns true if the value is a known datasource ID.
func (v Kind) Valid() bool {
	switch v {
	case DatasourceRandom, DatasourceCohereWikipediaEmbeddings, DatasourceCohereMSMarco, DatasourceDeep1B:
		return true
	default:
		return false
	}
}

// UnmarshalText implements encoding.TextUnmarshaler so TOML can decode
// a string into a VectorSource and reject unknown values.
func (v *Kind) UnmarshalText(text []byte) error {
	s := Kind(strings.TrimSpace(string(text)))
	if !s.Valid() {
		return fmt.Errorf("data source must be one of %q, %q, %q, or %q", DatasourceRandom, DatasourceCohereWikipediaEmbeddings, DatasourceCohereMSMarco, DatasourceDeep1B)
	}
	*v = s
	return nil
}

// Config configures a datasource.
type Config struct {
	CacheDir         string
	ParseConcurrency int
	Hooks            Hooks

	Seed             uint64
	VectorDimensions int

	// ParallelDownloadThreshold is the minimum file size in bytes to trigger
	// parallel chunked downloading. Defaults to 512 MiB when zero.
	ParallelDownloadThreshold int64
	// ParallelDownloadChunkSize is the size of each chunk in bytes used during
	// parallel downloading. Defaults to 512 MiB when zero.
	ParallelDownloadChunkSize int64
}

// Make creates a new datasource from the given configuration.
func Make(ctx context.Context, src Kind, cfg Config) Source {
	switch src {
	case DatasourceRandom:
		return RandomDatasource(ctx, cfg)
	case DatasourceCohereWikipediaEmbeddings:
		return CohereWikipediaEmbeddings(ctx, cfg)
	case DatasourceCohereMSMarco:
		return CohereMSMarco(ctx, cfg)
	case DatasourceDeep1B:
		return Deep1B(ctx, cfg)
	default:
		panic(fmt.Errorf("unknown datasource %q", src))
	}
}

// Hooks is a set of callbacks that can be used to track the progress of a
// datasource.
type Hooks struct {
	// OnDownload, if non-nil, is invoked whenever a file is downloaded. It's
	// invoked periodically with the current progress.
	OnDownload OnDownload

	// OnLoadCachedFile, if non-nil, is invoked whenever a source file is loaded
	// from the local filesystem cache instead of being downloaded.
	OnLoadCachedFile OnLoadCachedFile
}

type OnDownload func(sourceURL string, downloadedBytes, totalBytes int64)

type OnLoadCachedFile func(sourceURL string)

// lazyPull2 returns a thread-safe next function over the given sequence, but
// defers the iter.Pull2 call (and therefore any work the sequence performs on
// start) until the first invocation. This is important for datasources like
// CohereWikipedia where iter.Pull2 eagerly kicks off a download+parse pipeline;
// deferring it avoids unnecessary work during Init when the function may not be
// called at all (e.g. the sanity-check phase).
func lazyPull2[T any](makeSeq func() iter.Seq2[T, error]) func() (T, error, bool) {
	var (
		once sync.Once
		mu   sync.Mutex
		next func() (T, error, bool)
	)
	return func() (T, error, bool) {
		once.Do(func() {
			next, _ = iter.Pull2(makeSeq())
		})
		mu.Lock()
		defer mu.Unlock()
		return next()
	}
}

// MonotonicIDs returns a function that yields monotonically increasing uint64
// IDs on each invocation.
func MonotonicIDs() func() uint64 {
	x := new(atomic.Uint64)
	return func() uint64 { return x.Add(1) }
}

func vectorToString(vec []float32, dims int) string {
	vec = truncateOrExpandVector(vec, dims)
	var builder strings.Builder
	builder.Grow(dims * 4)
	builder.WriteByte('[')
	for i := range dims {
		if i > 0 {
			builder.WriteRune(',')
		}
		builder.WriteString(
			strconv.FormatFloat(
				float64(vec[i%len(vec)]), 'f', -1, 32,
			),
		)
	}
	builder.WriteByte(']')
	return builder.String()
}

// truncateOrExpandVector truncates or expands a vector to the given dimensions.
// Does this by either truncating, extending with repeated values, or returning as is
// if the vector is already of the desired length.
func truncateOrExpandVector(vector []float32, dims int) []float32 {
	if len(vector) == dims {
		return vector
	} else if len(vector) > dims {
		return vector[:dims]
	}
	var (
		v = make([]float32, dims)
		l = len(vector)
	)
	for i := range dims {
		v[i] = vector[i%l]
	}
	return v
}
