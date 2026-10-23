package datasource

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"sync"
	"text/template"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	clickBenchPartitionCount = 100
	clickBenchURLFmt         = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_%d.parquet"
	clickBenchCacheKeyFmt    = "clickbench/hits_%d.parquet"
	clickBenchRowBufSize     = 1024
)

// ClickBench returns a datasource that yields ClickBench rows from the 100
// partitioned parquet files (99,997,497 rows). Only the columns needed by the
// supported turbopuffer query mappings are read.
func ClickBench(_ context.Context, cfg Config) Source {
	cfg.ParseConcurrency = max(1, cfg.ParseConcurrency)
	return &clickBenchSource{dd: newDownloader(cfg)}
}

type clickBenchSource struct {
	dd *downloader

	once sync.Once
	next func() (hitRow, error, bool)
}

var _ Source = (*clickBenchSource)(nil)

// hitRow is the subset of a ClickBench hits row used by the benchmark, with
// EventTime/EventDate converted to RFC3339 for turbopuffer datetime fields.
type hitRow struct {
	WatchID            int64
	AdvEngineID        int32
	ResolutionWidth    int32
	UserID             int64
	URL                string
	SearchPhrase       string
	EventTime          string
	EventDate          string
	CounterID          int32
	IsRefresh          int32
	DontCountHits      int32
	TraficSourceID     int32
	RefererHash        int64
	URLHash            int64
	WindowClientWidth  int32
	WindowClientHeight int32
}

// hitParquetRow is the on-disk projection of ClickBench hits. EventTime is
// Unix epoch seconds (INT64); EventDate is days since 1970-01-01.
type hitParquetRow struct {
	WatchID            int64  `parquet:"WatchID"`
	AdvEngineID        int32  `parquet:"AdvEngineID"`
	ResolutionWidth    int32  `parquet:"ResolutionWidth"`
	UserID             int64  `parquet:"UserID"`
	URL                string `parquet:"URL"`
	SearchPhrase       string `parquet:"SearchPhrase"`
	EventTime          int64  `parquet:"EventTime"`
	EventDate          int32  `parquet:"EventDate"`
	CounterID          int32  `parquet:"CounterID"`
	IsRefresh          int32  `parquet:"IsRefresh"`
	DontCountHits      int32  `parquet:"DontCountHits"`
	TraficSourceID     int32  `parquet:"TraficSourceID"`
	RefererHash        int64  `parquet:"RefererHash"`
	URLHash            int64  `parquet:"URLHash"`
	WindowClientWidth  int32  `parquet:"WindowClientWidth"`
	WindowClientHeight int32  `parquet:"WindowClientHeight"`
}

func (r hitParquetRow) asHitRow() hitRow {
	return hitRow{
		WatchID:            r.WatchID,
		AdvEngineID:        r.AdvEngineID,
		ResolutionWidth:    r.ResolutionWidth,
		UserID:             r.UserID,
		URL:                r.URL,
		SearchPhrase:       r.SearchPhrase,
		EventTime:          time.Unix(r.EventTime, 0).UTC().Format(time.RFC3339),
		EventDate:          time.Unix(int64(r.EventDate)*86400, 0).UTC().Format("2006-01-02T00:00:00Z"),
		CounterID:          r.CounterID,
		IsRefresh:          r.IsRefresh,
		DontCountHits:      r.DontCountHits,
		TraficSourceID:     r.TraficSourceID,
		RefererHash:        r.RefererHash,
		URLHash:            r.URLHash,
		WindowClientWidth:  r.WindowClientWidth,
		WindowClientHeight: r.WindowClientHeight,
	}
}

func (s *clickBenchSource) FuncMap(ctx context.Context) template.FuncMap {
	s.once.Do(func() {
		s.next = lazyPull2(func() iter.Seq2[hitRow, error] {
			return parsingAndDownloadingIterator(ctx, s.dd, clickBenchURLs(), parseClickBench)
		})
	})
	return template.FuncMap{
		"hit": func() hitRow {
			row, err, ok := s.next()
			if !ok {
				panic("ClickBench hits source exhausted")
			} else if err != nil {
				panic(err)
			}
			return row
		},
	}
}

func clickBenchURLs() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for i := range clickBenchPartitionCount {
			if !yield(fmt.Sprintf(clickBenchCacheKeyFmt, i), fmt.Sprintf(clickBenchURLFmt, i)) {
				return
			}
		}
	}
}

func parseClickBench(mmapped *MemoryMappedFile) (iter.Seq[hitRow], error) {
	f, err := parquet.OpenFile(bytes.NewReader(mmapped.Data), int64(len(mmapped.Data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickBench parquet file: %w", err)
	}
	return func(yield func(hitRow) bool) {
		buf := make([]hitParquetRow, clickBenchRowBufSize)
		for _, rg := range f.RowGroups() {
			r := parquet.NewGenericRowGroupReader[hitParquetRow](rg)
			for {
				n, err := r.Read(buf)
				for i := range n {
					if !yield(buf[i].asHitRow()) {
						r.Close()
						return
					}
				}
				if err == io.EOF {
					break
				} else if err != nil {
					r.Close()
					panic(fmt.Errorf("reading ClickBench parquet rows: %w", err))
				}
			}
			if err := r.Close(); err != nil {
				panic(fmt.Errorf("closing ClickBench parquet reader: %w", err))
			}
		}
	}, nil
}
