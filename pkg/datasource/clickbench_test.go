package datasource

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestClickBenchHitsKind(t *testing.T) {
	if !DatasourceClickBenchHits.Valid() {
		t.Fatal("ClickBenchHits should be a valid datasource kind")
	}
	src := Make(context.Background(), DatasourceClickBenchHits, Config{})
	if _, ok := src.(*clickBenchHitsSource); !ok {
		t.Fatalf("expected *clickBenchHitsSource, got %T", src)
	}
}

func TestClickBenchHitsParseAndTemplate(t *testing.T) {
	const (
		userID      int64 = 435090932899640449
		eventTime   int64 = 1372636800 // 2013-07-01T00:00:00Z
		eventDate   int32 = 15887      // 2013-07-01
		refererHash int64 = 3594120000172545465
		urlHash     int64 = 2868770270353813622
	)
	rows := []hitParquetRow{
		{
			WatchID: 1, AdvEngineID: 0, ResolutionWidth: 1920, UserID: userID,
			URL: "https://example.com/", SearchPhrase: "", EventTime: eventTime,
			EventDate: eventDate, CounterID: 62, IsRefresh: 0, DontCountHits: 0,
			TraficSourceID: -1, RefererHash: refererHash, URLHash: urlHash,
			WindowClientWidth: 1024, WindowClientHeight: 768,
		},
		{
			WatchID: 2, AdvEngineID: 2, ResolutionWidth: 1280, UserID: userID + 1,
			URL: "https://www.google.com/search", SearchPhrase: "turbopuffer", EventTime: eventTime + 60,
			EventDate: eventDate + 1, CounterID: 62, IsRefresh: 1, DontCountHits: 1,
			TraficSourceID: 6, RefererHash: refererHash + 1, URLHash: urlHash + 1,
			WindowClientWidth: 800, WindowClientHeight: 600,
		},
	}
	fp := writeClickBenchParquet(t, rows)
	mmapped := mapTestParquet(t, fp)

	parsed, err := parseClickBenchHits(mmapped)
	if err != nil {
		t.Fatal(err)
	}
	var got []hitRow
	for row := range parsed {
		got = append(got, row)
	}
	if len(got) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(got), len(rows))
	}
	if got[0].WatchID != 1 || got[0].AdvEngineID != 0 || got[0].UserID != userID {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
	if got[0].EventTime != "2013-07-01T00:00:00Z" {
		t.Fatalf("unexpected EventTime: %q", got[0].EventTime)
	}
	if got[0].EventDate != "2013-07-01T00:00:00Z" {
		t.Fatalf("unexpected EventDate: %q", got[0].EventDate)
	}
	if got[1].URL != "https://www.google.com/search" || got[1].SearchPhrase != "turbopuffer" {
		t.Fatalf("unexpected second row strings: %+v", got[1])
	}
	if got[1].EventTime != time.Unix(eventTime+60, 0).UTC().Format(time.RFC3339) {
		t.Fatalf("unexpected second EventTime: %q", got[1].EventTime)
	}

	funcs := template.FuncMap{
		"hit": func() hitRow { return got[1] },
		"id":  func() uint64 { return 7 },
		"json": func(s string) string {
			b, _ := json.Marshal(s)
			return string(b)
		},
	}
	tmpl, err := template.New("doc").Funcs(funcs).Parse(
		`{{ $h := hit }}{"id":{{ id }},"UserID":{{ $h.UserID }},"URL":{{ json $h.URL }},"EventTime":{{ json $h.EventTime }},"AdvEngineID":{{ $h.AdvEngineID }}}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	if err := tmpl.Execute(&b, nil); err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(b.String()), &doc); err != nil {
		t.Fatalf("invalid json %q: %v", b.String(), err)
	}
	if doc["URL"] != "https://www.google.com/search" {
		t.Fatalf("unexpected URL in json: %v", doc["URL"])
	}
	if doc["EventTime"] != got[1].EventTime {
		t.Fatalf("unexpected EventTime in json: %v", doc["EventTime"])
	}
}

func TestClickBenchHitsIgnoresExtraColumns(t *testing.T) {
	type wideRow struct {
		hitParquetRow
		Title string `parquet:"Title"`
	}
	fp := filepath.Join(t.TempDir(), "wide.parquet")
	out, err := os.Create(fp)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[wideRow](out)
	if _, err := w.Write([]wideRow{{
		hitParquetRow: hitParquetRow{
			WatchID: 9, URL: "/extra", EventTime: 1372636800, EventDate: 15887,
		},
		Title: "ignored",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	parsed, err := parseClickBenchHits(mapTestParquet(t, fp))
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for row := range parsed {
		n++
		if row.WatchID != 9 || row.URL != "/extra" {
			t.Fatalf("unexpected row: %+v", row)
		}
	}
	if n != 1 {
		t.Fatalf("got %d rows, want 1", n)
	}
}

func TestClickBenchHitsLogicalTypes(t *testing.T) {
	// ClickBench parquet annotates smallints as INT16 and EventDate as UINT16
	// days since epoch. The reader projects those onto wider Go integers.
	type logicalRow struct {
		WatchID            int64  `parquet:"WatchID"`
		AdvEngineID        int16  `parquet:"AdvEngineID"`
		ResolutionWidth    int16  `parquet:"ResolutionWidth"`
		UserID             int64  `parquet:"UserID"`
		URL                string `parquet:"URL"`
		SearchPhrase       string `parquet:"SearchPhrase"`
		EventTime          int64  `parquet:"EventTime"`
		EventDate          uint16 `parquet:"EventDate"`
		CounterID          int32  `parquet:"CounterID"`
		IsRefresh          int16  `parquet:"IsRefresh"`
		DontCountHits      int16  `parquet:"DontCountHits"`
		TraficSourceID     int16  `parquet:"TraficSourceID"`
		RefererHash        int64  `parquet:"RefererHash"`
		URLHash            int64  `parquet:"URLHash"`
		WindowClientWidth  int16  `parquet:"WindowClientWidth"`
		WindowClientHeight int16  `parquet:"WindowClientHeight"`
	}
	fp := filepath.Join(t.TempDir(), "logical.parquet")
	out, err := os.Create(fp)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[logicalRow](out)
	if _, err := w.Write([]logicalRow{{
		WatchID: 3, AdvEngineID: 2, ResolutionWidth: 800, UserID: 99,
		URL: "/u", SearchPhrase: "q", EventTime: 1372636800, EventDate: 15887,
		CounterID: 62, IsRefresh: 0, DontCountHits: 0, TraficSourceID: -1,
		RefererHash: 1, URLHash: 2, WindowClientWidth: 640, WindowClientHeight: 480,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	parsed, err := parseClickBenchHits(mapTestParquet(t, fp))
	if err != nil {
		t.Fatal(err)
	}
	var row hitRow
	var n int
	for r := range parsed {
		n++
		row = r
	}
	if n != 1 {
		t.Fatalf("got %d rows, want 1", n)
	}
	if row.AdvEngineID != 2 || row.ResolutionWidth != 800 || row.TraficSourceID != -1 {
		t.Fatalf("unexpected ints: %+v", row)
	}
	if row.EventDate != "2013-07-01T00:00:00Z" {
		t.Fatalf("unexpected EventDate: %q", row.EventDate)
	}
}

func TestClickBenchHitsURLs(t *testing.T) {
	var n int
	for key, url := range clickBenchHitsURLs() {
		if n == 0 {
			if key != "clickbench/hits_0.parquet" {
				t.Fatalf("first cache key: %q", key)
			}
			if url != "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet" {
				t.Fatalf("first url: %q", url)
			}
		}
		n++
	}
	if n != clickBenchPartitionCount {
		t.Fatalf("got %d urls, want %d", n, clickBenchPartitionCount)
	}
}

func writeClickBenchParquet(tb testing.TB, rows []hitParquetRow) string {
	tb.Helper()
	fp := filepath.Join(tb.TempDir(), "hits.parquet")
	out, err := os.Create(fp)
	if err != nil {
		tb.Fatal(err)
	}
	w := parquet.NewGenericWriter[hitParquetRow](out)
	if _, err := w.Write(rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := out.Close(); err != nil {
		tb.Fatal(err)
	}
	return fp
}
