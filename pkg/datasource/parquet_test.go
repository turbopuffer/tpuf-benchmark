package datasource

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const (
	testTextColumn = 0
	testEmbColumn  = 1
)

// testRow mirrors the Cohere passage files: a string column and a repeated float column.
type testRow struct {
	Text string    `parquet:"text"`
	Emb  []float32 `parquet:"emb,plain"`
}

// testRowDict is testRow with the float column dictionary-encoded.
type testRowDict struct {
	Text string    `parquet:"text"`
	Emb  []float32 `parquet:"emb,dict"`
}

func testText(row int) string        { return fmt.Sprintf("passage-%06d", row) }
func testValue(row, dim int) float32 { return float32(row) + float32(dim)/1024 }

// writeTestParquet writes a test file, returning its path and row group count.
func writeTestParquet[T testRow | testRowDict](tb testing.TB, rows, dims int, rowsPerRowGroup int64, pageSize int) (string, int) {
	tb.Helper()

	fp := filepath.Join(tb.TempDir(), "test.parquet")
	out, err := os.Create(fp)
	if err != nil {
		tb.Fatal(err)
	}
	w := parquet.NewGenericWriter[T](out,
		parquet.MaxRowsPerRowGroup(rowsPerRowGroup),
		parquet.PageBufferSize(pageSize),
		parquet.Compression(&parquet.Snappy),
	)
	batch := make([]T, 1)
	for i := range rows {
		emb := make([]float32, dims)
		for j := range emb {
			emb[j] = testValue(i, j)
		}
		switch row := any(&batch[0]).(type) {
		case *testRow:
			row.Text, row.Emb = testText(i), emb
		case *testRowDict:
			row.Text, row.Emb = testText(i), emb
		}
		if _, err := w.Write(batch); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := out.Close(); err != nil {
		tb.Fatal(err)
	}

	raw, err := os.ReadFile(fp)
	if err != nil {
		tb.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		tb.Fatal(err)
	}
	return fp, len(f.RowGroups())
}

func mapTestParquet(tb testing.TB, fp string) *MemoryMappedFile {
	tb.Helper()
	mmapped, err := MemoryMapFile(fp)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(mmapped.Unmap)
	return mmapped
}

// readTestColumns checks that both columns come back complete and in row order.
func readTestColumns(tb testing.TB, fp string, rows, dims int) {
	tb.Helper()

	f, err := openParquetFile(mapTestParquet(tb, fp))
	if err != nil {
		tb.Fatal(err)
	}
	if got := f.numRows(); got != int64(rows) {
		tb.Fatalf("numRows() = %d, want %d", got, rows)
	}

	texts, err := f.strings(testTextColumn)
	if err != nil {
		tb.Fatal(err)
	}
	var row int
	for text := range texts {
		if want := testText(row); text != want {
			tb.Fatalf("row %d: text = %q, want %q", row, text, want)
		}
		row++
	}
	if row != rows {
		tb.Fatalf("read %d texts, want %d", row, rows)
	}

	vectors, err := f.float32Vectors(testEmbColumn, dims)
	if err != nil {
		tb.Fatal(err)
	}
	row = 0
	for vec := range vectors {
		if len(vec) != dims {
			tb.Fatalf("row %d: vector has %d dims, want %d", row, len(vec), dims)
		}
		for j, v := range vec {
			if want := testValue(row, j); v != want {
				tb.Fatalf("row %d dim %d: got %v, want %v", row, j, v, want)
			}
		}
		row++
	}
	if row != rows {
		tb.Fatalf("read %d vectors, want %d", row, rows)
	}
}

// TestParquetColumns reads a file whose rows straddle page and row group boundaries.
func TestParquetColumns(t *testing.T) {
	const rows, dims = 500, 64
	fp, rowGroups := writeTestParquet[testRow](t, rows, dims, 150, 7*dims*4/3)
	if rowGroups < 2 {
		t.Fatalf("expected multiple row groups, got %d", rowGroups)
	}
	readTestColumns(t, fp, rows, dims)
}

// TestParquetColumnsDictionaryEncoded covers the dictionary branch, where a page holds
// indexes rather than values.
func TestParquetColumnsDictionaryEncoded(t *testing.T) {
	const rows, dims = 300, 32
	fp, _ := writeTestParquet[testRowDict](t, rows, dims, 100, 8*dims*4)

	// Confirm the premise: without dictionary encoding this test proves nothing.
	raw, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	pages := f.RowGroups()[0].ColumnChunks()[testEmbColumn].Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	if page.Dictionary() == nil {
		t.Fatal("float column is not dictionary-encoded")
	}

	readTestColumns(t, fp, rows, dims)
}

func BenchmarkParquetColumnRead(b *testing.B) {
	for _, dims := range []int{256, 1024} {
		b.Run(fmt.Sprintf("dims=%d", dims), func(b *testing.B) {
			const rows = 8192
			fp, _ := writeTestParquet[testRow](b, rows, dims, int64(1)<<40, 1<<16)
			info, err := os.Stat(fp)
			if err != nil {
				b.Fatal(err)
			}
			mmapped := mapTestParquet(b, fp)

			b.SetBytes(info.Size())
			b.ReportAllocs()
			for b.Loop() {
				f, err := openParquetFile(mmapped)
				if err != nil {
					b.Fatal(err)
				}
				vectors, err := f.float32Vectors(testEmbColumn, dims)
				if err != nil {
					b.Fatal(err)
				}
				for range vectors {
				}
			}
		})
	}
}
