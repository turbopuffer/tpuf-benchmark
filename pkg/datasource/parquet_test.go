package datasource

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	testTextColumn int64 = 0
	testEmbColumn  int64 = 1
)

// writeTestParquet writes a parquet file shaped like the Cohere passage files:
// a string column followed by a repeated float column of fixed width. It
// returns the path of the file and the number of row groups it contains.
func writeTestParquet(tb testing.TB, rows, dims int, rowGroupSize, pageSize int64) (string, int) {
	tb.Helper()

	fp := filepath.Join(tb.TempDir(), "test.parquet")
	fw, err := local.NewLocalFileWriter(fp)
	if err != nil {
		tb.Fatal(err)
	}
	const md = `{
		"Tag": "name=root, repetitiontype=REQUIRED",
		"Fields": [
			{"Tag": "name=text, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
			{"Tag": "name=emb, type=FLOAT, repetitiontype=REPEATED"}
		]
	}`
	pw, err := writer.NewJSONWriter(md, fw, 1)
	if err != nil {
		tb.Fatal(err)
	}
	pw.RowGroupSize = rowGroupSize
	pw.PageSize = pageSize
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := range rows {
		emb := make([]float32, dims)
		for j := range emb {
			emb[j] = testValue(i, j)
		}
		rec, err := json.Marshal(map[string]any{"text": testText(i), "emb": emb})
		if err != nil {
			tb.Fatal(err)
		}
		if err := pw.Write(string(rec)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		tb.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		tb.Fatal(err)
	}
	return fp, len(pw.Footer.GetRowGroups())
}

func testText(row int) string        { return fmt.Sprintf("passage-%06d", row) }
func testValue(row, dim int) float32 { return float32(row) + float32(dim)/1024 }

func mapTestParquet(tb testing.TB, fp string) *MemoryMappedFile {
	tb.Helper()
	mmapped, err := MemoryMapFile(fp)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(mmapped.Unmap)
	return mmapped
}

// readTestColumns reads both columns of a test file through the given reader,
// checking that every value comes back in row order.
func readTestColumns(tb testing.TB, pr *reader.ParquetReader, rows, dims int) {
	tb.Helper()

	n := pr.GetNumRows()
	if n != int64(rows) {
		tb.Fatalf("GetNumRows() = %d, want %d", n, rows)
	}

	chunkRows := parquetChunkRows(1)
	var row int
	for rem := n; rem > 0; {
		batch := min(chunkRows, rem)
		chunk, _, _, err := pr.ReadColumnByIndex(testTextColumn, batch)
		if err != nil {
			tb.Fatal(err)
		}
		if int64(len(chunk)) != batch {
			tb.Fatalf("read %d texts, want %d", len(chunk), batch)
		}
		for _, v := range chunk {
			if want := testText(row); v.(string) != want {
				tb.Fatalf("row %d: text = %q, want %q", row, v, want)
			}
			row++
		}
		rem -= batch
	}
	if row != rows {
		tb.Fatalf("read %d texts, want %d", row, rows)
	}

	chunkRows = parquetChunkRows(int64(dims))
	row = 0
	for rem := n; rem > 0; {
		batch := min(chunkRows, rem)
		chunk, _, _, err := pr.ReadColumnByIndex(testEmbColumn, batch)
		if err != nil {
			tb.Fatal(err)
		}
		if int64(len(chunk)) != batch*int64(dims) {
			tb.Fatalf("read %d values, want %d", len(chunk), batch*int64(dims))
		}
		for i := range int(batch) {
			for j := range dims {
				if want := testValue(row, j); chunk[i*dims+j].(float32) != want {
					tb.Fatalf("row %d dim %d: got %v, want %v", row, j, chunk[i*dims+j], want)
				}
			}
			row++
		}
		rem -= batch
	}
	if row != rows {
		tb.Fatalf("read %d vectors, want %d", row, rows)
	}
}

// TestParquetColumns checks that a reader opened through
// openParquetColumnReader returns every value in row order, across page and row
// group boundaries. The clamped TotalCompressedSize must not truncate reads.
func TestParquetColumns(t *testing.T) {
	const (
		rows = 500
		dims = 64
	)
	// Small row groups and pages so that rows straddle both boundaries, and
	// chunks well under the clamped transport buffer size.
	fp, rowGroups := writeTestParquet(t, rows, dims, 150*dims*4, 7*dims*4/3)
	if rowGroups < 2 {
		t.Fatalf("expected multiple row groups, got %d", rowGroups)
	}
	pr, err := openParquetColumnReader(mapTestParquet(t, fp))
	if err != nil {
		t.Fatal(err)
	}
	readTestColumns(t, pr, rows, dims)
}

// TestParquetColumnsChunkLargerThanBuffer checks that a column chunk much
// bigger than parquetTransportBufferSize still reads correctly once its
// recorded TotalCompressedSize has been clamped below its real size.
func TestParquetColumnsChunkLargerThanBuffer(t *testing.T) {
	const (
		rows = 4096
		dims = 256
	)
	fp, rowGroups := writeTestParquet(t, rows, dims, int64(1)<<40, 1<<16)
	if rowGroups != 1 {
		t.Fatalf("expected a single row group, got %d", rowGroups)
	}
	mmapped := mapTestParquet(t, fp)

	// Confirm the premise: the chunk really is larger than the buffer we clamp
	// to, so this test would catch a clamp that truncated reads.
	unclamped, err := reader.NewParquetColumnReader(&mappedParquetFile{data: mmapped.Data}, 1)
	if err != nil {
		t.Fatal(err)
	}
	var embChunkSize int64
	for _, chunk := range unclamped.Footer.GetRowGroups()[0].GetColumns() {
		embChunkSize = max(embChunkSize, chunk.MetaData.GetTotalCompressedSize())
	}
	if embChunkSize <= parquetTransportBufferSize {
		t.Fatalf("largest column chunk is %d bytes, want > %d", embChunkSize, parquetTransportBufferSize)
	}

	pr, err := openParquetColumnReader(mmapped)
	if err != nil {
		t.Fatal(err)
	}
	readTestColumns(t, pr, rows, dims)
}

// TestParquetMappedFileOpenSharesBytes checks that Open hands out an
// independent cursor over the same backing array rather than a copy. This is
// the whole point of mappedParquetFile: reader.NewColumnBuffer calls Open once
// per column, and buffer.BufferFile.Open copies the entire file.
func TestParquetMappedFileOpenSharesBytes(t *testing.T) {
	data := []byte("PAR1 and then some payload bytes")
	f := &mappedParquetFile{data: data}
	if _, err := f.Seek(4, 0); err != nil {
		t.Fatal(err)
	}

	opened, err := f.Open("")
	if err != nil {
		t.Fatal(err)
	}
	shared, ok := opened.(*mappedParquetFile)
	if !ok {
		t.Fatalf("Open returned %T, want *mappedParquetFile", opened)
	}
	if &shared.data[0] != &data[0] {
		t.Error("Open copied the backing array instead of sharing it")
	}
	if shared.pos != 0 {
		t.Errorf("Open returned a cursor at %d, want an independent cursor at 0", shared.pos)
	}
	if f.pos != 4 {
		t.Errorf("Open moved the original cursor to %d, want 4", f.pos)
	}
}

// peakHeapReadingColumn reads the embedding column of a freshly written test
// file end to end, returning the peak heap held while doing so (net of the
// baseline before the read) along with the file's size on disk.
func peakHeapReadingColumn(
	tb testing.TB,
	rows, dims int,
	open func(*MemoryMappedFile) (*reader.ParquetReader, error),
) (peak, fileSize uint64) {
	tb.Helper()

	// One row group holding the whole file, so any per-row-group or per-file
	// buffering shows up as a multiple of the file size.
	fp, rowGroups := writeTestParquet(tb, rows, dims, int64(1)<<40, 1<<16)
	if rowGroups != 1 {
		tb.Fatalf("expected a single row group, got %d", rowGroups)
	}
	info, err := os.Stat(fp)
	if err != nil {
		tb.Fatal(err)
	}

	pr, err := open(mapTestParquet(tb, fp))
	if err != nil {
		tb.Fatal(err)
	}

	var before, during runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	chunkRows := parquetChunkRows(int64(dims))
	for rem := pr.GetNumRows(); rem > 0; {
		batch := min(chunkRows, rem)
		if _, _, _, err := pr.ReadColumnByIndex(testEmbColumn, batch); err != nil {
			tb.Fatal(err)
		}
		runtime.ReadMemStats(&during)
		peak = max(peak, during.HeapAlloc)
		rem -= batch
	}
	return peak - min(peak, before.HeapAlloc), uint64(info.Size())
}

// TestParquetColumnMemoryDoesNotScaleWithFile is a regression test for the
// reader holding a whole file, or a whole row group's column chunk, in memory
// at once. buffer.BufferFile.Open copies the entire source file, and
// source.ConvertToThriftReader sizes its thrift read and write buffers to the
// column chunk's compressed size. Together those made a hybrid benchmark over
// the MSMarco shards resident by ~27GB per file in flight.
//
// What the fix guarantees is not a particular number of bytes but that the
// number stops depending on how big the file is: the reader should hold two
// transport buffers and one decoded batch of parquetChunkValues values, all of
// which are fixed. So read a small file and a 4x larger one and require the
// peak not to grow with them.
func TestParquetColumnMemoryDoesNotScaleWithFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}
	const dims = 512

	smallPeak, smallSize := peakHeapReadingColumn(t, 2048, dims, openParquetColumnReader)
	largePeak, largeSize := peakHeapReadingColumn(t, 8192, dims, openParquetColumnReader)
	t.Logf("small: %d bytes on disk, %d peak heap", smallSize, smallPeak)
	t.Logf("large: %d bytes on disk, %d peak heap", largeSize, largePeak)

	if largeSize < 3*smallSize {
		t.Fatalf("expected the large file to be much bigger: %d vs %d bytes", largeSize, smallSize)
	}
	// Generous factor: the property under test is that this ratio is ~1x rather
	// than tracking the 4x growth in file size.
	if largePeak > 2*smallPeak {
		t.Errorf("peak heap grew from %d to %d bytes as the file grew %.1fx; "+
			"the reader is holding something proportional to the file",
			smallPeak, largePeak, float64(largeSize)/float64(smallSize))
	}
}

// benchmarkParquetColumn reads the embedding column of a test file end to end.
// Peak resident memory is covered by TestParquetColumnMemoryDoesNotScaleWithFile;
// this measures what that costs in throughput and allocation.
func benchmarkParquetColumn(b *testing.B, dims int, open func(*MemoryMappedFile) (*reader.ParquetReader, error)) {
	const rows = 8192
	fp, _ := writeTestParquet(b, rows, dims, int64(1)<<40, 1<<16)
	info, err := os.Stat(fp)
	if err != nil {
		b.Fatal(err)
	}
	mmapped := mapTestParquet(b, fp)
	chunkRows := parquetChunkRows(int64(dims))

	b.SetBytes(info.Size())
	b.ReportAllocs()
	for b.Loop() {
		pr, err := open(mmapped)
		if err != nil {
			b.Fatal(err)
		}
		for rem := pr.GetNumRows(); rem > 0; {
			batch := min(chunkRows, rem)
			if _, _, _, err := pr.ReadColumnByIndex(testEmbColumn, batch); err != nil {
				b.Fatal(err)
			}
			rem -= batch
		}
	}
}

// BenchmarkParquetColumnRead measures the fixed reader.
func BenchmarkParquetColumnRead(b *testing.B) {
	for _, dims := range []int{256, 1024} {
		b.Run(fmt.Sprintf("dims=%d", dims), func(b *testing.B) {
			benchmarkParquetColumn(b, dims, openParquetColumnReader)
		})
	}
}

// BenchmarkParquetColumnReadUnfixed measures the pre-fix path — a copying Open
// and chunk-sized transport buffers — as a baseline for the numbers above.
func BenchmarkParquetColumnReadUnfixed(b *testing.B) {
	open := func(mmapped *MemoryMappedFile) (*reader.ParquetReader, error) {
		return reader.NewParquetColumnReader(copyingParquetFile{&mappedParquetFile{data: mmapped.Data}}, 1)
	}
	for _, dims := range []int{256, 1024} {
		b.Run(fmt.Sprintf("dims=%d", dims), func(b *testing.B) {
			benchmarkParquetColumn(b, dims, open)
		})
	}
}

// copyingParquetFile reproduces buffer.BufferFile's copy-on-Open behaviour,
// without depending on that package staying broken.
type copyingParquetFile struct{ *mappedParquetFile }

func (f copyingParquetFile) Open(string) (source.ParquetFile, error) {
	data := make([]byte, len(f.mappedParquetFile.data))
	copy(data, f.mappedParquetFile.data)
	return copyingParquetFile{&mappedParquetFile{data: data}}, nil
}
