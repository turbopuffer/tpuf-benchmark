package datasource

import (
	"fmt"
	"io"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// parquetTransportBufferSize caps the buffer parquet-go allocates per column
// chunk. See clampParquetTransportBuffers.
const parquetTransportBufferSize = 1 << 20 // 1 MiB

// parquetChunkValues bounds how many values a single ReadColumnByIndex call
// materializes. parquet-go decodes into a []interface{}, so every value costs a
// slice entry plus a boxing allocation; batching by value count rather than by
// row keeps that cost the same for a 1024-dimensional embedding column as for a
// scalar one.
//
// Smaller is better on both axes here, up to a point: a batch spanning fewer
// pages means less repeated growing of the merged table. Measured over the
// 32Ki-8Mi range, 32Ki was both the fastest and by far the leanest (see
// BenchmarkParquetColumnRead).
const parquetChunkValues = 32 << 10

// openParquetColumnReader opens a column reader over a memory-mapped parquet
// file, without either of the two whole-file/whole-row-group copies parquet-go
// would otherwise make. See mappedParquetFile and clampParquetTransportBuffers.
func openParquetColumnReader(mmapped *MemoryMappedFile) (*reader.ParquetReader, error) {
	pr, err := reader.NewParquetColumnReader(&mappedParquetFile{data: mmapped.Data}, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	clampParquetTransportBuffers(pr.Footer)
	return pr, nil
}

// clampParquetTransportBuffers caps the TotalCompressedSize recorded for every
// column chunk in the footer.
//
// reader.ColumnBufferType.NextRowGroup passes that field to
// source.ConvertToThriftReader as the size of the chunk's thrift transport, and
// thrift.NewTBufferedTransport allocates a bufio read buffer *and* a write
// buffer of exactly that size. For the MSMarco embedding column that is tens of
// gigabytes per row group, twice over, and the read buffer is then filled in a
// single copy off the mapped file. The field is not used for anything else on
// the column-reader path, so shrinking it just makes the buffering sane; pages
// still stream through the smaller buffer correctly.
func clampParquetTransportBuffers(footer *parquet.FileMetaData) {
	for _, rowGroup := range footer.GetRowGroups() {
		for _, chunk := range rowGroup.GetColumns() {
			if chunk.MetaData.TotalCompressedSize > parquetTransportBufferSize {
				chunk.MetaData.TotalCompressedSize = parquetTransportBufferSize
			}
		}
	}
}

// parquetChunkRows returns the number of rows to request per read for a column
// with the given number of values per row.
func parquetChunkRows(valuesPerRow int64) int64 {
	return max(1, parquetChunkValues/valuesPerRow)
}

// mappedParquetFile is a read-only source.ParquetFile over a memory-mapped
// file.
//
// It exists because buffer.BufferFile.Open is NewBufferFileFromBytes, which
// copies the whole file onto the heap — reader.NewColumnBuffer calls Open once
// per column, so constructing the BufferFile with NewBufferFileFromBytesNoAlloc
// does not actually avoid the copy. Open below shares the mapped bytes and only
// gives the caller an independent cursor over them.
type mappedParquetFile struct {
	data []byte
	pos  int
}

var _ source.ParquetFile = (*mappedParquetFile)(nil)

func (f *mappedParquetFile) Open(string) (source.ParquetFile, error) {
	return &mappedParquetFile{data: f.data}, nil
}

func (f *mappedParquetFile) Clone() (source.ParquetFile, error) {
	return &mappedParquetFile{data: f.data}, nil
}

func (f *mappedParquetFile) Seek(offset int64, whence int) (int64, error) {
	pos := int64(f.pos)
	switch whence {
	case io.SeekStart:
		pos = offset
	case io.SeekCurrent:
		pos += offset
	case io.SeekEnd:
		pos = int64(len(f.data)) + offset
	default:
		return int64(f.pos), fmt.Errorf("invalid whence %d", whence)
	}
	if pos < 0 {
		return int64(f.pos), fmt.Errorf("cannot seek to a negative offset (%d)", pos)
	}
	f.pos = int(min(pos, int64(len(f.data))))
	return int64(f.pos), nil
}

func (f *mappedParquetFile) Read(p []byte) (int, error) {
	n := copy(p, f.data[f.pos:])
	f.pos += n
	if f.pos == len(f.data) {
		return n, io.EOF
	}
	return n, nil
}

func (f *mappedParquetFile) Close() error { return nil }

func (f *mappedParquetFile) Create(string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("mappedParquetFile is read-only")
}

func (f *mappedParquetFile) Write([]byte) (int, error) {
	return 0, fmt.Errorf("mappedParquetFile is read-only")
}
