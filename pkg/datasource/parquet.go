package datasource

import (
	"bytes"
	"fmt"
	"io"
	"iter"

	"github.com/parquet-go/parquet-go"
)

// parquetFile reads columns out of a memory-mapped parquet file, one page at a time.
type parquetFile struct {
	file *parquet.File
}

// openParquetFile reads the metadata of a memory-mapped parquet file.
func openParquetFile(mmapped *MemoryMappedFile) (*parquetFile, error) {
	f, err := parquet.OpenFile(bytes.NewReader(mmapped.Data), int64(len(mmapped.Data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	return &parquetFile{file: f}, nil
}

func (f *parquetFile) numRows() int64 { return f.file.NumRows() }

// checkColumn verifies that index names a leaf column of the file.
func (f *parquetFile) checkColumn(index int) error {
	if n := len(f.file.Schema().Columns()); index < 0 || index >= n {
		return fmt.Errorf("parquet column index %d out of range [0, %d)", index, n)
	}
	return nil
}

// pages calls visit with each page of a leaf column in row order, stopping early if visit returns false.
func (f *parquetFile) pages(index int, visit func(parquet.Page) bool) error {
	for _, rowGroup := range f.file.RowGroups() {
		pages := rowGroup.ColumnChunks()[index].Pages()
		for {
			page, err := pages.ReadPage()
			if err == io.EOF {
				break
			} else if err != nil {
				pages.Close()
				return fmt.Errorf("reading parquet column %d: %w", index, err)
			}
			ok := visit(page)
			parquet.Release(page)
			if !ok {
				pages.Close()
				return nil
			}
		}
		if err := pages.Close(); err != nil {
			return fmt.Errorf("reading parquet column %d: %w", index, err)
		}
	}
	return nil
}

// float32Vectors iterates fixed-width vectors from a repeated float column, taking leaf
// values as a flat stream because data page v1 does not align pages to row boundaries.
func (f *parquetFile) float32Vectors(index int, dims int) (iter.Seq[[]float32], error) {
	if err := f.checkColumn(index); err != nil {
		return nil, err
	}
	return func(yield func([]float32) bool) {
		var scratch []float32
		var stopped bool
		vector := make([]float32, 0, dims)
		err := f.pages(index, func(page parquet.Page) bool {
			for _, v := range pageFloats(page, &scratch) {
				vector = append(vector, v)
				if len(vector) == dims {
					if !yield(vector) {
						stopped = true
						return false
					}
					vector = make([]float32, 0, dims)
				}
			}
			return true
		})
		if err != nil {
			panic(err)
		}
		// Only a run to exhaustion says anything about the column's total length.
		if !stopped && len(vector) != 0 {
			panic(fmt.Errorf("reading parquet column %d: trailing partial vector of %d/%d values", index, len(vector), dims))
		}
	}, nil
}

// pageFloats returns a page's leaf values, resolving indexes through the dictionary page if there is one.
func pageFloats(page parquet.Page, scratch *[]float32) []float32 {
	data := page.Data()
	dict := page.Dictionary()
	if dict == nil {
		return data.Float()
	}
	entryData := dict.Page().Data()
	entries := entryData.Float()
	values := (*scratch)[:0]
	for _, i := range data.Int32() {
		if int(i) >= len(entries) {
			panic(fmt.Errorf("dictionary index %d out of range [0, %d)", i, len(entries)))
		}
		values = append(values, entries[i])
	}
	*scratch = values
	return values
}

// strings iterates the values of a string column, one per row.
func (f *parquetFile) strings(index int) (iter.Seq[string], error) {
	if err := f.checkColumn(index); err != nil {
		return nil, err
	}
	return func(yield func(string) bool) {
		err := f.pages(index, func(page parquet.Page) bool {
			pageData := page.Data()
			valueData := pageData
			var indexes []int32
			// A dictionary-encoded page holds indexes; the values live in the dictionary page.
			if dict := page.Dictionary(); dict != nil {
				valueData = dict.Page().Data()
				indexes = pageData.Int32()
			}
			data, offsets := valueData.ByteArray()
			at := func(i int) string {
				if i+1 >= len(offsets) {
					panic(fmt.Errorf("reading parquet column %d: value %d out of range", index, i))
				}
				return string(data[offsets[i]:offsets[i+1]])
			}
			if indexes == nil {
				for i := 0; i+1 < len(offsets); i++ {
					if !yield(at(i)) {
						return false
					}
				}
				return true
			}
			for _, i := range indexes {
				if !yield(at(int(i))) {
					return false
				}
			}
			return true
		})
		if err != nil {
			panic(err)
		}
	}, nil
}
