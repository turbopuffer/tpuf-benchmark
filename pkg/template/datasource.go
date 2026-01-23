package template

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"syscall"
)

// Datasource is a source of template data, i.e. providing real data for the templates.
type Datasource interface {
	NewIDSource() IDSource
	NewVectorSource() VectorSource
	NewTextSource() TextSource
}

// An IDSource can yield unique uint64 IDs.
type IDSource interface {
	Id() uint64
}

// MonotonicIDSource is a simple ID source that yields monotonically increasing IDs.
type MonotonicIDSource struct {
	current uint64
}

var _ IDSource = (*MonotonicIDSource)(nil)

func (s *MonotonicIDSource) Id() uint64 {
	id := s.current
	s.current++
	return id
}

// A VectorSource can yield vectors of a given dimension.
type VectorSource interface {
	Vector(dims int) ([]float32, error)
}

// RandomVectorSource is a vector source that generates random vectors.
type RandomVectorSource struct {
	rng *rand.Rand
}

var _ VectorSource = (*RandomVectorSource)(nil)

// NewRandomVectorSource creates a new random vector source with the given seed.
func NewRandomVectorSource(seed uint64, dims int) *RandomVectorSource {
	return &RandomVectorSource{
		rng: rand.New(rand.NewPCG(seed, 0)),
	}
}

func (s *RandomVectorSource) Vector(dims int) ([]float32, error) {
	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions %d, must be positive", dims)
	}
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = s.rng.Float32()
	}
	return vec, nil
}

type TextSource interface {
	Document() (string, error)
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

// TruncateOrExpandVector truncates or expands a vector to the given dimensions.
// Does this by either truncating, extending with repeated values, or returning as is
// if the vector is already of the desired length.
func TruncateOrExpandVector(vector []float32, dims int) []float32 {
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

// CleanText cleans a text string to be safely embedded in a JSON string.
func CleanText(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)    // Double up backslashes
	s = strings.ReplaceAll(s, "\u0000", "") // Remove null bytes
	s = strings.ReplaceAll(s, "\r", `\r`)   // Escape carriage returns
	s = strings.ReplaceAll(s, "\n", `\n`)   // Escape newlines
	s = strings.ReplaceAll(s, "\t", `\t`)   // Escape tabs
	s = strings.ReplaceAll(s, `"`, `\"`)    // Escape quotes
	return s
}
