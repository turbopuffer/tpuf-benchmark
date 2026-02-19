package output

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Logger writes timestamped log lines to an io.Writer. All methods are safe
// for concurrent use. Every line is prefixed with a timestamp and the current
// stage in brackets, e.g. "15:04:05 [initializing] ...".
type Logger struct {
	mu    sync.Mutex
	out   io.Writer
	stage Stage
}

// NewLogger creates a new Logger.
func NewLogger(out io.Writer) *Logger {
	return &Logger{out: out}
}

// prefix returns "HH:MM:SS [stage]" for use at the start of every log line.
// Must be called with l.mu held.
func (l *Logger) prefix() string {
	return time.Now().Format("15:04:05") + " [" + l.stage.String() + "]"
}

func (l *Logger) NextStage(stage Stage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stage = stage
	fmt.Fprintf(l.out, "%s\n", l.prefix())
}

func (l *Logger) Task(name string, total int) *TaskProgress {
	return &TaskProgress{
		logger: l,
		name:   name,
		total:  total,
	}
}

func (l *Logger) Detailf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.out, "%s "+format+"\n", append([]any{l.prefix()}, args...)...)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.out, "%s error: "+format+"\n", append([]any{l.prefix()}, args...)...)
}

func (l *Logger) OnDownload(sourceURL string, downloadedBytes, totalBytes int64) {
	if downloadedBytes == totalBytes {
		l.mu.Lock()
		defer l.mu.Unlock()
		fmt.Fprintf(l.out, "%s download complete: %s\n", l.prefix(), sourceURL)
	}
}

func (l *Logger) OnLoadCachedFile(sourceURL string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.out, "%s loaded from cache: %s\n", l.prefix(), sourceURL)
}

// WriteLines calls fn with the current log prefix and writer while holding
// the logger's mutex. This allows callers to write multi-line output
// atomically with a consistent prefix.
func (l *Logger) WriteLines(fn func(prefix string, w io.Writer)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn(l.prefix(), l.out)
}

// TaskProgress tracks progress of a named task.
type TaskProgress struct {
	logger  *Logger
	name    string
	total   int
	current atomic.Int64
}

func (t *TaskProgress) Advance(n int, format string, args ...any) {
	current := t.current.Add(int64(n))
	if format != "" {
		t.logger.mu.Lock()
		defer t.logger.mu.Unlock()
		fmt.Fprintf(t.logger.out, "%s [%s] %.1f%% %s\n", t.logger.prefix(), t.name,
			float64(current)/float64(t.total)*100, fmt.Sprintf(format, args...))
	}
}

func (t *TaskProgress) SetProgression(v int, format string, args ...any) {
	t.current.Store(int64(v))
	if format != "" {
		t.logger.mu.Lock()
		defer t.logger.mu.Unlock()
		fmt.Fprintf(t.logger.out, "%s [%s] %s\n", t.logger.prefix(), t.name, fmt.Sprintf(format, args...))
	}
}

func (t *TaskProgress) Detailf(format string, args ...any) {
	t.logger.Detailf(format, args...)
}

func (t *TaskProgress) Errorf(format string, args ...any) {
	t.logger.Errorf(format, args...)
}

// Stage represents a high-level benchmark stage.
type Stage int

const (
	StageInit Stage = iota
	StageSettingUpNamespaces
	StageUpserting
	StageIndexing
	StagePurgingCache
	StageWarmingCache
	StageBenchmark
	StageDone
)

func (s Stage) String() string {
	switch s {
	case StageInit:
		return "initializing"
	case StageSettingUpNamespaces:
		return "namespace setup"
	case StageUpserting:
		return "upserting"
	case StageIndexing:
		return "indexing"
	case StagePurgingCache:
		return "purging cache"
	case StageWarmingCache:
		return "warming cache"
	case StageBenchmark:
		return "benchmark"
	case StageDone:
		return "done"
	default:
		return "unknown"
	}
}
