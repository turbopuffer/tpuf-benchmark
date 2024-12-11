package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

var logLevels = map[string]slog.Level{
	"DEBUG": slog.LevelDebug,
	"INFO":  slog.LevelInfo,
	"WARN":  slog.LevelWarn,
	"ERROR": slog.LevelError,
}

func newLogger() *slog.Logger {
	var logLevel slog.Level
	if level, ok := logLevels[os.Getenv("LOG_LEVEL")]; ok {
		logLevel = level
	}
	handler := &loggingHandler{
		level: logLevel,
	}
	return slog.New(handler)
}

type loggingHandler struct {
	level slog.Level
	attrs []slog.Attr
}

var _ slog.Handler = (*loggingHandler)(nil)

func (lh *loggingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= lh.level
}

func (lh *loggingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	combined := make([]slog.Attr, 0, len(lh.attrs)+len(attrs))
	copy(combined, lh.attrs)
	for _, attr := range attrs {
		if !isDefaultAttr(attr) {
			combined = append(combined, attr)
		}
	}
	return &loggingHandler{
		level: lh.level,
		attrs: combined,
	}
}

func (lh *loggingHandler) WithGroup(_ string) slog.Handler {
	panic("not implemented")
}

const queryTemperatureAttrKey = "__query_temperature"

func (lh *loggingHandler) Handle(_ context.Context, record slog.Record) error {
	var builder strings.Builder

	if !record.Time.IsZero() {
		builder.WriteRune('[')
		builder.WriteString(record.Time.Format(time.RFC3339))
		builder.WriteString("] ")
	}

	switch record.Level {
	case slog.LevelWarn:
		builder.WriteString("[WARN] ")
	case slog.LevelError:
		builder.WriteString("[ERROR] ")
	default:
	}

	var queryTempStr string
	for _, attr := range lh.attrs {
		if attr.Key == queryTemperatureAttrKey {
			queryTempStr = attr.Value.String()
			break
		}
	}
	if queryTempStr == "" {
		record.Attrs(func(a slog.Attr) bool {
			if a.Key == queryTemperatureAttrKey {
				queryTempStr = a.Value.String()
				return false
			}
			return true
		})
	}
	if qt := queryTemperature(queryTempStr); qt.valid() {
		switch qt {
		case queryTemperatureCold: // cyan (36)
			builder.WriteString("\x1b[36m")
		case queryTemperatureWarm: // yellow (33)
			builder.WriteString("\x1b[33m")
		case queryTemperatureHot: // red (31)
			builder.WriteString("\x1b[31m")
		default:
			panic("unreachable")
		}
		builder.WriteRune('[')
		builder.WriteString(string(qt))
		builder.WriteString("]\x1b[0m ")
	}

	builder.WriteString(record.Message)

	for _, attr := range lh.attrs {
		if attr.Key == queryTemperatureAttrKey {
			continue
		}
		builder.WriteRune(' ')
		builder.WriteString(attr.Key)
		builder.WriteString("=")
		builder.WriteString(attr.Value.String())
	}

	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == queryTemperatureAttrKey {
			return true
		}
		builder.WriteRune(' ')
		builder.WriteString(attr.Key)
		builder.WriteString("=")
		builder.WriteString(attr.Value.String())
		return true
	})

	fmt.Println(builder.String())

	return nil
}

func isDefaultAttr(attr slog.Attr) bool {
	return attr.Equal(slog.Attr{})
}
