// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

const (
	latencyTraceBufferSize           = 8192
	defaultLatencyTraceFlushBatch    = 60
	defaultLatencyTraceFlushInterval = 250 * time.Millisecond
)

type latencyTraceConfig struct {
	enabled    bool
	outputPath string
	flushBatch int
	flushEvery time.Duration
}

type latencyTraceSink struct {
	output   io.WriteCloser
	writer   *bufio.Writer
	events   chan TraceEvent
	shutdown chan struct{}
	stopped  atomic.Bool
	dropped  atomic.Uint64
	wg       sync.WaitGroup
	path     string
	flushBatch int
	flushEvery time.Duration
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}

func loadLatencyTraceConfig() latencyTraceConfig {
	cfg := latencyTraceConfig{
		enabled:    true,
		outputPath: strings.TrimSpace(os.Getenv("MILVUS_LATENCY_TRACE_OUTPUT")),
		flushBatch: defaultLatencyTraceFlushBatch,
		flushEvery: defaultLatencyTraceFlushInterval,
	}

	if raw := strings.TrimSpace(os.Getenv("MILVUS_LATENCY_TRACE_ENABLED")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			log.Warn("invalid MILVUS_LATENCY_TRACE_ENABLED value, keep default", zap.String("value", raw), zap.Error(err))
		} else {
			cfg.enabled = enabled
		}
	}

	if raw := strings.TrimSpace(os.Getenv("MILVUS_LATENCY_TRACE_FLUSH_BATCH")); raw != "" {
		flushBatch, err := strconv.Atoi(raw)
		if err != nil || flushBatch < 0 {
			log.Warn("invalid MILVUS_LATENCY_TRACE_FLUSH_BATCH value, keep default",
				zap.String("value", raw),
				zap.Error(err),
			)
		} else {
			cfg.flushBatch = flushBatch
		}
	}

	if raw := strings.TrimSpace(os.Getenv("MILVUS_LATENCY_TRACE_FLUSH_INTERVAL_MS")); raw != "" {
		flushIntervalMs, err := strconv.Atoi(raw)
		if err != nil || flushIntervalMs < 0 {
			log.Warn("invalid MILVUS_LATENCY_TRACE_FLUSH_INTERVAL_MS value, keep default",
				zap.String("value", raw),
				zap.Error(err),
			)
		} else {
			cfg.flushEvery = time.Duration(flushIntervalMs) * time.Millisecond
		}
	}

	return cfg
}

func defaultLatencyTraceOutput() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("milvus_latency_trace_%d.jsonl", os.Getpid()))
}

func newLatencyTracer(cfg latencyTraceConfig) *LatencyTracer {
	tracer := &LatencyTracer{
		enabled:    cfg.enabled,
		outputPath: cfg.outputPath,
	}

	if !cfg.enabled {
		return tracer
	}

	sink, path, err := newLatencyTraceSink(cfg)
	if err != nil {
		log.Warn("failed to initialize latency trace sink, falling back to temp file",
			zap.String("requested_output", cfg.outputPath),
			zap.Error(err))
		fallbackCfg := cfg
		fallbackCfg.outputPath = ""
		sink, path, err = newLatencyTraceSink(fallbackCfg)
		if err != nil {
			log.Error("failed to initialize latency trace sink", zap.Error(err))
			tracer.enabled = false
			return tracer
		}
	}

	tracer.outputPath = path
	tracer.sink = sink
	return tracer
}

func newLatencyTraceSink(cfg latencyTraceConfig) (*latencyTraceSink, string, error) {
	target := strings.TrimSpace(cfg.outputPath)

	var output io.WriteCloser
	switch strings.ToLower(target) {
	case "":
		target = defaultLatencyTraceOutput()
		file, err := createLatencyTraceFile(target)
		if err != nil {
			return nil, "", err
		}
		output = file
	case "stdout":
		target = "stdout"
		output = nopWriteCloser{Writer: os.Stdout}
	case "stderr":
		target = "stderr"
		output = nopWriteCloser{Writer: os.Stderr}
	default:
		file, err := createLatencyTraceFile(target)
		if err != nil {
			return nil, "", err
		}
		output = file
	}

	sink := &latencyTraceSink{
		output:     output,
		writer:     bufio.NewWriterSize(output, 1<<20),
		events:     make(chan TraceEvent, latencyTraceBufferSize),
		shutdown:   make(chan struct{}),
		path:       target,
		flushBatch: cfg.flushBatch,
		flushEvery: cfg.flushEvery,
	}
	sink.wg.Add(1)
	go sink.run()

	return sink, target, nil
}

func createLatencyTraceFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

func (s *latencyTraceSink) enqueue(event TraceEvent) {
	if s == nil || s.stopped.Load() {
		return
	}

	select {
	case s.events <- event:
	default:
		s.dropped.Add(1)
	}
}

func (s *latencyTraceSink) run() {
	defer s.wg.Done()

	var ticker *time.Ticker
	var flushC <-chan time.Time
	if s.flushEvery > 0 {
		ticker = time.NewTicker(s.flushEvery)
		flushC = ticker.C
		defer ticker.Stop()
	}

	batch := 0
	flush := func() bool {
		if batch == 0 {
			return true
		}
		if err := s.writer.Flush(); err != nil {
			log.Warn("failed to flush latency trace sink", zap.String("output", s.path), zap.Error(err))
			s.stopped.Store(true)
			return false
		}
		batch = 0
		return true
	}
	write := func(event TraceEvent) bool {
		if err := writeTraceEvent(s.writer, event); err != nil {
			log.Warn("failed to write latency trace event", zap.String("output", s.path), zap.Error(err))
			s.stopped.Store(true)
			return false
		}
		batch++
		if s.flushBatch > 0 && batch >= s.flushBatch {
			return flush()
		}
		return true
	}

	for {
		select {
		case <-s.shutdown:
			for {
				select {
				case event := <-s.events:
					if !write(event) {
						_ = s.closeOutput()
						return
					}
				default:
					flush()
					_ = s.closeOutput()
					return
				}
			}
		case event := <-s.events:
			if !write(event) {
				_ = s.closeOutput()
				return
			}
		case <-flushC:
			if !flush() {
				_ = s.closeOutput()
				return
			}
		}
	}
}

func (s *latencyTraceSink) closeOutput() error {
	if s == nil || s.output == nil {
		return nil
	}

	if err := s.writer.Flush(); err != nil {
		log.Warn("failed to flush latency trace sink on close", zap.String("output", s.path), zap.Error(err))
	}
	if syncer, ok := s.output.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			log.Warn("failed to sync latency trace sink", zap.String("output", s.path), zap.Error(err))
		}
	}
	if err := s.output.Close(); err != nil {
		log.Warn("failed to close latency trace sink", zap.String("output", s.path), zap.Error(err))
	}
	return nil
}

func (s *latencyTraceSink) Close() {
	if s == nil {
		return
	}

	if s.stopped.CompareAndSwap(false, true) {
		close(s.shutdown)
	}
	s.wg.Wait()
	if dropped := s.dropped.Load(); dropped > 0 {
		log.Warn("latency trace events were dropped because the async queue was full",
			zap.String("output", s.path),
			zap.Uint64("dropped", dropped),
		)
	}
}

func writeTraceEvent(w io.Writer, event TraceEvent) error {
	if _, err := fmt.Fprintf(
		w,
		"[LATENCY_TRACE] trace_id=%s operation=%s stage=%s component=%s duration_ms=%.2f",
		event.TraceID, event.Operation, event.Stage, event.Component, event.Duration,
	); err != nil {
		return err
	}

	for key, value := range event.Metadata {
		if _, err := fmt.Fprintf(w, " %s=%v", key, value); err != nil {
			return err
		}
	}

	_, err := fmt.Fprintln(w)
	return err
}
