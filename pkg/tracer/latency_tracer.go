package tracer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// TraceEvent represents a single tracing event
type TraceEvent struct {
	TraceID   string    `json:"trace_id"`
	RequestID string    `json:"request_id,omitempty"`
	Operation string    `json:"operation"` // Insert, Upsert, Search, Query, LoadCollection, etc.
	Stage     string    `json:"stage"`     // serialize, mq_produce, deserialize, s3_write, etc.
	Component string    `json:"component"` // proxy, datanode, querynode, querycoord
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Duration  float64   `json:"duration_ms"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// LatencyTracer manages distributed tracing for latency analysis
type LatencyTracer struct {
	enabled    bool
	outputFile *os.File
	logger     *zap.Logger
	mu         sync.Mutex
}

var (
	globalTracer *LatencyTracer
	once         sync.Once
)

// InitGlobalTracer initializes the global tracer
func InitGlobalTracer(enabled bool, outputPath string, logger *zap.Logger) error {
	var err error
	once.Do(func() {
		globalTracer = &LatencyTracer{
			enabled: enabled,
			logger:  logger,
		}
		if enabled && outputPath != "" {
			globalTracer.outputFile, err = os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		}
	})
	return err
}

// GetGlobalTracer returns the global tracer instance
func GetGlobalTracer() *LatencyTracer {
	if globalTracer == nil {
		// Return a disabled tracer if not initialized
		// This ensures the code doesn't crash if tracer is not initialized
		once.Do(func() {
			globalTracer = &LatencyTracer{enabled: false}
		})
	}
	return globalTracer
}

// GenerateTraceID generates a new trace ID
func GenerateTraceID() string {
	return uuid.New().String()
}

// GetTraceIDFromContext retrieves trace ID from context
func GetTraceIDFromContext(ctx context.Context) string {
	if traceID, ok := ctx.Value("traceID").(string); ok {
		return traceID
	}
	return ""
}

// SetTraceIDToContext adds trace ID to context
func SetTraceIDToContext(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, "traceID", traceID)
}

// SpanContext holds timing information for a span
type SpanContext struct {
	TraceID   string
	RequestID string
	Operation string
	Stage     string
	Component string
	StartTime time.Time
	Metadata  map[string]interface{}
}

// StartSpan starts a new tracing span
func (t *LatencyTracer) StartSpan(traceID, requestID, operation, stage, component string, metadata map[string]interface{}) *SpanContext {
	if !t.enabled {
		return nil
	}
	return &SpanContext{
		TraceID:   traceID,
		RequestID: requestID,
		Operation: operation,
		Stage:     stage,
		Component: component,
		StartTime: time.Now(),
		Metadata:  metadata,
	}
}

// EndSpan ends a tracing span and records the event
func (t *LatencyTracer) EndSpan(span *SpanContext) {
	if !t.enabled || span == nil {
		return
	}

	endTime := time.Now()
	duration := endTime.Sub(span.StartTime).Milliseconds()

	event := TraceEvent{
		TraceID:   span.TraceID,
		RequestID: span.RequestID,
		Operation: span.Operation,
		Stage:     span.Stage,
		Component: span.Component,
		StartTime: span.StartTime,
		EndTime:   endTime,
		Duration:  float64(duration),
		Metadata:  span.Metadata,
	}

	t.recordEvent(event)
}

// RecordEvent records a tracing event directly
func (t *LatencyTracer) RecordEvent(traceID, requestID, operation, stage, component string, duration time.Duration, metadata map[string]interface{}) {
	if !t.enabled {
		return
	}

	now := time.Now()
	event := TraceEvent{
		TraceID:   traceID,
		RequestID: requestID,
		Operation: operation,
		Stage:     stage,
		Component: component,
		StartTime: now.Add(-duration),
		EndTime:   now,
		Duration:  float64(duration.Milliseconds()),
		Metadata:  metadata,
	}

	t.recordEvent(event)
}

func (t *LatencyTracer) recordEvent(event TraceEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Write to file as JSON
	if t.outputFile != nil {
		data, err := json.Marshal(event)
		if err == nil {
			t.outputFile.Write(data)
			t.outputFile.Write([]byte("\n"))
		}
	}

	// Also log for debugging
	if t.logger != nil {
		t.logger.Info("latency_trace",
			zap.String("trace_id", event.TraceID),
			zap.String("operation", event.Operation),
			zap.String("stage", event.Stage),
			zap.String("component", event.Component),
			zap.Float64("duration_ms", event.Duration),
		)
	}
}

// Close closes the tracer and flushes data
func (t *LatencyTracer) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.outputFile != nil {
		return t.outputFile.Close()
	}
	return nil
}

// Helper functions for common operations

// TraceInsert traces an insert operation span
func TraceInsert(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	tracer := GetGlobalTracer()
	return tracer.StartSpan(traceID, "", "Insert", stage, getCurrentComponent(), metadata)
}

// TraceSearch traces a search operation span
func TraceSearch(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	tracer := GetGlobalTracer()
	return tracer.StartSpan(traceID, "", "Search", stage, getCurrentComponent(), metadata)
}

// TraceQuery traces a query operation span
func TraceQuery(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	tracer := GetGlobalTracer()
	return tracer.StartSpan(traceID, "", "Query", stage, getCurrentComponent(), metadata)
}

// TraceLoadCollection traces a load collection operation span
func TraceLoadCollection(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	tracer := GetGlobalTracer()
	return tracer.StartSpan(traceID, "", "LoadCollection", stage, getCurrentComponent(), metadata)
}

// EndTrace is a convenience function to end a span
func EndTrace(span *SpanContext) {
	if span != nil {
		GetGlobalTracer().EndSpan(span)
	}
}

// getCurrentComponent tries to detect current component from environment or module
func getCurrentComponent() string {
	// This is a placeholder - in real implementation, you'd detect this from
	// environment variables or module initialization
	// For now, we'll detect based on package usage context
	return "unknown"
}

// SetComponent allows explicitly setting component name for a span
func (s *SpanContext) SetComponent(component string) {
	if s != nil {
		s.Component = component
	}
}

// AddMetadata adds metadata to a span
func (s *SpanContext) AddMetadata(key string, value interface{}) {
	if s != nil {
		if s.Metadata == nil {
			s.Metadata = make(map[string]interface{})
		}
		s.Metadata[key] = value
	}
}

// FormatTraceLog formats a trace event for logging
func FormatTraceLog(traceID, operation, stage string, durationMs float64) string {
	return fmt.Sprintf("[TRACE] trace_id=%s operation=%s stage=%s duration_ms=%.2f",
		traceID, operation, stage, durationMs)
}
