package tracer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// TraceEvent represents a single tracing event
type TraceEvent struct {
	TraceID   string
	RequestID string
	Operation string // Insert, Upsert, Search, Query, LoadCollection, etc.
	Stage     string // serialize, mq_produce, deserialize, s3_write, etc.
	Component string // proxy, datanode, querynode, querycoord
	StartTime time.Time
	EndTime   time.Time
	Duration  float64 // milliseconds
	Metadata  map[string]interface{}
}

// LatencyTracer manages distributed tracing for latency analysis
type LatencyTracer struct {
	enabled    bool
	outputPath string
	sink       *latencyTraceSink
}

var (
	globalTracer *LatencyTracer
	once         sync.Once
)

// InitGlobalTracer initializes the global tracer once.
func InitGlobalTracer(enabled bool) {
	once.Do(func() {
		cfg := loadLatencyTraceConfig()
		cfg.enabled = enabled
		globalTracer = newLatencyTracer(cfg)
	})
}

// GetGlobalTracer returns the global tracer instance.
// Auto-initializes from environment if InitGlobalTracer has not been called yet.
func GetGlobalTracer() *LatencyTracer {
	once.Do(func() {
		globalTracer = newLatencyTracer(loadLatencyTraceConfig())
	})
	return globalTracer
}

// GenerateTraceID generates a new trace ID
func GenerateTraceID() string {
	return uuid.New().String()
}

// GetTraceIDFromContext retrieves trace ID from context
func GetTraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if traceID, ok := ctx.Value("traceID").(string); ok && traceID != "" {
		return traceID
	}
	otelTraceID := oteltrace.SpanContextFromContext(ctx).TraceID()
	if otelTraceID.IsValid() {
		return otelTraceID.String()
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
	if traceID == "" {
		traceID = GenerateTraceID()
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

// EndSpan ends a tracing span and enqueues the event for asynchronous export.
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
	if traceID == "" {
		traceID = GenerateTraceID()
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

// recordEvent enqueues a trace event for background export.
func (t *LatencyTracer) recordEvent(event TraceEvent) {
	if t == nil || !t.enabled || t.sink == nil {
		return
	}
	t.sink.enqueue(event)
}

// Close flushes and stops the background sink.
func (t *LatencyTracer) Close() {
	if t == nil || t.sink == nil {
		return
	}
	t.sink.Close()
}

// Helper functions for common operations

// TraceInsert traces an insert operation span
func TraceInsert(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	return GetGlobalTracer().StartSpan(traceID, "", "Insert", stage, getCurrentComponent(), metadata)
}

// TraceSearch traces a search operation span
func TraceSearch(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	return GetGlobalTracer().StartSpan(traceID, "", "Search", stage, getCurrentComponent(), metadata)
}

// TraceQuery traces a query operation span
func TraceQuery(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	return GetGlobalTracer().StartSpan(traceID, "", "Query", stage, getCurrentComponent(), metadata)
}

// TraceLoadCollection traces a load collection operation span
func TraceLoadCollection(ctx context.Context, stage string, metadata map[string]interface{}) *SpanContext {
	traceID := GetTraceIDFromContext(ctx)
	if traceID == "" {
		return nil
	}
	return GetGlobalTracer().StartSpan(traceID, "", "LoadCollection", stage, getCurrentComponent(), metadata)
}

// EndTrace is a convenience function to end a span
func EndTrace(span *SpanContext) {
	if span != nil {
		GetGlobalTracer().EndSpan(span)
	}
}

// getCurrentComponent is a placeholder; component is set explicitly via SetComponent
func getCurrentComponent() string {
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

// FormatTraceLog formats a trace line for logging (convenience helper)
func FormatTraceLog(traceID, operation, stage string, durationMs float64) string {
	return fmt.Sprintf("[LATENCY_TRACE] trace_id=%s operation=%s stage=%s duration_ms=%.2f",
		traceID, operation, stage, durationMs)
}
