package tracer

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

// InitLatencyTracer initializes the global latency tracer.
// Tracing defaults to an async file sink and can be disabled via env.
func InitLatencyTracer(component string) {
	tracer := GetGlobalTracer()
	if tracer == nil {
		log.Warn("Latency tracer is not available", zap.String("component", component))
		return
	}
	log.Info("Latency tracer initialized",
		zap.String("component", component),
		zap.Bool("enabled", tracer.enabled),
		zap.String("output", tracer.outputPath),
	)
	if tracer.sink != nil {
		log.Info("Latency trace flush policy",
			zap.Int("flush_batch", tracer.sink.flushBatch),
			zap.Duration("flush_interval", tracer.sink.flushEvery),
		)
	}
}

// InitFromEnv kept for call-site compatibility.
func InitFromEnv(component string) error {
	InitLatencyTracer(component)
	return nil
}

// InitFromConfig kept for call-site compatibility.
func InitFromConfig(component string) error {
	InitLatencyTracer(component)
	return nil
}

// MustInit initializes the latency tracer; kept for call-site compatibility.
func MustInit(component string) {
	InitLatencyTracer(component)
}

// IsEnabled returns whether the global tracer is enabled.
// CloseLatencyTracer flushes and stops the asynchronous latency sink.
func CloseLatencyTracer() {
	GetGlobalTracer().Close()
}

func IsEnabled() bool {
	t := GetGlobalTracer()
	return t != nil && t.enabled
}
