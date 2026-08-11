package tracer

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

// InitLatencyTracer initializes the global latency tracer.
// Tracing is always enabled; events are written directly to stdout.
func InitLatencyTracer(component string) {
	InitGlobalTracer(true)
	log.Info("Latency tracer initialized", zap.String("component", component))
}

// InitFromEnv kept for call-site compatibility; tracing is now always enabled.
func InitFromEnv(component string) error {
	InitLatencyTracer(component)
	return nil
}

// InitFromConfig kept for call-site compatibility; tracing is now always enabled.
func InitFromConfig(component string) error {
	InitLatencyTracer(component)
	return nil
}

// MustInit initializes the latency tracer; kept for call-site compatibility.
func MustInit(component string) {
	InitLatencyTracer(component)
}

// IsEnabled returns whether the global tracer is enabled.
func IsEnabled() bool {
	t := GetGlobalTracer()
	return t != nil && t.enabled
}
