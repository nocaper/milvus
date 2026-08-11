package tracer

import (
	"os"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

// InitFromEnv initializes the global tracer from environment variables
// This should be called in each component's initialization (Proxy, DataNode, QueryNode)
func InitFromEnv(component string) error {
	// Check if latency tracing is enabled via environment variable
	enabled := os.Getenv("MILVUS_LATENCY_TRACE_ENABLED") == "true"
	if !enabled {
		log.Info("Latency tracing is disabled", zap.String("component", component))
		return nil
	}

	// Get output file path from environment or use default
	outputPath := os.Getenv("MILVUS_LATENCY_TRACE_OUTPUT")
	if outputPath == "" {
		outputPath = "/tmp/milvus_latency_trace_" + component + ".jsonl"
	}

	log.Info("Initializing latency tracer",
		zap.String("component", component),
		zap.String("output", outputPath))

	return InitGlobalTracer(true, outputPath, log.L())
}

// InitFromConfig initializes the global tracer from paramtable config
func InitFromConfig(component string) error {
	// For now, fallback to environment variables
	// TODO: add config entries to paramtable if needed
	return InitFromEnv(component)
}

// MustInit initializes the tracer, logs a warning on error but does not panic
// Tracing is optional — failure should not block the component from starting
func MustInit(component string) {
	if err := InitFromEnv(component); err != nil {
		log.Warn("Failed to initialize latency tracer, continuing without tracing",
			zap.String("component", component),
			zap.Error(err))
	}
}

// IsEnabled returns whether the global tracer is enabled
func IsEnabled() bool {
	t := GetGlobalTracer()
	return t != nil && t.enabled
}

