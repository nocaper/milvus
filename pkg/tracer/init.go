package tracer

import (
	"os"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// InitFromEnv initializes the global tracer from environment variables
// This should be called in each component's initialization (Proxy, DataNode, QueryNode)
func InitFromEnv(component string) error {
	// Check if latency tracing is enabled via environment variable
	enabled := os.Getenv("MILVUS_LATENCY_TRACE_ENABLED") == "true"
	if !enabled {
		log.Info("Latency tracing is disabled", log.String("component", component))
		return nil
	}

	// Get output file path from environment or use default
	outputPath := os.Getenv("MILVUS_LATENCY_TRACE_OUTPUT")
	if outputPath == "" {
		// Default to /tmp/milvus_latency_trace.jsonl
		outputPath = "/tmp/milvus_latency_trace_" + component + ".jsonl"
	}

	log.Info("Initializing latency tracer",
		log.String("component", component),
		log.String("output", outputPath))

	return InitGlobalTracer(true, outputPath, log.L())
}

// InitFromConfig initializes the global tracer from paramtable config
func InitFromConfig(component string) error {
	// Try to get config from paramtable
	// For now, fallback to environment variables
	// TODO: add config entries to paramtable if needed
	return InitFromEnv(component)
}

// MustInit initializes the tracer and panics on error
// Use this in component initialization where failure should be fatal
func MustInit(component string) {
	if err := InitFromEnv(component); err != nil {
		log.Warn("Failed to initialize latency tracer, continuing without tracing",
			log.String("component", component),
			log.Error(err))
		// Don't panic - tracing is optional
	}
}

// IsEnabled returns whether the global tracer is enabled
func IsEnabled() bool {
	tracer := GetGlobalTracer()
	return tracer != nil && tracer.enabled
}
