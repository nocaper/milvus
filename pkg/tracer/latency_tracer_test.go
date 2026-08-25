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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetLatencyTracerForTest() {
	if globalTracer != nil {
		globalTracer.Close()
	}
	globalTracer = nil
	once = sync.Once{}
}

func TestLatencyTracerWritesToFile(t *testing.T) {
	resetLatencyTracerForTest()
	t.Setenv("MILVUS_LATENCY_TRACE_ENABLED", "true")

	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	t.Setenv("MILVUS_LATENCY_TRACE_OUTPUT", tracePath)

	tracer := GetGlobalTracer()
	require.NotNil(t, tracer)
	require.True(t, tracer.enabled)

	tracer.RecordEvent("trace-1", "", "Query", "route", "querynode", 5*time.Millisecond,
		map[string]interface{}{
			"collection_id": 123,
		},
	)
	tracer.Close()

	data, err := os.ReadFile(tracePath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "[LATENCY_TRACE] trace_id=trace-1 operation=Query stage=route component=querynode duration_ms=5.00")
	assert.Contains(t, string(data), "collection_id=123")
}

func TestLatencyTracerDisabled(t *testing.T) {
	resetLatencyTracerForTest()
	t.Setenv("MILVUS_LATENCY_TRACE_ENABLED", "false")

	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	t.Setenv("MILVUS_LATENCY_TRACE_OUTPUT", tracePath)

	tracer := GetGlobalTracer()
	require.NotNil(t, tracer)
	require.False(t, tracer.enabled)

	tracer.RecordEvent("trace-2", "", "Query", "route", "querynode", 5*time.Millisecond, nil)
	tracer.Close()

	_, err := os.Stat(tracePath)
	assert.Error(t, err)
}
