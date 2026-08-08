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

package querynodev2

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
)

func traceIDForPathTrace(ctx context.Context) string {
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	if spanCtx.HasTraceID() {
		return spanCtx.TraceID().String()
	}
	return ""
}

func logSearchRequestPathTrace(ctx context.Context, nodeID int64, entry string, event string, req *querypb.SearchRequest, fields ...zap.Field) {
	baseFields := []zap.Field{
		zap.String("operation", metrics.SearchLabel),
		zap.String("entry", entry),
		zap.String("event", event),
		zap.String("trace_id", traceIDForPathTrace(ctx)),
		zap.Int64("node_id", nodeID),
	}
	if req != nil {
		baseFields = append(baseFields,
			zap.Int64("msg_id", req.GetReq().GetBase().GetMsgID()),
			zap.Int64("collection_id", req.GetReq().GetCollectionID()),
			zap.Int64("db_id", req.GetReq().GetDbID()),
			zap.String("scope", req.GetScope().String()),
			zap.Int64("nq", req.GetReq().GetNq()),
			zap.Int64("top_k", req.GetReq().GetTopk()),
			zap.Bool("is_advanced", req.GetReq().GetIsAdvanced()),
			zap.Int("channel_count", len(req.GetDmlChannels())),
			zap.Int("segment_count", len(req.GetSegmentIDs())),
			zap.Int32("total_channel_num", req.GetTotalChannelNum()),
			zap.Uint64("guarantee_timestamp", req.GetReq().GetGuaranteeTimestamp()),
			zap.Uint64("mvcc_timestamp", req.GetReq().GetMvccTimestamp()),
		)
	}
	log.Ctx(ctx).Info("querynode request path trace", append(baseFields, fields...)...)
}

func logQueryRequestPathTrace(ctx context.Context, nodeID int64, entry string, event string, req *querypb.QueryRequest, fields ...zap.Field) {
	baseFields := []zap.Field{
		zap.String("operation", metrics.QueryLabel),
		zap.String("entry", entry),
		zap.String("event", event),
		zap.String("trace_id", traceIDForPathTrace(ctx)),
		zap.Int64("node_id", nodeID),
	}
	if req != nil {
		baseFields = append(baseFields,
			zap.Int64("msg_id", req.GetReq().GetBase().GetMsgID()),
			zap.Int64("collection_id", req.GetReq().GetCollectionID()),
			zap.Int64("db_id", req.GetReq().GetDbID()),
			zap.String("scope", req.GetScope().String()),
			zap.Int("channel_count", len(req.GetDmlChannels())),
			zap.Int("segment_count", len(req.GetSegmentIDs())),
			zap.Int("output_field_count", len(req.GetReq().GetOutputFieldsId())),
			zap.Int64("limit", req.GetReq().GetLimit()),
			zap.Bool("is_count", req.GetReq().GetIsCount()),
			zap.Uint64("guarantee_timestamp", req.GetReq().GetGuaranteeTimestamp()),
			zap.Uint64("mvcc_timestamp", req.GetReq().GetMvccTimestamp()),
		)
	}
	log.Ctx(ctx).Info("querynode request path trace", append(baseFields, fields...)...)
}
