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

package segments

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	qpsPathTraceEvent = "milvus_qps_path_trace"

	qpsPathKindNone                 = "none"
	qpsPathKindDataNode             = "datanode_minio_querynode"
	qpsPathKindDataNodeStorageV2    = "datanode_minio_querynode_storage_v2"
	qpsPathKindIndex                = "index_minio_querynode"
	qpsPathKindDataNodeAndIndex     = "datanode_and_index_minio_querynode"
	qpsPathKindDataNodeV2AndIndex   = "datanode_storage_v2_and_index_minio_querynode"
)

type QPSBinlogSummary struct {
	fileCount  int
	logBytes   int64
	memBytes   int64
	entryCount int64
}

type QPSPathSummary struct {
	Insert      QPSBinlogSummary
	Stats       QPSBinlogSummary
	Delta       QPSBinlogSummary
	IndexFiles  int
	IndexBytes  int64
	StorageV2   bool
	StorageVer  int64
	IndexStoreV int
}

func summarizeBinlogs(fieldBinlogs []*datapb.FieldBinlog) QPSBinlogSummary {
	var summary QPSBinlogSummary
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog == nil {
				continue
			}
			summary.fileCount++
			summary.logBytes += binlog.GetLogSize()
			summary.memBytes += binlog.GetMemorySize()
			summary.entryCount += binlog.GetEntriesNum()
		}
	}
	return summary
}

func SummarizeLoadInfo(loadInfo *querypb.SegmentLoadInfo) QPSPathSummary {
	if loadInfo == nil {
		return QPSPathSummary{}
	}

	summary := QPSPathSummary{
		Insert:     summarizeBinlogs(loadInfo.GetBinlogPaths()),
		Stats:      summarizeBinlogs(loadInfo.GetStatslogs()),
		Delta:      summarizeBinlogs(loadInfo.GetDeltalogs()),
		StorageV2:  loadInfo.GetStorageVersion() > 0,
		StorageVer: loadInfo.GetStorageVersion(),
	}

	for _, indexInfo := range loadInfo.GetIndexInfos() {
		summary.IndexFiles += len(indexInfo.GetIndexFilePaths())
		summary.IndexBytes += indexInfo.GetIndexSize()
		if indexInfo.GetIndexStoreVersion() > 0 {
			summary.IndexStoreV++
		}
	}

	return summary
}

func SummarizeFieldBinlog(field *datapb.FieldBinlog, storageVersion int64) QPSPathSummary {
	var fields []*datapb.FieldBinlog
	if field != nil {
		fields = []*datapb.FieldBinlog{field}
	}
	return QPSPathSummary{
		Insert:     summarizeBinlogs(fields),
		StorageV2:  storageVersion > 0,
		StorageVer: storageVersion,
	}
}

func SummarizeDeltaLogs(deltaLogs []*datapb.FieldBinlog) QPSPathSummary {
	return QPSPathSummary{
		Delta: summarizeBinlogs(deltaLogs),
	}
}

func SummarizeStatsLogs(statsLogs []*datapb.FieldBinlog, storageVersion int64) QPSPathSummary {
	return QPSPathSummary{
		Stats:      summarizeBinlogs(statsLogs),
		StorageV2:  storageVersion > 0,
		StorageVer: storageVersion,
	}
}

func SummarizeIndexInfo(indexInfo *querypb.FieldIndexInfo) QPSPathSummary {
	if indexInfo == nil {
		return QPSPathSummary{}
	}
	summary := QPSPathSummary{
		IndexFiles: len(indexInfo.GetIndexFilePaths()),
		IndexBytes: indexInfo.GetIndexSize(),
	}
	if indexInfo.GetIndexStoreVersion() > 0 {
		summary.IndexStoreV = 1
	}
	return summary
}

func (s QPSPathSummary) datanodeFileCount() int {
	return s.Insert.fileCount + s.Stats.fileCount + s.Delta.fileCount
}

func (s QPSPathSummary) datanodeLogBytes() int64 {
	return s.Insert.logBytes + s.Stats.logBytes + s.Delta.logBytes
}

func (s QPSPathSummary) datanodeMemBytes() int64 {
	return s.Insert.memBytes + s.Stats.memBytes + s.Delta.memBytes
}

func (s QPSPathSummary) entryCount() int64 {
	return s.Insert.entryCount + s.Stats.entryCount + s.Delta.entryCount
}

func (s QPSPathSummary) remoteBytes() int64 {
	return s.datanodeLogBytes() + s.IndexBytes
}

func (s QPSPathSummary) dataNodePathHit() bool {
	return s.datanodeFileCount() > 0 || s.StorageV2
}

func (s QPSPathSummary) objectStoragePathHit() bool {
	return s.dataNodePathHit() || s.IndexFiles > 0 || s.IndexStoreV > 0
}

func (s QPSPathSummary) pathKind() string {
	dataNode := s.dataNodePathHit()
	index := s.IndexFiles > 0 || s.IndexStoreV > 0
	switch {
	case dataNode && index && s.StorageV2 && s.datanodeFileCount() == 0:
		return qpsPathKindDataNodeV2AndIndex
	case dataNode && index:
		return qpsPathKindDataNodeAndIndex
	case dataNode && s.StorageV2 && s.datanodeFileCount() == 0:
		return qpsPathKindDataNodeStorageV2
	case dataNode:
		return qpsPathKindDataNode
	case index:
		return qpsPathKindIndex
	default:
		return qpsPathKindNone
	}
}

func qpsPathStatus(err error) string {
	if err != nil {
		return metrics.FailLabel
	}
	return metrics.SuccessLabel
}

func recordQPSPathMetrics(op string, pathHit bool, pathKind string, status string, latency time.Duration, remoteBytes int64) {
	nodeID := fmt.Sprint(paramtable.GetNodeID())
	pathHitLabel := strconv.FormatBool(pathHit)
	metrics.QueryNodeQPSPathEventCount.WithLabelValues(nodeID, op, pathHitLabel, pathKind, status).Inc()
	metrics.QueryNodeQPSPathLatency.WithLabelValues(nodeID, op, pathHitLabel, pathKind, status).Observe(float64(latency.Milliseconds()))
	if remoteBytes > 0 {
		metrics.QueryNodeQPSPathRemoteBytes.WithLabelValues(nodeID, op, pathKind).Observe(float64(remoteBytes))
	}
}

func RecordQPSPathEvent(ctx context.Context, op string, summary QPSPathSummary, latency time.Duration, err error, fields ...zap.Field) {
	pathKind := summary.pathKind()
	pathHit := summary.dataNodePathHit()
	status := qpsPathStatus(err)
	remoteBytes := summary.remoteBytes()

	recordQPSPathMetrics(op, pathHit, pathKind, status, latency, remoteBytes)

	baseFields := []zap.Field{
		zap.String("op", op),
		zap.Bool("path_hit", pathHit),
		zap.Bool("datanode_minio_querynode_path_hit", pathHit),
		zap.Bool("object_storage_path_hit", summary.objectStoragePathHit()),
		zap.String("path_kind", pathKind),
		zap.String("status", status),
		zap.Duration("latency", latency),
		zap.Int64("latency_ms", latency.Milliseconds()),
		zap.Int("datanode_remote_file_count", summary.datanodeFileCount()),
		zap.Int64("datanode_remote_log_bytes", summary.datanodeLogBytes()),
		zap.Int64("datanode_remote_mem_bytes", summary.datanodeMemBytes()),
		zap.Int64("datanode_entry_count", summary.entryCount()),
		zap.Int("insert_log_file_count", summary.Insert.fileCount),
		zap.Int64("insert_log_bytes", summary.Insert.logBytes),
		zap.Int("stats_log_file_count", summary.Stats.fileCount),
		zap.Int64("stats_log_bytes", summary.Stats.logBytes),
		zap.Int("delta_log_file_count", summary.Delta.fileCount),
		zap.Int64("delta_log_bytes", summary.Delta.logBytes),
		zap.Int("index_file_count", summary.IndexFiles),
		zap.Int64("index_bytes", summary.IndexBytes),
		zap.Bool("storage_v2", summary.StorageV2),
		zap.Int64("storage_version", summary.StorageVer),
	}
	if err != nil {
		baseFields = append(baseFields, zap.Error(err))
	}
	baseFields = append(baseFields, fields...)
	log.Ctx(ctx).Info(qpsPathTraceEvent, baseFields...)
}

func RecordQPSNoPathEvent(ctx context.Context, op string, latency time.Duration, err error, fields ...zap.Field) {
	RecordQPSPathEvent(ctx, op, QPSPathSummary{}, latency, err, fields...)
}

func RecordQPSSegmentLoadEvent(ctx context.Context, op string, loadInfo *querypb.SegmentLoadInfo, segmentType string, latency time.Duration, err error, fields ...zap.Field) {
	summary := SummarizeLoadInfo(loadInfo)
	baseFields := []zap.Field{
		zap.Int64("collectionID", loadInfo.GetCollectionID()),
		zap.Int64("partitionID", loadInfo.GetPartitionID()),
		zap.Int64("segmentID", loadInfo.GetSegmentID()),
		zap.String("segmentType", segmentType),
		zap.String("segmentLevel", loadInfo.GetLevel().String()),
		zap.Int64("row_count", loadInfo.GetNumOfRows()),
	}
	baseFields = append(baseFields, fields...)
	RecordQPSPathEvent(ctx, op, summary, latency, err, baseFields...)
}

func RecordQPSSegmentLoadSkipped(ctx context.Context, loadInfo *querypb.SegmentLoadInfo, segmentType string, reason string) {
	summary := SummarizeLoadInfo(loadInfo)
	RecordQPSNoPathEvent(ctx, "load_segment_skipped", 0, nil,
		zap.Int64("collectionID", loadInfo.GetCollectionID()),
		zap.Int64("partitionID", loadInfo.GetPartitionID()),
		zap.Int64("segmentID", loadInfo.GetSegmentID()),
		zap.String("segmentType", segmentType),
		zap.String("segmentLevel", loadInfo.GetLevel().String()),
		zap.Int64("row_count", loadInfo.GetNumOfRows()),
		zap.String("skip_reason", reason),
		zap.Bool("planned_datanode_minio_querynode_path_hit", summary.dataNodePathHit()),
		zap.String("planned_path_kind", summary.pathKind()),
		zap.Int("planned_datanode_remote_file_count", summary.datanodeFileCount()),
		zap.Int64("planned_datanode_remote_log_bytes", summary.datanodeLogBytes()),
		zap.Int("planned_index_file_count", summary.IndexFiles),
		zap.Int64("planned_index_bytes", summary.IndexBytes),
	)
}
