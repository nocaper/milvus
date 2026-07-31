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
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type binlogTraceSummary struct {
	FieldCount      int     `json:"field_count"`
	FileCount       int     `json:"file_count"`
	LogSize         int64   `json:"log_size"`
	MemorySize      int64   `json:"memory_size"`
	EntryCount      int64   `json:"entry_count"`
	LogSizeMiB      float64 `json:"log_size_mib"`
	MemorySizeMiB   float64 `json:"memory_size_mib"`
	MemoryLabel     string  `json:"memory_label"`
	PathSample       string  `json:"path_sample,omitempty"`
	PathSampleBase   string  `json:"path_sample_base,omitempty"`
	PathSampleLogID  int64   `json:"path_sample_log_id,omitempty"`
	PathSampleLogIdx string  `json:"path_sample_log_idx,omitempty"`
}

type indexTraceSummary struct {
	FieldCount       int      `json:"field_count"`
	FileCount        int      `json:"file_count"`
	IndexSize        int64    `json:"index_size"`
	IndexSizeMiB     float64  `json:"index_size_mib"`
	IndexFieldIDs    []int64  `json:"index_field_ids"`
	IndexNames       []string `json:"index_names"`
	IndexTypes       []string `json:"index_types"`
	MmapEnabledCount int      `json:"mmap_enabled_count"`
	DiskLoadCount    int      `json:"disk_load_count"`
	PathSample        string   `json:"path_sample,omitempty"`
	PathSampleBase    string   `json:"path_sample_base,omitempty"`
}

type segmentLoadTraceSummary struct {
	CollectionID       int64              `json:"collection_id"`
	PartitionID        int64              `json:"partition_id"`
	SegmentID          int64              `json:"segment_id"`
	Channel            string             `json:"channel"`
	RowCount           int64              `json:"row_count"`
	Level              string             `json:"level"`
	StorageVersion     int64              `json:"storage_version"`
	Insert             binlogTraceSummary `json:"insert"`
	Stats              binlogTraceSummary `json:"stats"`
	Delta              binlogTraceSummary `json:"delta"`
	Index              indexTraceSummary  `json:"index"`
	TotalObjectCount   int                `json:"total_object_count"`
	TotalObjectSize    int64              `json:"total_object_size"`
	TotalObjectSizeMiB float64            `json:"total_object_size_mib"`
	EstimatedMemory    uint64             `json:"estimated_memory"`
	EstimatedDisk      uint64             `json:"estimated_disk"`
	EstimatedVASize    uint64             `json:"estimated_va_size"`
	EstimatedVASizeMiB float64            `json:"estimated_va_size_mib"`
	VASizeLabel        string             `json:"va_size_label,omitempty"`
}

type remoteObjectPathTrace struct {
	RemoteKind    string
	CollectionID  int64
	PartitionID   int64
	SegmentID     int64
	FieldID       int64
	LogID         int64
	BuildID       int64
	IndexVersion  int64
	PathBase      string
}

func mib(size int64) float64 {
	return float64(size) / 1024 / 1024
}

func mibUint(size uint64) float64 {
	return float64(size) / 1024 / 1024
}

func parseTraceInt64(value string) int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return -1
	}
	return parsed
}

func parseRemoteObjectPathForTrace(remotePath string) remoteObjectPathTrace {
	trace := remoteObjectPathTrace{
		RemoteKind:   "unknown",
		CollectionID: -1,
		PartitionID:  -1,
		SegmentID:    -1,
		FieldID:      -1,
		LogID:        -1,
		BuildID:      -1,
		IndexVersion: -1,
		PathBase:     path.Base(remotePath),
	}
	parts := strings.Split(remotePath, "/")
	for i, part := range parts {
		switch part {
		case common.SegmentInsertLogPath:
			if i+5 < len(parts) {
				trace.RemoteKind = "segment"
				trace.CollectionID = parseTraceInt64(parts[i+1])
				trace.PartitionID = parseTraceInt64(parts[i+2])
				trace.SegmentID = parseTraceInt64(parts[i+3])
				trace.FieldID = parseTraceInt64(parts[i+4])
				trace.LogID = parseTraceInt64(parts[i+5])
				return trace
			}
		case common.SegmentStatslogPath:
			if i+5 < len(parts) {
				trace.RemoteKind = "stats"
				trace.CollectionID = parseTraceInt64(parts[i+1])
				trace.PartitionID = parseTraceInt64(parts[i+2])
				trace.SegmentID = parseTraceInt64(parts[i+3])
				trace.FieldID = parseTraceInt64(parts[i+4])
				trace.LogID = parseTraceInt64(parts[i+5])
				return trace
			}
		case common.SegmentDeltaLogPath:
			if i+4 < len(parts) {
				trace.RemoteKind = "delta"
				trace.CollectionID = parseTraceInt64(parts[i+1])
				trace.PartitionID = parseTraceInt64(parts[i+2])
				trace.SegmentID = parseTraceInt64(parts[i+3])
				trace.LogID = parseTraceInt64(parts[i+4])
				return trace
			}
		case common.SegmentIndexPath:
			if i+4 < len(parts) {
				trace.RemoteKind = "index"
				trace.BuildID = parseTraceInt64(parts[i+1])
				trace.IndexVersion = parseTraceInt64(parts[i+2])
				trace.PartitionID = parseTraceInt64(parts[i+3])
				trace.SegmentID = parseTraceInt64(parts[i+4])
				return trace
			}
		case "raw_datas":
			if i+2 < len(parts) {
				trace.RemoteKind = "raw_data"
				trace.SegmentID = parseTraceInt64(parts[i+1])
				trace.FieldID = parseTraceInt64(parts[i+2])
				if i+3 < len(parts) {
					trace.LogID = parseTraceInt64(parts[i+3])
				}
				return trace
			}
		}
	}
	return trace
}

func logRemoteObjectFetchTrace(ctx context.Context, event string, storageVersion string, remotePath string, encodedBytes int64, duration time.Duration, fields ...zap.Field) {
	trace := parseRemoteObjectPathForTrace(remotePath)
	log.Ctx(ctx).Info("querynode remote object fetch trace",
		append([]zap.Field{
			zap.String("event", event),
			zap.String("storage_version", storageVersion),
			zap.String("remote_kind", trace.RemoteKind),
			zap.String("remote_path", remotePath),
			zap.String("path_base", trace.PathBase),
			zap.Int64("collection_id", trace.CollectionID),
			zap.Int64("partition_id", trace.PartitionID),
			zap.Int64("segment_id", trace.SegmentID),
			zap.Int64("field_id", trace.FieldID),
			zap.Int64("log_id", trace.LogID),
			zap.Int64("build_id", trace.BuildID),
			zap.Int64("index_version", trace.IndexVersion),
			zap.Int64("encoded_bytes", encodedBytes),
			zap.Float64("encoded_bytes_mib", mib(encodedBytes)),
			zap.Float64("duration_ms", float64(duration.Microseconds())/1000),
		}, fields...)...,
	)
}

func summarizeFieldBinlogs(fields []*datapb.FieldBinlog) binlogTraceSummary {
	summary := binlogTraceSummary{}
	for _, field := range fields {
		if field == nil {
			continue
		}
		summary.FieldCount++
		for _, binlog := range field.GetBinlogs() {
			if binlog == nil {
				continue
			}
			if summary.PathSample == "" {
				summary.PathSample = binlog.GetLogPath()
				summary.PathSampleBase = path.Base(binlog.GetLogPath())
				summary.PathSampleLogID = binlog.GetLogID()
				_, summary.PathSampleLogIdx = path.Split(binlog.GetLogPath())
			}
			summary.FileCount++
			summary.LogSize += binlog.GetLogSize()
			summary.MemorySize += binlog.GetMemorySize()
			summary.EntryCount += binlog.GetEntriesNum()
		}
	}
	summary.LogSizeMiB = mib(summary.LogSize)
	summary.MemorySizeMiB = mib(summary.MemorySize)
	summary.MemoryLabel = "memory_size_from_datacoord_binlog_meta"
	return summary
}

func summarizeIndexes(indexes []*querypb.FieldIndexInfo) indexTraceSummary {
	summary := indexTraceSummary{}
	fieldIDs := make(map[int64]struct{})
	indexNames := make(map[string]struct{})
	indexTypes := make(map[string]struct{})
	for _, indexInfo := range indexes {
		if indexInfo == nil {
			continue
		}
		if _, ok := fieldIDs[indexInfo.GetFieldID()]; !ok {
			fieldIDs[indexInfo.GetFieldID()] = struct{}{}
			summary.IndexFieldIDs = append(summary.IndexFieldIDs, indexInfo.GetFieldID())
		}
		if name := indexInfo.GetIndexName(); name != "" {
			indexNames[name] = struct{}{}
		}
		if indexType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.IndexTypeKey, indexInfo.GetIndexParams()); err == nil {
			indexTypes[indexType] = struct{}{}
			if indexType == indexparamcheck.IndexDISKANN || indexType == indexparamcheck.IndexINVERTED {
				summary.DiskLoadCount++
			}
		}
		if isIndexMmapEnable(indexInfo) {
			summary.MmapEnabledCount++
		}
		if summary.PathSample == "" && len(indexInfo.GetIndexFilePaths()) > 0 {
			summary.PathSample = indexInfo.GetIndexFilePaths()[0]
			summary.PathSampleBase = path.Base(summary.PathSample)
		}
		summary.FileCount += len(indexInfo.GetIndexFilePaths())
		summary.IndexSize += indexInfo.GetIndexSize()
	}
	summary.FieldCount = len(fieldIDs)
	summary.IndexSizeMiB = mib(summary.IndexSize)
	summary.IndexNames = lo.Keys(indexNames)
	summary.IndexTypes = lo.Keys(indexTypes)
	return summary
}

func summarizeSegmentLoadTrace(loadInfo *querypb.SegmentLoadInfo) segmentLoadTraceSummary {
	summary := segmentLoadTraceSummary{
		CollectionID:   loadInfo.GetCollectionID(),
		PartitionID:    loadInfo.GetPartitionID(),
		SegmentID:      loadInfo.GetSegmentID(),
		Channel:        loadInfo.GetInsertChannel(),
		RowCount:       loadInfo.GetNumOfRows(),
		Level:          loadInfo.GetLevel().String(),
		StorageVersion: loadInfo.GetStorageVersion(),
		Insert:         summarizeFieldBinlogs(loadInfo.GetBinlogPaths()),
		Stats:          summarizeFieldBinlogs(loadInfo.GetStatslogs()),
		Delta:          summarizeFieldBinlogs(loadInfo.GetDeltalogs()),
		Index:          summarizeIndexes(loadInfo.GetIndexInfos()),
	}
	summary.TotalObjectCount = summary.Insert.FileCount + summary.Stats.FileCount + summary.Delta.FileCount + summary.Index.FileCount
	summary.TotalObjectSize = summary.Insert.LogSize + summary.Stats.LogSize + summary.Delta.LogSize + summary.Index.IndexSize
	summary.TotalObjectSizeMiB = mib(summary.TotalObjectSize)
	return summary
}

func logSegmentLoadTrace(ctx context.Context, msg string, loadInfo *querypb.SegmentLoadInfo, fields ...zap.Field) {
	if loadInfo == nil {
		return
	}
	summary := summarizeSegmentLoadTrace(loadInfo)
	log.Ctx(ctx).Info(msg,
		append([]zap.Field{
			zap.Int64("collectionID", summary.CollectionID),
			zap.Int64("partitionID", summary.PartitionID),
			zap.Int64("segmentID", summary.SegmentID),
			zap.String("channel", summary.Channel),
			zap.Int64("rowCount", summary.RowCount),
			zap.String("level", summary.Level),
			zap.Int64("storageVersion", summary.StorageVersion),
			zap.Any("loadObjectSummary", summary),
		}, fields...)...,
	)
}

func logFieldLoadTrace(ctx context.Context, segment *LocalSegment, fieldID int64, rowCount int64, field *datapb.FieldBinlog, mmapEnabled bool, stage string, fields ...zap.Field) {
	summary := summarizeFieldBinlogs(nil)
	if field != nil {
		summary = summarizeFieldBinlogs([]*datapb.FieldBinlog{field})
	}
	log.Ctx(ctx).Info("querynode load field data from remote storage",
		append([]zap.Field{
			zap.String("stage", stage),
			zap.Int64("collectionID", segment.Collection()),
			zap.Int64("partitionID", segment.Partition()),
			zap.Int64("segmentID", segment.ID()),
			zap.Int64("fieldID", fieldID),
			zap.Int64("rowCount", rowCount),
			zap.Bool("mmapEnabled", mmapEnabled),
			zap.Any("binlogSummary", summary),
		}, fields...)...,
	)
}

func logMultiFieldLoadTrace(ctx context.Context, segment *LocalSegment, rowCount int64, fields []*datapb.FieldBinlog, stage string) {
	summary := summarizeFieldBinlogs(fields)
	log.Ctx(ctx).Info("querynode load multi field data from remote storage",
		zap.String("stage", stage),
		zap.Int64("collectionID", segment.Collection()),
		zap.Int64("partitionID", segment.Partition()),
		zap.Int64("segmentID", segment.ID()),
		zap.Int64("rowCount", rowCount),
		zap.String("segmentType", segment.Type().String()),
		zap.Any("binlogSummary", summary),
	)
}

func logDeltaLoadTrace(ctx context.Context, segment Segment, deltaLogs []*datapb.FieldBinlog, stage string, fields ...zap.Field) {
	summary := summarizeFieldBinlogs(deltaLogs)
	log.Ctx(ctx).Info("querynode load delta logs from remote storage",
		append([]zap.Field{
			zap.String("stage", stage),
			zap.Int64("collectionID", segment.Collection()),
			zap.Int64("partitionID", segment.Partition()),
			zap.Int64("segmentID", segment.ID()),
			zap.String("segmentType", segment.Type().String()),
			zap.Any("deltaSummary", summary),
		}, fields...)...,
	)
}

func logIndexLoadTrace(ctx context.Context, segment *LocalSegment, indexInfo *querypb.FieldIndexInfo, fieldType string, stage string, fields ...zap.Field) {
	summary := summarizeIndexes([]*querypb.FieldIndexInfo{indexInfo})
	log.Ctx(ctx).Info("querynode load index from remote storage",
		append([]zap.Field{
			zap.String("stage", stage),
			zap.Int64("collectionID", segment.Collection()),
			zap.Int64("partitionID", segment.Partition()),
			zap.Int64("segmentID", segment.ID()),
			zap.Int64("fieldID", indexInfo.GetFieldID()),
			zap.Int64("indexID", indexInfo.GetIndexID()),
			zap.Int64("buildID", indexInfo.GetBuildID()),
			zap.Int64("indexVersion", indexInfo.GetIndexVersion()),
			zap.Int64("indexStoreVersion", indexInfo.GetIndexStoreVersion()),
			zap.Int32("currentIndexVersion", indexInfo.GetCurrentIndexVersion()),
			zap.String("fieldType", fieldType),
			zap.Any("indexSummary", summary),
		}, fields...)...,
	)
}

func withResourceEstimate(summary segmentLoadTraceSummary, usage *ResourceUsage) segmentLoadTraceSummary {
	if usage == nil {
		return summary
	}
	summary.EstimatedMemory = usage.MemorySize
	summary.EstimatedDisk = usage.DiskSize
	summary.EstimatedVASize = usage.MemorySize + usage.DiskSize
	summary.EstimatedVASizeMiB = mibUint(summary.EstimatedVASize)
	summary.VASizeLabel = "estimated_memory_plus_mmap_or_disk_footprint"
	return summary
}

func logSegmentResourceTrace(ctx context.Context, collectionID int64, usage *ResourceUsage, loadInfo *querypb.SegmentLoadInfo) {
	if loadInfo == nil || usage == nil {
		return
	}
	summary := withResourceEstimate(summarizeSegmentLoadTrace(loadInfo), usage)
	log.Ctx(ctx).Info("querynode segment remote load resource estimate",
		zap.Int64("collectionID", collectionID),
		zap.Int64("segmentID", loadInfo.GetSegmentID()),
		zap.Float64("estimatedMemoryMiB", mibUint(usage.MemorySize)),
		zap.Float64("estimatedDiskMiB", mibUint(usage.DiskSize)),
		zap.Float64("estimatedVASizeMiB", mibUint(usage.MemorySize+usage.DiskSize)),
		zap.Int("mmapFieldCount", usage.MmapFieldCount),
		zap.Any("loadObjectSummary", summary),
	)
}

func logStorageV2Trace(ctx context.Context, segment *LocalSegment, stage string, uri string, storageVersion int64, fields ...zap.Field) {
	log.Ctx(ctx).Info("querynode load storage v2 segment data from remote storage",
		append([]zap.Field{
			zap.String("stage", stage),
			zap.Int64("collectionID", segment.Collection()),
			zap.Int64("partitionID", segment.Partition()),
			zap.Int64("segmentID", segment.ID()),
			zap.String("uri", uri),
			zap.Int64("storageVersion", storageVersion),
			zap.Bool("enableStorageV2", paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool()),
		}, fields...)...,
	)
}

func storageV2CurrentVersion(segment *LocalSegment) (int64, bool) {
	if segment == nil || segment.space == nil {
		return 0, false
	}
	return segment.space.GetCurrentVersion(), true
}
