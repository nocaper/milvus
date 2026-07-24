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
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

const queryNodeLoadTraceTag = "[querynode-load-trace]"

func segmentLoadInfoTraceFields(loadInfo *querypb.SegmentLoadInfo) []mlog.Field {
	binlogFieldCount, binlogFileCount, binlogEntries, binlogDiskSize, binlogMemorySize := summarizeFieldBinlogs(loadInfo.GetBinlogPaths())
	statsFieldCount, statsFileCount, statsEntries, statsDiskSize, statsMemorySize := summarizeFieldBinlogs(loadInfo.GetStatslogs())
	deltaFieldCount, deltaFileCount, deltaEntries, deltaDiskSize, deltaMemorySize := summarizeFieldBinlogs(loadInfo.GetDeltalogs())
	bm25FieldCount, bm25FileCount, bm25Entries, bm25DiskSize, bm25MemorySize := summarizeFieldBinlogs(loadInfo.GetBm25Logs())
	indexFieldCount, indexFileCount, indexRows, indexSize := summarizeIndexInfos(loadInfo.GetIndexInfos())
	textStatsCount, textStatsFileCount, textStatsDiskSize, textStatsMemorySize := summarizeTextStats(loadInfo.GetTextStatsLogs())
	jsonStatsCount, jsonStatsFileCount, jsonStatsDiskSize, jsonStatsMemorySize := summarizeJSONStats(loadInfo.GetJsonKeyStatsLogs())

	return []mlog.Field{
		mlog.FieldCollectionID(loadInfo.GetCollectionID()),
		mlog.FieldPartitionID(loadInfo.GetPartitionID()),
		mlog.FieldSegmentID(loadInfo.GetSegmentID()),
		mlog.Int64("dbID", loadInfo.GetDbID()),
		mlog.Int64("numRows", loadInfo.GetNumOfRows()),
		mlog.Int64("segmentSize", loadInfo.GetSegmentSize()),
		mlog.Int64("storageVersion", loadInfo.GetStorageVersion()),
		mlog.Int32("dataVersion", loadInfo.GetDataVersion()),
		mlog.String("level", loadInfo.GetLevel().String()),
		mlog.String("priority", loadInfo.GetPriority().String()),
		mlog.String("insertChannel", loadInfo.GetInsertChannel()),
		mlog.String("manifestPath", loadInfo.GetManifestPath()),
		mlog.Int("childManifestCount", len(loadInfo.GetChildManifestPaths())),
		mlog.Int("binlogFieldCount", binlogFieldCount),
		mlog.Int("binlogFileCount", binlogFileCount),
		mlog.Int64("binlogEntries", binlogEntries),
		mlog.Int64("binlogDiskSize", binlogDiskSize),
		mlog.Int64("binlogMemorySize", binlogMemorySize),
		mlog.Int("statsFieldCount", statsFieldCount),
		mlog.Int("statsFileCount", statsFileCount),
		mlog.Int64("statsEntries", statsEntries),
		mlog.Int64("statsDiskSize", statsDiskSize),
		mlog.Int64("statsMemorySize", statsMemorySize),
		mlog.Int("deltaFieldCount", deltaFieldCount),
		mlog.Int("deltaFileCount", deltaFileCount),
		mlog.Int64("deltaEntries", deltaEntries),
		mlog.Int64("deltaDiskSize", deltaDiskSize),
		mlog.Int64("deltaMemorySize", deltaMemorySize),
		mlog.Int("bm25FieldCount", bm25FieldCount),
		mlog.Int("bm25FileCount", bm25FileCount),
		mlog.Int64("bm25Entries", bm25Entries),
		mlog.Int64("bm25DiskSize", bm25DiskSize),
		mlog.Int64("bm25MemorySize", bm25MemorySize),
		mlog.Int("indexFieldCount", indexFieldCount),
		mlog.Int("indexFileCount", indexFileCount),
		mlog.Int64("indexRows", indexRows),
		mlog.Int64("indexFileSize", indexSize),
		mlog.Int("textStatsCount", textStatsCount),
		mlog.Int("textStatsFileCount", textStatsFileCount),
		mlog.Int64("textStatsDiskSize", textStatsDiskSize),
		mlog.Int64("textStatsMemorySize", textStatsMemorySize),
		mlog.Int("jsonStatsCount", jsonStatsCount),
		mlog.Int("jsonStatsFileCount", jsonStatsFileCount),
		mlog.Int64("jsonStatsDiskSize", jsonStatsDiskSize),
		mlog.Int64("jsonStatsMemorySize", jsonStatsMemorySize),
	}
}

func summarizeFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog) (fieldCount, fileCount int, entries, diskSize, memorySize int64) {
	fieldCount = len(fieldBinlogs)
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			fileCount++
			entries += binlog.GetEntriesNum()
			diskSize += binlog.GetLogSize()
			memorySize += binlog.GetMemorySize()
		}
	}
	return
}

func summarizeIndexInfos(indexInfos []*querypb.FieldIndexInfo) (fieldCount, fileCount int, rows, indexSize int64) {
	fieldIDs := make(map[int64]struct{})
	for _, indexInfo := range indexInfos {
		fieldIDs[indexInfo.GetFieldID()] = struct{}{}
		fileCount += len(indexInfo.GetIndexFilePaths())
		rows += indexInfo.GetNumRows()
		indexSize += indexInfo.GetIndexSize()
	}
	return len(fieldIDs), fileCount, rows, indexSize
}

func summarizeTextStats(stats map[int64]*datapb.TextIndexStats) (statsCount, fileCount int, diskSize, memorySize int64) {
	statsCount = len(stats)
	for _, stat := range stats {
		fileCount += len(stat.GetFiles())
		diskSize += stat.GetLogSize()
		memorySize += stat.GetMemorySize()
	}
	return
}

func summarizeJSONStats(stats map[int64]*datapb.JsonKeyStats) (statsCount, fileCount int, diskSize, memorySize int64) {
	statsCount = len(stats)
	for _, stat := range stats {
		fileCount += len(stat.GetFiles())
		diskSize += stat.GetLogSize()
		memorySize += stat.GetMemorySize()
	}
	return
}

func traceGoLoadedObjects(ctx context.Context, segmentID int64, objectType string, paths []string, values [][]byte) {
	for i, objectPath := range paths {
		size := 0
		var va uintptr
		if i < len(values) {
			size = len(values[i])
			if size > 0 {
				va = uintptr(unsafe.Pointer(&values[i][0]))
			}
		}
		mlog.Info(ctx, queryNodeLoadTraceTag+" go object loaded",
			mlog.FieldSegmentID(segmentID),
			mlog.String("objectType", objectType),
			mlog.String("objectPath", objectPath),
			mlog.Int("objectSize", size),
			mlog.Uintptr("goBufferVA", va))
	}
}
