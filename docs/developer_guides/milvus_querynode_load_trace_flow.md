# Milvus QueryNode Load Trace Flow

本文面向 `milvus-trace-loader` 场景：分析 Milvus 从写入、落对象存储，到 QueryNode 从对象存储加载 segment 和索引进入内存的链路，并说明本次插桩能够观测到的虚拟地址、长度和关联字段。

## 目标

- 梳理 Milvus 整体写入和读取流程。
- 重点说明 `DataNode -> MinIO/S3 -> QueryNode` 的数据流。
- 在 QueryNode 加载路径记录可用于性能分析的日志，包括 `segmentID`、对象大小、QueryNode 进程内 VA、索引文件信息和加载耗时。

这里的 VA 都是 QueryNode 进程虚拟地址，不是物理地址：

- `cSegmentVA`：Go 持有的 C++ segcore segment 对象句柄地址。
- `object_va`：C++ 从对象存储下载到 `uint8_t[]` 后的原始对象 buffer 地址。
- `buffer_va`：流式读取 packed index 时，调用方传给 `RemoteInputStream` 的目标 buffer 地址。
- `chunk_va` / `index_va` / `column_group_va`：segcore/caching layer 中物化出的 Chunk、Index、ColumnGroup 对象地址。

## 整体读写流程

### 写入主流程

1. Client 通过 SDK 发起 insert/delete/upsert。
2. Proxy 校验 collection schema、时间戳和权限，并把写请求写入 WAL/msgstream。
3. DataNode 或 StreamingNode 消费对应 insert channel。
4. 写入数据先进入 write buffer，按 segment/channel 组织为内存批次。
5. flush 触发后，`internal/flushcommon/syncmgr.SyncTask.Run` 把 `SyncPack` 写成 binlog、statslog、deltalog、BM25 stats 或 Storage V2/V3 column group/manifest。
6. 写出的对象通过 `storage.ChunkManager` 持久化到 MinIO/S3/local storage。
7. `SyncTask.writeMeta` 通过 `MetaWriter.UpdateSync` 把 binlog 路径、行数、大小、manifest path 等提交给 DataCoord。
8. DataCoord 持有 segment 元数据，并为 QueryCoord 的加载调度提供 `SegmentLoadInfo`。

### 查询加载主流程

1. QueryCoord 根据 load collection/partition、balance、handoff 等决策生成加载任务。
2. QueryNode 收到 `querypb.SegmentLoadInfo`，其中包含：
   - collection/partition/segment ID
   - binlog/statslog/deltalog 路径和大小
   - index file paths、index size、buildID、indexID
   - storage version、manifest path、load priority
3. `internal/querynodev2/segments.segmentLoader.Load` 做去重、资源预估和 `LocalSegment` 创建。
4. `LocalSegment` 创建 C++ segcore segment，Go 层持有 `cSegmentVA`。
5. sealed segment 通过 `LocalSegment.Load -> segcore C API -> ChunkedSegmentSealedImpl::Load` 进入 C++ load diff 流程。
6. C++ `ApplyLoadDiff` 根据 segment load info 加载 index、field data、manifest column group、text/json stats 等。
7. 加载完成后，QueryNode segment manager 提交 segment，查询请求可以在该 segment 上 search/retrieve。

## DataNode -> MinIO

### 入口

核心入口在：

- `internal/flushcommon/syncmgr/task.go`
- `internal/flushcommon/syncmgr/pack_writer.go`
- `internal/flushcommon/syncmgr/pack_writer_v2.go`
- `internal/flushcommon/syncmgr/pack_writer_v3.go`
- `internal/flushcommon/syncmgr/storage_serializer.go`
- `internal/storage/remote_chunk_manager.go`

`SyncTask.Run` 的主要逻辑：

1. 从 metacache 获取 segment 信息。
2. 根据 `segmentInfo.GetStorageVersion()` 选择写入格式。
3. 调用 writer 的 `Write(ctx, pack)`。
4. 得到 insert/delta/stats/BM25 binlog 元数据、manifest path 和 flushed size。
5. 调用 `writeMeta` 保存路径到 DataCoord。
6. 更新 metacache 和 metrics。

### Storage V1

Storage V1 使用 `BulkPackWriter`：

- `storageV1Serializer.serializeBinlog` 把 insert data 序列化为每个 field 的 binlog blob。
- `serializeStatslog` 生成 PK bloom filter stats。
- `writeDelta` 生成 deltalog。
- `writeLog` 通过 `path.Join(root, insert_log|stats_log|delta_log, collection/partition/segment/field/logID)` 生成对象路径。
- `RemoteChunkManager.Write` 调用对象存储 put object，把 bytes 写入 MinIO/S3。

DataCoord 中保存的是 field binlog 元数据：路径、entries num、log size、memory size、timestamp range。

### Storage V2/V3

Storage V2/V3 使用 packed writer：

- V2/V3 按 column group 写入 parquet/packed 数据。
- V3 使用 manifest，两阶段流程是：
  - Phase 1 写数据文件、delta、stats/BM25。
  - Phase 2 调用 `packed.CommitManifestUpdates` 提交 manifest 更新，得到新的 manifest path。
- `SegmentLoadInfo.ManifestPath` 让 QueryNode 后续按 manifest/column group 加载。

### 索引写入

索引由 DataCoord/IndexCoord 调度，DataNode index service 执行 index build：

- 输入通常来自 insert binlog 或 Storage V2/V3 manifest。
- index builder 产出 index files。
- C++ storage file manager 或 Go chunk manager 把 index files 写到对象存储。
- DataCoord 保存 `FieldIndexInfo`，其中包含 `indexID`、`buildID`、`indexVersion`、`indexFilePaths`、`indexSize` 和 `numRows`。

## MinIO -> QueryNode

### Go 层加载入口

核心入口：

- `internal/querynodev2/segments/segment_loader.go`
- `internal/querynodev2/segments/segment.go`
- `internal/querynodev2/segments/load_index_info.go`

关键调用链：

```text
segmentLoader.Load
  -> NewSegment
      -> collection.CreateCSegment
      -> LocalSegment{ptr, csegment}
  -> segmentLoader.LoadSegment
      -> loadSealedSegment
          -> LocalSegment.Load
              -> segcore.CSegment.Load
```

本次 Go 层插桩记录：

- `csegment created`：创建 C++ segment 后记录 `cSegmentVA`、storage version、manifest path、binlog/index 文件数量和大小。
- `segment files load start/done`：记录加载耗时、`relatedDataSize`、`loadedBinlogMemorySize`、`cSegmentMemSize`、`rowNumAfterLoad`。
- `c load index info prepared`：记录每个 index 的 field、indexID、buildID、index file paths、index size、mmap、`cLoadIndexInfoVA`。
- `go object loaded`：Go 侧读取 PK stats、BM25 stats、delta log 时记录 byte slice VA 和长度。

### C++ segcore 加载入口

核心调用链：

```text
internal/util/segcore/segment.go
  -> C.AsyncSegmentLoad
internal/core/src/segcore/segment_c.cpp
  -> segment->Load(trace_ctx, op_ctx)
internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp
  -> ChunkedSegmentSealedImpl::Load
  -> ApplyLoadDiff
```

`ApplyLoadDiff` 会分别处理：

- `LoadBatchIndexes`：加载 DataCoord 已构建好的索引。
- `LoadBatchFieldData`：加载 Storage V1 field binlog。
- `LoadColumnGroups`：加载 Storage V2/V3 manifest column group。
- text/json stats、default value、interim index 等补充资源。

### Storage V1 field data

核心调用链：

```text
LoadBatchFieldData
  -> LoadFieldData
  -> storagev1translator::ChunkTranslator::get_cells
  -> LoadArrowReaderFromRemote
  -> storage::GetObjectData
  -> ChunkManager::Size / Read
  -> DeserializeFileData
  -> create_chunk
```

本次 C++ 插桩记录：

- `remote object loaded`：每个对象文件的 `object_va`、`object_size`、`bytes_read`、bucket、root、耗时。
- `field chunks load start`：segment、field、cid、文件路径、row count、memory size、mmap。
- `field chunk materialized`：segment、field、cid、`chunk_va`、row count、memory size。

这条路径能直接看到从 MinIO/S3 读到 QueryNode 进程中的原始对象 buffer VA。

### index data

核心调用链：

```text
LoadBatchIndexes
  -> LoadIndexData
  -> storagev1translator::SealedIndexTranslator::get_cells
  -> IndexFactory::CreateIndex
  -> index->Load / index->LoadUnified
```

常见 index 读取方式：

- 内存索引：`MemFileManagerImpl::LoadIndexToMemory -> storage::GetObjectData`。
- 磁盘索引：`DiskFileManagerImpl::CacheIndexToDisk -> storage::GetObjectData`。
- packed scalar index：`FileManager::OpenInputStream -> RemoteInputStream::Read/ReadAt`。

本次 C++ 插桩记录：

- `index load start/done`：segment、field、indexID、`index_va`、index type、index size、index files、mmap、资源估算。
- `remote object loaded`：经过 `GetObjectData` 的 index 文件原始 buffer VA 和长度。
- `remote input stream read/read_at/read_to_file`：packed index 流式读取时的目标 `buffer_va`、读取长度、offset、file size。

### Storage V2/V3 manifest column group

Storage V2/V3 的数据读取由 milvus-storage reader 接管：

```text
LoadColumnGroups
  -> milvus_storage::api::Reader::create
  -> reader->get_chunk_reader
  -> storagev2translator::ManifestGroupTranslator
  -> ChunkedColumnGroup / ProxyChunkColumn
```

本次插桩记录：

- `manifest column groups load start`：segment、collection、partition、storage version、manifest path、column group 数量。
- `manifest column groups reader created`：reader 对象地址。
- `manifest column group load start`：column group index、field IDs、needed columns、mmap、priority。
- `manifest column group materialized`：`column_group_va`。

注意：这一路径的 parquet/page 级真实 buffer VA 在 milvus-storage 内部，目前本补丁只记录 QueryNode/segcore 可见的 reader 和 column group 对象地址。若需要每个 parquet page 的 VA/length，需要继续在 milvus-storage FFI 或 reader 内部插桩。

## 日志字段

统一过滤关键字：

```text
[querynode-load-trace]
```

关键字段：

- `collectionID` / `partitionID` / `segmentID`
- `numRows`
- `storageVersion`
- `manifestPath`
- `binlogFileCount` / `binlogDiskSize` / `binlogMemorySize`
- `indexID` / `buildID` / `indexFiles` / `indexFileSize`
- `cSegmentVA` / `cLoadIndexInfoVA`
- `object_va` / `object_size` / `bytes_read`
- `buffer_va` / `requested_size` / `offset`
- `chunk_va` / `index_va` / `column_group_va`
- `loadDuration` / `size_us` / `read_us` / `total_us`

## 关联方式

1. 先按 `segmentID` 找 `csegment created`，得到本次 load request 的整体元数据和 `cSegmentVA`。
2. 按同一个 `segmentID` 找 `c load index info prepared`，得到每个 index 的 `indexFiles`。
3. 用 `indexFiles` 中的对象路径去匹配 C++ `remote object loaded` 或 `remote input stream read`。
4. field data 通过 `field chunks load start` 的 `files` 和 `cids` 关联到 `remote object loaded`。
5. 最终通过 `field chunk materialized`、`index load done`、`manifest column group materialized` 关联到 QueryNode 内部对象地址。

## 插桩位置

Go：

- `internal/querynodev2/segments/load_trace.go`
- `internal/querynodev2/segments/segment.go`
- `internal/querynodev2/segments/segment_loader.go`

C++：

- `internal/core/src/storage/Util.cpp`
- `internal/core/src/storage/RemoteInputStream.h`
- `internal/core/src/storage/RemoteInputStream.cpp`
- `internal/core/src/storage/FileManager.h`
- `internal/core/src/segcore/storagev1translator/ChunkTranslator.cpp`
- `internal/core/src/segcore/storagev1translator/SealedIndexTranslator.cpp`
- `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`

## 使用建议

运行 QueryNode 后过滤日志：

```bash
grep -F "[querynode-load-trace]" querynode.log
```

如果只看某个 segment：

```bash
grep -F "[querynode-load-trace]" querynode.log | grep "segmentID"
```

如果要分析 MinIO 到 QueryNode 的对象下载耗时，优先看：

- `remote object loaded`
- `remote input stream read`
- `remote input stream read_at`
- `remote input stream read_to_file`

如果要分析对象最终进入 segcore 的位置，优先看：

- `field chunk materialized`
- `index load start/done`
- `manifest column group materialized`
