# Milvus 读写流程与 QueryNode 远端加载日志分析

本文基于 Milvus v2.4.5 代码梳理整体读写链路，重点展开
`DataNode -> MinIO -> QueryNode` 的全流程，并说明本次新增的 QueryNode
远端加载日志字段。

## 1. 总体链路

### 写入链路

1. Client 发起 insert/delete 请求到 Proxy。
2. Proxy 校验 schema，分配 timestamp 和 segment 信息，将 DML 消息写入
   insert channel。
3. DataNode 消费 channel 中的 insert/delete 消息，写入按 channel 和 segment
   组织的 write buffer。
4. flush、seal、内存水位或 sync policy 触发时，DataNode 将 buffer 转成 sync
   task。
5. sync task 把 insert binlog、statslog、deltalog 写入 MinIO 等对象存储。
6. DataNode 通过 `SaveBinlogPaths` 把对象路径、大小、行数、checkpoint 等元
   数据上报给 DataCoord。
7. DataCoord 保存 segment 元数据，推进 segment flushed 状态，并驱动 IndexNode
   为 flushed segment 构建索引。
8. IndexNode 生成索引文件，DataCoord 保存 indexID、buildID、index file path、
   index size 等索引元数据。
9. QueryCoord 从 DataCoord 拉取 segment/index 元数据，生成 `LoadSegments`
   请求。
10. QueryNode 根据 `SegmentLoadInfo` 从 MinIO 或 storage v2 space 拉取 segment
    数据和索引，加载到 segcore 后对外提供 search/query。

### 读取链路

1. Client 发起 search/query 到 Proxy。
2. Proxy 根据 collection、partition、channel 和 shard leader 信息路由请求。
3. QueryNode shard delegator 等待 tSafe，pin 当前可读的 sealed/growing
   segments，并可按统计信息做 segment prune。
4. delegator 将任务拆给本节点或 worker QueryNode，执行 `SearchSegments` 或
   `QuerySegments`。
5. segcore 在已加载的 sealed/growing segment 上执行向量检索、标量过滤和 delete
   可见性判断。
6. QueryNode 和 Proxy 汇总 partial results，返回最终结果。

## 2. DataNode -> MinIO -> QueryNode 详细流程

### 2.1 DataNode buffer 与序列化

核心入口在 `internal/datanode/writebuffer/write_buffer.go`。

`writeBufferBase.BufferData` 将 insert/delete 消息追加到 per-segment buffer。
`EvictBuffer`、`SealSegments` 和 flush timestamp policy 会选择需要落盘的
segment，生成 `SyncPack` 并交给 sync manager。

storage v1 的序列化在
`internal/datanode/syncmgr/storage_serializer.go`：

- `serializeBinlog` 调用 `storage.InsertCodec.Serialize`，按字段生成 insert
  binlog blob。
- `serializeStatslog` / `serializeMergedPkStats` 生成主键 statslog，供后续
  bloom filter 和 PK 裁剪使用。
- `serializeDeltalog` 调用 `storage.DeleteCodec.Serialize` 生成 delete log。
- `processInsertBlobs`、`processStatsBlob`、`processDeltaBlob` 将 blob 转成对象
  存储 key，并填充 `datapb.Binlog` 元数据。

storage v1 的对象路径形态为：

- insert: `<root>/insert_log/<collection>/<partition>/<segment>/<field>/<logID>`
- stats: `<root>/stats_log/<collection>/<partition>/<segment>/<field>/<logID>`
- delta: `<root>/delta_log/<collection>/<partition>/<segment>/<logID>`

每个 `datapb.Binlog` 会记录：

- `LogPath`: MinIO 对象路径
- `LogSize`: 编码后对象大小
- `MemorySize`: 解码或加载后的内存估算大小
- `EntriesNum`: 行数或 delete 条数
- `TimestampFrom` / `TimestampTo`: 数据时间范围

storage v2 的序列化在
`internal/datanode/syncmgr/storage_v2_serializer.go`：

- insert/delete 数据被转换成 Arrow record reader。
- `SyncTaskV2.writeSpace` 通过 milvus-storage `Space` transaction 写入数据。
- `SyncTaskV2.writeMeta` 将 `space.GetCurrentVersion()` 写成 segment 的
  `StorageVersion`。
- storage v2 不再在 load info 中逐个携带字段 binlog path，而是通过
  `StorageVersion` 定位 space 中的一致快照。

### 2.2 DataNode 写 MinIO 与上报元数据

storage v1 的实际写对象逻辑在 `internal/datanode/syncmgr/task.go`：

1. `SyncTask.Run` 获取 segment 元数据并预分配 log ID。
2. `processInsertBlobs` / `processStatsBlob` / `processDeltaBlob` 构造
   `segmentData map[path][]byte`。
3. `writeLogs` 调用 `ChunkManager.MultiWrite`，把所有对象写入 MinIO 或其它远
   端对象存储。
4. 写对象成功后，`writeMeta` 调用 MetaWriter 更新 DataCoord。

`internal/datanode/syncmgr/meta_writer.go` 中的 `UpdateSync` 构造
`SaveBinlogPathsRequest`，关键字段包括：

- `Field2BinlogPaths`: insert binlog path 列表
- `Field2StatslogPaths`: statslog path 列表
- `Deltalogs`: delete log path 列表
- `CheckPoints`: segment 行数和 channel checkpoint
- `StartPositions`: segment 起始消费位置
- `Flushed` / `Dropped`: segment 状态变化
- `Channel` / `SegLevel`: channel 和 segment level

storage v2 的 `UpdateSyncV2` 主要上报：

- `StorageVersion`
- `CheckPoints`
- `StartPositions`
- `Flushed` / `Dropped`
- `Channel`

### 2.3 DataCoord 与索引元数据

DataCoord 持久化 DataNode 上报的 segment 信息，并维护 flushed、flushing、
dropped 等状态。QueryCoord 后续加载 segment 时依赖这些元数据：

- collectionID、partitionID、segmentID、insert channel、segment level
- row count、start position、delta position/checkpoint
- insert binlogs、statslogs、deltalogs
- storage v2 的 storage version
- 索引元数据，包括 indexID、buildID、fieldID、index name、index type、
  index version、index file paths、index size

索引构建由 DataCoord/IndexNode 流程完成。QueryCoord 准备 load 请求时会通过
broker 拉取索引状态和索引文件信息。

### 2.4 QueryCoord 组装 LoadSegments

`internal/querycoordv2/task/executor.go` 的 `getLoadInfo` 是关键入口：

1. `broker.GetSegmentInfo` 从 DataCoord 拉取 segment 元数据。
2. `broker.GetIndexInfo` 拉取该 segment 的索引元数据。
3. `broker.ListIndexes` 拉取 collection 级索引信息，并补齐用户可配置 index
   params。
4. 调用 `utils.PackSegmentLoadInfo` 生成 `querypb.SegmentLoadInfo`。

`internal/querycoordv2/utils/types.go` 的 `PackSegmentLoadInfo` 会把 DataCoord
元数据映射为 QueryNode load 所需字段：

- `BinlogPaths`: storage v1 insert binlog path
- `Statslogs`: PK statslog path
- `Deltalogs`: delete log path
- `IndexInfos`: 索引文件和索引参数
- `StorageVersion`: storage v2 快照版本
- `DeltaPosition`: channel checkpoint，用于一致性和 delete 增量
- `Level`: L0/L1/Legacy segment level

`internal/querycoordv2/task/utils.go` 的 `packLoadSegmentRequest` 再将
`SegmentLoadInfo`、schema、load meta、index info list 和 load scope 打包成
`querypb.LoadSegmentsRequest`。

### 2.5 QueryNode 接收 LoadSegments

`internal/querynodev2/services.go:LoadSegments` 是 QueryNode load 入口：

1. 打印 collection、partition、channel、segment、level、load scope 等请求
   信息。
2. 检查 collection index 信息是否存在。
3. 对兼容旧版本的 binlog，如果 `MemorySize == 0`，用 `LogSize` 回填。
4. 如果 `NeedTransfer` 为 true，先交给 shard delegator 转发到目标 worker。
5. `LoadScope_Delta` 调用 `node.loadDeltaLogs`。
6. `LoadScope_Index` 调用 `node.loadIndex`。
7. full load 调用 `node.loader.Load`，进入 segment loader。

### 2.6 QueryNode 从 MinIO 加载 storage v1 segment

storage v1 loader 位于
`internal/querynodev2/segments/segment_loader.go`。

sealed segment full load 的主流程：

1. `segmentLoader.Load` 过滤已经 loaded/loading 的 segment，并做资源预估。
2. `LoadSegment` 开始单个 segment 加载。
3. `loadSealedSegment` 将字段拆成已建索引字段和需要加载 raw binlog 的字段。
4. `AddFieldDataInfo` 先把字段/binlog 元数据注册到 segcore。
5. `loadFieldsIndex` / `loadFieldIndex` 调用 `LocalSegment.LoadIndex` 加载索引。
6. `loadSealedSegmentFields` 调用 `LocalSegment.LoadFieldData` 加载 raw field
   binlog。
7. `LoadDeltaLogs` 通过 `ChunkManager.Read` 读取 deltalog，反序列化后调用
   `LoadDeltaData` 加载 delete record。
8. 对 growing segment，还会通过 `loadBloomFilter` 读取 statslog 构建 PK bloom
   filter。

实际对象下载进入 C++：

- `internal/core/src/storage/Util.cpp:DownloadAndDecodeRemoteFile`
- 先调用 `ChunkManager.Size(file)` 获取对象大小
- 再调用 `ChunkManager.Read(file, ...)` 从 MinIO 拉取 bytes
- 最后 `DeserializeFileData` 解码为 segcore 可加载数据

storage v1 索引加载也通过 index file manager 读取远端对象。内存索引常见路径为
`MemFileManagerImpl.LoadIndexToMemory -> GetObjectData ->
DownloadAndDecodeRemoteFile`；磁盘索引或 mmap 场景会通过 disk file manager 拉取
远端 index slices 到本地文件。

### 2.7 QueryNode 加载 storage v2 segment

storage v2 loader 是 `segmentLoaderV2`：

1. `NewSegmentV2` 根据 `SegmentLoadInfo.StorageVersion` 打开 segment 对应的
   milvus-storage `Space`。
2. `LoadSegment` 加载 PK stats、index、field data 和 delete data。
3. `LocalSegment.LoadFieldData` / `LoadMultiFieldData` 将 storage URI 和当前
   storage version 传入 segcore。
4. `LocalSegment.LoadDeltaData2` 调用 `space.ScanDelete()` 读取 delete record。
5. `LocalSegment.LoadIndex` 在 storage v2 下走 `AppendIndexV3`，由 C++ index
   实现从 space 中读取索引 blob。

storage v2 的 C++ 数据加载关键路径包括：

- `internal/core/src/segcore/Utils.cpp:LoadFieldDatasFromRemote2`
- `internal/core/src/storage/Util.cpp:DownloadAndDecodeRemoteFileV2`
- `internal/core/src/index/*` 中各类 index 的 `LoadV2`

## 3. 查询执行路径

QueryNode 查询入口在 `internal/querynodev2/services.go`：

- `Search` 按 channel 并发调用 `searchChannel`，最后 reduce shard 结果。
- `Query` 按 channel 并发调用 `queryChannel`，最后执行 retrieve reduce。
- `SearchSegments` / `QuerySegments` 是 shard delegator 调用 worker 的实际执行
  入口。

shard delegator 位于 `internal/querynodev2/delegator/delegator.go`：

- `Search` / `Query` 先等待 tSafe，确保读到满足 guarantee timestamp 的数据。
- `distribution.PinReadableSegments` 固定当前可读 sealed/growing segment 快照。
- `PruneSegments` 可基于 partition stats 做 segment 裁剪。
- `organizeSubTask` 按 worker 和 data scope 拆分任务。
- `executeSubTasks` 并发调用 worker 的 `SearchSegments` / `QuerySegments`。

加载流程的结果是 QueryNode manager 中存在可读 segment，查询阶段不会再次从
MinIO 拉取完整 segment；只有 lazy load、chunk cache 或特定 mmap/index 机制可能
在查询时触发局部数据访问。

## 4. Size 与 VA size 语义

本次日志中会同时出现几类 size：

- `LogSize`: DataNode 写入对象存储后的编码对象大小。
- `MemorySize`: DataCoord/binlog 元数据中的解码后内存估算。
- `IndexSize`: DataCoord 索引元数据中的索引文件大小。
- `segmentMemSize`: segment 加载后 segcore 上报的内存使用。
- `estimatedMemory`: QueryNode resource estimator 预测的内存占用。
- `estimatedDisk`: QueryNode resource estimator 预测的本地磁盘或 mmap footprint。
- `estimatedVASize`: 本次新增的估算字段，定义为
  `estimatedMemory + estimatedDisk`。
- `segment_length_bytes`: segcore 构造出字段列对象后记录的实际 segment field
  映射长度；对 mmap 字段通常对应文件映射长度，对匿名内存映射字段对应列对象
  当前占用长度。
- `column_byte_size`: `ColumnBase::ByteSize()` 返回值，用于和
  `segment_length_bytes` 对照；稀疏向量非 mmap 场景下还需要结合 `source_bytes`
  判断原始数据长度。
- `data_va` / `mmap_va`: segcore 列对象暴露的实际虚拟地址。普通字段通常二者
  相同；稀疏向量可能出现 `data_va` 指向 sparse row 视图、`mmap_va` 指向原始
  mmap buffer 的情况。

Milvus v2.4.5 的 load 元数据中没有独立的一等 `VA size` 字段。因此新增日志用
`va_size_label=estimated_memory_plus_mmap_or_disk_footprint` 明确说明其含义：
这是用于排查加载压力的估算值，尤其适合 mmap 或 disk-load index 让单纯进程内存
指标不够直观的场景。真正的 VA 地址和字段加载长度以后续 C++ 日志
`querynode segment field data va trace` 和
`querynode chunk cache mmap va trace` 为准。

## 5. 本次新增日志

新增 Go helper 文件：

- `internal/querynodev2/segments/load_trace.go`

该 helper 会汇总：

- collectionID、partitionID、segmentID、channel、level、storageVersion、rowCount
- insert/stats/delta 的 field count、file count、object size、memory size、
  entry count、样例 path
- index 的 field count、file count、index size、field IDs、index names、
  index types、mmap enabled count、disk load count、样例 path
- total object count、total object size
- resource estimate 中的 estimated memory、estimated disk、estimated VA size

新增 Go 日志点：

- segment load prepared/start/finish，覆盖 storage v1 和 storage v2
- `checkSegmentSize` 中的 segment 级资源预估
- field/multi-field data load 的 before/after
- storage v2 URI/version 传给 segcore 前的记录
- index load dispatch、append index info 前、成功 update 后
- delta log 读取前后，以及实际读取 bytes
- PK statslog 或 storage v2 stats blob 读取

新增 C++ 日志点：

- `DownloadAndDecodeRemoteFile`: 记录 storage v1 对象 path 和 `Size(file)` 返回
  的对象大小。
- `DownloadAndDecodeRemoteFileV2`: 记录 storage v2 blob 名称和
  `GetBlobByteSize` 返回的大小。
- `SegmentSealedImpl::LoadFieldData`: 记录非 mmap 字段列对象的
  `segment_id`、`field_id`、`segment_length_bytes`、`column_byte_size`、
  `source_bytes`、`data_va` 和 `mmap_va`。
- `SegmentSealedImpl::MapFieldData`: 记录 mmap 字段列对象的实际映射长度、
  VA 地址和 mmap 临时文件路径。
- `ChunkCache::Mmap`: 记录 chunk cache 从远端对象解码并 mmap 到本地后的
  cache file、数据类型、行数、长度和 VA 地址。

## 6. 建议排查方式

常用过滤关键词：

- `querynode prepared segment load from remote storage`
- `querynode segment remote load resource estimate`
- `querynode load field data from remote storage`
- `querynode load index from remote storage`
- `querynode load delta logs from remote storage`
- `querynode load storage v2 segment data from remote storage`
- `segcore download object from remote storage`
- `segcore download storage v2 blob from remote storage`
- `querynode segment field data va trace`
- `querynode chunk cache mmap va trace`

建议按以下字段关联：

- `collectionID`
- `partitionID`
- `segmentID`
- `channel`
- `storageVersion`
- `fieldID`
- `indexID`
- `buildID`

排查慢加载或大 segment 时，先看 prepared 和 resource estimate 日志，确认
`loadObjectSummary` 中的对象数量、索引大小、binlog 大小和 `estimatedVASize`。
随后按 `segmentID` 关联 field/index/delta 读取日志。如果 QueryNode 看到的对象
元数据大小与 C++ 实际下载大小不一致，应重点检查 DataCoord 元数据新鲜度、索引
构建状态和对象存储一致性。
