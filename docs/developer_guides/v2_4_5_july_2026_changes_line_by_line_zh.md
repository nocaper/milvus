# Milvus v2.4.5 2026 年 7 月代码修改逐行解析

源代码仓库：`D:\project\v2.4.5\milvus`

分析范围：该仓库在 2026-07-01 到 2026-07-31 之间的所有 git commit。

实际命中的 7 月提交只有 3 个：

| 提交 | 时间 | 标题 | 作用 |
| --- | --- | --- | --- |
| `414b8602e1` | 2026-07-31 09:52:38 +0800 | `v2.4.5加trace` | 给 QueryNode / segcore 远端加载链路增加结构化 trace |
| `e784f490c4` | 2026-07-31 16:10:27 +0800 | `脚本` | 增强 remote object trace，并新增日志分析脚本 |
| `9ce6cd5cd1` | 2026-07-31 16:16:30 +0800 | `compile error fix` | 修复编译依赖和 zap 字段类型 |

整体变更目标一句话概括：这些提交不是改业务语义，而是在 Milvus v2.4.5 的 QueryNode segment load 链路里加可观测性，追踪从 DataCoord 元数据、MinIO/object storage 对象、storage v2 blob、segcore 内存/mmap 映射，到 Python 离线统计脚本的完整链路。

## 1. 文件变更总览

| 文件 | 修改类型 | 主要含义 |
| --- | --- | --- |
| `internal/querynodev2/segments/load_trace.go` | 新增 | Go 侧统一 trace helper，负责汇总 binlog/index/load/resource 信息 |
| `internal/querynodev2/segments/segment.go` | 修改 | 在 field/index/delta load 的入口和出口打 trace |
| `internal/querynodev2/segments/segment_loader.go` | 修改 | 在 segment loader、bloom filter、delta、patchEntryNumber 等读取路径加 trace |
| `internal/core/src/storage/Util.cpp` | 修改 | C++ 侧对远端对象 size/read/decode 阶段打 trace |
| `internal/core/src/segcore/SegmentSealedImpl.cpp` | 修改 | 记录字段加载后的虚拟地址、mmap 地址、字段大小 |
| `internal/core/src/storage/ChunkCache.cpp` | 修改 | 记录 chunk cache mmap 后的地址和大小 |
| `internal/core/conanfile.py` | 修改 | 增加 `thrift/0.20.0` 依赖，修复编译 |
| `tools/querynode_trace/analyze_minio_trace.py` | 新增 | 解析 Milvus JSON 日志和 C++ 文本日志，统计远端对象读取 |
| `docs/developer_guides/milvus_querynode_load_trace_flow.md` | 新增/修改 | 说明 DataNode -> MinIO -> QueryNode load 流程和新增日志字段 |

## 2. `internal/querynodev2/segments/load_trace.go`

这是本次最核心的 Go 新文件。它不参与数据加载逻辑决策，只负责把 load 过程里的元数据整理成结构化日志。

### 2.1 文件头与包声明

| 代码 | 含义 |
| --- | --- |
| `// Licensed ...` | Milvus 标准 Apache/LF AI 版权头，没有执行逻辑。 |
| `package segments` | 这个 helper 属于 `segments` 包，可以直接访问该包内的 `LocalSegment`、`ResourceUsage`、`isIndexMmapEnable` 等类型/函数。 |

### 2.2 import 行逐项解释

| 代码 | 含义 |
| --- | --- |
| `context` | 所有日志函数都接收 `ctx`，用于 `log.Ctx(ctx)` 带上请求上下文。 |
| `path` | 用 `path.Base` / `path.Split` 从 MinIO 对象路径里提取文件名和尾部路径。 |
| `strconv` | 第二个提交新增，用于把路径里的 collection/segment/log id 字符串解析成 `int64`。 |
| `strings` | 第二个提交新增，用于按 `/` 拆分远端对象路径。 |
| `time` | 第二个提交新增，用于记录对象读取耗时。 |
| `github.com/samber/lo` | 用 `lo.Keys` 从 map 中取 index name/type 列表。 |
| `go.uber.org/zap` | Milvus Go 侧结构化日志字段类型。 |
| `datapb` | 读取 `FieldBinlog` 和 `Binlog` 元数据。 |
| `querypb` | 读取 `SegmentLoadInfo` 和 `FieldIndexInfo`。 |
| `common` | 读取 Milvus 标准对象路径常量和 index type key。 |
| `log` | Milvus v2.4.5 里的日志包。 |
| `funcutil` | 从 index params 的 KV 列表中取 `index_type`。 |
| `indexparamcheck` | 判断 `DISKANN` / `INVERTED` 等磁盘加载类 index。 |
| `paramtable` | 读取 `EnableStorageV2`、mmap 目录等配置。 |

### 2.3 `binlogTraceSummary`

这个结构体用于描述 insert/stats/delta binlog 的汇总信息。

| 字段 | 含义 |
| --- | --- |
| `FieldCount` | 有多少个字段携带 binlog。 |
| `FileCount` | 所有字段 binlog 文件数量总和。 |
| `LogSize` | DataCoord 元数据里记录的对象存储编码后大小总和。 |
| `MemorySize` | DataCoord 元数据里记录的解码后内存估算总和。 |
| `EntryCount` | binlog 内记录的 entry/row/delete 数总和。 |
| `LogSizeMiB` | `LogSize` 换算成 MiB，方便读日志。 |
| `MemorySizeMiB` | `MemorySize` 换算成 MiB。 |
| `MemoryLabel` | 明确说明 `MemorySize` 来源是 DataCoord binlog meta，不是实时 RSS。 |
| `PathSample` | 随机取第一个对象路径样例，便于关联 MinIO 路径。 |
| `PathSampleBase` | 样例路径最后一段文件名。 |
| `PathSampleLogID` | 样例 binlog 的 log id。 |
| `PathSampleLogIdx` | `path.Split` 得到的最后路径段；这里和 base 语义接近，主要用于排查路径解析。 |

### 2.4 `indexTraceSummary`

这个结构体用于描述 index 文件元数据。

| 字段 | 含义 |
| --- | --- |
| `FieldCount` | 有索引的字段数量，按 fieldID 去重。 |
| `FileCount` | 索引文件路径数量总和。 |
| `IndexSize` | DataCoord 元数据里记录的索引大小。 |
| `IndexSizeMiB` | 索引大小 MiB。 |
| `IndexFieldIDs` | 哪些 fieldID 有索引。 |
| `IndexNames` | 索引名去重列表。 |
| `IndexTypes` | 索引类型去重列表，例如 IVF、HNSW、DISKANN、INVERTED。 |
| `MmapEnabledCount` | 开启 mmap 的索引数量。 |
| `DiskLoadCount` | 被认为走磁盘加载路径的索引数量；当前统计 DISKANN 和 INVERTED。 |
| `PathSample` | 第一条索引文件路径样例。 |
| `PathSampleBase` | 索引样例路径最后一段文件名。 |

### 2.5 `segmentLoadTraceSummary`

这个结构体是单个 segment load 的总视图。

| 字段 | 含义 |
| --- | --- |
| `CollectionID` / `PartitionID` / `SegmentID` | load 的 collection、partition、segment 标识。 |
| `Channel` | segment 对应的 insert channel。 |
| `RowCount` | segment 行数。 |
| `Level` | segment level，例如 L0/L1/Legacy。 |
| `StorageVersion` | storage v2 的 space version；storage v1 通常为 0。 |
| `Insert` | insert binlog 汇总。 |
| `Stats` | statslog 汇总。 |
| `Delta` | deltalog 汇总。 |
| `Index` | index file 汇总。 |
| `TotalObjectCount` | insert/stats/delta/index 对象数量总和。 |
| `TotalObjectSize` | insert/stats/delta/index 元数据大小总和。 |
| `TotalObjectSizeMiB` | `TotalObjectSize` 的 MiB 形式。 |
| `EstimatedMemory` | QueryNode resource estimator 估算内存。 |
| `EstimatedDisk` | QueryNode resource estimator 估算磁盘/mmap footprint。 |
| `EstimatedVASize` | `EstimatedMemory + EstimatedDisk`，用于粗略表达 VA 压力。 |
| `EstimatedVASizeMiB` | VA 估算 MiB。 |
| `VASizeLabel` | 明确 VA 估算含义，避免误认为真实进程 RSS。 |

### 2.6 `remoteObjectPathTrace`

第二个提交新增，用于把远端对象路径解析成结构化字段。

| 字段 | 含义 |
| --- | --- |
| `RemoteKind` | 对象类型：`segment`、`stats`、`delta`、`index`、`raw_data`、`unknown`。 |
| `CollectionID` / `PartitionID` / `SegmentID` | 从路径里解析出的 ID。 |
| `FieldID` | insert/stats/raw_data 路径里的 field id。 |
| `LogID` | binlog/deltalog/raw_data 路径里的 log id。 |
| `BuildID` | index path 里的 build id。 |
| `IndexVersion` | index path 里的 index version。 |
| `PathBase` | 远端路径最后一段，便于 grep。 |

### 2.7 工具函数逐行解释

| 函数 | 代码含义 |
| --- | --- |
| `mib(size int64)` | 把字节数转成 MiB。 |
| `mibUint(size uint64)` | 同上，但输入是 `uint64`，用于 `ResourceUsage`。 |
| `parseTraceInt64` | 尝试把字符串解析成 int64；失败返回 `-1`，这样日志字段仍然存在但表示解析失败。 |
| `parseRemoteObjectPathForTrace` 初始化 `RemoteKind: "unknown"` | 默认无法识别路径时标成 unknown，而不是空字符串。 |
| `PathBase: path.Base(remotePath)` | 即使路径无法解析，也保留文件名样例。 |
| `parts := strings.Split(remotePath, "/")` | 按对象路径分段。 |
| `case common.SegmentInsertLogPath` | 识别 insert binlog 路径。 |
| `i+5 < len(parts)` | 确保路径后面至少有 collection/partition/segment/field/log 五段。 |
| `trace.RemoteKind = "segment"` | insert_log 被归类成 segment 原始数据对象。 |
| `trace.CollectionID = ...` 到 `trace.LogID = ...` | 从标准路径固定位置解析 ID。 |
| `case common.SegmentStatslogPath` | 识别 statslog，结构和 insert_log 一致。 |
| `case common.SegmentDeltaLogPath` | 识别 deltalog；delta path 没有 fieldID，所以只解析到 logID。 |
| `case common.SegmentIndexPath` | 识别 index file；解析 buildID/indexVersion/partitionID/segmentID。 |
| `case "raw_datas"` | 识别 raw data 路径，常见于 index/chunk cache 需要原始字段数据。 |
| `return trace` | 一旦识别出一种路径，就立即返回，避免后续路径段误匹配。 |

### 2.8 `logRemoteObjectFetchTrace`

这个函数是 Go 侧直接读取对象时的统一日志入口。

| 代码/字段 | 含义 |
| --- | --- |
| `event` | 记录事件阶段，例如 `read_begin`、`read_done`。 |
| `storage_version` | 字符串 `v1` 或 `v2`，这里 Go 侧新增点主要是 v1 chunk manager。 |
| `remote_kind` | 从路径解析出的对象类型。 |
| `remote_path` | 完整对象路径。 |
| `path_base` | 最后一级路径名。 |
| `collection_id` 等 ID 字段 | 统一字段名用 snake_case，方便 Python 脚本解析。 |
| `encoded_bytes` | 对象存储读取字节数，或元数据里预计字节数。 |
| `encoded_bytes_mib` | 字节数转 MiB。 |
| `duration_ms` | 用 `duration.Microseconds()/1000` 输出毫秒，保留小数精度。 |
| `fields ...zap.Field` | 允许调用方补充 caller、entry_count、memory_size 等上下文。 |

### 2.9 汇总函数逐行解释

| 函数/逻辑 | 含义 |
| --- | --- |
| `summarizeFieldBinlogs(fields)` | 汇总一组 `FieldBinlog`。 |
| `if field == nil { continue }` | 防御式跳过空字段，避免 panic。 |
| `summary.FieldCount++` | 每遇到一个非空字段就计数。 |
| `for _, binlog := range field.GetBinlogs()` | 遍历字段下所有对象文件。 |
| `if binlog == nil { continue }` | 防御式跳过空 binlog。 |
| `if summary.PathSample == ""` | 只保存第一个路径样例，避免日志太大。 |
| `summary.FileCount++` | 累加对象文件数。 |
| `summary.LogSize += binlog.GetLogSize()` | 累加远端对象编码大小。 |
| `summary.MemorySize += binlog.GetMemorySize()` | 累加 DataCoord 记录的内存估算。 |
| `summary.EntryCount += binlog.GetEntriesNum()` | 累加行数/entry 数。 |
| `summary.LogSizeMiB = mib(...)` | 生成可读单位。 |
| `summary.MemoryLabel = ...` | 明确 MemorySize 的来源。 |
| `summarizeIndexes(indexes)` | 汇总索引元数据。 |
| `fieldIDs/indexNames/indexTypes := map...` | 用 map 去重。 |
| `indexInfo.GetFieldID()` | 统计索引所属字段。 |
| `GetAttrByKeyFromRepeatedKV(common.IndexTypeKey, ...)` | 从 index params 取 index type。 |
| `IndexDISKANN || IndexINVERTED` | 这两个被认为可能走 disk load 或 mmap-like 路径，所以计数。 |
| `isIndexMmapEnable(indexInfo)` | 复用原有判断索引 mmap 开关的函数。 |
| `FileCount += len(indexInfo.GetIndexFilePaths())` | 索引文件数量。 |
| `IndexSize += indexInfo.GetIndexSize()` | 索引文件元数据大小。 |
| `lo.Keys(...)` | 把去重 map 转成数组写进日志。 |
| `summarizeSegmentLoadTrace(loadInfo)` | 把 SegmentLoadInfo 映射成总 summary。 |
| `TotalObjectCount = insert + stats + delta + index` | 统一看一次 load 涉及多少远端对象。 |
| `TotalObjectSize = insert + stats + delta + index` | 统一看一次 load 计划读多少字节。 |

### 2.10 日志函数逐行解释

| 函数 | 含义 |
| --- | --- |
| `logSegmentLoadTrace` | 打 segment 级 summary 日志，适合 load 开始/结束/资源估算。 |
| `if loadInfo == nil { return }` | 防止空 loadInfo 导致 panic。 |
| `zap.Any("loadObjectSummary", summary)` | 把完整 summary 挂到日志，后续 Python 脚本可解析。 |
| `logFieldLoadTrace` | 打单字段 raw binlog load 前后日志。 |
| `summarizeFieldBinlogs(nil)` | 当 field 为空时仍输出一个空 summary。 |
| `zap.Bool("mmapEnabled", mmapEnabled)` | 记录该字段是否 mmap 加载。 |
| `logMultiFieldLoadTrace` | 打多字段一起 load 的日志，常见于 growing/storage v2。 |
| `logDeltaLoadTrace` | 打 deltalog 读取前后日志。 |
| `logIndexLoadTrace` | 打索引加载阶段日志，包含 fieldID/indexID/buildID/version。 |
| `withResourceEstimate` | 把 resource estimator 的 memory/disk 估算补进 summary。 |
| `EstimatedVASize = MemorySize + DiskSize` | 这里的 VA size 是估算，不是 Linux `/proc` 的真实 VIRT。 |
| `logSegmentResourceTrace` | 在资源检查阶段输出估算内存、磁盘、VA size。 |
| `logStorageV2Trace` | 专门记录 storage v2 URI/version 进入 segcore 前后的事件。 |
| `storageV2CurrentVersion` | 安全获取 `segment.space.GetCurrentVersion()`。 |
| `if segment == nil || segment.space == nil` | 防止 storage v2 space 为空时 panic；调用方会返回错误。 |

## 3. `internal/querynodev2/segments/segment.go`

这个文件里的改动都是在原有加载逻辑周围加 trace，同时修正了异步 load pool 错误没有被捕获的问题。

### 3.1 `LoadMultiFieldData`

| 修改行 | 含义 |
| --- | --- |
| `logMultiFieldLoadTrace(..., "before-submit")` | 在提交 C load 任务前记录这次多字段加载的 binlog 数量、大小、rowCount。 |
| `_, err = GetLoadPool().Submit(...).Await()` | 原来没有接收 `Await()` 返回的 error；现在把 load pool 内部返回的 Go error 保存到 `err`。 |
| `storageVersion, ok := storageV2CurrentVersion(s)` | 不再直接 `s.space.GetCurrentVersion()`，改用安全 helper。 |
| `if !ok { return nil, fmt.Errorf(...) }` | 如果 storage v2 space 为空，返回明确错误。 |
| `appendStorageVersion(storageVersion)` | 继续把当前 storage version 传给 C++。 |
| `logStorageV2Trace(..., "load-multi-field-data", ...)` | 记录 storage v2 URI/version、rowCount、binlog summary。 |
| `if err != nil { return err }` | 如果 load pool 内部返回 Go error，直接向上返回。 |
| `logMultiFieldLoadTrace(..., "after-load")` | C status 成功后记录多字段加载完成。 |

### 3.2 `LoadFieldData`

| 修改行 | 含义 |
| --- | --- |
| `logFieldLoadTrace(..., "before-submit")` | 单字段 load 前记录 fieldID、rowCount、是否 mmap、binlog summary。 |
| `_, err = GetLoadPool().Submit(...).Await()` | 捕获 load pool 内部 error。 |
| `storageVersion, ok := storageV2CurrentVersion(s)` | 安全读取 storage v2 current version。 |
| `if !ok { return nil, fmt.Errorf(...) }` | storage v2 segment space 缺失时返回明确错误。 |
| `logStorageV2Trace(..., "load-field-data", ...)` | 记录单字段 storage v2 加载的 URI/version/fieldID/mmap。 |
| `zap.Any("binlogSummary", summarizeFieldBinlogs(...))` | 把这个字段的 binlog 元数据一并写到日志。 |
| `if err != nil { return err }` | load pool 内部失败不再被吞掉。 |
| `logFieldLoadTrace(..., "after-load")` | C status 成功后记录单字段加载完成。 |

### 3.3 `LoadDeltaData2`

| 修改行 | 含义 |
| --- | --- |
| `storageVersion, ok := storageV2CurrentVersion(s)` | delete 数据扫描也记录 storage v2 版本。 |
| `if !ok { return fmt.Errorf(...) }` | 防止 `s.space` 为空时 panic。 |
| `logStorageV2Trace(..., "scan-delete", "", storageVersion)` | 开始扫描 storage v2 delete record 前打日志。 |
| `logStorageV2Trace(..., "load-delete-done", ..., zap.Int("rowNum", len(tss)))` | delete record 加载完成后记录删除记录数。 |

### 3.4 `AddFieldDataInfo`

| 修改行 | 含义 |
| --- | --- |
| `logMultiFieldLoadTrace(..., "add-field-data-info")` | 在把字段/binlog 元数据注册给 segcore 后记录一次摘要；这不是实际下载，只是把 load 元数据传入 C++。 |

### 3.5 `LoadIndex`

| 修改行 | 含义 |
| --- | --- |
| `logStorageV2Trace(..., "load-index", ...)` | storage v2 索引加载前记录 URI、indexStoreVersion、fieldID、indexID、buildID。 |
| `logIndexLoadTrace(..., "before-append-index-info")` | 把 indexInfo append 到 C 结构之前记录索引 summary。 |
| 非向量字段或已有 raw data 的提前返回分支新增 `log.Info("Finish loading index", ...)` | 原来该分支直接 return，日志不完整；现在也记录耗时。 |
| 同一分支新增 `logIndexLoadTrace(..., "after-load-index", warmupChunkCache=false)` | 即使不需要 warmup chunk cache，也记录 index load 完成。 |
| `zap.Duration("updateIndexInfoSpan", warmupChunkCacheSpan)` 改成 `zap.Duration("warmupChunkCacheSpan", warmupChunkCacheSpan)` | 修正字段名重复的日志 bug；原来两个字段都叫 `updateIndexInfoSpan`，后者含义其实是 warmup 耗时。 |
| 正常向量索引分支新增 `logIndexLoadTrace(..., "after-load-index", warmupChunkCache=true)` | 记录索引 load 完成并标记执行了 chunk cache warmup。 |

## 4. `internal/querynodev2/segments/segment_loader.go`

这个文件的修改最多，目标是在 segment loader 各阶段插入 trace。

### 4.1 `segmentLoaderV2.Load`

| 修改行 | 含义 |
| --- | --- |
| `for _, info := range infos` | 遍历经过 prepare/filter 后真正要 load 的 segment。 |
| `logSegmentLoadTrace(..., "querynode prepared storage v2 segment load from remote storage", info)` | 在资源申请前记录 storage v2 segment 的计划加载对象摘要。 |
| `segmentMemSize := segment.MemSize()` | segment load 完成后读取 segcore 当前内存估算。 |
| `log.Info("load segment done", segmentMemSize, segmentMemSizeMiB)` | 在普通完成日志里加内存大小。 |
| `logSegmentLoadTrace(..., "querynode finished storage v2 segment load from remote storage", ..., segmentMemSize)` | storage v2 segment load 完成后记录 summary + 实际 segment memory。 |

### 4.2 `segmentLoaderV2.loadBloomFilter`

| 修改行 | 含义 |
| --- | --- |
| `var statsBlobBytes int64` | 累加 storage v2 stats blob 的总字节数。 |
| `statsBlobBytes += int64(statsBlob.Size)` | 每个 stats blob 都加入总大小。 |
| `log.Info("querynode load storage v2 pk stats blob from remote storage", ...)` | 读取每个 PK stats blob 前记录 blobName、大小、storageVersion。 |
| `zap.Int64("blobSize", statsBlob.Size)` | 第三个提交把 `zap.Int` 改成 `zap.Int64`，因为 `statsBlob.Size` 类型是 int64。 |
| `Successfully load pk stats` 新增 `statsBlobNum/statsBlobBytes/statsBlobBytesMiB` | bloom filter 构建完成后输出 stats blob 数量和读取字节数。 |

### 4.3 `segmentLoaderV2.LoadSegment`

| 修改行 | 含义 |
| --- | --- |
| `logSegmentLoadTrace(..., "querynode start storage v2 segment load from remote storage", loadInfo)` | 单 segment storage v2 load 刚开始时输出计划对象摘要。 |
| `err = loader.LoadDelta(...)` | 原来直接 `return loader.LoadDelta(...)`；现在拆开，为了在成功后继续记录 trace。 |
| `if err != nil { return err }` | delta load 失败仍立即返回。 |
| `segmentMemSize := segment.MemSize()` | delta 加载后读取 segment 内存大小。 |
| `logSegmentLoadTrace(..., "querynode storage v2 segment data loaded from remote storage", ...)` | storage v2 单 segment 的数据和 delta 都加载完成后记录最终摘要。 |
| `return nil` | 替代原来的直接返回 `LoadDelta`。 |

### 4.4 `segmentLoader.Load` storage v1

| 修改行 | 含义 |
| --- | --- |
| `logSegmentLoadTrace(..., "querynode prepared segment load from remote storage", info)` | storage v1 资源申请前记录计划加载对象。 |
| defer 里 `segmentMemSize := segment.MemSize()` | 每个 goroutine 退出时记录 segment 当前内存，方便失败或成功后看残留。 |
| `logger.Info("load segment done", segmentMemSize, segmentMemSizeMiB)` | 成功日志里加内存大小。 |
| `logSegmentLoadTrace(..., "querynode finished segment load from remote storage", ...)` | storage v1 load 完成后记录最终对象摘要和内存大小。 |

### 4.5 `loadSealedSegment`

| 修改行 | 含义 |
| --- | --- |
| `logSegmentLoadTrace(..., "querynode start sealed segment load from remote storage", loadInfo)` | sealed segment 进入字段/index load 前记录摘要。 |
| `segmentMemSize := segment.MemSize()` | sealed segment 完成 fields/index/delta 相关处理后取内存大小。 |
| `Finish loading segment` 日志新增 `segmentMemSize` / `segmentMemSizeMiB` | 原有耗时日志补充内存维度。 |
| `logSegmentLoadTrace(..., "querynode sealed segment data loaded from remote storage", ...)` | sealed segment load 完成后输出各阶段耗时和对象摘要。 |

### 4.6 `segmentLoader.LoadSegment` storage v1

| 修改行 | 含义 |
| --- | --- |
| `logSegmentLoadTrace(..., "querynode start segment load from remote storage", loadInfo)` | storage v1 单 segment 开始 load 时记录计划对象。 |
| `segmentMemSize := segment.MemSize()` | 单 segment load 完成后读取内存。 |
| `logSegmentLoadTrace(..., "querynode segment data loaded from remote storage", ...)` | 完成日志补充 summary 和内存。 |

### 4.7 `loadFieldsIndex` / `loadFieldIndex`

| 修改行 | 含义 |
| --- | --- |
| `logIndexLoadTrace(..., "before-load-field-index", rawFieldBinlogSummary=...)` | 每个 indexed field 加载前，记录索引元数据和该字段 raw binlog 摘要。 |
| `logIndexLoadTrace(..., "dispatch-load-field-index")` | 根据 schema 拿到 fieldType 后，记录即将分派到 `LocalSegment.LoadIndex`。 |

### 4.8 `loadBloomFilter` storage v1

| 修改行 | 含义 |
| --- | --- |
| `log.Info("querynode read pk stats logs from remote storage", binlogPaths, statsLogNum, statsLogType)` | 批量读取 statslog 前记录所有路径和数量。 |
| `for _, binlogPath := range binlogPaths { logRemoteObjectFetchTrace(..., "read_begin") }` | 每个 statslog 读之前输出统一 remote object trace。 |
| `readStart := time.Now()` | 记录 `MultiRead` 总耗时。 |
| `readDuration := time.Since(readStart)` | 统计这一批 MultiRead 耗时。 |
| `for idx, value := range values` | 按返回值和路径配对。 |
| `if idx >= len(binlogPaths) { break }` | 防御性避免返回值多于路径导致越界。 |
| `logRemoteObjectFetchTrace(..., "read_done", ..., int64(len(value)), ...)` | 每个 statslog 读完后记录实际 bytes。 |
| `batch_duration_ms` | MultiRead 是批量接口，单对象没有独立耗时，所以记录整个 batch 耗时。 |
| `loadedBytes := lo.SumBy(values, ...)` | 统计实际读回的 statslog 字节数。 |
| `Successfully load pk stats` 新增 `loadedBytes/loadedBytesMiB` | bloom filter 构建完成后输出实际读取量。 |

### 4.9 `LoadDeltaLogs`

| 修改行 | 含义 |
| --- | --- |
| `logDeltaLoadTrace(..., "before-read-delta")` | deltalog 读取前输出 delta 文件摘要。 |
| `logRemoteObjectFetchTrace(..., "read_begin", bLog.GetLogPath(), bLog.GetLogSize(), ...)` | 每个 deltalog 读取前记录路径、预计对象大小、entry_count、memory_size。 |
| `readStart := time.Now()` | 单个 deltalog 读取开始时间。 |
| `value, err := loader.cm.Read(...)` | 原有读取逻辑不变。 |
| `logRemoteObjectFetchTrace(..., "read_done", ..., int64(len(value)), time.Since(readStart), ...)` | 每个 deltalog 读取完成后记录真实 bytes 和耗时。 |
| `loadedBytes := lo.SumBy(blobs, ...)` | 统计所有 deltalog blob 实际字节数。 |
| `logDeltaLoadTrace(..., "after-read-delta", deleteCount, loadedBytes, loadedBytesMiB)` | delta 解码加载完成后记录删除数和实际读取量。 |

### 4.10 `patchEntryNumber`

| 修改行 | 含义 |
| --- | --- |
| `logRemoteObjectFetchTrace(..., "read_begin", binlog.LogPath, ...)` | legacy rowID binlog 读取前记录路径、大小、entry_count。 |
| `readStart := time.Now()` | 记录读取起点。 |
| `bs, err := loader.cm.Read(...)` | 原有读取 rowID binlog 的逻辑。 |
| `logRemoteObjectFetchTrace(..., "read_done", ..., int64(len(bs)), time.Since(readStart), ...)` | 读取完成后记录真实 bytes 和耗时。 |

### 4.11 `checkSegmentSize`

| 修改行 | 含义 |
| --- | --- |
| `logSegmentResourceTrace(ctx, loadInfo.GetCollectionID(), usage, loadInfo)` | 资源预估时把 memory/disk/VA estimate 和 loadObjectSummary 一起写入日志。 |

### 4.12 `LoadIndex`

| 修改行 | 含义 |
| --- | --- |
| `for _, info := range infos` | 遍历真正要加载索引的 segment。 |
| `logSegmentLoadTrace(..., "querynode prepared segment index load from remote storage", info)` | 单独 load index 的请求，也记录计划对象摘要。 |

## 5. C++: `internal/core/src/storage/Util.cpp`

这个文件是实际从对象存储读取 binlog/blob 并解码的 C++ 入口。第二个提交把原本简单的 size 日志扩展成 size/read/decode 三阶段 trace。

### 5.1 include 修改

| 新增 include | 含义 |
| --- | --- |
| `<chrono>` | 统计 size/read/decode 耗时。 |
| `<cstdint>` | 使用 `uintptr_t` 打印虚拟地址。 |
| `<sstream>` | 按 `/` 拆路径时使用 stringstream。 |
| `<string>` / `<string_view>` | path/event/storage_version 字符串处理。 |
| `<utility>` | `std::move(part)`。 |
| `<vector>` | 保存路径分段。 |

### 5.2 匿名 namespace helper

| 代码 | 含义 |
| --- | --- |
| `namespace { ... }` | 这些 helper 只在当前 cpp 文件内部可见。 |
| `RemoteObjectPathInfo` | C++ 侧远端路径解析结果，字段和 Go/Python 侧保持一致。 |
| 默认 ID 为 `-1` | 无法解析时仍输出字段，便于聚合。 |
| `split_path` | 把对象路径按 `/` 切成非空段。 |
| `parse_int64` | 用 `std::stoll` 解析整数，并要求整个字符串都被消费。 |
| `catch (...) { return false; }` | 解析失败不抛异常，保持 trace 不影响 load。 |
| `parse_remote_object_path` | 识别 insert/stats/delta/index/raw_data 路径。 |
| `INDEX_ROOT_PATH` | index 文件根路径常量，用于识别索引对象。 |
| `RAWDATA_ROOT_PATH` | raw data 根路径常量，用于识别 raw data 对象。 |
| `duration_ms` | 把 steady_clock 起点到现在转成毫秒。 |
| `pointer_to_va` | 把数据指针转成整数，日志中用十六进制地址输出。 |
| `log_remote_object_event` | C++ 统一 remote object trace 日志函数。 |

### 5.3 `log_remote_object_event` 字段解释

| 字段 | 含义 |
| --- | --- |
| `event` | `size_done`、`read_begin`、`read_done`、`decode_done`。 |
| `storage_version` | 字符串 `v1` 或 `v2`，不是 segment storage version 数字。 |
| `remote_kind` | segment/stats/delta/index/raw_data/unknown。 |
| `remote_path` | 对象存储 path 或 storage v2 blob name。 |
| `path_base` | 最后一段路径。 |
| `collection_id` 等 | 从 path 解析出的关联字段。 |
| `encoded_bytes` | 对象存储原始字节数。 |
| `decoded_bytes` | 反序列化后 FieldData 的 bytes。 |
| `row_count` | 解码后的行数。 |
| `dim` | 向量维度或 FieldData 维度。 |
| `data_type` | C++ `DataType` 的整数值。 |
| `data_va` | 解码后 FieldData data 指针的虚拟地址。 |
| `duration_ms` | 当前阶段耗时。 |

### 5.4 `DownloadAndDecodeRemoteFile` storage v1

| 修改行 | 含义 |
| --- | --- |
| `auto info = parse_remote_object_path(file);` | 先解析对象路径，后面每条日志都带相同 ID 维度。 |
| `auto size_start = std::chrono::steady_clock::now();` | 记录调用 `Size` 前的时间。 |
| `auto fileSize = chunk_manager->Size(file);` | 原有逻辑：查询对象大小。 |
| `auto fileSizeBytes = static_cast<int64_t>(fileSize);` | 转成统一的 int64 字节数。 |
| `LOG_INFO("segcore download object ...")` | 保留旧格式日志，兼容已有 grep。 |
| `log_remote_object_event("size_done", "v1", ...)` | 新增统一 size 阶段日志。 |
| `auto buf = shared_ptr<uint8_t[]>(new uint8_t[fileSize]);` | 原有逻辑：按对象大小分配 buffer。 |
| `log_remote_object_event("read_begin", "v1", ...)` | 读取前打点。 |
| `auto read_start = steady_clock::now();` | 记录 read 起点。 |
| `auto readBytes = chunk_manager->Read(...)` | 原有读取逻辑，但现在接收返回值。 |
| `auto readBytesSize = static_cast<int64_t>(readBytes);` | 读取返回字节数转 int64。 |
| `log_remote_object_event("read_done", "v1", ...)` | 读取完成日志，记录实际 read bytes 和耗时。 |
| `auto decode_start = steady_clock::now();` | 解码阶段起点。 |
| `auto codec = DeserializeFileData(buf, fileSize);` | 原有解码逻辑。 |
| `auto field_data = codec->GetFieldData();` | 从 codec 里取 FieldData，用于统计解码后大小。 |
| `decoded_bytes/row_count/dim/data_type/data_va` | 如果 field_data 不为空，就提取解码后 payload 信息。 |
| `log_remote_object_event("decode_done", "v1", ...)` | 解码完成日志，记录 decoded bytes、rows、dim、data type、VA。 |
| `return codec;` | 返回解码结果，不改变原有功能。 |

### 5.5 `DownloadAndDecodeRemoteFileV2`

| 修改行 | 含义 |
| --- | --- |
| `auto info = parse_remote_object_path(file);` | storage v2 blob name 也尝试按标准路径解析。 |
| `auto size_start = ...` | 统计 `GetBlobByteSize` 耗时。 |
| `auto fileSize = space->GetBlobByteSize(file);` | 原有逻辑：查询 blob 大小。 |
| `if (!fileSize.ok()) PanicInfo(...)` | 原有错误处理。 |
| `LOG_INFO("segcore download storage v2 blob ...")` | 保留旧格式日志。 |
| `auto fileSizeBytes = ...` | 统一字节数类型。 |
| `log_remote_object_event("size_done", "v2", ...)` | storage v2 size 阶段日志。 |
| `auto buf = ...` | 原有分配 buffer。 |
| `log_remote_object_event("read_begin", "v2", ...)` | 读 blob 前日志。 |
| `auto read_start = ...` | read 起点。 |
| `auto status = space->ReadBlob(file, buf.get())` | 原有读取 storage v2 blob。 |
| `if (!status.ok()) PanicInfo(...)` | 原有错误处理。 |
| `log_remote_object_event("read_done", "v2", ...)` | 读完日志。 |
| `auto codec = DeserializeFileData(...)` | 原有解码逻辑。 |
| `field_data ? ... : ...` | 防御式提取 decoded bytes、row count、dim、type、VA。 |
| `log_remote_object_event("decode_done", "v2", ...)` | 解码完成日志。 |
| `return codec;` | 功能不变。 |

## 6. C++: `internal/core/src/segcore/SegmentSealedImpl.cpp`

这个文件记录字段数据真正进入 segcore column 后的虚拟地址和大小。

### 6.1 新增 helper

| 代码 | 含义 |
| --- | --- |
| `pointer_to_va(const char* ptr)` | 把 `char*` 指针转成整数地址，日志用 `0x{:x}` 输出。 |
| `segment_length_bytes(...)` | 计算字段列应该记录的长度。 |
| `auto column_bytes = column.ByteSize()` | 先用 ColumnBase 自己的 ByteSize。 |
| `if column_bytes == 0 && sparse vector` | 稀疏向量某些场景 ByteSize 为 0，用 source_bytes 兜底。 |
| `return column_bytes` | 普通字段返回列对象大小。 |
| `log_field_data_va_trace(...)` | 统一打印字段 VA trace。 |
| `segment_id/field_id/data_type` | 标识哪个字段被加载。 |
| `load_mode` | `memory` 或 `mmap`，区分匿名内存和 mmap。 |
| `expected_rows` / `column_rows` | 对比预期行数和实际 Column 行数。 |
| `segment_length_bytes` | 字段列长度，用于分析 VA size。 |
| `column_byte_size` | ColumnBase 原始 ByteSize。 |
| `source_bytes` | 从 FieldData 解码出来的原始 payload bytes。 |
| `data_va` / `mmap_va` | Column 暴露的数据地址和 mmap 地址。 |
| `mmap_file` | mmap 模式下的本地临时文件路径。 |

### 6.2 `LoadFieldData`

| 修改行 | 含义 |
| --- | --- |
| `size_t source_data_size = 0;` | 统计从 channel 中取出的 FieldData 总 bytes。 |
| 每个 `while (data.channel->pop(field_data))` 内新增 `source_data_size += field_data->Size();` | 不同字段类型都会累计原始解码数据大小。 |
| variable/string/json/array/sparse/dense 分支都加同样统计 | 确保所有内存加载类型都能输出 source bytes。 |
| `log_field_data_va_trace(..., "memory", source_data_size, "")` | 字段 load 到内存后，记录 column 真实大小和 VA 地址。 |
| 日志放在 `fields_.emplace` 前 | 表示 column 已经构建完成但尚未注册进 segment map，便于定位加载阶段。 |

### 6.3 `MapFieldData`

| 修改行 | 含义 |
| --- | --- |
| `size_t source_data_size = 0;` | mmap 场景也统计原始 FieldData bytes。 |
| `source_data_size += field_data->Size();` | 每批 FieldData 写入 mmap 文件前累计大小。 |
| `log_field_data_va_trace(..., "mmap", source_data_size, filepath.string())` | mmap 文件写完并创建 Column 后，记录 mmap 文件路径、VA、大小。 |
| `load_mode = "mmap"` | 后续日志分析脚本可以按 memory/mmap 分组。 |

## 7. C++: `internal/core/src/storage/ChunkCache.cpp`

这个文件记录 chunk cache mmap 的 VA 地址。

| 修改行 | 含义 |
| --- | --- |
| `#include <cstdint>` | 使用 `uintptr_t`。 |
| `pointer_to_va(const char* ptr)` | 把指针转成整数地址。 |
| `LOG_INFO("querynode chunk cache mmap va trace", ...)` | mmap 完成后打印 chunk cache 的 cache file、数据类型、行数、大小、VA 地址。 |
| `cache_file` | 本地 chunk cache 文件路径。 |
| `data_type/dim/rows` | 字段数据基本信息。 |
| `segment_length_bytes` | column 映射长度。 |
| `column_byte_size` | ColumnBase ByteSize。 |
| `source_bytes` | mmap 源数据大小。 |
| `data_va/mmap_va` | column data 指针和 mmap 指针。 |
| 日志放在 `unlink(path.c_str())` 前 | 文件还存在时记录路径，便于排查。 |

## 8. `internal/core/conanfile.py`

| 修改行 | 含义 |
| --- | --- |
| `+ "thrift/0.20.0",` | 给 C++ Conan 依赖列表补上 thrift。 |
| 为什么需要 | 新增或已有 C++ 链路在当前分支编译时需要 thrift 包，之前依赖没有显式声明。 |
| 影响 | 只影响依赖解析和编译环境，不改变运行时逻辑。 |

## 9. `tools/querynode_trace/analyze_minio_trace.py`

这是新增的离线日志分析脚本。它读取 QueryNode JSON 日志和 segcore C++ 文本日志，统计远端对象读取量、耗时、VA length。

### 9.1 文件头与 import

| 代码 | 含义 |
| --- | --- |
| `#!/usr/bin/env python3` | 允许在 Linux 下直接执行脚本。 |
| 模块 docstring | 说明脚本关注 QueryNode/segcore 远端对象读取 trace。 |
| `from __future__ import annotations` | 延迟类型注解求值，减少运行时类型引用问题。 |
| `argparse` | 解析命令行参数。 |
| `csv` | 输出 CSV。 |
| `glob` | 支持输入日志路径通配符。 |
| `json` | 解析 Milvus JSON 日志。 |
| `math` | 计算 percentile 的 floor/ceil。 |
| `re` | 解析 C++ 文本日志的 `key=value`。 |
| `statistics` | 计算 mean/stddev。 |
| `sys` | `sys.exit(main())`。 |
| `defaultdict` | group by 聚合。 |
| `Path` | 路径处理。 |
| `typing` | 类型注解。 |

### 9.2 常量

| 常量 | 含义 |
| --- | --- |
| `REMOTE_TRACE_MSG` | 新统一日志关键字。 |
| `LEGACY_V1_MSG` | 旧 C++ storage v1 size 日志关键字。 |
| `LEGACY_V2_MSG` | 旧 C++ storage v2 size 日志关键字。 |
| `SEGMENT_VA_MSG` | segcore 字段 VA trace 关键字。 |
| `CHUNK_CACHE_VA_MSG` | chunk cache VA trace 关键字。 |
| `PAIR_RE` | 从文本日志里提取 `key=value` 对。 |

### 9.3 基础解析函数

| 函数 | 逐行含义 |
| --- | --- |
| `to_int` | 把日志字段转 int；空值返回默认值；先试 int，再试 float 转 int。 |
| `to_float` | 把日志字段转 float；失败返回默认值。 |
| `mib` | bytes 转 MiB。 |
| `parse_pairs` | 用正则把 C++ 文本日志里的 `a=b, c=d` 转成 dict。 |
| `normalize_path` | 把 Windows 反斜杠替换成 `/`，再去掉空 path segment。 |
| `parse_remote_path` | Python 侧复刻 Go/C++ 远端路径解析逻辑。 |
| `log_message` | 兼容 `msg`、`message`、`M` 等不同日志字段名。 |
| `parse_json_line` | 如果一行是 JSON，就解析成 dict；失败返回 None。 |
| `merge_path_info` | 把 path 解析结果和日志原字段合并。 |
| `make_remote_record` | 规范化一条远端对象读取记录，字段统一成 snake_case。 |

### 9.4 JSON/text 日志抽取函数

| 函数/逻辑 | 含义 |
| --- | --- |
| `extract_records_from_json` | 从 JSON 日志里提取 remote/planned/VA 三类记录。 |
| `if REMOTE_TRACE_MSG in msg` | 新格式日志优先直接解析。 |
| `LEGACY_V1_MSG` / `LEGACY_V2_MSG` | 兼容旧 C++ size 日志，标记为 legacy。 |
| `if "loadObjectSummary" in data` | 解析 Go 侧计划加载对象摘要。 |
| `if SEGMENT_VA_MSG ...` | 解析 VA trace。 |
| `extract_records_from_text` | 对非 JSON 的 C++ 文本日志做同样解析。 |
| `re.search(r"file=...")` | 从旧格式文本日志里抓 file 和 size。 |
| `parse_pairs(payload)` | 从新 C++ 文本 trace 里抓 key=value。 |

### 9.5 `extract_planned_summary`

| 代码 | 含义 |
| --- | --- |
| `summary = data["loadObjectSummary"]` | 读取 Go 侧 segment summary。 |
| `base = {...}` | 提取 collection/partition/segment/row/storageVersion 等公共字段。 |
| 遍历 `("segment","insert")` 等四类 | 把 insert/stats/delta/index 都转成统一 planned record。 |
| `file_count` | 计划加载对象数量。 |
| `planned_bytes` | 元数据里计划加载字节数。 |
| `memory_size` | DataCoord 元数据内存估算。 |
| `entry_count` | 行数或 delete 条数。 |
| `path_sample` | 样例路径。 |

### 9.6 `make_va_record`

| 字段 | 含义 |
| --- | --- |
| `remote_kind` | chunk cache 日志标成 `chunk_cache`，segment 字段日志标成 `segment`。 |
| `segment_length_bytes` | 字段/缓存映射长度。 |
| `column_byte_size` | ColumnBase ByteSize。 |
| `source_bytes` | 源数据 bytes。 |
| `data_va/mmap_va` | 虚拟地址字符串。 |
| `mmap_file` | mmap 文件或 cache file。 |

### 9.7 聚合和输出函数

| 函数 | 含义 |
| --- | --- |
| `iter_input_files` | 支持 glob，去重，只返回真实文件。 |
| `parse_logs` | 遍历所有日志行，分别累积 remote/planned/VA 记录。 |
| `percentile` | 手写 percentile，支持插值。 |
| `stats` | 统计 count/sum/min/max/mean/stddev/p50/p90/p95/p99。 |
| `group_by` | 按指定字段分组。 |
| `summarize_group` | 对每组统计 bytes、duration、吞吐。 |
| `print_group_table` | 打印 top N 分组表。 |
| `print_report` | 输出完整命令行报告。 |
| `write_csv` | 可选输出 CSV 明细。 |
| `main` | 解析 CLI 参数，调用 parse/report/csv/json。 |

### 9.8 脚本输出含义

| 输出项 | 含义 |
| --- | --- |
| `scanned_lines` | 扫描日志总行数。 |
| `remote_trace_records` | 解析到的 remote object trace 总数。 |
| `actual_or_legacy_read_records` | 实际读取完成或 legacy size 记录。 |
| `new_read_done_records` | 新格式 `read_done` 数量。 |
| `decode_done_records` | C++ 解码完成记录数。 |
| `planned_summary_records` | Go 侧计划加载 summary 记录数。 |
| `va_records` | VA trace 记录数。 |
| `Actual Reads By Object Kind` | 按 segment/index/stats/delta/raw_data 聚合真实读取。 |
| `Actual Reads By Segment` | 按 segmentID 聚合读取量。 |
| `Actual Index Reads By Build` | 按 buildID/indexVersion/segmentID 聚合索引读取。 |
| `Decoded Payload By Object Kind` | 按对象类型统计解码后 payload。 |
| `Planned Metadata Bytes By Object Kind` | 按元数据里的计划加载量统计。 |
| `Loaded VA Length By Mode` | 按 memory/mmap/chunk_cache 统计 VA length。 |

## 10. `docs/developer_guides/milvus_querynode_load_trace_flow.md`

这是配套说明文档，不改变代码执行逻辑。

| 章节 | 含义 |
| --- | --- |
| 总体链路 | 解释 insert/delete 从 Proxy 到 DataNode、MinIO、DataCoord、IndexNode、QueryNode 的路径。 |
| DataNode buffer 与序列化 | 说明 binlog/statslog/deltalog 如何生成。 |
| DataNode 写 MinIO | 说明对象路径和 DataCoord 元数据上报。 |
| DataCoord 与索引元数据 | 说明 QueryNode load 依赖哪些元数据。 |
| QueryCoord 组装 LoadSegments | 说明 SegmentLoadInfo 从哪里来。 |
| QueryNode 接收 LoadSegments | 说明 LoadScope_Delta/Index/full load 的分派。 |
| QueryNode 加载 storage v1/v2 | 说明本次 trace 覆盖的真实加载路径。 |
| Size 与 VA size 语义 | 解释 LogSize、MemorySize、IndexSize、segmentMemSize、estimatedVASize、data_va/mmap_va。 |
| 本次新增日志 | 列出 Go/C++ 新增日志点。 |
| 第二个提交新增内容 | 补充统一 `querynode remote object fetch trace` 和分析脚本用法。 |

## 11. 编译修复提交

| 文件 | 修改 | 含义 |
| --- | --- | --- |
| `internal/core/conanfile.py` | 新增 `thrift/0.20.0` | 让 C++ 依赖解析时显式拉取 thrift，修复 compile error。 |
| `segment_loader.go` | `zap.Int("blobSize", statsBlob.Size)` 改成 `zap.Int64("blobSize", statsBlob.Size)` | `statsBlob.Size` 是 int64，zap.Int 需要 int；改成 zap.Int64 后类型匹配。 |

## 12. 这些修改对运行行为的影响

| 维度 | 结论 |
| --- | --- |
| 数据正确性 | 没有改变 search/load 的业务语义，主要是增加日志。 |
| 性能 | 每个 load 阶段多了一些日志构造和输出，正常情况下开销来自日志量；如果日志级别开启 Info，远端对象多时日志会明显增多。 |
| 稳定性 | `LoadFieldData` / `LoadMultiFieldData` 捕获 load pool error 是正向修复，避免 Go error 被吞掉。 |
| 可观测性 | 大幅增强，可以关联计划加载对象、真实读取对象、解码后 payload、segcore column VA、mmap/chunk cache。 |
| 风险 | Info 日志量较大；路径解析依赖 Milvus 标准对象路径；estimated VA size 是估算，不是真实进程 VIRT。 |

## 13. 如果你看日志，应该如何串起来

1. 先找 `querynode prepared segment load from remote storage`，看 `loadObjectSummary` 里的计划对象数量和大小。
2. 再找 `querynode segment remote load resource estimate`，看 `estimatedMemoryMiB`、`estimatedDiskMiB`、`estimatedVASizeMiB`。
3. 再按 `segmentID` 找 `querynode remote object fetch trace`，看真实 `read_begin/read_done/decode_done`。
4. 对 index 看 `build_id/index_version`，对 binlog 看 `collection_id/partition_id/segment_id/field_id/log_id`。
5. 如果 mmap 或 chunk cache 相关，继续找 `querynode segment field data va trace` 和 `querynode chunk cache mmap va trace`。
6. 最后用 `tools/querynode_trace/analyze_minio_trace.py` 对日志做总量统计。

## 14. 总结

这 3 个 7 月提交的核心不是“重写加载逻辑”，而是“把 QueryNode 从远端加载 segment 的每个阶段都打上可关联的 trace”：

- Go 侧知道计划加载哪些 segment/binlog/index/delta/stats。
- C++ 侧知道实际从对象存储读了多少、耗时多少、解码后多大。
- segcore 侧知道字段数据最终在内存或 mmap 中占多大、地址是多少。
- Python 脚本能把这些日志汇总成对象类型、segment、index build、VA mode 的统计报表。

因此，如果你逐行看这些修改，可以把它们理解成一套链路追踪系统：元数据计划量 -> 对象存储真实读取 -> 解码后 payload -> segcore 内存/mmap 映射 -> 离线统计分析。
