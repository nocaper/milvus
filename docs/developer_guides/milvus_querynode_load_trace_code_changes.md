# Milvus QueryNode Load Trace 代码修改说明

本文说明本次针对 QueryNode 从对象存储加载 segment 和索引的代码修改思路、修改方向和预期效果。完整读写流程分析见 `docs/developer_guides/milvus_querynode_load_trace_flow.md`。

## 修改目标

本次改动的目标是让 QueryNode 加载链路具备可观测性，重点回答以下问题：

- QueryNode 加载的是哪个 `segmentID`、哪个 field、哪个 index。
- 从 MinIO/S3/local object storage 读取了哪些对象文件。
- 对象文件进入 QueryNode 进程时的虚拟地址和长度是多少。
- segment、field chunk、index、manifest column group 在 QueryNode/segcore 中被物化后的对象地址是什么。
- 每次对象读取和 segment/index 加载的耗时是多少。

统一日志关键字为：

```text
[querynode-load-trace]
```

## 修改思路

### 分层插桩

Milvus 当前 sealed segment 加载并不是单纯在 Go 层下载文件后传给 segcore，而是：

```text
Go segmentLoader
  -> LocalSegment.Load
  -> C++ segcore
  -> storage / file manager / cache layer
  -> object storage
```

因此只在 Go 层插日志无法拿到真实对象 buffer 的 VA；只在 C++ storage 层插日志又缺少 `segmentID/indexID/fieldID` 的上层上下文。

本次采用两层插桩：

- Go 层：记录加载请求元数据、C segment 句柄地址、index load info、segment 加载前后状态。
- C++ 层：记录真实远端对象读取 buffer VA、stream read buffer VA、field chunk/index/column group 物化地址。

这样可以通过 `segmentID + object path + indexFiles/binlog files` 把上层元数据和底层对象读取日志串起来。

### 保守低侵入

本次改动不改变加载流程、不修改资源管理策略、不改变对象存储读写语义，只增加日志：

- 不改变 `SegmentLoadInfo`、proto 或存储格式。
- 不改变 MinIO/S3 文件路径生成方式。
- 不改变 cache layer 的加载和释放逻辑。
- 不引入新的配置项，默认直接输出 Info 日志，便于快速验证。

## 修改方向

### 1. Go 层加载元数据追踪

新增文件：

- `internal/querynodev2/segments/load_trace.go`

作用：

- 汇总 `SegmentLoadInfo` 中的 binlog、statslog、deltalog、BM25、index、text/json stats 的数量和大小。
- 提供统一 trace tag。
- 对 Go 侧已经读到的 `[][]byte` 记录 buffer VA 和长度。

修改文件：

- `internal/querynodev2/segments/segment.go`
- `internal/querynodev2/segments/segment_loader.go`

主要日志点：

- `csegment created`
  - 记录 `collectionID`、`partitionID`、`segmentID`
  - 记录 `storageVersion`、`manifestPath`、`numRows`
  - 记录 `cSegmentVA`
  - 记录 binlog/index 文件数和大小估算

- `c load index info prepared`
  - 记录 `fieldID`、`indexID`、`buildID`
  - 记录 `indexVersion`、`indexFileSize`、`numRows`
  - 记录 `indexFiles`
  - 记录 `cLoadIndexInfoVA`

- `csegment load start/done`
  - 记录 segment load 前后的耗时
  - 记录 `cSegmentMemSize`

- `segment files load start/done`
  - 记录 QueryNode loader 层的 segment 加载耗时
  - 记录 `relatedDataSize`、`loadedBinlogMemorySize`
  - 记录加载后的 `rowNumAfterLoad`

- `go object loaded`
  - 覆盖 Go 侧直接读取的 PK stats、BM25 stats、部分 delta log
  - 记录 `goBufferVA` 和 `objectSize`

### 2. C++ 对象存储读取追踪

修改文件：

- `internal/core/src/storage/Util.cpp`

主要日志点：

- `remote object loaded`

记录字段：

- `file`
- `object_va`
- `object_size`
- `bytes_read`
- `is_field_data`
- `chunk_manager`
- `bucket`
- `root`
- `size_us`
- `read_us`
- `total_us`

这是本次最关键的插桩点。它记录的是对象文件从 MinIO/S3 读入 QueryNode 进程后 `uint8_t[]` 的真实虚拟地址和长度。

### 3. packed index 流式读取追踪

修改文件：

- `internal/core/src/storage/RemoteInputStream.h`
- `internal/core/src/storage/RemoteInputStream.cpp`
- `internal/core/src/storage/FileManager.h`

修改内容：

- `RemoteInputStream` 增加 `remote_path_`，用于在 stream read 日志中记录对象路径。
- `FileManager::OpenInputStream` 创建 `RemoteInputStream` 时传入 remote path。
- `RemoteInputStream::Read`、`ReadAt`、`Read(int fd, size_t size)` 增加日志。

覆盖场景：

- packed scalar index
- 通过 Arrow filesystem / milvus-storage stream 读取的 index 文件

记录字段：

- `object_path`
- `buffer_va`
- `requested_size`
- `bytes_read`
- `offset`
- `file_size`
- `read_us`

### 4. field data 物化追踪

修改文件：

- `internal/core/src/segcore/storagev1translator/ChunkTranslator.cpp`

主要日志点：

- `field chunks load start`
  - 记录 `segment`、`field`、`cids`
  - 记录 `files`、`row_counts`、`memory_sizes`
  - 记录 mmap 和 priority

- `field chunk materialized`
  - 记录 `chunk_va`
  - 记录 `row_count`、`memory_size`

作用：

把 Storage V1 field binlog 的对象路径、cid 和最终 segcore chunk 地址关联起来。

### 5. index 物化追踪

修改文件：

- `internal/core/src/segcore/storagev1translator/SealedIndexTranslator.cpp`

主要日志点：

- `index load start`
- `index load done`

记录字段：

- `segment`
- `field`
- `index`
- `index_va`
- `index_type`
- `index_size`
- `index_file_count`
- `index_files`
- `enable_mmap`
- `final_memory_cost`
- `final_disk_cost`
- `max_memory_cost`
- `max_disk_cost`

作用：

把 `FieldIndexInfo.indexFiles` 和最终创建出的 index object 地址关联起来。

### 6. Storage V2/V3 manifest column group 追踪

修改文件：

- `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`

主要日志点：

- `manifest column groups load start`
- `manifest column groups reader created`
- `manifest column group load start`
- `manifest column group materialized`

记录字段：

- `segment`
- `collection`
- `partition`
- `storage_version`
- `manifest_path`
- `column_group_count`
- `reader_va`
- `cg_index`
- `field_ids`
- `needed_columns`
- `column_group_va`
- `use_mmap`
- `priority`

作用：

为 Storage V2/V3 manifest 加载提供关联锚点。当前能记录 segcore 可见的 reader 和 column group 地址；parquet/page 级内部 buffer 仍需要继续深入 milvus-storage reader 插桩。

## 修改效果

### 可建立完整关联链

修改后可以按如下方式串联一次加载：

```text
csegment created
  -> c load index info prepared
  -> field chunks load start / index load start
  -> remote object loaded 或 remote input stream read
  -> field chunk materialized / index load done / manifest column group materialized
  -> csegment load done
```

### 可观测 MinIO 到 QueryNode 的对象读入

对经过 `storage::GetObjectData` 的对象，可以直接看到：

- 对象路径
- QueryNode 进程内对象 buffer VA
- 对象长度
- 实际读取字节数
- 对象存储 bucket/root
- size/read/total 耗时

这覆盖 Storage V1 field binlog，以及大量 index/raw data 读取路径。

### 可观测 packed index 的流式读取

对不经过 `GetObjectData`、而是通过 `RemoteInputStream` 流式读取的 packed index，可以看到：

- remote object path
- 每次 read 的目标 buffer VA
- 请求长度和实际读取长度
- offset 和 file size
- read 耗时

### 可观测 segcore 内部物化对象

加载完成后，可以看到：

- segment 的 C++ 句柄地址：`cSegmentVA`
- field chunk 地址：`chunk_va`
- index object 地址：`index_va`
- manifest column group 地址：`column_group_va`

这些地址可以辅助分析对象生命周期、内存布局、cache layer 行为和性能瓶颈。

## 覆盖边界

当前已经覆盖：

- Go QueryNode segment load 入口。
- Go index load info 转换。
- Go 侧 PK stats、BM25 stats、部分 delta log byte slice。
- C++ Storage V1 field binlog 下载。
- C++ 多数 index 文件下载。
- C++ packed index stream read。
- C++ field chunk/index/manifest column group 物化。

当前没有完全覆盖：

- Storage V2/V3 manifest 里 parquet/page 级内部 buffer VA。
- milvus-storage FFI 内部 reader 的更细粒度对象分片读取。
- index 库内部二次分配后的所有内部结构地址。

如果后续需要更细粒度分析，可以继续在以下方向扩展：

- milvus-storage reader / parquet page reader 内部插桩。
- Knowhere index load 内部 binary set 或 mmap 文件映射点插桩。
- cache layer cell pin/unpin、evict、warmup 状态变化插桩。

## 使用方式

过滤所有 trace 日志：

```bash
grep -F "[querynode-load-trace]" querynode.log
```

按对象下载分析：

```bash
grep -F "[querynode-load-trace] remote object loaded" querynode.log
grep -F "[querynode-load-trace] remote input stream read" querynode.log
```

按 segment 分析：

```bash
grep -F "[querynode-load-trace]" querynode.log | grep "segmentID"
```

按 index 分析：

```bash
grep -F "[querynode-load-trace] index load" querynode.log
```

