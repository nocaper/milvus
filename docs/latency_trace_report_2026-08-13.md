# Milvus Latency Trace 改造与分析报告

日期：2026-08-13  
代码范围：当前仓库从 2026-08-11 00:00:00 到当前 HEAD 的所有提交，以及与本次 trace 相关的 Milvus 原有链路代码。  
基线：`d1effb4429^`，即 `60695bdb448174661d82c80fd2db7acca49b48b3`。

## 1. 摘要

本次改造的目标是为 Milvus 写入链路、Search/Query 路由与执行链路、QueryNode segment 冷加载链路增加低侵入的时延 trace，用统一的 `[LATENCY_TRACE]` 文本日志输出，配套 `scripts/analyze_latency_traces.py` 做离线解析。核心要回答三类问题：

1. Insert 写入链路中，Proxy 序列化/repack、MQ produce、DataNode 消费滞后、DataNode buffer 处理、Flush 写对象存储分别耗时多少。
2. Search/Query 请求命中了多少 sealed/growing segment，请求是否走 QueryNode 已有 segment 执行，是否触发 lazy-load DiskCache miss 后从对象存储加载 segment。
3. LoadSegment 的整体耗时和若干子阶段耗时在哪里，包括 index、field data、bloom filter、delta log、统计信息/删除日志反序列化等。

当前代码已经可以把 Query/Search 的两类路径分开统计：

- QueryNode 已有 segment 上执行：`operation=Search stage=segment_search source=querynode_cache`，`operation=Query stage=segment_query source=querynode_cache`。
- Lazy-load DiskCache 访问：`segment_cache_hit` 表示 lazy-load cache 命中，`segment_cache_miss` 表示 lazy-load cache 未命中；未命中后由 `operation=LoadSegment stage=segment_cache_load source=object_store trigger=Search|Query` 记录从对象存储加载的整体等待耗时。

需要强调的限制：

- `segment_cache_hit/miss` 只覆盖 lazy-load DiskCache 路径；普通已加载在 QueryNode 内存中的 segment 不会记为 `segment_cache_hit`，而是通过 `segment_search/segment_query` 统计执行耗时。
- `LoadSegment total_load` 覆盖底层加载总耗时，但 Go 层当前不能完全拆分 C++ 内部对象存储读取和反序列化；`load_field_data/load_index/load_multi_field_data` 是 Go 调 C/C++ 加载接口的整体耗时。
- 当前 `duration_ms` 使用 `time.Duration.Milliseconds()`，亚毫秒耗时会被截断为 `0.00 ms`，因此 route 等很快阶段显示 0 不代表完全没有开销。
- 当前已补齐 `LoadCollection/LoadPartition` 的 QueryCoord 生命周期打点：同步提交阶段、observer 进入 loading、observer 判断加载完成/超时/取消。真实 segment 文件读取仍以 QueryNode 的 `LoadSegment` 打点为准。

## 2. 提交与修改范围

2026-08-11 到当前 HEAD 的相关提交如下：

| 提交 | 时间 | 主题 | 主要影响 |
|---|---:|---|---|
| `d1effb4429` | 2026-08-11 16:49:55 +0800 | `feat: Add latency tracing...` | 新增 tracer、初始写入链路和 QueryNode load/search 相关打点、初版解析脚本和文档 |
| `b3c10a7db2` | 2026-08-11 20:58:00 +0800 | `compile bugfix` | 修复 tracer 初始化/编译问题 |
| `9f253b8a2f` | 2026-08-11 22:07:35 +0800 | `no log bugfix` | 将输出改为直接 stdout，避免 trace 日志没有落到 Milvus 日志中 |
| `8fc17b4e63` | 2026-08-11 22:54:31 +0800 | `compile bugfix2` | 文档/占位文件调整 |
| `00503a02bd` | 2026-08-12 11:41:31 +0800 | `metadata bug fix` | 修复 metadata 输出/解析，支持解析 metadata 字段 |
| `9bc21dbd03` | 2026-08-12 16:09:10 +0800 | `no s3 bugfix` | 调整 DataNode 打点和脚本，解决之前 S3/Flush 数据解析不到的问题之一 |
| `6f11a46846` | 2026-08-12 19:51:32 +0800 | `no s3 bugfix_2` | 增强 QueryNode `segment_loader.go` 的 LoadSegment 相关打点和脚本解析 |
| `b8a841c135` | 2026-08-13 14:20:23 +0800 | `loadsegment` | 补齐 QueryNode delegator、lazy-load DiskCache、segment 执行、LoadSegment 子阶段和脚本配套解析 |

相对 2026-08-11 前基线，代码修改范围：

- 新增 tracer 基础设施：`pkg/tracer/init.go`、`pkg/tracer/latency_tracer.go`。
- 写入链路：`internal/proxy/task_insert.go`、`internal/datanode/flow_graph_write_node.go`、`internal/datanode/syncmgr/task.go`。
- Search/Query 路由链路：`internal/querynodev2/delegator/delegator.go`。
- QueryNode segment 执行与 lazy-load cache 链路：`internal/querynodev2/segments/search.go`、`internal/querynodev2/segments/segment_do.go`、`internal/querynodev2/segments/segment.go`、`internal/querynodev2/segments/manager.go`。
- QueryNode LoadSegment 加载链路：`internal/querynodev2/segments/segment_loader.go`。
- QueryCoord LoadCollection/LoadPartition 生命周期链路：当前保守版本只在 `internal/querycoordv2/job/job_load.go`、`internal/querycoordv2/observers/collection_observer.go` 增加低侵入生命周期打点；不修改 QueryCoord meta，不新增 `LoadTraceID` 字段，也不增加自定义 gRPC metadata 传播。
- 解析与辅助脚本：`scripts/analyze_latency_traces.py`、`scripts/demo_latency_tracing.py`、`scripts/run_milvus_with_tracing.sh`、`scripts/pre_commit_check.sh`、`scripts/validate_changes.sh`、`scripts/git_commit.sh`。
- 说明文档：`README_LATENCY_TRACING.md`、`docs/LATENCY_ANALYSIS_README.md`、`docs/QUICKSTART_LATENCY_ANALYSIS.md`、`docs/CHANGES_SUMMARY.md` 等。

生成本报告后又针对稳定性和解析口径做过修正。当前未提交的真实业务/脚本 diff 主要包括：`internal/querycoordv2/job/job_load.go`、`internal/querycoordv2/observers/collection_observer.go`、`internal/querynodev2/segments/plan.go`、`scripts/analyze_latency_traces.py`；本报告也同步更新这些修正后的结论。

## 3. Trace 基础设计

### 3.1 输出格式

`pkg/tracer/latency_tracer.go` 中统一输出：

```text
[LATENCY_TRACE] trace_id=<id> operation=<op> stage=<stage> component=<component> duration_ms=<ms> <metadata...>
```

metadata 以 `key=value` 追加，例如：

```text
[LATENCY_TRACE] trace_id=... operation=LoadSegment stage=segment_cache_load component=querynode duration_ms=123.00 source=object_store collection_id=... segment_id=... trigger=Search
```

这样设计的原因：

- 不依赖复杂外部 trace backend，直接随 Milvus stdout/stderr 日志采集。
- grep 友好，用户可以直接在源日志中搜索 `operation=LoadSegment`、`stage=segment_cache_load`、`stage=segment_search` 等字段。
- 解析脚本只依赖稳定文本格式，适合 benchmark 后离线分析。

### 3.2 trace id 来源与请求相关性

`GetTraceIDFromContext(ctx)` 的顺序是：

1. 优先读取 `ctx.Value("traceID")`。
2. 如果没有自定义 trace id，则读取 OpenTelemetry `SpanContextFromContext(ctx).TraceID()`。
3. 仍没有则返回空字符串；`StartSpan/RecordEvent` 在 trace id 为空时会生成新的 uuid。

这带来两个结论：

- 在同一进程、同一 context 链路内，trace 能跟随请求。例如 QueryNode delegator 生成 trace id 后，route、segment stats、下游本地 segment 执行、lazy-load cache miss 在 context 传递正常时可以关联。
- 跨 MQ 或跨 gRPC 时，自定义 `context.Value("traceID")` 不一定会自动传播。Milvus 原有 MQ/gRPC 链路会传播 OpenTelemetry context；因此跨进程关联依赖实际 OTel propagation 是否生效。如果没有传下来，下游事件仍会记录，但 trace id 可能变成 OTel trace id 或新 uuid，不能保证与上游自定义 uuid 完全一致。

### 3.3 精度限制

当前 `EndSpan` 和 `RecordEvent` 用 `duration.Milliseconds()` 记录，单位是毫秒整数，再格式化成 `%.2f`。因此：

- 真实耗时 `<1ms` 会显示为 `0.00 ms`。
- route 等轻量阶段经常为 0，不应解读为完全没有开销。
- 如果需要更细粒度，需要把 tracer 改为 `float64(duration.Nanoseconds()) / 1e6`。

## 4. 打点清单与加点原因

### 4.1 Insert 写入路径

| 路径 | operation/stage | component | metadata | 请求相关性 | 为什么加 |
|---|---|---|---|---|---|
| `internal/proxy/task_insert.go` `insertTask.Execute` | `Insert/serialize` | `proxy` | `collection_id`、`num_rows` | Proxy 为 insert 请求生成 trace id，并写入 task context；repack 使用 `it.TraceCtx()` | 衡量字段校验后、发 MQ 前的 segmentID 分配和 repack 相关耗时。stage 名称沿用 `serialize`，但当前代码包住的是 repack/segment 分配流程，不应理解为只统计 protobuf 序列化 |
| `internal/proxy/task_insert.go` `insertTask.Execute` | `Insert/mq_produce` | `proxy` | `collection_id`、`num_messages` | 与同一 Proxy insert 请求同 trace id | 衡量 Proxy 向 DML stream produce 的耗时，判断 MQ 写入是否是前端写入瓶颈 |
| `internal/datanode/flow_graph_write_node.go` `writeNode.Operate` | `Insert/consume_lag` | `datanode` | `channel`、`segment_id`、`num_rows` | 从 InsertMsg 的 TraceCtx/OTel context 获取 trace id；跨 MQ 是否与 Proxy 同 id 取决于 context propagation | 衡量消息物理时间戳到 DataNode 处理时刻的滞后，判断 MQ 积压/消费延迟 |
| `internal/datanode/flow_graph_write_node.go` `writeNode.Operate` | `Insert/datanode_process` | `datanode` | `channel`、`num_inserts`、`num_deletes` | 取当前 flow graph 中第一条 insert message 的 TraceCtx | 衡量 DataNode buffer 数据处理耗时，判断写入进入 buffer 是否成为瓶颈 |
| `internal/datanode/syncmgr/task.go` `SyncTask.Run` | `Flush/s3_write` | `datanode` | `segment_id`、`is_flush`、`level`、`serialize_ms` | 当前代码用空 trace id 启动 span，会生成独立 trace id；按 segment/时间窗口分析 | 衡量 Flush 时将 insert/stats/delta blobs 写入对象存储的耗时，同时记录写前序列化耗时 |

说明：

- `s3_write` 的 operation 是 `Flush`，不是 `Insert`。这是合理的，因为写对象存储发生在 DataNode flush/sync task，不是 Proxy insert 请求的同步返回路径。
- `serialize_ms` 是 metadata，不是一个单独的 trace event；解析脚本会把它统计成 `datanode_serialize`。
- `datanode_process` 在一个 flow graph 批次里使用第一条 insert message 的 trace id。若同一批次混合多个上游请求，它是批次级处理耗时，不是每条 insert 请求严格独立的耗时。

### 4.2 Search/Query 路由路径

| 路径 | operation/stage | component | metadata | 请求相关性 | 为什么加 |
|---|---|---|---|---|---|
| `internal/querynodev2/delegator/delegator.go` `shardDelegator.Search` | `Search/route` | `querynode` | `collection_id`、`partitions`、结束时追加 `sealed_count`、`growing_count` | Search 请求进入 delegator 后生成 trace id 并写入 context | 衡量 wait tsafe 之后、PinReadableSegments 和候选 segment 统计的路由阶段耗时 |
| `internal/querynodev2/delegator/delegator.go` `shardDelegator.Search` | `Search/segment_stats` | `querynode` | `sealed_segments`、`growing_segments` | 与 Search route 使用同一 trace id | 给解析脚本稳定提供候选 sealed/growing segment 数 |
| `internal/querynodev2/delegator/delegator.go` `shardDelegator.Query` | `Query/route` | `querynode` | `collection_id`、`partitions`、`sealed_count`、`growing_count` | Query 请求进入 delegator 后生成 trace id 并写入 context | 衡量 Query 路由阶段耗时 |
| `internal/querynodev2/delegator/delegator.go` `shardDelegator.Query` | `Query/segment_stats` | `querynode` | `sealed_segments`、`growing_segments` | 与 Query route 使用同一 trace id | 给解析脚本稳定提供 Query 候选 segment 数 |

这部分有一个已经修正过的重要问题：`sealed` 在 delegator 中是 `[]SnapshotItem`，每个 `SnapshotItem` 里有多个 segment。不能用 `len(sealed)` 表示 sealed segment 数，最新代码使用：

```go
sealedNum := lo.SumBy(sealed, func(item SnapshotItem) int { return len(item.Segments) })
```

因此当前 `sealed_count/sealed_segments` 表示候选 sealed segment 的真实数量，而不是 worker/snapshot item 数量。

注意：`route/segment_stats` 的 sealed/growing 统计点在 `PinReadableSegments` 后、segment prune 前或部分 prune 前，代表候选分布，不等同于最终实际执行次数。最终执行次数应看 `segment_search/segment_query` 的 count。

### 4.3 QueryNode 已有 segment 执行路径

| 路径 | operation/stage | component | source | metadata | 为什么加 |
|---|---|---|---|---|---|
| `internal/querynodev2/segments/segment.go` `LocalSegment.Search` | `Search/segment_search` | `querynode` | `querynode_cache` | `collection_id`、`partition_id`、`segment_id`、`segment_type`、`nq` | 统计 Search 在 QueryNode 已有 segment 上执行的 per-segment 耗时 |
| `internal/querynodev2/segments/segment.go` `LocalSegment.Retrieve` | `Query/segment_query` | `querynode` | `querynode_cache` | `collection_id`、`partition_id`、`segment_id`、`segment_type`、`msg_id` | 统计 Query/Retrieve 在 QueryNode 已有 segment 上执行的 per-segment 耗时 |

这里的 `source=querynode_cache` 表示执行发生在 QueryNode 当前可访问的 LocalSegment 上。它不是 lazy-load DiskCache hit 的同义词。普通非 lazy-load segment 不经过 `DiskCache.Do`，但仍会产生 `segment_search/segment_query`。

### 4.4 Lazy-load DiskCache 命中/未命中路径

| 路径 | operation/stage | component | source | metadata | 为什么加 |
|---|---|---|---|---|---|
| `internal/querynodev2/segments/search.go` `searchSegments` / `searchSegmentsStreamly` | `Search/segment_cache_hit` | `querynode` | `querynode_cache` | `collection_id`、`partition_id`、`segment_id`、`segment_type` | lazy-load Search 经 `DiskCache.Do` 且没有 missing，统计 lazy-load cache hit 次数 |
| `internal/querynodev2/segments/search.go` `searchSegments` / `searchSegmentsStreamly` | `Search/segment_cache_miss` | `querynode` | `object_store` | 同上 | lazy-load Search 经 `DiskCache.Do` 且 missing，统计请求触发对象存储加载次数 |
| `internal/querynodev2/segments/segment_do.go` `doOnSegment` | `Query/segment_cache_hit` | `querynode` | `querynode_cache` | `collection_id`、`partition_id`、`segment_id`、`segment_type` | lazy-load Query cache hit 次数 |
| `internal/querynodev2/segments/segment_do.go` `doOnSegment` | `Query/segment_cache_miss` | `querynode` | `object_store` | 同上 | lazy-load Query cache miss 次数 |

`segment_cache_hit/miss` 的 duration 当前为 0，是事件计数，不是耗时统计。真正的 miss 加载耗时由下一节 `segment_cache_load` 统计。

### 4.5 Search/Query 触发的对象存储冷加载路径

| 路径 | operation/stage | component | source | metadata | 为什么加 |
|---|---|---|---|---|---|
| `internal/querynodev2/segments/manager.go` DiskCache loader | `LoadSegment/segment_cache_load` | `querynode` | `object_store` | `collection_id`、`partition_id`、`segment_id`、`segment_type`、`num_rows`、`trigger=Search|Query|Unknown` | 在 lazy-load cache miss 后包住 `manager.Loader.LoadLazySegment(...)`，直接统计 Query/Search 触发对象存储加载的次数和整体等待耗时 |

这是回答“Query/Search 时 QueryNode 没有，需要从 S3/对象存储拿 segment 的整体时延和次数”的关键打点。

需要注意：

- 它只在 lazy-load DiskCache loader 被触发时出现。
- 如果 benchmark 是普通 `load_collection` 后所有 segment 已经常驻 QueryNode，Search 时不会产生 `segment_cache_load`。
- 如果 lazyload 配置未开启，或 workload 没有释放/驱逐 segment，Search 阶段也不会出现 cache miss load。

### 4.6 LoadSegment 总耗时和子阶段

| 路径 | operation/stage | component | source | metadata | 为什么加 |
|---|---|---|---|---|---|
| `internal/querynodev2/segments/segment_loader.go` `segmentLoaderV2.LoadSegment` | `LoadSegment/total_load` | `querynode` | `object_store` | `collection_id`、`partition_id`、`segment_id`、`segment_type`、`num_rows` | Storage V2 路径的底层 LoadSegment 总耗时 |
| `internal/querynodev2/segments/segment_loader.go` `segmentLoader.LoadSegment` | `LoadSegment/total_load` | `querynode` | `object_store` | `collection_id`、`partition_id`、`segment_id`、`segment_type`、`num_rows`、`storage_version` | 默认 Storage V1 路径的底层 LoadSegment 总耗时 |
| `internal/querynodev2/segments/segment.go` `LoadIndex` | `LoadSegment/load_index` | `querynode` | `object_store` | `field_id`、`index_id`、`index_file_count`、`field_type` 等 | 统计加载 index 信息、更新 index、必要时 warmup chunk cache 的整体耗时 |
| `internal/querynodev2/segments/segment.go` `LoadFieldData` | `LoadSegment/load_field_data` | `querynode` | `object_store` | `field_id`、`row_count`、`binlog_count`、`use_mmap` 等 | 统计单 field binlog 加载到 segcore 的整体耗时 |
| `internal/querynodev2/segments/segment.go` `LoadMultiFieldData` | `LoadSegment/load_multi_field_data` | `querynode` | `object_store` | `field_count`、`row_count` | 统计多 field data 批量加载耗时 |
| `internal/querynodev2/segments/segment_loader.go` `loadBloomFilter` | `LoadSegment/load_bloom_filter` | `querynode` | `object_store` | `binlog_count`、`log_type` | 统计 pk stats/bloom filter binlog 从对象存储读取耗时 |
| `internal/querynodev2/segments/segment_loader.go` `loadBloomFilter` | `LoadSegment/deserialize_stats` | `querynode` | `object_store` | `blob_count`、`log_type` | 统计 stats/bloom filter 反序列化耗时 |
| `internal/querynodev2/segments/segment_loader.go` `LoadDeltaLogs` | `LoadSegment/load_delta_logs` | `querynode` | `object_store` | `field_binlog_count` | 统计 delete/delta log 从对象存储读取耗时 |
| `internal/querynodev2/segments/segment_loader.go` `LoadDeltaLogs` | `LoadSegment/deserialize_delta` | `querynode` | `object_store` | `blob_count` | 统计 delete log 反序列化耗时 |
| `internal/querynodev2/segments/segment_loader.go` `LoadDeltaLogs` | `LoadSegment/load_delta_apply` | `querynode` | `object_store` | `delete_count` | 统计 delta/delete 数据应用到 segment 的耗时 |

这些子阶段用于把 `total_load` 拆到更接近具体瓶颈的位置。它们不是严格互斥的 CPU/S3 纯分解，而是按 Milvus Go 层调用边界划分。

### 4.7 LoadCollection/LoadPartition 生命周期

| 路径 | operation/stage | component | metadata | 为什么加 |
|---|---|---|---|---|
| `internal/querycoordv2/job/job_load.go` `LoadCollectionJob.Execute` | `LoadCollection/querycoord_submit` | `querycoord` | `collection_id`、`partition_count`、`replica_number`、`resource_group_count`、`refresh` | 统计 QueryCoord load collection job 创建 collection/partition loading meta 前后的同步提交阶段耗时 |
| `internal/querycoordv2/job/job_load.go` `LoadPartitionJob.Execute` | `LoadPartition/querycoord_submit` | `querycoord` | `collection_id`、`partition_count`、`replica_number`、`resource_group_count`、`refresh` | 统计 QueryCoord load partition job 创建/更新 loading meta 前后的同步提交阶段耗时 |
| `internal/querycoordv2/observers/collection_observer.go` `LoadCollection/LoadPartitions` | `LoadCollection|LoadPartition/load_start` | `querycoord` | `collection_id`、`partition_count`、`load_type`、`status=loading` | 记录异步 load 生命周期被 observer 接管的起点 |
| `internal/querycoordv2/observers/collection_observer.go` `observeLoadStatus` | `LoadCollection|LoadPartition/load_complete` | `querycoord` | `collection_id`、`partition_count`、`load_type`、`status=loaded` | 记录 observer 判断目标 partition 全部 100% loaded 的端到端耗时 |
| `internal/querycoordv2/observers/collection_observer.go` `observeTimeout/observeLoadStatus` | `LoadCollection|LoadPartition/load_timeout` / `load_canceled` | `querycoord` | `collection_id`、`partition_count`、`load_type`、`status` | 区分真正完成、超时和被 release/移除导致的取消 |

这里要明确区分两个层次：

- `querycoord_submit` 不是“所有 segment 已从 S3 加载完成”，它只是 load 请求在 QueryCoord 同步提交阶段的耗时。
- `load_complete` 是 QueryCoord observer 根据 load percentage 判断加载完成的生命周期耗时，包含 QueryCoord 调度、QueryNode 加载、分布上报和 observer 轮询间隔。
- 真正每个 sealed segment 的对象存储读取/反序列化耗时仍看 `operation=LoadSegment`，其中 `total_load` 是 segment 总加载耗时，子阶段用于继续拆解瓶颈。

稳定性检查后，当前版本刻意避免把自定义 trace 状态写入 QueryCoord collection meta，也不新增 `milvus-latency-trace-id` 这类自定义 gRPC metadata。这样可以降低对 QueryCoord load 状态机和跨组件调用链的影响。

因此，`LoadCollection/LoadPartition` 生命周期数据可以用于统计 QueryCoord 视角的 load 提交、开始、完成、超时和取消；每个 sealed segment 的对象存储加载仍以 QueryNode 侧 `operation=LoadSegment` 为准。严格把某一次 LoadCollection 与底层每个 LoadSegment 串成同一个 trace，需要依赖 Milvus 现有 OTel context 传播和完整 QueryCoord/QueryNode 日志共同验证，当前版本不额外保证自定义 trace id 贯穿。

## 5. 为什么这些打点与请求相关

### 5.1 Insert 请求

Proxy 的 `insertTask.Execute` 在请求进入执行阶段生成 trace id，并写回 task context。`serialize` 和 `mq_produce` 直接使用同一个 context，因此是请求内打点。

DataNode 的 `consume_lag` 和 `datanode_process` 从 InsertMsg 的 `TraceCtx()` 或 OTel context 中取 trace id。Milvus 原有 msgstream 代码会在发送消息时通过 `InjectCtx` 注入 OTel context，在消费时通过 `ExtractCtx` 取回。因此这些事件是消息请求相关的。但自定义 `context.Value("traceID")` 是否跨 MQ 保留，取决于是否被 OTel context 承接；不能保证一定和 Proxy 生成的 uuid 完全一致。

Flush 的 `s3_write` 当前是以 SyncTask/segment 为中心，代码里明确使用空 trace id 启动 span，因此它不是单个 Insert 请求同步路径的一部分。它仍然是写入链路分析的重要数据，因为它反映 DataNode 持久化 segment 数据到对象存储的耗时。

### 5.2 Search/Query 请求

QueryNode delegator 的 `Search` 和 `Query` 在每个请求入口生成 trace id，并写入 context。route、segment_stats、组织 subtask、下游 worker 调用都使用该 context。

如果下游是本地 QueryNode worker，`context.Value("traceID")` 可以随调用传递，`segment_search/segment_query/segment_cache_load` 可以和 route 使用同一 trace id。  
如果下游是 remote worker，经 gRPC 传递时 Go context value 通常不会自动跨进程传播；此时依赖 OTel trace context。当前 tracer 有 OTel trace id fallback，所以仍有机会用 OTel trace id 串联，但是否完全一致需要用真实部署日志确认。

### 5.3 Lazy-load 请求触发路径

Search/Query 如果遇到 `seg.IsLazyLoad()`，会将 context 增加一个内部 operation 标记：`withLatencyTraceOperation(ctx, "Search")` 或 `"Query"`，然后调用 `mgr.DiskCache.Do(...)`。DiskCache loader miss 后读取该标记，写入 `segment_cache_load` 的 `trigger` metadata。  
因此 `segment_cache_load trigger=Search|Query` 是请求触发的冷加载事件，不是后台预加载或普通 load task 的泛化指标。

## 6. 解析脚本能力

脚本：`scripts/analyze_latency_traces.py`。

### 6.1 解析格式

脚本匹配：

```text
[LATENCY_TRACE] trace_id=... operation=... stage=... component=... duration_ms=... metadata...
```

metadata 使用正则解析 `key=value`，支持数字、字符串、`[ ... ]` 形式的列表文本。

### 6.2 输出的主要分析

1. Write Path
   - `serialize`
   - `mq_produce`
   - `consume_lag`
   - `datanode_process`
   - `datanode_serialize`，来自 `s3_write` metadata 的 `serialize_ms`
   - `s3_write`

2. Search/Query Path
   - Search/Query 请求数。
   - route latency，按 `Search` 和 `Query` 分开。
   - sealed/growing segment 候选数量。
   - lazy-load DiskCache hit/miss 次数，按 `Search` 和 `Query` 分开。
   - `segment_stats` 缺 metadata 时会提示 `segment_stats_without_counts`。
   - 如果没有 `segment_stats`，但 route metadata 有 `sealed_count/growing_count`，会 fallback 到 route metadata。

3. Segment Load
   - `cache_miss_load`：`LoadSegment/segment_cache_load`，即 Query/Search cache miss 触发对象存储加载。
   - `total_load`：所有 `LoadSegment/total_load`。
   - `cache_miss_by_request_operation`：按 `trigger=Search|Query|Unknown` 拆分。
   - `total_load_by_operation`：按同一 trace id 上的 `LoadCollection/LoadPartition/Search/Query/Unknown` 拆分 `LoadSegment/total_load`。
   - `substages`：`load_index/load_field_data/load_multi_field_data/load_bloom_filter/deserialize_stats/load_delta_logs/deserialize_delta/load_delta_apply`。

4. LoadCollection/LoadPartition
   - `querycoord_submit`：QueryCoord 同步提交阶段。
   - `load_start`：observer 接管异步 load 的起点。
   - `load_complete`：observer 判断加载完成。
   - `load_timeout/load_canceled`：超时或取消。

5. QueryNode Cache Execution
   - `Search/segment_search`：Search 在 QueryNode 已有 segment 上的执行耗时。
   - `Query/segment_query`：Query 在 QueryNode 已有 segment 上的执行耗时。
   - 按 `segment_type` 进一步统计。

6. 输出文件
   - 如果安装 pandas/matplotlib/seaborn，会输出 CSV 和图。
   - 如果未安装这些库，只输出文本报告。

### 6.3 脚本与当前代码的配套性

当前脚本已经匹配最新代码新增的关键事件：

- `segment_cache_hit`
- `segment_cache_miss`
- `segment_cache_load`
- `segment_search`
- `segment_query`
- `total_load`
- LoadSegment 子阶段
- `querycoord_submit`
- `load_start`
- `load_complete`
- `load_timeout`
- `load_canceled`

如果新日志仍然显示：

```text
LoadCollection/LoadPartition: No data
```

不能直接解释为 Milvus 没有 load collection 行为，只能说明当前采集到的 `[LATENCY_TRACE]` 事件里没有 `operation=LoadCollection/LoadPartition`。常见原因是运行的二进制不是包含本次改动的版本，或者没有采集到 QueryCoord 日志。

## 7. 这些 trace 能支撑的分析

### 7.1 写入链路瓶颈定位

可以回答：

- Proxy 端 `serialize/repack` 是否耗时异常。
- MQ produce 是否拉高 insert 请求同步时延。
- DataNode `consume_lag` 是否说明 MQ 消费或 flow graph 存在积压。
- DataNode buffer 处理是否成为瓶颈。
- Flush 写对象存储耗时与序列化耗时占比。

支撑的数据：

- 各 stage 的 count、mean、median、P95、P99、min/max。
- `s3_write` 的 `serialize_ms` 可分出写前序列化开销。

### 7.2 Search/Query 负载画像

可以回答：

- Benchmark 中 Search 与 Query 请求数分别是多少。
- 请求路由阶段是否有异常耗时。
- 请求面对的是 growing 还是 sealed segment 为主。
- 候选 segment 数与最终执行 segment 次数是否一致；如果差异大，需要看 prune、target node、ignore growing 等因素。

支撑的数据：

- `route` latency。
- `segment_stats` 的 sealed/growing 候选数量。
- `segment_search/segment_query` 的真实 per-segment 执行次数。

### 7.3 QueryNode 热执行 vs 对象存储冷加载

可以回答：

- Search/Query 在 QueryNode 已有 segment 上执行的平均/P95/P99 耗时。
- lazy-load DiskCache 是否命中。
- lazy-load miss 后，从对象存储加载 segment 的次数和整体耗时。
- 冷加载耗时和热执行耗时的量级差异。

这是共享内存池或跨组件复用优化最需要的数据。判断方式：

- 如果 `segment_cache_load` count 高，且 P95/P99 明显高于 `segment_search/segment_query`，说明对象存储冷加载是明确瓶颈。
- 如果 `segment_cache_load` 没有数据，但 `segment_search/segment_query` 很多，说明 benchmark 大部分查询在已加载 segment 上运行，需要确认是否开启 lazyload、是否发生 release/evict、是否 benchmark 之前已经 `load_collection` 完成。
- 如果只有 `route` 没有 `segment_search/segment_query`，需要确认 QueryNode worker 日志是否被采集到、是否 remote worker trace id 未串上、或者打点版本不是最新。

### 7.4 LoadSegment 子阶段拆解

可以回答：

- 冷加载慢主要来自 index、field data、bloom filter、delta log，还是反序列化/应用 delete 数据。
- 如果 `load_field_data/load_index` 慢，说明 Go 调 C/C++ 的加载接口整体耗时高；还需要更底层 C++ trace 才能进一步拆分对象存储读、mmap、deserialize、index warmup。
- 如果 `deserialize_stats/deserialize_delta` 慢，说明反序列化或 delete/stats 处理可能是 CPU/内存瓶颈。

### 7.5 解释“为什么没有 S3 访问”

如果源日志中全局搜索不到：

```text
operation=LoadSegment
stage=segment_cache_load
stage=total_load
```

只能说明当前采集到的 `[LATENCY_TRACE]` 中没有 QueryNode LoadSegment 事件，不能由解析脚本凭空推导 S3 访问。常见原因包括：

1. 运行的 Milvus 二进制不是最新包含 `b8a841c135` 的代码。
2. 采集的日志只包含 Proxy/DataNode，缺 QueryNode stdout/stderr。
3. benchmark 前已经完成 load，Search 时 segment 已常驻 QueryNode，不触发 lazy-load miss。
4. `queryNode.lazyload.enabled` 未开启，Search 阶段不走 DiskCache lazy miss 路径。
5. workload 只做 insert，没有真正发出 Search/Query 或 LoadSegment。
6. 触发的是普通 LoadCollection/LoadSegment 背景加载，但日志没采到 QueryNode，或 trace id 与请求不同导致按请求聚合时看不到。

结合用户之前的 `901.log` 现象：日志里只有 `operation=Insert`，解析结果没有 S3 load 是合理的脚本行为。根因更可能是运行版本/采集范围/benchmark 场景没有产出 QueryNode load trace，而不是解析脚本把 `LoadSegment` 漏掉。

## 8. 对 768D 1M benchmark 的口径

“第一次从数据集中获取数据始终要 load”在 Milvus 里要区分两个阶段：

- `load_collection/load_partition` 阶段：把 sealed segment 加载到 QueryNode，使其可被 search/query。这个过程可能在 benchmark 搜索前已经完成。
- `search/query` 阶段：如果 segment 已加载，查询直接在 QueryNode segment 上执行，不会每次都从 S3 读取 segment。

因此，如果 768D 1M benchmark 的流程是：

1. insert 数据；
2. flush/index/load collection；
3. 等待 load 完成；
4. 再跑 search；

那么 search 日志中看不到 S3 load 是正常的，应该看到的是 `segment_search/segment_query`。  
如果希望评估 Query/Search 过程中“QueryNode 没有 segment，需要从 S3 拉取”的时延，必须构造会触发 lazy-load miss 的场景，例如开启 `queryNode.lazyload.enabled=true`，确保 segment 初始不在 DiskCache 中，或发生 cache evict/release 后再 search/query。

## 9. 已确认的改动收益

本次 trace 能为优化方案提供以下证据链：

1. 写入端是否值得优化
   - 如果 `serialize/mq_produce/consume_lag/datanode_process/s3_write` 中某阶段 P95/P99 高，可以定位写入链路瓶颈。

2. 冷加载是否是瓶颈
   - `segment_cache_load` 直接给出 Search/Query 触发对象存储冷加载的次数和耗时。
   - `total_load` 给出底层 LoadSegment 总耗时。

3. 热路径基线
   - `segment_search/segment_query` 给出 QueryNode 已有 segment 时的执行耗时，是和冷加载对比的基线。

4. 优化收益估算
   - 如果共享内存池能避免对象存储读取和部分反序列化，可用 `segment_cache_load` 或 `total_load` 的 P95/P99 作为上限估计。
   - 更保守的估算应使用：冷加载耗时减去热执行耗时，即 `segment_cache_load - segment_search/segment_query` 的同分位量近似差值。

5. 子阶段定位
   - 如果 `load_index/load_field_data/load_bloom_filter/load_delta_logs` 占比高，优先优化对象存储读、mmap、cache、index warmup。
   - 如果 `deserialize_stats/deserialize_delta/load_delta_apply` 占比高，优先优化 CPU 反序列化和 delete/stats 应用。

## 10. 当前仍不能下结论的内容

以下内容不能在没有真实新日志和脚本输出的情况下下结论：

- 不能声称当前 benchmark 的真实性能瓶颈一定是 S3/object store。
- 不能声称 768D 1M 的 Search 一定触发 lazy-load miss。
- 不能用旧日志证明 `LoadCollection/LoadPartition` 的真实性能表现；该链路已补 trace，但需要用包含本次改动的新二进制重新采集 QueryCoord 日志。
- 不能精确拆分 `load_field_data/load_index` 内部的对象存储读取耗时与 C++ 反序列化耗时。
- 不能保证跨 MQ 或跨 QueryCoord/QueryNode gRPC 的所有事件都和入口请求使用同一个自定义 uuid；当前版本没有新增自定义 latency trace metadata 传播，跨组件串联需要依赖 Milvus 现有 OTel context 传播和真实日志验证。

## 11. 建议的验证命令

确认源日志里有哪些事件：

```bash
grep "\[LATENCY_TRACE\]" milvus.log | awk '{for(i=1;i<=NF;i++) if($i ~ /^operation=|^stage=|^component=/) printf "%s ", $i; print ""}' | sort | uniq -c
```

直接查 QueryNode 冷加载：

```bash
grep "\[LATENCY_TRACE\]" milvus.log | grep "operation=LoadSegment"
grep "\[LATENCY_TRACE\]" milvus.log | grep "stage=segment_cache_load"
grep "\[LATENCY_TRACE\]" milvus.log | grep "stage=total_load"
```

查 Query/Search 热执行：

```bash
grep "\[LATENCY_TRACE\]" milvus.log | grep "stage=segment_search"
grep "\[LATENCY_TRACE\]" milvus.log | grep "stage=segment_query"
```

查 lazy-load cache 命中/未命中：

```bash
grep "\[LATENCY_TRACE\]" milvus.log | grep "stage=segment_cache_hit"
grep "\[LATENCY_TRACE\]" milvus.log | grep "stage=segment_cache_miss"
```

运行解析脚本：

```bash
python scripts/analyze_latency_traces.py milvus.log
python scripts/analyze_latency_traces.py milvus.log --output ./latency_report
```

如果要验证 Query/Search 触发 S3/object-store 冷加载，建议明确记录：

- Milvus commit id。
- 是否包含 QueryNode 日志。
- `queryNode.lazyload.enabled` 是否开启。
- 是否在 search/query 前执行了 `load_collection` 并等待完成。
- cache 容量、是否发生 evict/release。
- benchmark 中 insert、flush、index、load、search/query 的实际顺序。

## 12. 本地验证状态

已完成：

- `git log --since="2026-08-11 00:00:00"` 梳理提交。
- `git diff --name-status d1effb4429^..HEAD` 梳理修改文件范围。
- trace 相关代码静态阅读。
- `git diff --check` 通过。

未完成：

- 当前环境 `go` 不在 PATH，无法执行 Go 编译、单测或 gofmt。
- 当前环境 `python.exe` 启动失败，`py` 不存在，无法实际运行 `scripts/analyze_latency_traces.py`。
- 没有新的 benchmark 完整日志，无法给出真实 count/mean/P95/P99 结论。

## 13. 汇报口径建议

可以对领导这样概括：

本次改造新增了一套面向瓶颈定位的 Milvus latency trace，覆盖写入、查询路由、QueryNode 热执行、lazy-load cache miss 后对象存储冷加载、以及 LoadSegment 子阶段。打点不是泛泛记录日志，而是围绕“共享内存池/缓存复用能不能减少 S3 读和反序列化开销”这个问题设计：

- 写入侧用 `serialize/mq_produce/consume_lag/datanode_process/s3_write` 判断数据进入对象存储之前各阶段是否是瓶颈。
- 查询侧用 `route/segment_stats` 判断请求面对的 sealed/growing segment 规模。
- 用 `segment_search/segment_query` 建立 QueryNode 已有 segment 的热路径基线。
- 用 `segment_cache_miss + segment_cache_load` 统计 Query/Search 触发对象存储冷加载的次数和耗时。
- 用 `LoadCollection/LoadPartition` 的 `querycoord_submit/load_start/load_complete/load_timeout/load_canceled` 统计 QueryCoord 视角的 load 生命周期；该链路当前是低侵入事件打点，不修改 QueryCoord meta，也不承诺把入口 load 请求与每个 QueryNode `LoadSegment` 强绑定为同一个自定义 trace id。
- 用 `total_load` 和 load 子阶段拆解 LoadSegment 慢在哪里。

对当前旧日志中“没有 S3 load”的解释应保持严谨：如果源日志没有 `operation=LoadSegment` 或 `stage=segment_cache_load/total_load`，解析脚本不会也不应该输出 S3 load 数据。这说明该次运行没有采集到 QueryNode LoadSegment trace，可能是版本、日志采集范围、lazyload 配置或 benchmark 流程导致。最新代码已经补齐 QueryCoord load 生命周期、QueryNode load/cache/search/query 相关打点；下一步需要用包含最新改动的 Milvus 二进制和完整 QueryCoord/QueryNode 日志重新跑一次，才能形成真实性能数据结论。
