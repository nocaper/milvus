# QueryNode 远端加载路径 Trace 与 QPS 收益评估说明

本文说明本次为量化 `DataNode -> MinIO/Object Storage -> QueryNode` 路径收益而新增的
QueryNode trace、CSV 解析流程和推荐 benchmark 场景。

## 1. 目标

目标不是证明 warm search QPS 能提升，而是量化：

1. 一次 VectorDBBench search benchmark 中，有多少请求实际遇到 QueryNode 远端加载路径。
2. 这些请求在 lazy segment access、disk cache load、object storage read/decode 上花了多少时间和字节。
3. 如果 FPGA/共享内存原型能把该路径加速 `S` 倍，端到端 QPS 理论上最多能提升多少。

## 2. 新增日志点

### 2.1 Request 级日志

文件：

- `internal/querynodev2/path_trace.go`
- `internal/querynodev2/services.go`

日志消息：

- `querynode request path trace`

覆盖入口：

- `Search`
- `SearchSegments`
- `Query`
- `QuerySegments`

关键字段：

- `operation`: `search` 或 `query`
- `entry`: `search`、`search_segments`、`query`、`query_segments`
- `trace_id`: OpenTelemetry trace id，开启 trace 时用于精确关联
- `msg_id`: Milvus request msg id，trace 未开启时作为降级关联键
- `collection_id` / `db_id` / `scope`
- `nq` / `top_k`
- `channel_count`
- `segment_count`
- `total_channel_num`

用途：

- `request.csv` 的来源。
- search 请求比例的分母。
- `entry=search` 优先代表客户端 search 请求；如果日志只来自 worker，可退化使用 `entry=search_segments`。

### 2.2 Segment access 级日志

文件：

- `internal/querynodev2/segments/load_trace.go`
- `internal/querynodev2/segments/search.go`
- `internal/querynodev2/segments/segment_do.go`

日志消息：

- `querynode segment access path trace`

关键字段：

- `operation`
- `trace_id`
- `msg_id`
- `segment_id`
- `row_count`
- `is_lazy_load`
- `cache_miss`
- `duration_ms`
- `wait_cache_ms`
- `estimated_memory_bytes`
- `estimated_disk_bytes`
- `group_size`

用途：

- `access.csv` 的来源。
- `cache_miss=true` 表示本次 search/query segment access 等待了 lazy segment load。
- `group_size` 用于处理 QueryNode search task merge。如果多个 search 请求被合并，access 日志通常只带合并后 task 的第一个 `msg_id`，需要用 `group_size` 估算受影响请求数。

### 2.3 Disk cache load 日志

文件：

- `internal/querynodev2/segments/manager.go`

日志消息：

- `querynode disk cache load trace`

用途：

- 记录 lazy segment 实际加载耗时。
- 和 `access.csv` 中的 `segment_access` 一起区分“请求等待时间”和“segment load 时间”。

### 2.4 Object storage read/decode 日志

文件：

- `internal/querynodev2/segments/load_trace.go`
- `internal/core/src/storage/Util.cpp`

日志消息：

- `querynode remote object fetch trace`

用途：

- `remote.csv` 的来源。
- 表示真正从 MinIO/Object Storage 读取对象、decode payload 的记录。
- 这是分析远端对象粒度和字节量的主要依据。

### 2.5 VA 日志

文件：

- `internal/core/src/segcore/SegmentSealedImpl.cpp`
- `internal/core/src/storage/ChunkCache.cpp`

日志消息：

- `querynode segment field data va trace`
- `querynode chunk cache mmap va trace`

用途：

- `va.csv` 的来源。
- 表示数据进入 segcore 后的列内存或 mmap 视图。
- 它不是 MinIO 传输粒度，不应用来直接表示 `DataNode -> MinIO -> QueryNode` 路径粒度。

## 3. 解析流程

第一步，从 Milvus 日志抽取结构化 CSV：

```bash
python tools/querynode_trace/analyze_minio_trace.py "logs/*.log" --csv-prefix out/milvus_trace
```

输出：

- `out/milvus_trace.request.csv`
- `out/milvus_trace.access.csv`
- `out/milvus_trace.remote.csv`
- `out/milvus_trace.planned.csv`
- `out/milvus_trace.va.csv`

第二步，汇总请求比例和假设 QPS 收益：

```bash
python tools/querynode_trace/summarize_path_benefit.py \
  --csv-prefix out/milvus_trace \
  --old-qps 1000 \
  --old-avg-latency-ms 20 \
  --path-speedup 5
```

也可以显式给 CSV：

```bash
python tools/querynode_trace/summarize_path_benefit.py \
  --request-csv out/milvus_trace.request.csv \
  --access-csv out/milvus_trace.access.csv \
  --remote-csv out/milvus_trace.remote.csv \
  --old-qps 1000 \
  --path-fraction 0.25 \
  --path-speedup 5 \
  --json-out out/path_benefit.json \
  --summary-csv out/path_benefit.csv
```

## 4. 结果指标解释

`summarize_path_benefit.py` 的核心输出：

- `join_key`: 实际使用的关联键，优先 `trace_id`，否则 `msg_id`
- `search_request_count`: search 请求分母
- `search_cache_miss_request_count`: 命中 cache miss 的 search 请求数
- `search_cache_miss_request_ratio`: 请求级 cache miss 比例
- `search_cache_miss_request_group_size_estimate`: 考虑 task merge 后的受影响请求数估算
- `search_cache_miss_request_group_size_ratio`: 考虑 task merge 后的受影响请求比例
- `search_segment_cache_miss_ratio`: segment access 级 cache miss 比例
- `remote_read_mib`: object storage 读取总 MiB
- `remote_read_duration_ms_sum`: object storage read 耗时总和
- `estimated_path_fraction_from_latency`: 根据 `wait_cache_ms / avg_latency` 估算的路径时间占比
- `estimated_new_qps`: 假设优化后的 QPS 估计
- `estimated_qps_gain_percent`: QPS 提升百分比

注意：

- `search_cache_miss_request_ratio` 是“多少请求遇到该路径”。
- QPS 收益不能直接等于这个比例。
- QPS 收益应使用路径时间占比 `F`，而不是只使用请求命中比例 `p`。

## 5. QPS 收益模型

如果 FPGA/共享内存原型测得该路径加速倍数为 `S`，端到端路径时间占比为 `F`，则：

```text
QPS_new / QPS_old ~= 1 / (1 - F + F / S)
```

其中：

- `QPS_old`: 未优化 Milvus 在同一 benchmark 下测得的 QPS
- `S`: 原型平台测得的路径加速倍数
- `F`: 旧系统中该路径占端到端请求时间或吞吐瓶颈时间的比例

如果提供 `--old-avg-latency-ms`，脚本会用：

```text
F ~= sum(cache_miss_wait_ms * group_size) / (search_request_count * old_avg_latency_ms)
```

如果你已经有更可靠的瓶颈时间占比，可以直接传：

```bash
--path-fraction 0.25
```

## 6. 推荐 benchmark 场景

### 6.1 不推荐只跑 warm search QPS

warm search 通常表示：

- collection 已加载
- index/data 已在 QueryNode 或 cache 中
- search 不再触发 MinIO/Object Storage 读取

这种场景下 `search_cache_miss_request_ratio` 很可能接近 0，优化 `DataNode -> MinIO -> QueryNode`
路径不会反映到 search QPS。

### 6.2 推荐场景一：cold / lazy-load search QPS

配置：

- 开启 `queryNode.lazyload.enabled: true`
- 关闭或弱化 search task merge，建议 `queryNode.grouping.maxNQ: 1`
- 开启 trace，建议：
  - `trace.exporter: stdout` 或 `jaeger`
  - `trace.sampleFraction: 1`

流程：

1. 清空 QueryNode cache，或重启 QueryNode。
2. load collection。
3. 立即运行 VectorDBBench search。
4. 用 `request.csv + access.csv` 统计 cache miss 请求比例。
5. 用 `remote.csv` 统计远端读取字节和耗时。

### 6.3 推荐场景二：insert/flush/handoff + search 混合 QPS

流程：

1. 持续 insert。
2. 周期性 flush，使新 sealed segment 产生。
3. 同时运行 search QPS。
4. 观察 search QPS 在 segment handoff/load 期间的下降。
5. 统计对应时间窗口内的 `request.csv/access.csv/remote.csv`。

这个场景更适合证明在线写入业务中，远端加载路径会干扰 search serving。

### 6.4 推荐场景三：recovery / rebalance / rolling restart

流程：

1. 已有 collection 和数据。
2. 重启 QueryNode，或触发 rebalance。
3. 节点恢复后立刻跑 search。
4. 计算恢复窗口中的 effective QPS：

```text
effective_QPS = successful_search_count / (recovery_or_load_time + search_time)
```

这个场景适合量化系统恢复速度收益。

## 7. 建议实验口径

建议至少保留三组结果：

1. warm search QPS：作为上界/对照，通常不体现该路径收益。
2. cold lazy-load search QPS：作为最直接收益场景。
3. mixed insert/flush/search QPS：作为在线写入收益场景。

如果只做一组，请优先做 cold lazy-load search QPS。

如果要做严格请求级比例，建议设置：

```yaml
trace:
  exporter: stdout
  sampleFraction: 1

queryNode:
  lazyload:
    enabled: true
  grouping:
    maxNQ: 1
```

如果不能关闭 merge，则使用脚本输出的 `*_group_size_ratio` 作为请求比例估算。
