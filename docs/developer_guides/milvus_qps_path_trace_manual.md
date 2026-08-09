# Milvus QPS 路径追踪修改说明书

本文档说明本次为了量化 `DataNode -> MinIO -> QueryNode` 路径瓶颈而加入的代码改动、日志字段、指标口径、解析脚本和端到端性能提升估算方法。

## 1. 目标

当前无法在真实 Milvus 服务器上直接运行“DataNode 到 QueryNode 共享内存优化版”，因此采用 what-if 量化：

1. 在真实 Milvus 环境测量基线端到端性能：总耗时 `T_base`、基线 QPS `QPS_base`。
2. 在真实 Milvus 日志中抓取一次测试里有多少事件走到了 `DataNode -> MinIO -> QueryNode` 路径、多少事件没有走到该路径，以及各自时延。
3. 在 FPGA 原型平台上测量同一类数据路径优化后的时延 `T_path_new`，或得到路径加速比 `S_path`。
4. 用基线路径耗时占比估算端到端提升：

```text
T_new = T_base - T_path_old + T_path_new
QPS_new_est = QPS_base * T_base / T_new
improvement_percent = (QPS_new_est / QPS_base - 1) * 100%
```

如果只有路径加速比：

```text
p = T_path_old / T_base
speedup_total = 1 / (1 - p + p / S_path)
improvement_percent = (speedup_total - 1) * 100%
```

## 2. 采集口径

新增统一日志事件名：

```text
milvus_qps_path_trace
```

该事件用于统计 Milvus 逻辑路径是否命中 `DataNode -> MinIO -> QueryNode`。

核心字段：

| 字段 | 含义 |
| --- | --- |
| `op` | 当前被采集的操作，例如 `load_segment`、`load_field_data`、`search`、`datanode_write_logs` |
| `path_hit` | 是否命中 DataNode 产出的数据经对象存储被 QueryNode 使用的路径 |
| `datanode_minio_querynode_path_hit` | QueryNode 侧同义字段，便于日志检索 |
| `object_storage_path_hit` | 是否发生对象存储路径访问；索引读取也会为 true |
| `path_kind` | 路径分类 |
| `status` | `success` 或 `fail` |
| `latency_ms` | 当前事件耗时，单位毫秒 |
| `datanode_remote_file_count` | QueryNode 将读取的 DataNode 产出日志文件数 |
| `datanode_remote_log_bytes` | QueryNode 将读取的 DataNode 产出日志字节数 |
| `insert_log_bytes` / `stats_log_bytes` / `delta_log_bytes` | insert、stats、delta 日志字节数 |
| `index_bytes` | 索引文件字节数，不计入 DataNode 共享内存优化收益 |
| `row_count` | 相关 segment 行数 |
| `collectionID` / `partitionID` / `segmentID` / `fieldID` | 定位采集对象 |

`path_kind` 的取值：

| path_kind | 说明 | 是否计入 DataNode 共享内存优化收益 |
| --- | --- | --- |
| `datanode_minio_querynode` | QueryNode 读取 DataNode 写出的 v1 binlog/stats/delta 数据 | 是 |
| `datanode_minio_querynode_storage_v2` | QueryNode 读取 storage v2 数据 | 是 |
| `datanode_and_index_minio_querynode` | 同一 load 同时包含 DataNode 数据和 index 文件 | DataNode 部分计入，index 部分不计入 |
| `datanode_storage_v2_and_index_minio_querynode` | storage v2 数据和 index 文件混合 | DataNode/storage v2 部分计入 |
| `index_minio_querynode` | 只读取 index 文件 | 否 |
| `none` | search/query 稳态请求或跳过事件，没有命中该路径 | 否 |

注意：稳态 search/query 已经在 QueryNode 内存中执行时，通常不会走 `DataNode -> MinIO -> QueryNode`。如果测试是纯预加载后的 search QPS，优化该路径对端到端 QPS 的估计提升会接近 0。

## 3. 新增功能点

### 3.1 QueryNode 路径摘要 helper

新增文件：

```text
internal/querynodev2/segments/qps_path_trace.go
```

主要功能：

1. 汇总 `SegmentLoadInfo` 中 insert/stats/delta binlog 的文件数、字节数和 entry 数。
2. 汇总 index 文件数和字节数。
3. 根据摘要自动判断 `path_hit` 和 `path_kind`。
4. 统一输出 `milvus_qps_path_trace` 日志。
5. 同步写入 Prometheus 指标。

关键代码示例：

```go
func RecordQPSPathEvent(ctx context.Context, op string, summary QPSPathSummary, latency time.Duration, err error, fields ...zap.Field) {
    pathKind := summary.pathKind()
    pathHit := summary.dataNodePathHit()
    status := qpsPathStatus(err)
    remoteBytes := summary.remoteBytes()

    recordQPSPathMetrics(op, pathHit, pathKind, status, latency, remoteBytes)

    log.Ctx(ctx).Info("milvus_qps_path_trace",
        zap.String("op", op),
        zap.Bool("path_hit", pathHit),
        zap.Bool("datanode_minio_querynode_path_hit", pathHit),
        zap.String("path_kind", pathKind),
        zap.Int64("latency_ms", latency.Milliseconds()),
        zap.Int64("datanode_remote_log_bytes", summary.datanodeLogBytes()),
    )
}
```

### 3.2 QueryNode load/search/query 采集点

修改文件：

```text
internal/querynodev2/handlers.go
internal/querynodev2/segments/segment.go
internal/querynodev2/segments/segment_loader.go
```

采集点：

| op | 位置 | 目的 |
| --- | --- | --- |
| `search` | QueryNode search handler | 记录 search 请求未走 DataNode-MinIO-QueryNode 路径的基线事件 |
| `query` | QueryNode query handler | 记录 query 请求未走该路径的基线事件 |
| `query_stream` | QueryNode stream query handler | 记录 streaming query 未走该路径 |
| `load_segment` | segment loader | 记录每个 segment load 的总时延和路径分类 |
| `load_segment_skipped` | segment loader prepare 后 | 记录 segment 已加载或正在加载导致跳过 |
| `load_multi_field_data` | `LocalSegment.LoadMultiFieldData` | 记录批量字段数据读取与 decode 时延 |
| `load_field_data` | `LocalSegment.LoadFieldData` | 记录单字段 binlog 读取与 load 时延 |
| `load_bloom_filter` | bloom filter 加载 | 记录 stats log 读取与反序列化时延 |
| `load_delta_logs` | delta log 加载 | 记录 delete log 读取与应用时延 |
| `load_index` | index 加载 | 标记 index-only 对象存储路径，避免误算为 DataNode 优化收益 |

QueryNode 侧典型代码形态：

```go
loadStart := time.Now()
defer func() {
    RecordQPSPathEvent(ctx, "load_field_data",
        SummarizeFieldBinlog(field, loadInfo.GetStorageVersion()),
        time.Since(loadStart),
        err,
        zap.Int64("collectionID", s.Collection()),
        zap.Int64("segmentID", s.ID()),
        zap.Int64("fieldID", fieldID),
        zap.Int64("row_count", rowCount),
    )
}()
```

search/query 没有命中该路径时使用：

```go
segments.RecordQPSNoPathEvent(ctx, metrics.SearchLabel, latency, nil,
    zap.Int64("collectionID", req.GetReq().GetCollectionID()),
    zap.String("channel", channel),
    zap.Int64("nq", req.GetReq().GetNq()),
    zap.Int64("topk", req.GetReq().GetTopk()),
)
```

### 3.3 DataNode 写对象存储采集点

修改文件：

```text
internal/datanode/syncmgr/task.go
internal/datanode/syncmgr/taskv2.go
```

新增采集：

| op | 说明 |
| --- | --- |
| `datanode_write_logs` | v1 binlog/stats/delta 写入 MinIO 的耗时、文件数、字节数 |
| `datanode_write_storage_v2` | storage v2 写入提交耗时、stats blob 信息、storage version |

代码示例：

```go
start := time.Now()
err := retry.Do(context.Background(), func() error {
    return t.chunkManager.MultiWrite(context.Background(), t.segmentData)
}, t.writeRetryOpts...)
t.recordQPSDataNodeWrite(time.Since(start), err)
return err
```

DataNode 侧日志用于确认“DataNode 产生了多少远端数据”。端到端 QueryNode 读路径收益估算时，优先使用 QueryNode 读侧事件，避免把写入侧耗时和读取侧耗时重复算入同一个收益项。

### 3.4 对象存储 I/O 采集点

修改文件：

```text
internal/storage/remote_chunk_manager.go
internal/core/src/storage/MinioChunkManager.cpp
internal/core/src/storage/OpenDALChunkManager.cpp
internal/core/src/storage/Util.cpp
```

新增统一日志事件名：

```text
milvus_qps_path_object_io
```

该事件记录 Go 和 C++ 路径上的对象存储读写 I/O：

| op | 说明 |
| --- | --- |
| `remote_write` | Go `RemoteChunkManager.Write` |
| `remote_read` | Go `RemoteChunkManager.Read` |
| `remote_read_at` | Go `RemoteChunkManager.ReadAt` |
| `remote_read_cpp` | C++ ChunkManager 读取 |
| `remote_write_cpp` | C++ MinIO 写入 |
| `remote_download_decode_cpp` | C++ 下载并 decode |
| `remote_read_storage_v2_cpp` | C++ storage v2 blob 读取 |

对象存储 I/O 日志用于校验底层读写量和耗时，不建议直接作为端到端收益模型的唯一输入，因为一个 `load_segment` 会包含多个子 I/O，直接累加容易重复计算。

Go 侧代码示例：

```go
start := time.Now()
err := mcm.putObject(ctx, mcm.bucketName, filePath, bytes.NewReader(content), int64(len(content)))
logRemoteObjectIO(ctx, "remote_write", mcm.bucketName, filePath, int64(len(content)), time.Since(start), err)
```

C++ 侧日志示例：

```cpp
LOG_INFO(
    "milvus_qps_path_object_io op=remote_read_cpp path_kind=object_storage_io object_storage_path_hit=true path={} bytes={} latency_ms={} status=success",
    filepath,
    read_size,
    latency_ms);
```

### 3.5 Prometheus 指标

修改文件：

```text
pkg/metrics/querynode_metrics.go
```

新增指标：

| 指标 | 类型 | label | 含义 |
| --- | --- | --- | --- |
| `milvus_querynode_qps_path_event_count` | Counter | `node_id, op, path_hit, path_kind, status` | 路径事件次数 |
| `milvus_querynode_qps_path_latency` | Histogram | `node_id, op, path_hit, path_kind, status` | 路径事件时延，单位 ms |
| `milvus_querynode_qps_path_remote_bytes` | Histogram | `node_id, op, path_kind` | 路径关联远端字节数 |

PromQL 示例：

```promql
sum(increase(milvus_querynode_qps_path_event_count{path_hit="true",path_kind=~"datanode.*",status="success"}[10m]))
```

```promql
sum(increase(milvus_querynode_qps_path_event_count{path_hit="false",status="success"}[10m]))
```

```promql
sum(increase(milvus_querynode_qps_path_latency_sum{path_hit="true",path_kind=~"datanode.*",status="success"}[10m]))
```

```promql
histogram_quantile(0.95, sum(rate(milvus_querynode_qps_path_latency_bucket{path_hit="true",path_kind=~"datanode.*",status="success"}[10m])) by (le, op))
```

```promql
sum(increase(milvus_querynode_qps_path_remote_bytes_sum{path_kind=~"datanode.*"}[10m]))
```

## 4. 日志样例

QueryNode JSON 日志示例：

```json
{
  "msg": "milvus_qps_path_trace",
  "op": "load_field_data",
  "path_hit": true,
  "datanode_minio_querynode_path_hit": true,
  "object_storage_path_hit": true,
  "path_kind": "datanode_minio_querynode",
  "status": "success",
  "latency_ms": 43,
  "datanode_remote_file_count": 2,
  "datanode_remote_log_bytes": 8388608,
  "insert_log_bytes": 8388608,
  "row_count": 50000,
  "collectionID": 100,
  "partitionID": 101,
  "segmentID": 102,
  "fieldID": 103
}
```

QueryNode no-path 日志示例：

```json
{
  "msg": "milvus_qps_path_trace",
  "op": "search",
  "path_hit": false,
  "path_kind": "none",
  "status": "success",
  "latency_ms": 12,
  "collectionID": 100,
  "nq": 16,
  "topk": 10
}
```

C++ 对象存储 I/O 日志示例：

```text
milvus_qps_path_object_io op=remote_read_cpp path_kind=object_storage_io object_storage_path_hit=true path=files/insert_log/100/101/102/103/1 bytes=8388608 latency_ms=27 status=success
```

## 5. 解析脚本

新增脚本：

```text
scripts/qps_path_trace_parser.py
```

能力：

1. 支持解析 Go zap JSON 日志。
2. 支持解析 C++ 文本日志。
3. 统计 `path_hit=true`、`path_hit=false` 的事件数和比例。
4. 汇总 path-hit/no-path 的时延。
5. 导出事件明细 CSV 和按 op 分组的 CSV。
6. 根据 FPGA 测得的优化后路径耗时或加速比，估算端到端 QPS 提升。

基本用法：

```powershell
python scripts\qps_path_trace_parser.py logs\querynode.log logs\datanode.log
```

导出 CSV 和 JSON：

```powershell
python scripts\qps_path_trace_parser.py logs\*.log --events-csv qps_events.csv --ops-csv qps_ops.csv --summary-json qps_summary.json
```

使用路径加速比估算端到端 QPS：

```powershell
python scripts\qps_path_trace_parser.py logs\querynode*.log --base-total-ms 600000 --path-speedup 3.2 --qps-base 1200
```

使用 FPGA 实测优化后路径总耗时：

```powershell
python scripts\qps_path_trace_parser.py logs\querynode*.log --base-total-ms 600000 --optimized-path-ms 18500 --qps-base 1200
```

从标准输入读取：

```powershell
Get-Content logs\querynode.log | python scripts\qps_path_trace_parser.py -
```

### 5.1 model profile

脚本提供 `--model-profile`，用于控制 what-if 模型选择哪些事件：

| profile | 说明 | 建议使用场景 |
| --- | --- | --- |
| `querynode_read_side` | 默认，只选择 `load_field_data/load_multi_field_data/load_bloom_filter/load_delta_logs` | 估算 QueryNode 读侧路径优化收益，避免把 `load_segment` 和子事件重复计数 |
| `segment_load_topline` | 只选择 `load_segment` | 只想用 segment load 总耗时作为路径旧耗时 |
| `datanode_write_side` | 只选择 DataNode 写入事件 | 分析写入/flush 路径，不用于 QueryNode 读侧收益 |
| `all_datanode_trace` | 选择所有 path-hit trace 事件 | 排查用，通常不用于最终收益模型 |

如果脚本输出 warning：

```text
selected path latency is larger than base total time
```

说明所选事件的时延存在并发重叠或重复计数，应切换 profile，或用更细的测试窗口重新采集。

## 6. 推荐测试步骤

### 步骤 1：准备基线测试窗口

在真实 Milvus 服务器上清理或标记日志起点，记录测试开始和结束时间。建议单独跑一次目标 workload，避免其他 collection 或后台任务干扰。

需要记录：

```text
T_base: 端到端测试总耗时，单位 ms
QPS_base: 真实 Milvus 基线 QPS
workload: search/query/load/insert/flush 的比例
collectionID: 测试 collection
时间窗口: start_time 到 end_time
```

### 步骤 2：采集真实 Milvus 日志和指标

采集 QueryNode 日志：

```text
milvus_qps_path_trace
milvus_qps_path_object_io
```

如果 workload 包含写入或 flush，也采集 DataNode 日志中的：

```text
datanode_write_logs
datanode_write_storage_v2
```

同时保留 Prometheus 指标作为交叉验证。

### 步骤 3：解析路径命中比例和旧路径耗时

执行：

```powershell
python scripts\qps_path_trace_parser.py logs\querynode*.log --events-csv qps_events.csv --ops-csv qps_ops.csv --summary-json qps_summary.json
```

重点看输出：

```text
QueryNode trace only
trace success events
datanode path hit events
no-path events
path hit ratio
datanode path latency sum ms
no-path latency sum ms
old path latency sum ms
old path fraction of base
```

其中 `QueryNode trace only` 是端到端读侧收益估算的主口径；`DataNode write trace only` 用于确认写入侧远端数据量和耗时。如果把 QueryNode 和 DataNode 日志一起传入脚本，不要直接用全量 `trace success events` 做读侧命中比例。

`old path latency sum ms` 是 what-if 模型使用的 `T_path_old`。

### 步骤 4：接入 FPGA 原型平台测量结果

如果 FPGA 已经给出优化后路径总耗时：

```powershell
python scripts\qps_path_trace_parser.py logs\querynode*.log --base-total-ms <T_base> --optimized-path-ms <T_path_new> --qps-base <QPS_base>
```

如果 FPGA 给出的是路径加速比：

```powershell
python scripts\qps_path_trace_parser.py logs\querynode*.log --base-total-ms <T_base> --path-speedup <S_path> --qps-base <QPS_base>
```

脚本会输出：

```text
new total ms
estimated speedup
estimated improvement percent
qps estimated
```

### 步骤 5：形成结论

建议报告中同时给出：

1. 基线 QPS 和测试时长。
2. 命中 `DataNode -> MinIO -> QueryNode` 的事件数、未命中事件数、命中比例。
3. `T_path_old`、`T_path_new`、路径加速比。
4. 端到端估算 QPS、提升百分比。
5. 是否存在并发重叠或重复计数风险。
6. 是否包含 index-only 路径，以及是否从收益中排除。

## 7. 常见口径问题

### 7.1 为什么 index-only 不算收益？

本次目标是假设优化 `DataNode -> QueryNode` 共享内存路径。index 文件通常由 IndexNode 产生，经对象存储被 QueryNode 加载，不属于 DataNode 产出数据路径。因此 `index_minio_querynode` 不计入 DataNode 共享内存优化收益。

### 7.2 `load_segment` 和 `load_field_data` 能不能一起累加？

不建议。`load_segment` 是顶层事件，`load_field_data/load_bloom_filter/load_delta_logs/load_index` 是子路径事件。两者直接累加会重复计数。

默认脚本使用 `querynode_read_side`，选择子路径事件作为读侧优化模型输入。如果你只相信顶层总耗时，可以改用：

```powershell
python scripts\qps_path_trace_parser.py logs\querynode*.log --model-profile segment_load_topline
```

### 7.3 为什么有 `path_hit=false` 但 `object_storage_path_hit=true`？

这通常是 index-only 路径。它走对象存储，但不是 DataNode 产出数据路径，所以不应该算入 DataNode 共享内存优化收益。

### 7.4 如何解释纯 search QPS 测试？

如果 collection 已经 load 完成，search/query 请求在 QueryNode 内存中执行，不会触发 DataNode binlog 远端读取。此时 `search/query` 会记录为 `path_hit=false`，估算出来的共享内存路径优化收益应接近 0。这是合理结论，不是采集失败。

### 7.5 如何避免后台任务污染？

使用独立 collection、独立时间窗口，并在解析时优先筛选目标 QueryNode/DataNode 日志。必要时根据 `collectionID`、`segmentID`、时间范围过滤 CSV。
