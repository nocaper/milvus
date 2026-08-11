# Milvus 延迟分析和瓶颈建模

本目录包含用于 Milvus 性能瓶颈分析和共享内存池优化预期收益建模的工具。

## 概览

通过在关键路径上添加细粒度的延迟打点，我们可以量化分析：
1. 写路径（Insert/Upsert）的各阶段耗时
2. 读路径（Search/Query）的瓶颈所在
3. Segment 从 S3 加载的延迟（共享内存池优化的核心目标）
4. Growing segment vs Sealed segment 的查询模式

## 架构设计

### 打点体系

```
写路径 (Insert/Upsert):
  ├─ serialize        : Proxy 序列化数据
  ├─ mq_produce       : Proxy 写入 MQ
  ├─ consume_lag      : DataNode 消费延迟 (MQ timestamp → 处理时间)
  ├─ datanode_process : DataNode 处理数据
  └─ s3_write         : DataNode 写入 S3

Load 路径:
  └─ total_load       : QueryNode 从 S3 加载 segment（优化核心目标）

Search/Query 路径:
  ├─ route            : QueryNode 路由决策
  ├─ segment_stats    : Growing vs Sealed segment 统计
  └─ total_load       : 如果触发了 segment load，记录等待时间
```

### Trace ID 传播

每个请求生成一个 `trace_id`，贯穿整个请求路径：
- Insert: Proxy → MQ → DataNode → S3
- Search: Proxy → QueryNode → (可能触发) Segment Load

## 使用指南

### 1. 编译启用打点的 Milvus

已修改的文件：
```
pkg/tracer/latency_tracer.go                    # 打点框架
internal/proxy/task_insert.go                   # Proxy Insert 打点
internal/datanode/flow_graph_write_node.go      # DataNode 消费打点
internal/datanode/syncmgr/task.go               # DataNode S3 写入打点
internal/querynodev2/delegator/delegator.go     # QueryNode Search 打点
internal/querynodev2/segments/segment_loader.go # QueryNode Segment Load 打点
```

**注意**：还需要在这些文件中添加 import:
```go
import "github.com/milvus-io/milvus/pkg/tracer"
```

编译 Milvus:
```bash
cd /d/project/claude/c_qps2.4.5/milvus
make milvus
```

### 2. 配置 Milvus 启用 Tracing

在 Milvus 配置文件中添加（需要修改启动逻辑来初始化 tracer）：

```yaml
# 示例配置（需要在各组件启动时调用 tracer.InitGlobalTracer）
latency_trace:
  enabled: true
  output_file: /var/log/milvus/latency_trace.jsonl
```

**在各组件的 main 函数或初始化函数中添加**：
```go
import "github.com/milvus-io/milvus/pkg/tracer"

func init() {
    // 在 proxy, datanode, querynode 的初始化中分别调用
    err := tracer.InitGlobalTracer(
        true,  // enabled
        "/var/log/milvus/latency_trace.jsonl",
        log.L(),  // zap logger
    )
    if err != nil {
        log.Warn("Failed to init latency tracer", zap.Error(err))
    }
}
```

### 3. 运行 Benchmark

运行你的测试负载（例如使用 Milvus 自带的 benchmark 工具）：

```bash
# 示例：运行插入和搜索混合负载
go run tests/python_client/chaos/scripts/hello_milvus.py \
    --host localhost \
    --port 19530 \
    --collection test_collection \
    --dim 768 \
    --insert_count 100000 \
    --search_count 10000
```

或者使用自定义的负载生成器。

### 4. 分析 Trace 数据

```bash
# 安装依赖
pip install pandas matplotlib seaborn

# 运行分析脚本
python scripts/analyze_latency_traces.py \
    /var/log/milvus/latency_trace.jsonl \
    --output ./latency_report
```

分析脚本会生成：
- `latency_report/latency_analysis.csv` - 原始 trace 数据
- `latency_report/trace_summary.csv` - 按 trace_id 聚合的摘要
- `latency_report/write_path_latency.png` - 写路径各阶段延迟对比
- `latency_report/segment_load_distribution.png` - Segment 加载延迟分布（**核心优化目标**）
- `latency_report/segment_type_distribution.png` - Growing vs Sealed segment 查询分布

### 5. 解读分析结果

分析报告会输出以下关键指标：

#### 写路径分析
```
serialize: Proxy 序列化延迟
mq_produce: MQ 写入延迟
consume_lag: DataNode 消费延迟
s3_write: S3 写入延迟（优化目标之一）
```

#### Segment Load 分析（核心）
```
count: segment 加载次数
mean/p95/p99: 延迟统计
```

**这是共享内存池优化的核心目标。如果 mean 为 X ms，优化后可以：**
- 消除 S3 读取延迟
- 消除反序列化延迟
- LoadCollection/LoadPartition 时间趋近于零
- 预期 QPS 提升：~(1000/X) * count ops/sec

#### Search 路径分析
```
growing_segment_hits: 从内存直接查询（已经很快）
sealed_segment_hits: 需要加载的查询（优化目标）
```

**优化预期**：
- Growing segment 查询：无额外收益（本来就快）
- Sealed segment 查询：节省 segment_load_latency

## 预期优化收益建模

基于分析结果，可以计算共享内存池的预期收益：

```python
# 假设分析结果
segment_load_mean = 500  # ms (从 S3 加载平均延迟)
segment_load_count = 100  # 每秒加载次数
sealed_segment_queries = 1000  # 每秒查询次数

# 优化后
optimized_load_latency = 10  # ms (从共享内存加载)

# QPS 提升
qps_improvement = (1000 / segment_load_mean) * segment_load_count
# = (1000 / 500) * 100 = 200 ops/sec

# 延迟降低
latency_reduction = segment_load_mean - optimized_load_latency
# = 500 - 10 = 490 ms

# 吞吐量提升比例
throughput_increase = (segment_load_mean / optimized_load_latency - 1) * 100
# = (500/10 - 1) * 100 = 4900% (50x)
```

## 进一步分析

### 查看特定 trace 的完整路径

```bash
# 提取特定 trace_id 的所有事件
grep "trace_id_here" /var/log/milvus/latency_trace.jsonl | jq .
```

### 统计各阶段占比

```python
import pandas as pd

df = pd.read_csv('latency_report/latency_analysis.csv')

# 按 stage 分组统计
stage_stats = df.groupby('stage')['duration_ms'].agg(['count', 'mean', 'median', 'std'])
print(stage_stats)

# 计算各阶段占总延迟的比例
stage_totals = df.groupby('stage')['duration_ms'].sum()
print(stage_totals / stage_totals.sum() * 100)
```

### 识别异常值

```python
# 找出延迟异常高的 trace
high_latency_traces = df[df['duration_ms'] > df['duration_ms'].quantile(0.99)]
print(high_latency_traces)
```

## 优化验证

实施共享内存池后，重新运行相同的 benchmark 和分析：

```bash
# 优化前
python scripts/analyze_latency_traces.py baseline_trace.jsonl -o baseline_report

# 优化后
python scripts/analyze_latency_traces.py optimized_trace.jsonl -o optimized_report

# 对比
diff baseline_report/trace_summary.csv optimized_report/trace_summary.csv
```

预期结果：
- `segment_load_latency` 大幅降低（从 S3 读取 → 共享内存读取）
- `LoadCollection` 延迟接近于零
- `sealed_segment` 查询延迟显著降低
- 整体 QPS 提升

## 注意事项

1. **Trace 文件大小**：长时间运行会产生大量 trace 数据，建议：
   - 定期轮转日志文件
   - 只在 benchmark 期间启用 tracing
   - 使用 buffered writer 减少 I/O 开销

2. **性能影响**：打点本身会引入微小的性能开销（每个 span ~微秒级），生产环境建议：
   - 使用采样（例如只 trace 1% 的请求）
   - 通过配置动态开关 tracing

3. **Trace ID 传播**：
   - 需要确保 trace_id 在 MQ 消息中传播（可能需要修改 msgstream）
   - 目前 DataNode 和 QueryNode 的某些路径可能无法获取到 trace_id

4. **时钟同步**：分布式环境中确保各节点时钟同步，否则延迟计算会不准确

## 代码完善建议

当前实现是初步版本，还需要完善：

1. **在 msgstream 中传播 trace_id**：
   - 修改 `msgstream.MsgPack` 添加 trace_id 字段
   - 在 Proxy 写入 MQ 时设置 trace_id
   - 在 DataNode 消费时读取 trace_id

2. **添加组件初始化逻辑**：
   - 在 Proxy/DataNode/QueryNode 的 main 函数中调用 `tracer.InitGlobalTracer`
   - 从配置文件读取 tracer 配置

3. **添加更多打点**：
   - QueryCoord 调度延迟
   - 索引构建延迟
   - 结果合并延迟

4. **优化 tracer 性能**：
   - 使用 buffered channel 异步写入
   - 批量写入磁盘
   - 支持采样率配置

## 联系和反馈

如有问题或建议，请提交 issue 或 PR。
