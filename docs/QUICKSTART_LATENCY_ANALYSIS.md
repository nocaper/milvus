# 快速开始：Milvus 延迟分析和瓶颈建模

## 目标

量化分析 Milvus 的性能瓶颈，特别是：
- **从 S3 加载 Segment 的延迟**（共享内存池优化的核心目标）
- 写路径各阶段的延迟分解
- Growing vs Sealed segment 的查询分布

## 一、编译支持打点的 Milvus

```bash
cd D:\project\claude\c_qps2.4.5\milvus

# 编译（已添加打点代码）
make milvus

# 或者只编译特定组件
make milvus-components
```

## 二、启动 Milvus（Tracing 默认开启）

### 方式 1：直接启动并捕获日志（推荐 - 裸机 Standalone）

```bash
# Tracing 已在代码中硬编码启用，无需设置环境变量
# 将 stdout/stderr 重定向到文件，[LATENCY_TRACE] 行会自动写入
export MILVUS_LATENCY_TRACE_ENABLED=true
export MILVUS_LATENCY_TRACE_OUTPUT=/tmp/milvus_traces/latency_trace.jsonl
mkdir -p /tmp/milvus_traces
./bin/milvus run standalone > /tmp/milvus.log 2>&1
```

### 方式 2：使用启动脚本（推荐 - 自动化）

```bash
# 脚本会检查 Milvus 状态、启动并等待服务就绪
chmod +x scripts/run_milvus_with_tracing.sh
./scripts/run_milvus_with_tracing.sh
```

脚本会自动：
- ✓ 检查是否已有 Milvus 运行
- ✓ 启动 Milvus standalone（日志写到 `/tmp/milvus_standalone.log`）
- ✓ 等待服务就绪
- ✓ 显示分析命令和下一步操作

## 三、运行测试负载

### 选项 A：使用 Demo 脚本（快速验证）

```bash
# 安装依赖
pip install pymilvus

# 运行 demo（会自动生成 trace 数据）
python scripts/demo_latency_tracing.py
```

### 选项 B：使用自定义 Benchmark

运行你自己的测试负载，例如：

```python
from pymilvus import connections, Collection
import numpy as np

# 连接
connections.connect(host="localhost", port="19530")

# 创建 collection 并插入数据
# ... (你的测试代码)

# Load collection（会触发 segment load，这是关键！）
collection.load()

# 执行 searches
results = collection.search(...)
```

## 四、分析 Trace 数据

### 1. 安装分析工具依赖

```bash
pip install pandas matplotlib seaborn
```

### 2. 运行分析脚本

```bash
# 从已捕获的日志文件分析
python scripts/analyze_latency_traces.py /tmp/milvus_traces/latency_trace.jsonl --output ./latency_report

# 或者从 stdin 实时分析
tail -n +1 -f /tmp/milvus_traces/latency_trace.jsonl | python scripts/analyze_latency_traces.py -
```

### 3. 查看结果

脚本会生成：

```
latency_report/
├── latency_analysis.csv              # 原始 trace 数据
├── trace_summary.csv                 # 按 trace 聚合的摘要
├── write_path_latency.png            # 写路径各阶段延迟对比图
├── segment_load_distribution.png     # Segment 加载延迟分布图（核心！）
└── segment_type_distribution.png     # Growing vs Sealed 查询分布
```

终端输出示例：

```
========================================
MILVUS LATENCY ANALYSIS REPORT
========================================

### WRITE PATH ANALYSIS (Insert/Upsert)
------------------------------------------------
serialize:
  Count:    1000
  Mean:     5.20 ms
  P95:      8.10 ms

mq_produce:
  Count:    1000
  Mean:     2.10 ms
  P95:      3.50 ms

s3_write:
  Count:    50
  Mean:     120.50 ms
  P95:      250.30 ms

### SEGMENT LOAD ANALYSIS (OPTIMIZATION TARGET)
------------------------------------------------
Segment Load from S3:
  Count:    125
  Mean:     486.20 ms    ← 这是优化目标！
  Median:   420.00 ms
  P95:      890.50 ms
  P99:      1200.00 ms

### SEARCH/QUERY PATH ANALYSIS
------------------------------------------------
Total searches: 10000
Growing segment hits: 3500 (35% - 已经很快，无需优化)
Sealed segment hits:  6500 (65% - 优化目标)

### BOTTLENECK SUMMARY & OPTIMIZATION OPPORTUNITIES
------------------------------------------------
🎯 KEY FINDINGS:

1. SEGMENT LOAD FROM S3 (Primary Optimization Target):
   - 125 segment loads observed
   - Mean latency: 486.20 ms
   - P95 latency:  890.50 ms
   💡 OPTIMIZATION: Shared memory pool can eliminate this latency
      Expected QPS improvement: ~257 ops/sec

2. 65% of searches hit sealed segments
   ✅ Optimization has clear value

📊 EXPECTED OPTIMIZATION IMPACT:
   With shared memory pool between DataNode and QueryNode:
   ✓ Eliminate S3 read latency for segment loads
   ✓ Eliminate deserialization overhead (direct memory access)
   ✓ LoadCollection/LoadPartition latency → near zero
   ✓ Sealed segment queries: latency reduction = segment_load_latency
```

## 五、解读结果并建模

### 关键指标

| 指标 | 含义 | 优化目标 |
|-----|------|---------|
| **Segment Load Mean** | 从 S3 加载 segment 的平均延迟 | **核心！** 这就是共享内存池要消除的延迟 |
| **Segment Load Count** | 触发了多少次 segment load | 频率越高，优化收益越大 |
| **Sealed Segment Hits** | 查询命中 sealed segment 的次数 | 占比越高，优化适用范围越广 |
| **Growing Segment Hits** | 查询命中 growing segment 的次数 | 这部分已经很快，无需优化 |
| **S3 Write Latency** | DataNode 写 S3 的延迟 | 可以与共享内存 push 并行 |

### 优化收益计算公式

假设分析结果显示：
- Segment load 平均延迟：`L_s3 = 500ms`
- 共享内存池加载延迟：`L_mem = 10ms`（假设）
- 每秒触发 segment load 次数：`N = 100`

**延迟降低**：
```
ΔL = L_s3 - L_mem = 500 - 10 = 490ms (98% 降低)
```

**QPS 提升**：
```
QPS_before = 1000 / L_s3 = 1000 / 500 = 2 ops/sec/load
QPS_after  = 1000 / L_mem = 1000 / 10 = 100 ops/sec/load
QPS_gain   = (QPS_after - QPS_before) × N
           = (100 - 2) × 100
           = 9800 ops/sec
```

**吞吐量提升**：
```
Throughput_increase = L_s3 / L_mem = 500 / 10 = 50x
```

### 判断是否值得优化

✅ **优化价值高**，如果：
- Segment load 延迟 > 100ms
- Sealed segment 查询占比 > 30%
- 频繁触发 LoadCollection/LoadPartition

⚠️ **优化价值低**，如果：
- 大部分查询都是 growing segment（已经在内存）
- Segment load 很少触发
- 延迟瓶颈在其他地方（如网络、计算）

## 六、常见问题

### Q1: 没有生成 trace 文件？

**检查**：
```bash
# 确认环境变量已设置
echo $MILVUS_LATENCY_TRACE_ENABLED
echo $MILVUS_LATENCY_TRACE_OUTPUT

# 检查文件权限
ls -la /tmp/milvus_traces/

# 查看 Milvus 日志确认 tracer 初始化
grep "Initializing latency tracer" /path/to/milvus.log
```

### Q2: Trace 文件为空或数据很少？

**原因**：
- Milvus 未执行触发打点的操作（Insert/Search/Load）
- Tracer 未初始化成功

**解决**：
- 运行 demo 脚本验证：`python scripts/demo_latency_tracing.py`
- 确保执行了 LoadCollection（这会触发 segment load）

### Q3: 分析脚本报错？

**常见错误**：
```bash
# 缺少依赖
pip install pandas matplotlib seaborn

# JSONL 格式错误
# 检查 trace 文件每行是否都是有效的 JSON
head -n 1 /tmp/milvus_traces/latency_trace.jsonl | jq .
```

### Q4: 如何只追踪特定操作？

当前版本追踪所有操作。如需采样，可以修改 `tracer.GenerateTraceID()` 添加采样逻辑：

```go
func GenerateTraceID() string {
    // 10% 采样
    if rand.Float64() > 0.1 {
        return ""  // 空 trace ID 表示不追踪
    }
    return uuid.New().String()
}
```

## 七、下一步

1. **收集 Baseline 数据**
   ```bash
   # 运行足够长的测试（至少 10 分钟）
   # 确保覆盖各种场景：Insert、Search、Load
   ```

2. **分析瓶颈**
   ```bash
   python scripts/analyze_latency_traces.py baseline.jsonl -o baseline_report
   ```

3. **设计共享内存池方案**（基于分析结果）
   - 如果 segment load 延迟 > 200ms → 高优先级优化
   - 如果 sealed segment 占比 > 50% → 优化适用范围广

4. **实施优化后再次测量**
   ```bash
   python scripts/analyze_latency_traces.py optimized.jsonl -o optimized_report
   # 对比两份报告
   ```

## 八、代码修改总结

### 已修改的文件（5个）

1. `internal/proxy/task_insert.go`
   - 添加 trace ID 生成
   - 打点：serialize、mq_produce

2. `internal/datanode/flow_graph_write_node.go`
   - 打点：consume_lag、datanode_process

3. `internal/datanode/syncmgr/task.go`
   - 打点：s3_write

4. `internal/querynodev2/delegator/delegator.go`
   - 添加 trace ID 生成
   - 打点：route、segment_stats

5. `internal/querynodev2/segments/segment_loader.go`
   - 打点：total_load（**核心优化目标**）

### 新增的文件（6个）

1. `pkg/tracer/latency_tracer.go` - 追踪框架
2. `pkg/tracer/init.go` - 初始化逻辑
3. `scripts/analyze_latency_traces.py` - 分析脚本
4. `scripts/demo_latency_tracing.py` - Demo 脚本
5. `scripts/run_milvus_with_tracing.sh` - 启动脚本
6. `docs/LATENCY_ANALYSIS_README.md` - 详细文档

### 编译确认

```bash
# 确保可以编译
cd D:\project\claude\c_qps2.4.5\milvus
make milvus

# 如果有编译错误，检查 import 语句
```

## 九、联系和支持

遇到问题请检查：
1. 环境变量是否正确设置
2. Trace 文件是否生成
3. 是否执行了触发打点的操作（特别是 LoadCollection）

Good luck! 🚀
