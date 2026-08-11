# 裸机 Standalone 部署 - 检查清单

> **本工具专为裸机 Standalone 部署设计和测试**

---

## ✅ 部署方式确认

- ✅ **支持**：裸机 Standalone 部署（直接运行 `./bin/milvus run standalone`）
- ✅ **支持**：本地编译的二进制文件
- ⚠️ **部分支持**：Docker 部署（需要手动配置环境变量）
- ❌ **不支持**：集群模式（Distributed mode）- 需要单独配置每个组件

---

## 🚀 裸机 Standalone 快速启动

### 前提条件

```bash
# 1. 已编译 Milvus
cd D:\project\claude\c_qps2.4.5\milvus
make milvus

# 2. 确认二进制文件存在
ls -lh ./bin/milvus

# 3. 确认依赖服务已启动（etcd, MinIO/S3, Pulsar/Kafka）
```

### 启动方式 1：手动启动（完全控制）

```bash
# 设置环境变量
export MILVUS_LATENCY_TRACE_ENABLED=true
export MILVUS_LATENCY_TRACE_OUTPUT=/tmp/milvus_traces/latency_trace.jsonl
mkdir -p /tmp/milvus_traces

# 启动 Milvus standalone
./bin/milvus run standalone

# 或后台运行
nohup ./bin/milvus run standalone > /tmp/milvus.log 2>&1 &

# 查看日志
tail -f /tmp/milvus.log
```

### 启动方式 2：使用脚本（推荐）

```bash
# 一键启动（会自动设置环境变量、检查状态、等待就绪）
chmod +x scripts/run_milvus_with_tracing.sh
./scripts/run_milvus_with_tracing.sh
```

脚本功能：
- ✓ 自动设置环境变量
- ✓ 检查 ./bin/milvus 是否存在
- ✓ 检查是否已有 Milvus 运行
- ✓ 自动清理旧的 trace 文件
- ✓ 后台启动 Milvus
- ✓ 等待服务就绪（检查健康状态）
- ✓ 显示 PID、日志位置、下一步操作

---

## 📋 验证 Tracing 是否启用

### 1. 检查环境变量

```bash
echo $MILVUS_LATENCY_TRACE_ENABLED
# 应该输出: true

echo $MILVUS_LATENCY_TRACE_OUTPUT
# 应该输出: /tmp/milvus_traces/latency_trace.jsonl
```

### 2. 检查 Milvus 是否运行

```bash
# 检查进程
ps aux | grep "milvus run standalone"

# 检查端口
netstat -tuln | grep 19530

# 检查健康状态
curl http://localhost:9091/healthz
```

### 3. 检查 trace 文件

```bash
# 运行一些操作后，检查 trace 文件是否生成
ls -lh /tmp/milvus_traces/latency_trace.jsonl

# 查看前几行
head -5 /tmp/milvus_traces/latency_trace.jsonl
```

---

## 🔧 常见问题排查

### ❌ 问题 1：trace 文件未生成

**检查**：
```bash
# 1. 确认环境变量在 Milvus 进程中生效
ps e $(pgrep -f "milvus run standalone") | grep MILVUS_LATENCY_TRACE

# 2. 检查文件权限
ls -la /tmp/milvus_traces/

# 3. 检查 Milvus 日志是否有错误
grep -i "latency tracer" /tmp/milvus.log
grep -i "trace" /tmp/milvus.log
```

**解决方案**：
```bash
# 重启 Milvus，确保环境变量生效
pkill -f "milvus run standalone"
export MILVUS_LATENCY_TRACE_ENABLED=true
export MILVUS_LATENCY_TRACE_OUTPUT=/tmp/milvus_traces/latency_trace.jsonl
./bin/milvus run standalone
```

---

### ❌ 问题 2：trace 文件为空

**原因**：没有执行触发打点的操作

**解决方案**：
```bash
# 运行 demo 脚本（会自动触发 Insert、Load、Search）
python scripts/demo_latency_tracing.py

# 然后检查 trace 文件
wc -l /tmp/milvus_traces/latency_trace.jsonl
```

---

### ❌ 问题 3：Milvus 启动失败

**检查依赖服务**：
```bash
# etcd
curl http://localhost:2379/health

# MinIO/S3
curl http://localhost:9000/minio/health/live

# Pulsar (如果使用)
curl http://localhost:8080/admin/v2/brokers/health
```

**查看详细日志**：
```bash
tail -100 /tmp/milvus.log
```

---

## 🧪 测试流程

### 完整测试（5分钟）

```bash
# 1. 编译
cd D:\project\claude\c_qps2.4.5\milvus
make milvus

# 2. 启动 Milvus（启用 tracing）
./scripts/run_milvus_with_tracing.sh

# 3. 运行 demo
pip install pymilvus pandas matplotlib seaborn
python scripts/demo_latency_tracing.py

# 4. 验证 trace 文件
ls -lh /tmp/milvus_traces/latency_trace.jsonl
head -10 /tmp/milvus_traces/latency_trace.jsonl | jq .

# 5. 分析结果
python scripts/analyze_latency_traces.py \
    /tmp/milvus_traces/latency_trace.jsonl \
    -o ./latency_report

# 6. 查看报告
ls latency_report/
cat latency_report/trace_summary.csv
```

---

## 🎯 关键文件位置（裸机部署）

| 文件 | 默认位置 |
|-----|---------|
| Milvus 二进制 | `./bin/milvus` |
| Milvus 日志 | `/tmp/milvus.log` (或由脚本指定) |
| Trace 输出 | `/tmp/milvus_traces/latency_trace.jsonl` |
| 分析报告 | `./latency_report/` |
| 启动脚本 | `./scripts/run_milvus_with_tracing.sh` |

---

## 📝 停止和清理

### 停止 Milvus

```bash
# 方式 1: 使用 PID（由启动脚本显示）
kill <PID>

# 方式 2: 使用 pkill
pkill -f "milvus run standalone"

# 方式 3: 强制停止
pkill -9 -f "milvus run standalone"
```

### 清理数据

```bash
# 清理 trace 文件
rm -rf /tmp/milvus_traces/

# 清理分析报告
rm -rf ./latency_report/

# 清理 Milvus 数据（谨慎！）
rm -rf /var/lib/milvus/
```

---

## ✅ 检查清单总结

部署前：
- [ ] 确认是裸机 Standalone 部署
- [ ] 确认已编译 Milvus (`./bin/milvus` 存在)
- [ ] 确认依赖服务已启动（etcd, MinIO, Pulsar/Kafka）

启动时：
- [ ] 设置环境变量 `MILVUS_LATENCY_TRACE_ENABLED=true`
- [ ] 设置环境变量 `MILVUS_LATENCY_TRACE_OUTPUT`
- [ ] 创建 trace 输出目录 `/tmp/milvus_traces`
- [ ] 启动 Milvus standalone

验证：
- [ ] Milvus 进程正在运行
- [ ] 端口 19530 可访问
- [ ] Health check 返回成功
- [ ] Trace 文件已创建

测试：
- [ ] 运行 demo 或 benchmark
- [ ] Trace 文件有数据
- [ ] 分析脚本运行成功
- [ ] 生成了报告和图表

---

**所有脚本和文档都已针对裸机 Standalone 部署优化！** ✅
