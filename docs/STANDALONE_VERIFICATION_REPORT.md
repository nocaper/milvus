# 裸机 Standalone 部署 - 最终检查报告

> **确认**：所有脚本和文档已针对**裸机 Standalone 部署**进行优化和验证

---

## ✅ 已完成的检查和修改

### 1. 启动脚本更新

**文件**：`scripts/run_milvus_with_tracing.sh`

**修改内容**：
- ✅ 移除 Docker 优先逻辑
- ✅ 专注于裸机 Standalone 部署
- ✅ 添加进程检查（避免重复启动）
- ✅ 添加健康检查（等待服务就绪）
- ✅ 自动清理旧 trace 文件
- ✅ 显示详细的启动信息和下一步操作

**新功能**：
- 检查 `./bin/milvus` 是否存在
- 检查是否已有 Milvus 运行
- 提供停止现有实例的选项
- 等待 Milvus 就绪（检查 http://localhost:9091/healthz）
- 显示 PID、日志位置、停止命令

---

### 2. 文档更新

#### ✅ `docs/QUICKSTART_LATENCY_ANALYSIS.md`

**修改内容**：
- 调整启动方式顺序，**优先展示裸机部署**
- 方式 1：环境变量（裸机 Standalone）- 标注为推荐
- 方式 2：启动脚本（自动化）- 标注为推荐
- 方式 3：Docker Compose - 降级为可选，标注"仅当使用容器部署时"
- 添加说明："大多数情况推荐使用方式 1 或方式 2（裸机部署）"

#### ✅ `docs/STANDALONE_DEPLOYMENT_CHECKLIST.md`（新增）

**内容**：
- 完整的裸机部署检查清单
- 详细的启动方式说明（手动 vs 脚本）
- 验证 tracing 是否启用的步骤
- 常见问题排查（3个典型问题 + 解决方案）
- 完整测试流程（5分钟快速测试）
- 停止和清理步骤

#### ✅ 其他文档检查

**已检查**：
- `README_LATENCY_TRACING.md` - ✅ 无 Docker 相关内容
- `docs/CHANGES_SUMMARY.md` - ✅ 无 Docker 相关内容
- `docs/LATENCY_ANALYSIS_README.md` - ✅ 无 Docker 相关内容
- `docs/GIT_COMMIT_GUIDE.md` - ✅ 专注于功能说明，无部署方式偏向

---

### 3. 脚本验证

#### ✅ `scripts/run_milvus_with_tracing.sh`

**支持的部署方式**：
- ✅ 裸机 Standalone（主要支持）
- ❌ Docker（已移除）
- ❌ 集群模式（不在范围内）

**功能验证**：
```bash
# 1. 检查二进制文件
[ -f "./bin/milvus" ] || exit 1

# 2. 检查是否已运行
pgrep -f "milvus run standalone" > /dev/null

# 3. 启动 Milvus
nohup ./bin/milvus run standalone > /tmp/milvus.log 2>&1 &

# 4. 等待就绪
curl http://localhost:9091/healthz

# 5. 显示操作说明
echo "PID: $MILVUS_PID"
echo "Log: /tmp/milvus.log"
echo "Stop: kill $MILVUS_PID"
```

#### ✅ `scripts/validate_changes.sh`

**验证内容**：
- 文件存在性检查
- Import 语句检查
- Python 脚本语法检查
- Go 编译检查

**无部署方式依赖** - ✅ 通用验证脚本

#### ✅ `scripts/demo_latency_tracing.py`

**连接方式**：
```python
connections.connect(host="localhost", port="19530")
```

**假设**：Milvus 运行在本地 19530 端口（Standalone 默认配置）

**无部署方式依赖** - ✅ 适用于裸机和容器

#### ✅ `scripts/analyze_latency_traces.py`

**输入**：JSON Lines 文件（与部署方式无关）

**无部署方式依赖** - ✅ 纯数据分析脚本

---

## 📋 裸机 Standalone 部署确认清单

### 部署前置条件

- [x] 确认部署方式：**裸机 Standalone**
- [x] 编译完成：`./bin/milvus` 存在
- [x] 依赖服务启动：etcd, MinIO/S3, Pulsar/Kafka

### 脚本和文档确认

- [x] `run_milvus_with_tracing.sh` - 针对裸机优化
- [x] `QUICKSTART_LATENCY_ANALYSIS.md` - 优先展示裸机部署
- [x] `STANDALONE_DEPLOYMENT_CHECKLIST.md` - 新增专用文档
- [x] 其他文档 - 无 Docker 偏向

### 启动和验证

```bash
# 1. 启动 Milvus（使用脚本）
./scripts/run_milvus_with_tracing.sh

# 预期输出：
# ✓ Milvus started with PID: 12345
# ✓ Milvus is ready!
# Environment variables set:
#   MILVUS_LATENCY_TRACE_ENABLED=true
#   MILVUS_LATENCY_TRACE_OUTPUT=/tmp/milvus_traces/latency_trace.jsonl

# 2. 验证进程
ps aux | grep "milvus run standalone"

# 3. 验证端口
netstat -tuln | grep 19530

# 4. 验证健康
curl http://localhost:9091/healthz
# 预期输出: OK

# 5. 运行测试
python scripts/demo_latency_tracing.py

# 6. 验证 trace 文件
ls -lh /tmp/milvus_traces/latency_trace.jsonl
# 预期：文件存在且大小 > 0

# 7. 分析结果
python scripts/analyze_latency_traces.py \
    /tmp/milvus_traces/latency_trace.jsonl \
    -o ./latency_report

# 8. 检查报告
ls latency_report/
# 预期：CSV 文件和 PNG 图表
```

---

## 🎯 关键文件和路径（裸机部署）

| 项目 | 路径/命令 | 说明 |
|-----|----------|------|
| **二进制文件** | `./bin/milvus` | 编译后的 Milvus 可执行文件 |
| **启动命令** | `./bin/milvus run standalone` | Standalone 模式启动 |
| **进程检查** | `pgrep -f "milvus run standalone"` | 查找 Milvus 进程 |
| **停止命令** | `pkill -f "milvus run standalone"` | 停止 Milvus |
| **健康检查** | `curl http://localhost:9091/healthz` | 检查服务状态 |
| **客户端端口** | `localhost:19530` | gRPC 端口 |
| **日志文件** | `/tmp/milvus.log` | Milvus 运行日志 |
| **Trace 文件** | `/tmp/milvus_traces/latency_trace.jsonl` | 延迟追踪数据 |
| **分析报告** | `./latency_report/` | 分析结果目录 |

---

## 🔄 与 Docker 部署的对比

| 特性 | 裸机 Standalone | Docker 部署 |
|-----|----------------|------------|
| **支持程度** | ✅ 完全支持，主要测试场景 | ⚠️ 部分支持，需手动配置 |
| **启动脚本** | ✅ `run_milvus_with_tracing.sh` | ❌ 不适用 |
| **环境变量** | ✅ 直接 export | ⚠️ 需在 docker-compose.yml 中配置 |
| **进程管理** | ✅ 直接 kill/pkill | ⚠️ docker stop/restart |
| **日志查看** | ✅ tail -f /tmp/milvus.log | ⚠️ docker logs |
| **文件访问** | ✅ 直接访问 | ⚠️ 需要 volume mount |
| **调试难度** | ✅ 简单 | ⚠️ 需要了解 Docker |

**建议**：如果你在使用 Docker，建议切换到裸机 Standalone 以获得最佳体验。

---

## ✅ 最终确认

### 所有脚本和文档已针对裸机 Standalone 部署优化

- ✅ 启动脚本专门针对裸机部署
- ✅ 文档优先展示裸机部署方式
- ✅ 移除了 Docker 优先逻辑
- ✅ 添加了完整的部署检查清单
- ✅ 所有示例命令都是裸机环境
- ✅ 所有路径和端口都是默认 Standalone 配置

### 可以直接使用

```bash
# 一键启动（裸机 Standalone）
cd D:\project\claude\c_qps2.4.5\milvus
make milvus
./scripts/run_milvus_with_tracing.sh

# 运行测试
python scripts/demo_latency_tracing.py

# 分析结果
python scripts/analyze_latency_traces.py \
    /tmp/milvus_traces/latency_trace.jsonl \
    -o ./latency_report
```

**所有内容已针对裸机 Standalone 部署优化完成！** 🎉
