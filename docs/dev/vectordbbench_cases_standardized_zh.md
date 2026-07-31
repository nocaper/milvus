# VectorDBBench Benchmark Cases 标准化说明

> 这份文档按统一格式整理 VectorDBBench 的 benchmark case。
> 每个 case 类型都使用相同结构：测试目标、测试任务、测试流程、主要指标、用途、不能说明的问题、关注点。

## 1. 统一字段说明

| 字段 | 含义 |
| --- | --- |
| 测试目标 | 这个 case 想回答什么问题 |
| 测试任务 | benchmark 实际会做哪些动作 |
| 测试流程 | 从准备到输出结果的执行步骤 |
| 主要指标 | 应该重点看的结果字段 |
| 用途 | 这个 case 适合用在哪类评估 |
| 不能说明的问题 | 这个 case 的边界，不能拿它证明什么 |
| 关注点 | 结果异常时优先看哪些方向 |

## 2. Case 总览

| Case 类型 | 典型 CaseType | 核心问题 |
| --- | --- | --- |
| Capacity - Large Dim | `CapacityDim960` | 大维向量最多能稳定装载多少 |
| Capacity - Small Dim | `CapacityDim128` | 小维高条目数最多能稳定装载多少 |
| Search - XLarge Dataset | `Performance768D100M` | 100M 级超大规模向量搜索能力 |
| Search - Large Dataset | `Performance768D10M`, `Performance1536D5M`, `Performance1024D10M` | 5M 到 10M 级生产规模搜索能力 |
| Search - Medium Dataset | `Performance768D1M`, `Performance1536D500K`, `Performance1024D1M` | 500K 到 1M 级常规规模搜索能力 |
| Search - Small Dataset | `Performance1536D50K` | 小规模高维 smoke test 和开发验证 |
| Int Filter Search | `Performance768D10M1P`, `Performance768D10M99P`, `NewIntFilterPerformanceCase` | 整数标量过滤叠加向量搜索后的表现 |
| Label Filter Search | `LabelFilterPerformanceCase` | 标签/类别字段过滤叠加向量搜索后的表现 |
| Streaming Search | `StreamingPerformanceCase` | 持续写入期间搜索是否稳定 |
| Streaming Custom Dataset | `StreamingCustomDataset` | 真实数据持续写入期间搜索是否稳定 |
| Custom Dataset Performance | `PerformanceCustomDataset` | 用户自定义数据集上的搜索性能 |
| Cloud Insert | `CloudInsertCase` | insert 返回后多久 searchable / indexed |
| Cloud Payload Search | `CloudPayloadSearchCase` | 返回 payload 变大后搜索性能变化 |
| Cloud Multi-Tenant Search | `CloudMultiTenantSearchCase` | 多租户隔离和路由下的搜索表现 |
| Cloud Cold Latency | `CloudColdLatencyCase` | 冷态首查延迟 |
| Full Text Search Performance | `FTSBm25Performance` | BM25 全文检索性能 |

## 3. Capacity - Large Dim

**测试目标**

- 测大维向量场景下数据库的最大稳定装载能力。
- 观察高维向量对内存、索引体积、磁盘和构建任务的压力。

**测试任务**

- 使用 GIST 类 960D 数据持续插入。
- 按批次增加数据量。
- 观察写入、索引构建、加载或资源耗尽时的失败边界。

**测试流程**

1. 创建目标 collection。
2. 批量插入 960D 向量。
3. 周期性执行 flush / index / load / optimize。
4. 继续追加数据。
5. 达到数据集上限或失败阈值后停止。
6. 输出最大成功装载数量和失败原因。

**主要指标**

- 最大成功装载行数。
- load duration。
- 最后成功批次。
- 失败次数。
- 失败原因。

**用途**

- 评估大维向量容量边界。
- 对比不同后端在高维场景下的存储和索引承载能力。
- 验证系统是否能优雅处理 OOM、写入拒绝、索引失败。

**不能说明的问题**

- 不能说明搜索 QPS。
- 不能说明小维高条目数场景。
- 不能直接说明业务查询延迟。

**关注点**

- 内存峰值。
- 索引文件大小。
- 对象存储或磁盘写入。
- 索引构建失败。
- QueryNode / search node load 后内存占用。

## 4. Capacity - Small Dim

**测试目标**

- 测小维向量下的高条目数容量。
- 观察大量行数、segment、主键和元数据管理带来的压力。

**测试任务**

- 使用 SIFT 类 128D 数据持续写入。
- 通过更多条目而不是更大单条向量压数据库容量。

**测试流程**

1. 创建 collection。
2. 批量写入 128D 向量。
3. 等待必要的持久化和可见性处理。
4. 持续写入直到完成或失败。
5. 输出最大成功插入数量。

**主要指标**

- 最大成功插入数量。
- load duration。
- insert rows/s。
- 失败次数。
- 失败原因。

**用途**

- 评估高条目数容量。
- 观察 segment 数量、主键索引、元数据开销。
- 和 Large Dim case 对比“维度压力”和“行数压力”。

**不能说明的问题**

- 不能代表 768D / 1536D embedding 生产场景。
- 不能代表 search recall。
- 不能代表 payload 返回成本。

**关注点**

- segment 数量。
- 元数据膨胀。
- 小对象数量。
- flush / compaction 频率。
- 主键索引和 row count 管理。

## 5. Search - XLarge Dataset

**测试目标**

- 测 100M 级超大规模向量检索能力。
- 观察大规模索引、分片、加载、调度和并发查询能力。

**测试任务**

- 使用 LAION 100M / 768D 数据。
- 完整执行 load、optimize、serial search、concurrent search。
- 计算 recall、延迟和 QPS。

**测试流程**

1. 准备 train vectors、query vectors、ground truth。
2. 批量插入 100M 向量。
3. 构建索引并等待 ready。
4. 执行 serial search，确认 recall。
5. 执行 concurrent search，逐步增加并发。
6. 输出最大稳定 QPS 和尾延迟。

**主要指标**

- load duration。
- optimize duration。
- recall。
- serial p95 / p99 latency。
- concurrent QPS。
- concurrent p95 / p99 latency。
- failed requests。

**用途**

- 验证超大规模生产能力。
- 对比分布式架构、索引构建、查询调度能力。
- 评估 100M 级数据是否可用。

**不能说明的问题**

- 不能代表小数据集低延迟体验。
- 不能代表带 filter 或 payload 的实际业务成本。
- 不能代表持续写入下的搜索稳定性。

**关注点**

- 索引构建时间。
- segment / shard 分布。
- 查询调度。
- 内存和对象存储 IO。
- p99 是否随并发急剧上升。

## 6. Search - Large Dataset

**测试目标**

- 测 5M 到 10M 级生产规模向量搜索能力。
- 比较不同维度、不同 embedding 分布下的性能差异。

**测试任务**

- 跑 `Performance768D10M`、`Performance1536D5M`、`Performance1024D10M` 等 case。
- 评估 768D、1024D、1536D 在大规模下的 recall、latency、QPS。

**测试流程**

1. 选择数据集和维度。
2. 插入 5M 或 10M 级向量。
3. 构建索引或执行 optimize。
4. 串行搜索验证 recall。
5. 并发搜索测吞吐和尾延迟。
6. 对比不同维度和规模的性能变化。

**主要指标**

- load duration。
- optimize duration。
- recall。
- serial p99 latency。
- max QPS。
- concurrent p99 latency。

**用途**

- 评估常见生产规模。
- 对比 Cohere / OpenAI / Bioasq 等 embedding 数据分布。
- 判断维度增长带来的成本。

**不能说明的问题**

- 不能说明 100M 级极限能力。
- 不能说明 metadata filter 下的性能。
- 不能说明 cold latency。

**关注点**

- 维度变化导致的内存和计算成本。
- 数据规模增加后的 QPS 下降曲线。
- recall 是否随索引参数变化。
- index build 是否成为上线瓶颈。

## 7. Search - Medium Dataset

**测试目标**

- 测 500K 到 1M 级中等规模向量搜索。
- 建立常规性能基线。

**测试任务**

- 跑 `Performance768D1M`、`Performance1536D500K`、`Performance1024D1M`。
- 检查不同后端在中等规模下的基础 recall、latency、QPS。

**测试流程**

1. 准备对应数据集。
2. 插入 500K 或 1M 级向量。
3. 建索引并 ready。
4. 跑 serial search。
5. 跑 concurrent search。
6. 输出基线结果。

**主要指标**

- recall。
- serial latency。
- concurrent QPS。
- load duration。
- optimize duration。

**用途**

- 本地或小集群回归测试。
- 参数调优。
- 版本间性能对比。

**不能说明的问题**

- 不能代表大规模分布式压力。
- 不能代表最大容量。
- 不能代表强过滤或 payload 成本。

**关注点**

- recall 是否稳定。
- 单查询延迟是否异常。
- 并发曲线是否平滑。
- 客户端是否成为瓶颈。

## 8. Search - Small Dataset

**测试目标**

- 快速验证高维向量搜索流程是否可用。
- 用小规模数据做 smoke test。

**测试任务**

- 跑 `Performance1536D50K`。
- 验证连接、schema、insert、index、search、recall 计算链路。

**测试流程**

1. 准备 OpenAI 50K / 1536D 数据。
2. 插入数据。
3. 建索引。
4. 跑 serial search。
5. 可选跑小并发 concurrent search。

**主要指标**

- 是否成功完成。
- recall。
- serial latency。
- 基础 QPS。

**用途**

- 本地开发验证。
- CI smoke test。
- 快速排查 client 或环境问题。

**不能说明的问题**

- 不能代表生产性能。
- 不能代表 5M / 10M / 100M 级压力。
- 不能代表索引构建瓶颈。

**关注点**

- schema 是否正确。
- metric 是否和数据集匹配。
- ground truth 是否加载成功。
- 小数据集下结果波动较大。

## 9. Int Filter Search

**测试目标**

- 测整数标量过滤和向量搜索结合后的性能。
- 观察 filter selectivity 对 recall、latency、QPS 的影响。

**测试任务**

- 给向量数据附加整数标量字段。
- 构造 int filter 表达式。
- 执行 filtered vector search。
- 对比 1% 和 99% 命中比例。

**测试流程**

1. 准备向量数据和整数标量字段。
2. 插入向量和 scalar 字段。
3. 构建向量索引和可选 scalar index。
4. 生成 filter 表达式。
5. 跑 filtered serial search。
6. 使用 filtered ground truth 计算 recall。
7. 跑 filtered concurrent search。
8. 输出不同 filter rate 下的结果。

**主要指标**

- filtered recall。
- filtered serial p99 latency。
- filtered QPS。
- filter 1% / 99% 的性能差异。
- failed requests。

**用途**

- 评估时间戳、范围、状态码、分桶 ID 等业务过滤。
- 检查 scalar filter 是否能有效下推。
- 评估 ANN 和 scalar executor 组合成本。

**不能说明的问题**

- 不能代表 label/string filter 成本。
- 不能代表无 filter 的纯向量性能。
- 不能说明权限系统或业务 ACL 的完整成本。

**关注点**

- 1% filter 是否能快速缩小候选集。
- 99% filter 是否接近无 filter 性能。
- filtered ground truth 是否正确。
- scalar index 是否命中。
- bitset 构建是否过慢。

## 10. Label Filter Search

**测试目标**

- 测标签/类别型 metadata filter 下的向量搜索性能。
- 模拟 tenant、category、source、language、tag 等业务字段过滤。

**测试任务**

- 给向量数据附加 label 字段。
- 按 label percentage 控制命中比例。
- 执行带 label filter 的 serial 和 concurrent search。

**测试流程**

1. 选择数据集和 label percentage。
2. 生成或加载 label 字段。
3. 插入向量和 label。
4. 构建向量索引和可选 label index。
5. 跑 label filtered serial search。
6. 计算 filtered recall。
7. 跑 label filtered concurrent search。

**主要指标**

- label filtered recall。
- serial p99 latency。
- concurrent QPS。
- label percentage 对性能的影响。
- payload 或 metadata 返回成本。

**用途**

- 评估真实 metadata filter。
- 模拟多类别、多租户、业务标签检索。
- 验证 label 字段索引和 query planner。

**不能说明的问题**

- 不能完全代表 int range filter。
- 不能代表复杂权限系统。
- 不能代表 join 或跨 collection 查询。

**关注点**

- label cardinality。
- label 分布是否均匀。
- filter 是否下推到向量搜索前。
- 字符串比较和字典编码成本。
- 高选择性 label 下的尾延迟。

## 11. Streaming Search

**测试目标**

- 测持续写入期间搜索是否稳定。
- 观察 ingest、flush、index、compaction 对 search 的影响。

**测试任务**

- 按固定 insert rate 持续写入。
- 在写入进度达到指定阶段时执行搜索。
- 记录写入期间和写入后的搜索性能。

**测试流程**

1. 创建 collection。
2. 启动 insert runner。
3. 按 insert_rate 写入向量。
4. 在 search_stages 指定阶段启动搜索。
5. 采集写入期间 QPS、latency、失败数。
6. 写入完成后可选 optimize。
7. 再采集写入后的稳定态搜索结果。

**主要指标**

- insert rows/s。
- 写入失败数。
- 写入期间 QPS。
- 写入期间 p95 / p99 latency。
- 写入后 QPS。
- 写入后 p95 / p99 latency。

**用途**

- 模拟线上一边导入一边查询。
- 检查读写资源隔离。
- 评估 growing segment、flush、compaction 对搜索的影响。

**不能说明的问题**

- 不能代表纯只读最大 QPS。
- 不能代表完全离线批量导入能力。
- 不能代表冷启动首查延迟。

**关注点**

- 写入期间 p99 是否抖动。
- insert_rate 是否达到目标。
- 写入是否造成搜索 timeout。
- 后台 index / compaction 是否抢资源。

## 12. Streaming Custom Dataset

**测试目标**

- 在用户真实数据集上测试持续写入期间搜索稳定性。

**测试任务**

- 使用用户提供的 parquet 数据。
- 按 streaming 模式持续写入。
- 同时执行真实 query。

**测试流程**

1. 准备自定义 train/query/ground truth。
2. 注册 custom dataset。
3. 创建 collection。
4. 持续写入自定义向量。
5. 在指定写入阶段执行搜索。
6. 输出读写并发指标。

**主要指标**

- insert rows/s。
- 写入期间 QPS。
- 写入期间 p99 latency。
- custom recall。
- failed requests。

**用途**

- 验证真实业务数据和写入节奏。
- 比公开数据集更贴近生产。

**不能说明的问题**

- 不能和公开 leaderboard 直接对齐。
- 不能说明其他数据分布的表现。

**关注点**

- parquet schema。
- ground truth 生成方式。
- metric 是否匹配。
- 自定义数据分布是否偏斜。

## 13. Custom Dataset Performance

**测试目标**

- 测用户自定义数据集上的标准搜索性能。

**测试任务**

- 加载用户自己的 train/query/ground truth。
- 执行标准 load、optimize、serial search、concurrent search。

**测试流程**

1. 准备 parquet 数据。
2. 准备 query 和 ground truth。
3. 注册 dataset。
4. 选择 `PerformanceCustomDataset`。
5. 执行 load。
6. 执行 optimize。
7. 执行 serial search。
8. 执行 concurrent search。
9. 输出标准指标。

**主要指标**

- load duration。
- optimize duration。
- custom recall。
- serial p99 latency。
- concurrent QPS。
- failed requests。

**用途**

- 评估真实业务 embedding。
- 对比公开数据集和业务数据的性能差异。
- 验证特定维度、metric、数据分布。

**不能说明的问题**

- 不能代表公开 leaderboard。
- 不能说明其他业务数据。
- ground truth 错误时不能说明数据库搜索质量。

**关注点**

- train/query 维度一致。
- metric 和 ground truth 一致。
- id 类型和 schema 一致。
- query 数量足够。

## 14. Cloud Insert

**测试目标**

- 测 insert 返回、数据可搜索、索引完成之间的时间差。

**测试任务**

- 并发插入数据。
- 记录 insert 完成时间。
- 轮询直到数据 searchable。
- 继续轮询直到 indexed / optimize ready。

**测试流程**

1. 创建 collection。
2. 启动 concurrent insert runner。
3. 等待 insert 完成。
4. 记录 insert duration。
5. 轮询 search，直到新数据可被命中。
6. 轮询 index/ready 状态。
7. 输出 searchable delay 和 indexed delay。

**主要指标**

- inserted_count。
- insert_duration。
- insert_completion_seconds。
- searchable_after_insert_seconds。
- indexed_after_searchable_seconds。

**用途**

- 评估云服务写入可见性。
- 判断实时写入后多久能服务搜索。
- 评估回填、迁移、批量导入体验。

**不能说明的问题**

- 不能代表稳定态搜索最大 QPS。
- 不能代表 payload 返回成本。
- 不能代表多租户性能。

**关注点**

- insert 快但 searchable 慢。
- searchable 快但 indexed 慢。
- consistency、flush、load、index queue。
- 后台任务积压。

## 15. Cloud Payload Search

**测试目标**

- 测不同返回 payload 对搜索性能的影响。

**测试任务**

- 在相同数据和查询下切换 payload profile。
- 比较 ids only、scalar metadata、vector/text payload 的性能差异。

**测试流程**

1. 选择数据集。
2. 选择 payload_profile。
3. 可选叠加 filter。
4. 执行 load 和 optimize。
5. 跑 serial search。
6. 跑 concurrent search。
7. 对比不同 payload 的 QPS 和延迟。

**主要指标**

- recall。
- QPS。
- p95 / p99 latency。
- payload_estimated_bytes_per_query。
- text/vector payload 相对 ids only 的 QPS drop。

**用途**

- 评估真实应用返回字段成本。
- 区分搜索计算瓶颈和响应体瓶颈。
- 对比 metadata/vector/text 返回开销。

**不能说明的问题**

- 不能单独代表 ANN 搜索能力。
- 不能代表 insert 可见性。
- 不能代表 cold latency。

**关注点**

- output fields 读取。
- 序列化。
- 网络传输。
- client 响应解析。
- 大 payload 下 p99。

## 16. Cloud Multi-Tenant Search

**测试目标**

- 测多租户或 namespace 隔离下的搜索性能。

**测试任务**

- 将数据按 tenant 分配。
- 查询时指定 tenant。
- 测 tenant-aware search 的 recall、QPS、latency。

**测试流程**

1. 设置 tenant_count。
2. 写入时分配 tenant id。
3. 构建索引并 ready。
4. 查询时携带 tenant 条件或路由。
5. 跑 serial search。
6. 跑 concurrent search。
7. 输出 tenant-aware 指标。

**主要指标**

- tenant filtered recall。
- tenant-aware QPS。
- p95 / p99 latency。
- tenant 间延迟差异。
- failed requests。

**用途**

- 评估 SaaS 多租户场景。
- 检查 namespace / partition / metadata filter 路由成本。
- 判断 tenant 隔离是否影响尾延迟。

**不能说明的问题**

- 不能代表单租户纯搜索上限。
- 不能说明业务权限系统全部成本。
- 不能代表 cold start。

**关注点**

- tenant 分布是否均匀。
- tenant filter 是否下推。
- segment / shard 是否倾斜。
- 缓存命中是否不均。

## 17. Cloud Cold Latency

**测试目标**

- 测冷态首查延迟。
- 对比 cold 和 warm 查询体验。

**测试任务**

- 在已有 collection 上跳过 drop/load。
- 直接执行 search-only。
- 记录首批查询和预热后查询延迟。

**测试流程**

1. 确认 collection 已存在。
2. 跳过 drop old。
3. 跳过 load。
4. 冷态执行 serial search。
5. 记录 first query latency。
6. 继续执行 warm pass。
7. 对比 cold / warm latency。

**主要指标**

- first query latency。
- cold p95 / p99 latency。
- warm p95 / p99 latency。
- cold-to-warm ratio。

**用途**

- 评估 serverless、云托管、低频租户首查体验。
- 发现热跑 benchmark 看不到的冷缓存问题。

**不能说明的问题**

- 不能代表稳定态最大 QPS。
- 不能代表导入或索引构建能力。
- 不能代表持续写入下的搜索稳定性。

**关注点**

- index 冷加载。
- OS page cache。
- 远端对象存储读取。
- serverless 唤醒。
- 首次连接和执行路径初始化。

## 18. Full Text Search Performance

**测试目标**

- 测 BM25 全文检索的构建、召回、延迟和吞吐。
- 验证数据库全文索引路径是否能独立支撑 text retrieval。

**测试任务**

- 使用 MS MARCO 或 HotpotQA 文本文档。
- 插入 raw text corpus。
- 构建全文索引。
- 执行 query text 搜索。
- 使用 BM25 ground truth 计算 recall。
- 对比 ids_only 和 text payload。

**测试流程**

1. 通过 `ir_datasets` 加载 text corpus 和 query。
2. 转换成 `FtsDocument(doc_id, text)` 和 `FtsQuery(query_id, text)`。
3. 下载 `neighbors.parquet` ground truth。
4. 读取 `build_manifest.json`。
5. 校验 BM25 参数、analyzer 参数、doc_count、query_count。
6. 创建全文索引。
7. 批量插入文档。
8. 执行 optimize / readiness。
9. 跑 serial BM25 search，计算 recall。
10. 跑 concurrent BM25 search，计算 QPS 和延迟。
11. 切换 `ids_only` / `text` payload 做对比。

**主要指标**

- inserted_count。
- insert_duration。
- optimize_duration。
- load_duration。
- BM25 recall。
- serial p95 / p99 latency。
- concurrent QPS。
- concurrent p95 / p99 latency。
- payload_estimated_bytes_per_query。
- ids_only 与 text payload 的 QPS 差异。

**用途**

- 评估数据库 BM25 / keyword search 能力。
- 检查 hybrid search 中全文检索层是否是瓶颈。
- 对比 analyzer、BM25 参数、payload 返回成本。
- 验证 Tantivy / inverted index / text search 路径。

**不能说明的问题**

- 不能代表 dense vector ANN 性能。
- 不能代表 semantic relevance。
- 不能代表 reranker 后的最终排序质量。
- 不能替代端到端 RAG 质量评估。
- 不能直接说明 hybrid fusion 整体质量。

**关注点**

- analyzer 是否和 manifest 一致。
- BM25 `k1` / `b` / `avgdl` 是否应用。
- doc id 是否和 ground truth 对齐。
- tokenizer、lowercase、stop words、stemming。
- text payload 是否造成序列化和网络瓶颈。
- Milvus 下重点看 Tantivy index build、BM25 查询路径、text 字段读取。

## 19. 输出结论模板

```text
Case 类型:
CaseType:
数据集:
规模:
维度或文本类型:
Metric:
TopK:
Filter / Payload / Tenant / Streaming 参数:

测试目标:

测试结果:
- Load duration:
- Optimize duration:
- Recall:
- Serial p99:
- Concurrent QPS:
- Concurrent p99:
- 失败数:

结果解读:

不能说明的问题:

后续排查或验证:
```
