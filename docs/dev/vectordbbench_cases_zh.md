# VectorDBBench Benchmark Cases 详解

> 这份文档面向 Milvus / 向量数据库研发和性能分析场景。
> 目标是把 zilliztech/VectorDBBench 里的每类 benchmark case 讲清楚：它测什么、为什么测、实际做了哪些任务、执行流程是什么、指标应该怎么解读。
>
> 基于资料：
>
> - VectorDBBench README
> - `vectordb_bench/backend/cases.py`
> - `vectordb_bench/backend/dataset.py`
> - Cloud Leaderboard release notes
> - Full Text Search release notes
>
> 说明时间：2026-07-30。VectorDBBench 的 case 会持续演进，实际可运行列表以当前版本的 `vectordbbench <db> --help` 和源码 `CaseType` 为准。

## 1. 先理解 VectorDBBench 的 case 是什么

VectorDBBench 的 case 不是一个简单的压测脚本，而是一组固定的工作负载模板。每个 case 会定义：

- 用哪个数据集。
- 数据集规模是多少。
- 向量维度是多少。
- 距离度量是什么。
- 是否带标量字段。
- 是否带 filter。
- 是否持续写入。
- 是否只跑搜索。
- 是否测 payload 返回。
- 是否测 cold start。
- 最后采集哪些指标。

因此，读 benchmark 结果时不能只看 QPS。不同 case 的目标不一样：

- Capacity Case 看容量边界。
- Search Performance Case 看固定规模下的搜索吞吐、延迟和召回。
- Filtering Search Performance Case 看向量检索和标量过滤结合后的表现。
- Streaming Case 看写入和搜索同时发生时的稳定性。
- Custom Dataset Case 看用户真实数据集上的结果。
- Cloud Leaderboard Case 看托管云服务里的生产行为。
- Full Text Search Case 看 BM25 文本检索性能。

## 2. CaseType 总览

VectorDBBench 的 README 里仍会提到标准 benchmark 结果包含 15 个 case。这个说法主要对应早期 leaderboard 的标准矩阵。当前源码里的 `CaseType` 已经更多，新增了参数化过滤、Cloud Leaderboard、FTS 等扩展 case。

下面按类别整理常见 case。

| 类别 | 典型 CaseType | 主要测试目标 |
| --- | --- | --- |
| Capacity | `CapacityDim960`, `CapacityDim128` | 数据库最多能稳定装载多少向量 |
| Search Performance | `Performance768D100M`, `Performance768D10M`, `Performance768D1M`, `Performance1536D5M`, `Performance1536D500K`, `Performance1536D50K`, `Performance1024D10M`, `Performance1024D1M` | 固定规模向量检索的 recall、latency、QPS |
| Legacy Filtering | `Performance768D10M1P`, `Performance768D10M99P`, `Performance768D1M1P`, `Performance768D1M99P`, `Performance1536D5M1P`, `Performance1536D5M99P`, `Performance1536D500K1P`, `Performance1536D500K99P` | 固定过滤比例下的向量加标量过滤 |
| Parametric Filtering | `NewIntFilterPerformanceCase`, `LabelFilterPerformanceCase` | 可通过参数控制过滤比例和数据集 |
| Streaming | `StreamingPerformanceCase`, `StreamingCustomDataset` | 持续写入下的搜索能力 |
| Custom Dataset | `PerformanceCustomDataset` | 用户自定义 parquet 数据集上的性能 |
| Cloud | `CloudInsertCase`, `CloudPayloadSearchCase`, `CloudMultiTenantSearchCase`, `CloudColdLatencyCase` | 云服务生产行为：写入可见性、payload、多租户、冷查询 |
| FTS | `FTSBm25Performance` | BM25 全文检索性能 |

## 3. 通用 Benchmark 流程

大多数 search 类 case 共用下面的执行骨架。

### 3.1 准备阶段

**目标**

- 确保数据集、查询集、ground truth、标量字段等输入准备完整。

**做什么**

- 下载或读取数据集。
- 校验向量维度、距离度量、数据集大小。
- 准备 query vectors。
- 准备 ground truth。
- 如果是 filter case，还会准备带过滤条件的 ground truth。

**为什么重要**

- recall 必须依赖 ground truth。
- 如果 ground truth 和实际 filter 条件不一致，后面的 recall 就没有意义。

### 3.2 Drop / Load 阶段

**目标**

- 建立新的测试 collection / index 输入。
- 测装载耗时和写入吞吐。

**做什么**

- 如果没有跳过 drop，会删除旧 collection。
- 创建 schema。
- 批量插入向量。
- 如果 case 需要标量字段，会同时插入标量列。
- 等待数据库接受数据。

**常见指标**

- `Load_duration`
- 插入行数
- insert rows/s
- 插入失败次数

### 3.3 Optimize / Index 阶段

**目标**

- 等待数据库完成索引构建、flush、load、compaction 或 backend 自己定义的 optimize。

**做什么**

- 调用具体 database client 的 optimize 方法。
- 有些后端会创建向量索引。
- 有些后端会等待 segment loaded。
- 有些后端会等待 collection 可搜索。

**为什么不能跳过**

- 如果没等索引/加载完成就开始搜索，QPS、延迟、recall 都可能代表错误状态。

### 3.4 Serial Search 阶段

**目标**

- 在低并发下看单次搜索质量。

**做什么**

- 单线程或低并发执行 query。
- 计算 recall。
- 记录延迟分布。

**常见指标**

- recall
- average latency
- p95 latency
- p99 latency

**解读方式**

- serial search 更适合看召回和单次查询延迟。
- 如果 serial recall 低，concurrent QPS 再高也没有意义。

### 3.5 Concurrent Search 阶段

**目标**

- 在多并发下测吞吐上限。

**做什么**

- 以不同 concurrency 发起查询。
- 每个 concurrency 跑固定时长。
- 找到稳定可用的最大 QPS。

**常见指标**

- QPS
- max QPS
- p95 / p99 latency
- failed query count
- timeout count

**解读方式**

- concurrent search 更适合看生产吞吐。
- 需要和 recall、p99 latency 一起看，不能只看 QPS。

## 4. 数据集和规模

VectorDBBench 的 case 常用下面几类数据集。不同 case 会从这些数据集里选择固定规模。

| 数据集 | 典型规模 | 维度 | 距离度量 | 常见用途 |
| --- | --- | --- | --- | --- |
| SIFT | 500K 或小规模变体 | 128D | L2 | 小维容量、开发验证 |
| GIST | 100K | 960D | L2 | 大维容量 |
| LAION | 100M | 768D | L2 | 超大规模搜索 |
| Cohere | 100K / 1M / 10M | 768D | Cosine | 常规 768D embedding 搜索和 cloud/search case |
| OpenAI | 50K / 500K / 5M | 1536D | Cosine | 高维 embedding 搜索和过滤 |
| Bioasq | 1M / 10M | 1024D | Cosine | 1024D 医学文本 embedding 搜索 |
| MS MARCO | 100K / 1M / 8.84M | text | BM25 | FTS 文本检索 |
| HotpotQA | 100K / 1M / 5.23M | text | BM25 | FTS 文本检索 |

## 5. Capacity Case

Capacity Case 的问题很直接：这个系统在给定数据形态下最多能装多少。

它不是为了测搜索 QPS，而是为了找到容量、写入、索引或资源调度的边界。

### 5.1 CapacityDim960

**测试目标**

- 测大维向量场景下的容量上限。
- 检查大维向量带来的内存、索引体积、磁盘和构建压力。

**使用数据**

- GIST 类数据。
- 维度 960D。
- 距离度量 L2。

**测试任务**

- 持续把 960D 向量写入数据库。
- 按固定批次逐步增加数据量。
- 如果数据库开始拒绝写入、构建失败、OOM 或连续失败，就停止。

**测试流程**

1. 创建目标 collection。
2. 按批次插入 GIST 960D 向量。
3. 视后端能力执行 flush / index / load。
4. 继续追加数据。
5. 当达到全部数据、资源限制或失败阈值时结束。
6. 记录最终成功装载数量。

**主要指标**

- 最大成功装载行数。
- 装载耗时。
- 最后一次成功批次。
- 失败原因。

**它能说明什么**

- 大维度下的容量边界。
- 大向量对内存和索引构建的压力。
- 系统是否会优雅失败。

**它不能说明什么**

- 不能代表搜索 QPS。
- 不能代表小维高条目数场景。

### 5.2 CapacityDim128

**测试目标**

- 测小维向量场景下的高条目容量。
- 检查“更多向量条数”而不是“更大单条向量”的压力。

**使用数据**

- SIFT 类数据。
- 维度 128D。
- 距离度量 L2。

**测试任务**

- 持续插入 128D 向量。
- 观察系统能承载多少条目。

**测试流程**

1. 创建 collection。
2. 持续批量写入 128D 向量。
3. 周期性等待数据库完成必要的写入可见性处理。
4. 直到数据写完或达到失败边界。
5. 输出最大装载数量。

**主要指标**

- 最大成功插入数量。
- 写入耗时。
- 失败类型。

**它能说明什么**

- 小维、高条目场景下的容量效率。
- 元数据、主键、segment 数量、索引文件数量带来的额外开销。

**容易误读的点**

- CapacityDim128 成绩高，不代表 768D 或 1536D 的生产搜索一定强。
- 这个 case 的核心是容量，不是召回和延迟。

## 6. Search Performance Case

Search Performance Case 是最核心的标准搜索压测。它回答的问题是：在固定数据规模、固定 embedding 维度、固定距离度量下，这个数据库能以什么 recall、什么延迟、什么 QPS 完成近邻搜索。

### 6.1 共同测试目标

- 验证向量检索正确性。
- 测索引构建和数据加载耗时。
- 测单查询延迟。
- 测并发吞吐上限。
- 比较不同向量维度、数据规模和后端实现之间的差异。

### 6.2 共同测试任务

1. 加载 train vectors。
2. 建 schema 和 collection。
3. 插入向量。
4. 构建索引或执行 optimize。
5. 加载 query vectors。
6. 按 ground truth 计算 recall。
7. 跑 serial search。
8. 跑 concurrent search。
9. 输出 load、optimize、recall、latency、QPS。

### 6.3 Performance768D100M

**测试目标**

- 测 100M 级超大规模 768D 向量搜索。

**使用数据**

- LAION 100M。
- 768D。
- L2。

**测试任务**

- 装载 1 亿条向量。
- 构建索引。
- 搜索 topK。
- 评估 recall、serial latency、concurrent QPS。

**测试流程**

1. 准备 LAION 100M train/query/ground truth。
2. 批量插入 100M vectors。
3. 等待索引或服务端 optimize 完成。
4. 执行 serial search，先确认 recall。
5. 逐级增加 concurrency，执行 concurrent search。
6. 找到最大稳定 QPS。

**重点压力**

- 大规模索引构建。
- 分片或 segment 管理。
- 查询调度。
- 内存和磁盘 IO。
- 集群资源利用率。

**结果解读**

- 这是超大规模生产能力的核心 case。
- 如果这个 case load 很慢，说明导入和索引构建链路可能是瓶颈。
- 如果 recall 合格但 QPS 低，说明搜索并发或调度可能是瓶颈。
- 如果 QPS 高但 p99 非常高，说明尾延迟不稳定。

### 6.4 Performance768D10M

**测试目标**

- 测 10M 级 768D embedding 搜索。

**使用数据**

- Cohere 10M。
- 768D。
- Cosine。

**测试任务**

- 跑常见生产规模的语义向量搜索。
- 评估 768D embedding 在千万级数据上的性能。

**测试流程**

1. 载入 Cohere 10M。
2. 创建 collection 和向量索引。
3. 执行 optimize。
4. 跑 serial search 计算 recall。
5. 跑 concurrent search 计算 QPS 和延迟。

**重点压力**

- 真实 embedding 分布。
- Cosine 相似度搜索。
- 中大型 collection 的吞吐。

**结果解读**

- 适合评估多数 RAG / semantic search 生产场景。
- 比 100M case 更容易日常复测。

### 6.5 Performance768D1M

**测试目标**

- 测 1M 级 768D 搜索。

**使用数据**

- Cohere 1M。
- 768D。
- Cosine。

**测试任务**

- 在中等规模下建立性能基线。
- 用于快速比较不同数据库或不同参数配置。

**测试流程**

1. 载入 Cohere 1M。
2. 插入、建索引、等待 optimize。
3. serial search 看 recall 和基础延迟。
4. concurrent search 看吞吐。

**重点压力**

- 常规 production dataset size。
- 单机或小集群检索能力。

**结果解读**

- 这个 case 常用于对照。
- 如果 1M 都不稳定，说明配置或后端实现有基础问题。

### 6.6 Performance1536D5M

**测试目标**

- 测 5M 级 1536D 高维 embedding 搜索。

**使用数据**

- OpenAI 5M。
- 1536D。
- Cosine。

**测试任务**

- 检查高维向量对内存、索引构建和搜索计算的影响。

**测试流程**

1. 载入 OpenAI 5M。
2. 插入高维向量。
3. 构建或加载索引。
4. serial search 看 recall。
5. concurrent search 看吞吐和尾延迟。

**重点压力**

- 高维向量内存占用。
- 高维距离计算成本。
- 索引结构对 1536D 的适配。

**结果解读**

- 对 OpenAI embedding 类业务很有参考价值。
- 同样数据量下，1536D 通常比 768D 更吃资源。

### 6.7 Performance1536D500K

**测试目标**

- 测 500K 级 1536D 搜索。

**使用数据**

- OpenAI 500K。
- 1536D。
- Cosine。

**测试任务**

- 用中小规模高维数据检查高维搜索的基础性能。

**测试流程**

1. 载入 OpenAI 500K。
2. 插入和建索引。
3. serial search 计算 recall。
4. concurrent search 测 QPS。

**重点压力**

- 高维但数据量中等。
- 适合排查索引参数、内存限制和延迟基线。

**结果解读**

- 如果 500K 高维就出现明显性能异常，5M 高维通常会更差。

### 6.8 Performance1536D50K

**测试目标**

- 小规模高维搜索 smoke test。

**使用数据**

- OpenAI 50K。
- 1536D。
- Cosine。

**测试任务**

- 快速验证环境、连接、schema、索引和搜索流程。

**测试流程**

1. 载入小规模 OpenAI 数据。
2. 插入、索引、搜索。
3. 观察流程是否顺利完成。

**重点压力**

- 功能正确性。
- 开发环境可用性。

**结果解读**

- 适合本地调试，不适合代表真实性能上限。

### 6.9 Performance1024D10M

**测试目标**

- 测 10M 级 1024D embedding 搜索。

**使用数据**

- Bioasq 10M。
- 1024D。
- Cosine。

**测试任务**

- 检查 1024D 文本 embedding 在大规模数据下的检索能力。

**测试流程**

1. 载入 Bioasq 10M。
2. 插入向量和必要字段。
3. 建索引并 optimize。
4. serial search 计算 recall。
5. concurrent search 计算 QPS。

**重点压力**

- 介于 768D 和 1536D 之间的维度压力。
- 医学文本 embedding 分布。

**结果解读**

- 可以和 768D10M 对比维度增长带来的代价。

### 6.10 Performance1024D1M

**测试目标**

- 测 1M 级 1024D 搜索。

**使用数据**

- Bioasq 1M。
- 1024D。
- Cosine。

**测试任务**

- 建立 1024D 中等规模基线。

**测试流程**

1. 载入 Bioasq 1M。
2. 插入、索引、optimize。
3. serial search。
4. concurrent search。

**结果解读**

- 适合和 768D1M、1536D500K 对照，判断维度变化对性能的影响。

## 7. Filtering Search Performance Case

Filtering case 的核心是：搜索不再只是向量 topK，而是向量相似度加业务过滤条件。

真实业务里很常见：

- 只搜索某个租户的数据。
- 只搜索某种文档类型。
- 只搜索某个时间范围。
- 只搜索某个权限范围。
- 只搜索某个标签集合。

如果向量数据库不能高效处理 filter，那么 RAG、多租户、权限控制、内容分类都会受影响。

### 7.1 共同测试目标

- 验证 filter 条件和 ANN 搜索组合后的正确性。
- 测 filter selectivity 变化对搜索性能的影响。
- 检查标量过滤是否能下推。
- 检查过滤后 recall 是否仍然可信。
- 检查小候选集和大候选集下的执行路径差异。

### 7.2 过滤比例怎么理解

文档中常见的 `1P`、`99P` 可以理解为 filter 命中比例约 1% 或 99%。

- 1% 命中：过滤条件很严格，候选集很小。
- 99% 命中：过滤条件很宽松，候选集很大。

这两个极端会压不同的路径：

- 1% case 更考验 filter pushdown 和小候选集搜索。
- 99% case 更接近普通 ANN，但仍然带 filter 判断开销。

### 7.3 Int-Filter Case

**测试目标**

- 测整数标量过滤和向量搜索结合后的性能。

**典型业务含义**

- 文档 ID 范围。
- 时间戳范围。
- 数值状态码。
- 分桶 ID。
- 权限等级。

**测试任务**

- 给每条向量附加整数标量字段。
- 构造一个 int filter 表达式。
- 在 filter 条件下执行 topK 向量搜索。
- 用 filter 版本 ground truth 计算 recall。

**测试流程**

1. 准备向量数据。
2. 准备整数标量字段。
3. 根据 filter rate 生成过滤表达式。
4. 插入向量和标量字段。
5. 构建向量索引。
6. 执行带 int filter 的 serial search。
7. 执行带 int filter 的 concurrent search。
8. 汇总 recall、latency、QPS。

**关键 CaseType**

| CaseType | 数据集 | 命中比例 | 用途 |
| --- | --- | --- | --- |
| `Performance768D10M1P` | Cohere 10M / 768D | 约 1% | 大规模严格过滤 |
| `Performance768D10M99P` | Cohere 10M / 768D | 约 99% | 大规模宽松过滤 |
| `Performance768D1M1P` | Cohere 1M / 768D | 约 1% | 中规模严格过滤 |
| `Performance768D1M99P` | Cohere 1M / 768D | 约 99% | 中规模宽松过滤 |
| `Performance1536D5M1P` | OpenAI 5M / 1536D | 约 1% | 高维大规模严格过滤 |
| `Performance1536D5M99P` | OpenAI 5M / 1536D | 约 99% | 高维大规模宽松过滤 |
| `Performance1536D500K1P` | OpenAI 500K / 1536D | 约 1% | 高维中规模严格过滤 |
| `Performance1536D500K99P` | OpenAI 500K / 1536D | 约 99% | 高维中规模宽松过滤 |
| `NewIntFilterPerformanceCase` | 参数选择 | 参数选择 | 新版参数化 int filter |

**重点压力**

- scalar index 或 filter executor。
- ANN 搜索前过滤还是搜索后过滤。
- 候选集裁剪效率。
- filter ground truth 与 ANN result 的一致性。

**结果解读**

- 1% 命中时，如果 QPS 很低，可能说明严格过滤无法有效下推。
- 99% 命中时，如果性能明显差于无 filter case，说明 filter 判断本身开销较大。
- recall 异常时，要优先检查 filter ground truth 和表达式语义是否一致。

### 7.4 Label-Filter Case

**测试目标**

- 测标签/类别字段过滤下的向量搜索性能。

**典型业务含义**

- `category == "news"`。
- `tenant == "tenant_001"`。
- `source == "pdf"`。
- `language == "zh"`。
- `tag in ["finance", "legal"]`。

**测试任务**

- 给向量附加 label 字段。
- 按 label percentage 控制标签命中比例。
- 执行带 label filter 的向量搜索。
- 对比不同 label 选择性下的性能。

**测试流程**

1. 选择数据集和 label percentage。
2. 载入向量数据和 label 数据。
3. 建 schema，包含向量字段和 label 字段。
4. 插入数据。
5. 建索引或 optimize。
6. 执行带 label filter 的 serial search。
7. 执行带 label filter 的 concurrent search。
8. 输出 recall、latency、QPS。

**关键 CaseType**

| CaseType | 特点 |
| --- | --- |
| `LabelFilterPerformanceCase` | 新版参数化 label filter case，可配置数据集和 label 命中比例 |

**重点压力**

- 字符串或类别字段过滤。
- label 字段索引。
- metadata filter 和向量索引结合。
- 多标签分布下的查询稳定性。

**结果解读**

- label filter 通常比 int range filter 更接近业务 metadata 过滤。
- 如果 label 命中比例低，搜索系统需要快速缩小候选集。
- 如果 label 命中比例高，性能应该接近无 filter case，但仍会有 filter 额外成本。

### 7.5 Filtering Case 容易误读的点

- filter case 的 recall 必须使用 filter 后的 ground truth。
- 不能把 1% 和 99% case 的 QPS 直接等价比较，因为它们的候选集大小不同。
- filter QPS 高但 recall 低，通常不是好结果。
- filter 结果受 schema、scalar index、segment 布局和 query planner 影响很大。

## 8. Streaming Case

Streaming case 关注的是持续写入和搜索同时发生时，数据库是否还能保持稳定。

这类 case 更接近线上状态，因为真实系统通常不是“先完全导入，再只读搜索”。生产里常常是：

- 后台持续写入新文档。
- 前台持续服务搜索请求。
- 索引和 segment 持续刷新。
- flush、compaction、load、merge 等后台任务同时发生。

### 8.1 StreamingPerformanceCase

**测试目标**

- 测持续 insert 期间的搜索 QPS、延迟和稳定性。
- 观察 ingest 对 search 的影响。

**使用数据**

- 默认使用 Cohere 768D 小规模或可配置数据集。
- 支持配置 insert rate。
- 支持配置搜索阶段，例如写入到 50%、80% 时开始测搜索。

**测试任务**

- 一边按固定速率插入向量。
- 一边在不同写入进度阶段执行搜索。
- 记录搜索性能和写入性能。

**测试流程**

1. 创建 collection。
2. 启动写入 runner。
3. 按配置的 insert rate 持续插入。
4. 当写入进度达到指定 stage，启动搜索任务。
5. 在写入期间采集搜索 latency 和 QPS。
6. 写入结束后，可选择执行 optimize。
7. 再执行写入后的搜索，观察稳定态性能。

**主要参数**

| 参数 | 含义 |
| --- | --- |
| `insert_rate` | 目标写入速率 |
| `search_stages` | 在写入进度达到哪些比例时执行搜索 |
| `concurrency_duration` | 每个并发阶段持续多久 |
| `optimize_after_write` | 写入结束后是否 optimize |
| `read_after_write_duration` | 写入后继续读的时长 |

**主要指标**

- 写入 rows/s。
- 写入错误数。
- 写入期间 QPS。
- 写入期间 p95 / p99 latency。
- 写入后 QPS。
- 写入后 p95 / p99 latency。

**结果解读**

- 如果写入期间搜索 p99 飙升，说明读写资源隔离不足。
- 如果写入速率达不到配置目标，说明 ingest 是瓶颈。
- 如果写入后 optimize 很久，说明后台索引/合并成本高。
- 如果写入期间 recall 下降，要检查可见性和索引刷新策略。

### 8.2 StreamingCustomDataset

**测试目标**

- 在用户自定义数据集上测 streaming 场景。

**测试任务**

- 使用用户提供的 parquet 数据。
- 按 streaming 方式持续写入。
- 同时执行搜索。

**测试流程**

1. 准备自定义数据集。
2. 注册数据集配置。
3. 创建 collection。
4. 持续写入自定义 train vectors。
5. 在指定写入阶段执行 query。
6. 输出读写并发指标。

**结果解读**

- 比公开数据集更贴近业务数据分布。
- 适合验证真实 embedding、真实标量字段和真实写入节奏。

## 9. Custom Dataset Case

Custom Dataset Case 的目标是让 benchmark 使用你的业务数据，而不是只依赖公开数据集。

### 9.1 PerformanceCustomDataset

**测试目标**

- 测用户自定义向量数据上的搜索性能。

**适用场景**

- 公司内部 embedding 模型。
- 特定业务数据分布。
- 特定维度。
- 特定 metric type。
- 自定义 ground truth。

**测试任务**

- 读取用户准备的 parquet 数据。
- 插入到目标向量数据库。
- 执行标准 performance 流程。
- 使用用户提供的 ground truth 计算 recall。

**数据准备**

通常需要准备：

- train vectors。
- test/query vectors。
- neighbors 或 ground truth。
- 可选 scalar labels。
- 数据集配置描述。

**测试流程**

1. 准备 parquet 文件。
2. 准备 query 和 ground truth。
3. 在 VectorDBBench 中注册 dataset。
4. 选择 `PerformanceCustomDataset`。
5. 执行 load。
6. 执行 optimize。
7. 执行 serial search。
8. 执行 concurrent search。
9. 输出标准 search performance 指标。

**结果解读**

- 这是最贴近真实业务的 case。
- 公开 leaderboard 上的高低不一定代表你的业务数据结果。
- 如果自定义数据分布和公开数据差异很大，应优先看 custom dataset 的结果。

**容易出问题的点**

- query 和 train 的维度不一致。
- metric type 和 ground truth 不匹配。
- ground truth 不是基于相同 filter 或相同 metric 生成的。
- parquet 字段名和 client 预期不一致。

## 10. Cloud Leaderboard Cases

Cloud case 是 VectorDBBench 后续补充的一组 cloud-native benchmark。它们不是替代原有 search performance，而是补充云服务生产行为。

传统 search case 主要回答：

- load 多快。
- search 多快。
- recall 多高。

Cloud case 额外回答：

- 插入完成后多久能搜到。
- 索引完成滞后多久。
- 返回 payload 对性能影响多大。
- 多租户隔离下性能如何。
- 冷查询首包延迟如何。

### 10.1 CloudInsertCase

**测试目标**

- 测写入完成、数据可搜索、索引完成之间的时间差。

**为什么需要这个 case**

很多云数据库的 insert API 返回成功，并不代表数据已经：

- 对 search 可见。
- 已经建好索引。
- 已经达到最佳搜索性能。

所以单看 insert duration 会误导。CloudInsertCase 专门拆这三个阶段。

**测试任务**

- 并发插入数据。
- 记录 insert 完成时间。
- 轮询检查数据是否 searchable。
- 继续轮询检查索引是否 indexed。

**测试流程**

1. 创建 collection。
2. 启动并发插入。
3. 记录插入开始时间。
4. 等待 insert runner 完成。
5. 记录 insert 完成时间和插入行数。
6. 轮询数据库，直到新写入数据能被搜索命中。
7. 继续轮询，直到后端报告索引完成或 optimize 完成。
8. 输出三个阶段的耗时。

**主要指标**

| 指标 | 含义 |
| --- | --- |
| `inserted_count` | 成功写入数量 |
| `insert_duration` | insert runner 完成耗时 |
| `insert_completion_seconds` | 从开始到写入完成的总耗时 |
| `searchable_after_insert_seconds` | insert 完成后到可搜索的等待时间 |
| `indexed_after_searchable_seconds` | 可搜索后到索引完成的等待时间 |

**结果解读**

- `insert_duration` 低但 `searchable_after_insert_seconds` 高，说明写入返回快，但可见性滞后。
- `searchable_after_insert_seconds` 低但 `indexed_after_searchable_seconds` 高，说明能搜到，但进入最佳索引状态较慢。
- 对回填、数据迁移、实时 RAG 非常关键。

### 10.2 CloudPayloadSearchCase

**测试目标**

- 测不同返回 payload 对搜索性能的影响。

**为什么需要这个 case**

真实应用一般不会只要向量 ID。常见返回模式有：

- 只返回 ID。
- 返回 ID 加标量 metadata。
- 返回 ID 加 text。
- 返回 ID 加 vector。

payload 越大，序列化、网络传输、服务端读取字段的成本越高。

**测试任务**

- 使用相同数据集。
- 切换不同 payload profile。
- 可选叠加 int filter 或 label filter。
- 比较 QPS、latency、recall 和响应体大小。

**测试流程**

1. 选择数据集。
2. 选择 payload profile。
3. 可选配置 int filter。
4. 可选配置 label filter。
5. 执行 load / optimize。
6. 执行 serial search。
7. 执行 concurrent search。
8. 输出 payload-aware search 指标。

**常见 payload profile**

| Payload 类型 | 含义 | 压力点 |
| --- | --- | --- |
| ids only | 只返回 primary key | 最轻响应 |
| scalar / label | 返回 metadata 字段 | 字段读取和序列化 |
| vector | 返回原始向量 | 大响应体、网络和内存拷贝 |

**结果解读**

- ids only 是最接近纯搜索引擎吞吐的结果。
- scalar payload 更接近实际业务。
- vector payload 会显著放大响应体，QPS 下降是正常的。
- 如果返回少量 metadata 也导致明显下降，说明字段读取或序列化路径可能有瓶颈。

### 10.3 CloudMultiTenantSearchCase

**测试目标**

- 测多租户场景下的搜索性能。

**为什么需要这个 case**

很多云服务和 SaaS 系统中，数据天然按 tenant 隔离：

- 一个 collection 多 tenant。
- 多个 namespace。
- 多个 collection。
- 通过 metadata filter 隔离 tenant。

多租户会影响路由、缓存、过滤、索引局部性和权限判断。

**测试任务**

- 将数据按 deterministic rule 分配到多个 tenant。
- 查询时指定 tenant。
- 执行 search。
- 观察多租户路由和过滤带来的性能影响。

**测试流程**

1. 选择数据集。
2. 设置 tenant 数量。
3. 写入时给每条数据分配 tenant id。
4. 查询时构造 tenant 条件或路由。
5. 执行 serial search。
6. 执行 concurrent search。
7. 输出 latency、QPS 和可选 payload 指标。

**主要参数**

| 参数 | 含义 |
| --- | --- |
| tenant count | 租户数量 |
| tenant prefix | tenant id 前缀 |
| payload profile | 返回字段形态 |
| optional filter | 是否再叠加额外 filter |

**结果解读**

- 多租户 QPS 低，可能是 tenant filter 无法有效下推。
- p99 高，可能是 tenant 路由、缓存命中或 segment 分布不均。
- 多租户场景不一定追求最高整体 QPS，还要看 tenant 间隔离和尾延迟。

### 10.4 CloudColdLatencyCase

**测试目标**

- 测冷态下的首次查询延迟。

**为什么需要这个 case**

很多 benchmark 都是热缓存下跑的：

- collection 已经 load。
- index 已经在内存。
- OS page cache 已经命中。
- 服务端连接和执行路径已预热。

但真实用户可能遇到：

- 刚创建或刚恢复的 collection。
- 冷缓存。
- 低频访问 tenant。
- serverless 冷启动。

CloudColdLatencyCase 专门测这类首查体验。

**测试任务**

- 在已有 collection 上执行 search-only 流程。
- 不重新 drop。
- 不重新 load。
- 记录 cold search 的延迟。
- 可选再跑 warm pass 做对比。

**测试流程**

1. 确认目标 collection 已经存在。
2. 跳过 drop old。
3. 跳过 load。
4. 直接执行 serial search。
5. 记录首批查询延迟。
6. 继续执行 warm 查询。
7. 对比 cold 和 warm 的 latency。

**主要指标**

- first query latency。
- cold p95 / p99 latency。
- warm p95 / p99 latency。
- cold-to-warm ratio。

**结果解读**

- cold latency 高，可能是索引加载、缓存缺失、远端存储读取或 serverless 唤醒导致。
- warm latency 正常但 cold latency 高，说明常规 QPS benchmark 没覆盖用户首查体验。
- 对 serverless、云托管、低频租户非常重要。

## 11. Full Text Search Performance Case

Full Text Search Performance Case 是 VectorDBBench 在 2026-06 新增的全文检索 benchmark 路径。它的核心不是 dense vector ANN，而是 BM25-style full text retrieval。

注意命名：

- 发布说明里把这类测试称为 `FullTextSearchPerformance`。
- 当前源码里的可选 `CaseType` 是 `FTSBm25Performance`。
- 源码里的 `CaseLabel` 是 `FullTextSearchPerformance`。

也就是说，用户运行时选择的是 `--case-type FTSBm25Performance`，但它属于 Full Text Search Performance 这个大类。

### 11.1 这个 case 测什么

**核心问题**

同一份 raw text corpus，被不同数据库建立全文索引以后，在 BM25 排序语义下：

- 文档插入要多久。
- 全文索引或 optimize 要多久。
- BM25 topK 结果是否符合数学 ground truth。
- 单查询延迟是多少。
- 并发查询 QPS 是多少。
- 返回 text payload 后吞吐和延迟会下降多少。

它不是在测：

- embedding 相似度。
- dense vector ANN。
- semantic relevance。
- reranker 后的最终排序质量。
- 端到端 RAG 回答质量。

### 11.2 为什么需要 FTS case

现代向量数据库越来越多地支持混合检索：

- dense vector search。
- sparse vector search。
- keyword search。
- BM25 full text search。
- metadata filter。
- hybrid search。
- rerank。

如果 benchmark 只测 dense vector，会漏掉一个关键问题：数据库自己的全文索引路径到底强不强。

FTS case 单独把 text-only BM25 层拆出来测。这样可以回答：

- 这个数据库的全文索引构建是否快。
- tokenizer / analyzer 行为是否稳定。
- BM25 ranking 是否和声明的数学 ground truth 一致。
- 返回文本字段时，payload 成本有多大。
- 在 hybrid search 里，keyword/BM25 这一路是否会成为瓶颈。

### 11.3 和向量 Search Performance Case 的区别

| 对比项 | Vector Search Performance | Full Text Search Performance |
| --- | --- | --- |
| 输入主数据 | dense embedding vector | raw text document |
| 查询输入 | query vector | query text |
| 检索算法 | ANN / vector similarity | inverted index / BM25 |
| metric | L2 / Cosine / IP | BM25 |
| ground truth | 向量最近邻结果 | BM25 数学 topK |
| 召回含义 | ANN 是否找回向量近邻 | backend 是否符合 BM25 ranking contract |
| 主要压力 | 向量索引、距离计算、topK merge | analyzer、倒排索引、BM25 scorer、文本字段读取 |
| payload | ids / scalar / vector | ids / text |
| Milvus 相关路径 | Knowhere / QueryNode vector search | Tantivy / full text index / BM25 search |

### 11.4 CaseType: FTSBm25Performance

**测试目标**

- 测 BM25 全文检索的 end-to-end 行为。
- 同时覆盖 load、insert、optimize、serial recall、serial latency、concurrent QPS、payload profile。

**源码定义重点**

- `case_id`: `CaseType.FTSBm25Performance`
- `label`: `CaseLabel.FullTextSearchPerformance`
- `dataset`: `FtsDatasetManager`
- `metric_type`: `BM25`
- `filter_rate`: `None`
- `filters`: `non_filter`
- 默认数据集: `MS MARCO Small (100K documents)`

FTS case 当前不是 filter case。它默认不做 metadata filter，重点是文本检索路径本身。

### 11.5 支持的数据集和子 case

当前 FTS 数据集按 dataset-with-size-type 选择。

| DatasetWithSizeType | 文档数 | 适合测试什么 |
| --- | --- | --- |
| `MS MARCO Small (100K documents)` | 100K | 本地 smoke test、功能验证、小规模文本索引 |
| `MS MARCO Medium (1M documents)` | 1M | 中等规模 passage retrieval |
| `MS MARCO Large (8.8M documents)` | 8,841,823 | 大规模 passage retrieval |
| `HotpotQA Small (100K documents)` | 100K | 小规模多跳问答文本检索 |
| `HotpotQA Medium (1M documents)` | 1M | 中等规模问答文本检索 |
| `HotpotQA Large (5.2M documents)` | 5,233,329 | 大规模问答文本检索 |

### 11.6 MS MARCO 测试任务

**数据特点**

- Passage retrieval 数据。
- 文档一般是 passage text。
- query 是自然语言检索 query。

**测试目标**

- 检查数据库处理短文本 passage corpus 的 BM25 能力。
- 适合模拟搜索引擎、RAG passage recall、文档片段召回。

**测试流程**

1. 通过 `ir_datasets` 加载 MS MARCO 数据。
2. 将原始 query 转换成内部 `FtsQuery(query_id, text)`。
3. 将原始 doc 转换成内部 `FtsDocument(doc_id, text)`。
4. 清理 doc text 里的 tab 和换行。
5. 下载数学 BM25 ground truth。
6. 读取 build manifest 里的 BM25/analyzer 参数。
7. 插入前 N 条文档。
8. 构建全文索引。
9. 执行 query text 搜索。
10. 和 BM25 ground truth 比 recall。

**关注点**

- passage 数量增大以后，倒排索引构建是否线性变慢。
- query latency 是否随着 corpus 规模明显上升。
- analyzer 与 ground truth 的 analyzer 是否一致。
- text payload 返回是否显著拖慢结果。

### 11.7 HotpotQA 测试任务

**数据特点**

- 多跳问答数据。
- 文档可能包含 title 和 text。
- VectorDBBench 的 translator 会把 title 和 text 拼接成最终检索文本。

**测试目标**

- 检查数据库在问答类文档上的 BM25 检索能力。
- 更接近 QA / RAG 场景里的知识文本召回。

**测试流程**

1. 通过 `ir_datasets` 加载 HotpotQA。
2. 将 query 转换成 `FtsQuery(query_id, text)`。
3. 将文档的 title 和 text 拼接成 `FtsDocument(doc_id, text)`。
4. 清理 tab、换行和空白。
5. 下载数学 BM25 ground truth。
6. 读取 build manifest。
7. 插入文档。
8. 构建全文索引。
9. 执行 BM25 查询。
10. 汇总 recall、latency、QPS。

**关注点**

- title + body 拼接后 analyzer 行为是否一致。
- 长文本字段是否影响索引构建和查询延迟。
- 返回 text payload 时响应体更大，QPS 是否明显下降。

### 11.8 FTS 数据准备阶段

FTS 的 `prepare()` 和向量数据集不一样。

它会做这些事：

1. 通过 `ir_datasets` 下载或读取原始文本数据。
2. 使用 translator 把外部数据 schema 转成 VectorDBBench 内部 schema。
3. 预先遍历 documents，避免 `ir_datasets` 的 lazy document cache 把成本混进 timed insert。
4. 加载 query text。
5. 从远端下载数学 BM25 ground truth 文件。
6. 加载 build manifest。
7. 校验 manifest 和数据集规模是否一致。
8. 加载 BM25 参数和 analyzer 参数。

这个阶段很重要，因为 FTS case 的 correctness 强依赖 manifest。

### 11.9 FTS 内部数据结构

FTS 不使用 vector dataset 的 train/test parquet 结构，而使用独立的文本结构。

| 内部对象 | 字段 | 含义 |
| --- | --- | --- |
| `FtsQuery` | `query_id`, `text` | 一条文本查询 |
| `FtsDocument` | `doc_id`, `text` | 一条被索引的文本 |
| `FtsBaseDataset` | `name`, `size`, `metric_type`, `with_gt` | FTS 数据集元信息 |
| `FtsDatasetManager` | `queries_data`, `gt_data`, `bm25_params`, `analyzer_params` | FTS 数据集管理器 |

### 11.10 FTS ground truth 文件

FTS 数学 ground truth 主要依赖这些文件：

| 文件 | 作用 |
| --- | --- |
| `neighbors.parquet` | 每个 query 对应的 BM25 topK 文档 ID |
| `build_manifest.json` | 生成 ground truth 时使用的 BM25 和 analyzer 参数 |
| `manifest.json` | 数据集 manifest 元信息 |

源码里 ground truth 字段默认是：

```text
neighbors_id
```

需要特别注意：FTS math ground truth 使用的是 dense document row IDs，不一定是原始 `ir_datasets` 的 doc_id。VectorDBBench 在插入文档时会按插入顺序把 `doc.doc_id` 重新设置成 row id 字符串，这样搜索结果才能和 `neighbors.parquet` 对齐。

### 11.11 build manifest 里有什么

build manifest 可能包含：

- `source_ir_dataset`
- `doc_limit`
- `indexed_doc_count`
- `query_count`
- `bm25.k1`
- `bm25.b`
- `bm25.avgdl`
- `analyzer`

VectorDBBench 会做校验：

- manifest 的 source dataset 必须和当前 translator 对应的数据集一致。
- `doc_limit` / `indexed_doc_count` 必须和 case size 一致。
- `query_count` 必须和加载到的 query 数量一致。

如果这些不一致，FTS recall 就不能信。

### 11.12 BM25 参数和 analyzer 参数

FTS case 会在 backend 初始化前读取 manifest 参数，并尝试把它们应用到 database case config。

这些参数包括：

- `k1`
- `b`
- `avgdl`
- tokenizer 设置
- lowercase 设置
- stop words
- stemming
- token length limit
- field normalization

不同后端支持的参数不一样：

- 有些后端暴露 `k1` 和 `b`。
- 有些后端隐藏或固定 `avgdl`。
- 有些后端 analyzer 不可完全配置。
- 有些后端只能记录“未应用参数”。

所以比较 FTS 结果时，必须把 applied / unapplied BM25 参数和 analyzer 参数写到报告里。否则 recall 差异可能来自 analyzer 不一致，而不是引擎实现差异。

### 11.13 FTS load 阶段

**做什么**

- 创建全文检索 collection / index / table / namespace。
- 按 batch 插入 `FtsDocument`。
- 每条文档至少包含：
  - doc id。
  - text 字段。
- 记录 insert duration 和 inserted count。

**压什么**

- 文本字段写入。
- 文本字段序列化。
- tokenizer 前处理。
- 倒排索引写入。
- WAL / flush / segment。
- 后端批量写入能力。

**和向量 load 的区别**

- 不需要 vector dimension。
- 不做 vector normalization。
- 主要压力来自文本处理和倒排索引。

### 11.14 FTS optimize 阶段

**做什么**

- 调用 backend 的 optimize / index readiness。
- 等待全文索引可搜索。

不同后端里 optimize 的含义可能不同：

- Milvus/Zilliz Cloud: 等待 full text index / collection readiness。
- Elasticsearch/OpenSearch: 可能涉及 refresh、force merge 或 index readiness。
- Vespa: 可能涉及 document indexing readiness。
- turbopuffer: 可能涉及 namespace full text path readiness。

**压什么**

- 倒排索引构建。
- posting list 合并。
- term dictionary 构建。
- 文档长度统计。
- BM25 统计信息。
- 后台 merge / compaction。

### 11.15 FTS serial search 阶段

**做什么**

- 使用 query text 执行 BM25 检索。
- 返回 topK doc ids。
- 和 BM25 ground truth 比较。
- 输出 recall、p99、p95。

源码里 FTS serial search 的返回值和 vector search 不同：

- vector case: `recall`, `ndcg`, `serial_latency_p99`, `serial_latency_p95`
- FTS case: `recall`, `serial_latency_p99`, `serial_latency_p95`

FTS 当前不输出 NDCG 作为主结果。

**压什么**

- query analyzer。
- term lookup。
- posting list traversal。
- BM25 score 计算。
- topK heap / collector。
- score 排序。

### 11.16 FTS concurrent search 阶段

**做什么**

- 使用相同 query text 集合。
- 按 concurrency 列表启动多并发。
- 每个并发档位跑固定 duration。
- 输出 QPS、p99、p95、avg latency。

**压什么**

- 多 query 并发调度。
- 倒排索引并发访问。
- BM25 scorer 并行度。
- 内存缓存。
- response serialization。
- 网络。

**怎么解读**

- QPS 高但 p99 高，说明吞吐可能靠排队堆出来，线上体验未必好。
- QPS 高但 recall 低，说明结果不可信。
- text payload QPS 低，不一定是 BM25 scorer 慢，也可能是返回文本字段慢。

### 11.17 FTS payload profile

FTS case 重点支持两种 payload：

| payload_profile | 返回内容 | 用途 |
| --- | --- | --- |
| `ids_only` | 只返回文档 ID 和 score/distance 类信息 | 最干净的 recall 和吞吐基线 |
| `text` | 返回文档 ID 加文本字段 | 测真实应用返回文本片段的开销 |

源码里 payload 大小估算大致是：

- `ids_only`: 每个 hit 约 20 bytes。
- `text`: 每个 hit 约 20 + 512 bytes。

这只是估算，用来帮助解释响应体成本。真实大小取决于文本长度、编码、协议和 backend 返回格式。

### 11.18 ids_only 和 text payload 应该怎么跑

建议分两轮跑：

1. 先跑 `ids_only`。
   - 开启 serial search。
   - 重点确认 recall。
   - 得到最干净的 BM25 baseline。
2. 再跑 `text`。
   - 可以跳过 serial recall。
   - 重点看 concurrent QPS 和 latency。
   - 对比 text payload 带来的吞吐下降。

原因是：

- recall 的核心验证是 doc id 是否匹配 ground truth。
- text payload 主要测字段读取和响应体开销。
- 如果同一个索引已经被 ids_only 验证过，text payload run 可以专注于并发性能。

### 11.19 FTS 输出指标

FTS case 的结果至少包含：

| 指标 | 含义 |
| --- | --- |
| `inserted_count` | 成功插入的文档数量 |
| `insert_duration` | 文档插入耗时 |
| `optimize_duration` | 全文索引准备耗时 |
| `load_duration` | insert + optimize 总耗时 |
| `recall` | BM25 结果和数学 ground truth 的一致性 |
| `serial_latency_p99` | 串行文本查询 p99 |
| `serial_latency_p95` | 串行文本查询 p95 |
| `qps` | 最大并发 QPS |
| `conc_qps_list` | 每个并发档位的 QPS |
| `conc_latency_p99_list` | 每个并发档位的 p99 |
| `conc_latency_p95_list` | 每个并发档位的 p95 |
| `payload_profile` | `ids_only` 或 `text` |
| `payload_estimated_bytes_per_query` | 按 topK 估算的响应大小 |
| `additional_parameters.fts_manifest` | BM25 和 analyzer manifest |

### 11.20 FTS case 的测试任务拆解

如果把 `FTSBm25Performance` 拆成测试任务单，可以这样写。

| 任务 | 目标 | 成功标准 |
| --- | --- | --- |
| 数据准备 | 正确加载 text corpus、query、GT | query 数量和 GT 行数一致 |
| manifest 校验 | 确认 BM25/analyzer contract | doc_count、query_count、source dataset 对齐 |
| 插入文档 | 测 text insert 成本 | inserted_count 达到 case size |
| 构建索引 | 测 full text index readiness | optimize 不超时 |
| 串行搜索 | 测 BM25 正确性和单查询延迟 | recall 达标，p99 可接受 |
| 并发搜索 | 测 BM25 吞吐 | QPS 随 concurrency 上升到稳定峰值 |
| payload 对比 | 测返回文本字段成本 | text payload 的 QPS drop 可解释 |

### 11.21 常用命令形态

Milvus FTS，小规模 MS MARCO，只返回 ID：

```bash
vectordbbench milvusfts \
  --case-type FTSBm25Performance \
  --dataset-with-size-type "MS MARCO Small (100K documents)" \
  --uri "$MILVUS_URI" \
  --payload-profile ids_only \
  --load-concurrency 0 \
  --num-concurrency 40,80 \
  --task-label fts-milvus-msmarco-small-ids
```

HotpotQA 中规模，返回文本字段，偏并发吞吐：

```bash
vectordbbench milvusfts \
  --case-type FTSBm25Performance \
  --dataset-with-size-type "HotpotQA Medium (1M documents)" \
  --uri "$MILVUS_URI" \
  --payload-profile text \
  --skip-search-serial \
  --num-concurrency 40,80 \
  --concurrency-duration 30 \
  --task-label fts-milvus-hotpotqa-medium-text
```

### 11.22 结果解读

**recall 高**

说明 backend 返回结果和 declared BM25/analyzer contract 生成的数学 ground truth 一致。它不等于人工相关性高，也不代表 RAG 最终答案好。

**recall 低**

优先检查：

- analyzer 是否一致。
- tokenizer 是否一致。
- lowercase / stop words / stemming 是否一致。
- BM25 `k1` / `b` / `avgdl` 是否应用。
- doc id 是否和 dense row id 对齐。
- topK 是否一致。

**load duration 高**

可能是：

- 文本写入慢。
- tokenizer 成本高。
- 倒排索引构建慢。
- backend refresh / merge / compaction 慢。
- 对象存储或磁盘 IO 慢。

**optimize duration 高**

可能是：

- posting list merge 慢。
- force merge 慢。
- 全文索引 ready 慢。
- 后台 index deployment 慢。

**ids_only QPS 高但 text QPS 低**

说明 BM25 scorer 可能不是瓶颈，瓶颈可能在：

- stored field 读取。
- 文本字段反序列化。
- 网络传输。
- client 解析响应。

**p99 高**

可能是：

- 热词 query 的 posting list 很长。
- 部分 query 命中文档过多。
- 字段返回大小差异大。
- 后端 segment / shard 分布不均。
- merge / compaction 干扰。

### 11.23 FTS case 的常见误区

**误区 1: 把 BM25 recall 当成人工相关性**

FTS recall 只说明结果是否符合数学 BM25 ground truth。它不回答“这个文档对用户是否真的相关”。

**误区 2: 忽略 analyzer**

BM25 的结果高度依赖 analyzer。分词、大小写、停用词、词干化、字段长度归一化都会影响排名。

**误区 3: 不记录未应用参数**

如果 backend 不支持某些 BM25 参数，VectorDBBench 可能只能记录它们未应用。比较结果时必须说明。

**误区 4: 把 text payload 慢归因于搜索慢**

text payload 慢可能是字段读取或网络传输慢，不一定是倒排检索慢。

**误区 5: 用 FTS 结果代表 hybrid search**

FTS case 只测 BM25 text-only 层。Hybrid search 还包含 dense/sparse 融合、score normalize、rerank 等额外路径。

### 11.24 针对 Milvus 的 FTS 排查重点

如果用 `milvusfts` 跑这个 case，建议重点看：

- 全文索引创建是否成功。
- analyzer 参数是否和 manifest 对齐。
- BM25 参数是否实际应用。
- Tantivy index build 耗时。
- text 字段写入和存储成本。
- QueryNode 上 BM25 查询耗时。
- 返回 text payload 时 Proxy / QueryNode 的序列化和网络成本。
- sealed segment 数量是否过多。
- index load 是否完成。
- 是否存在 cold cache 导致的首轮抖动。

Milvus 结果分析建议：

- 先用 `ids_only` 确认 BM25 recall。
- 再用 `text` payload 测真实响应体成本。
- 如果 recall 异常，先排 analyzer/manifest，不要直接调 query 并发。
- 如果 QPS 异常，拆开看 query 执行、字段读取和网络返回。
 
### 11.25 FTS case 结论应该怎么写

一份 FTS benchmark 结论至少写清楚：

- CaseType: `FTSBm25Performance`
- dataset-with-size-type
- 文档数
- query 数
- topK
- payload_profile
- backend analyzer 设置
- manifest analyzer 设置
- BM25 `k1` / `b` / `avgdl`
- 哪些 BM25/analyzer 参数已应用
- 哪些参数未应用
- inserted_count
- insert_duration
- optimize_duration
- load_duration
- recall
- serial p95 / p99
- concurrent QPS
- concurrent p95 / p99
- text payload 与 ids_only 的 QPS 差异

缺少这些上下文时，FTS 结果很难和其他数据库或其他版本公平比较。

## 12. 指标含义速查

| 指标 | 看什么 | 主要相关 case |
| --- | --- | --- |
| Load duration | 数据导入和准备耗时 | Search、Filter、Custom、FTS |
| Optimize duration | 索引构建、加载、准备耗时 | Search、Filter、FTS |
| Max load count | 最大可装载数据量 | Capacity |
| Recall | 搜索结果和 ground truth 的一致性 | Search、Filter、FTS |
| Serial latency | 单查询或低并发延迟 | Search、Filter、Cloud Cold、FTS |
| Concurrent QPS | 多并发吞吐 | Search、Filter、Payload、MultiTenant、FTS |
| p95 / p99 latency | 尾延迟 | Search、Filter、Streaming、Cloud |
| Insert duration | 写入耗时 | Streaming、CloudInsert |
| Searchable delay | 写入完成后到可搜索的延迟 | CloudInsert |
| Indexed delay | 可搜索后到索引完成的延迟 | CloudInsert |
| Payload size impact | 返回字段大小对性能的影响 | CloudPayload |
| Cold latency | 冷态首查延迟 | CloudColdLatency |

## 13. 如何选择要跑的 case

| 你想知道的问题 | 优先跑的 case |
| --- | --- |
| 数据库最多能装多少 | CapacityDim128 / CapacityDim960 |
| 1M 到 10M 规模搜索性能如何 | Performance768D1M / Performance768D10M |
| 100M 级别是否能撑住 | Performance768D100M |
| OpenAI embedding 场景表现如何 | Performance1536D500K / Performance1536D5M |
| 业务过滤会不会拖慢搜索 | Int-Filter / Label-Filter |
| 一边写一边搜是否稳定 | StreamingPerformanceCase |
| 真实业务数据表现如何 | PerformanceCustomDataset |
| 插入后多久能搜到 | CloudInsertCase |
| 返回 metadata/vector 成本多大 | CloudPayloadSearchCase |
| SaaS 多租户性能如何 | CloudMultiTenantSearchCase |
| 冷启动首查慢不慢 | CloudColdLatencyCase |
| BM25 文本检索能力如何 | FTSBm25Performance |

## 14. 常见误区

### 14.1 只看 QPS

QPS 必须和 recall、p99 latency、payload profile 一起看。

一个系统可以通过降低 recall 或减少返回字段获得更高 QPS，但这不一定对业务有价值。

### 14.2 把 filter case 当成普通 search case

filter case 的候选集和 ground truth 都变了。它测试的是 query planner、scalar filter、ANN 搜索组合后的整体能力。

### 14.3 把 load duration 当成 insert API 耗时

load duration 往往包括更多步骤，例如批量插入、等待可见、构建索引、加载 collection。CloudInsertCase 才专门拆 insert、searchable、indexed 的时间差。

### 14.4 忽略 payload

只返回 ID 的搜索结果和返回完整 vector/text 的搜索结果，成本完全不同。CloudPayloadSearchCase 就是为了补这个盲点。

### 14.5 用小数据集代表大数据集

50K 或 500K case 适合开发验证，但不能代表 10M 或 100M 的索引、内存、IO 和调度压力。

### 14.6 忽略 cold latency

热缓存 QPS 很高，不代表用户第一次查询就快。CloudColdLatencyCase 专门测这个问题。

## 15. 源码层面的 case 配置怎么看

如果只看 README，很容易把 case 理解成“一个名字”。但从源码角度看，一个 case 更像是一组结构化配置。

一个 case 通常至少会决定下面这些内容。

| 配置维度 | 它控制什么 | 影响哪些结果 |
| --- | --- | --- |
| `case_type` | 用户在 CLI 或 Web UI 里选择的 case 名称 | 决定整个工作负载 |
| `case_label` | case 所属大类，例如 capacity、performance、filter、streaming、cloud、fts | 决定 runner 和统计口径 |
| `dataset` | 使用哪个数据集 | 决定数据分布、规模、维度、metric |
| `k` | topK 大小 | 影响 recall、latency、payload 大小 |
| `metric_type` | L2 / Cosine / BM25 等 | 决定距离计算和 ground truth |
| `num_per_batch` | 每批插入多少条 | 影响 load duration 和写入稳定性 |
| `load_timeout` | load 阶段最大等待时间 | 大数据集或慢后端容易触发 |
| `optimize_timeout` | index / optimize 最大等待时间 | 影响索引构建类 case |
| `concurrency` | 并发搜索线程或请求数 | 影响 QPS 和尾延迟 |
| `concurrency_duration` | 每个并发档位持续时长 | 影响 QPS 稳定性 |
| `filter_rate` | filter 命中比例 | 影响候选集大小 |
| `label_percentage` | label filter 的命中比例 | 影响 label 候选集 |
| `payload_profile` | 搜索返回字段类型 | 影响序列化和网络开销 |
| `tenant_count` | 多租户数量 | 影响路由、隔离和缓存命中 |
| `insert_rate` | streaming case 的目标写入速率 | 影响读写争抢 |
| `search_stages` | streaming 中在哪些写入进度点触发搜索 | 影响阶段性结果 |
| `drop_old` | 是否清理已有 collection | 影响是否从冷/空状态开始 |
| `skip_load` | 是否跳过 load 阶段 | 常用于 cloud cold 或 search-only |

这些字段会共同决定 benchmark 的含义。比如同样是 search：

- `Performance768D10M` 和 `CloudPayloadSearchCase` 都会执行查询，但后者还会控制返回字段。
- `Performance768D10M1P` 和 `Performance768D10M99P` 都是 10M Cohere 数据，但 filter 命中比例完全不同。
- `CloudColdLatencyCase` 也执行查询，但它的重点是冷态首查，不是稳定态 QPS。

## 16. Runner 和执行路径映射

理解 runner 很重要，因为不同 case 的执行路径不一样。

| Case 类别 | 主要执行路径 | 重点阶段 |
| --- | --- | --- |
| Capacity | 持续 insert，直到数据写完或失败边界 | insert / memory / storage / index build |
| Standard Performance | load -> optimize -> serial search -> concurrent search | recall / latency / QPS |
| Legacy Filter | load scalar fields -> optimize -> filtered serial search -> filtered concurrent search | filter selectivity / scalar executor |
| New Int Filter | 参数化生成 int filter -> filtered search | 可控 filter rate |
| Label Filter | 生成 label 字段 -> label filtered search | metadata filter |
| Streaming | insert runner 和 search runner 并行 | read/write interference |
| Custom Dataset | 使用用户 parquet -> 标准 performance 流程 | 真实数据分布 |
| CloudInsert | concurrent insert -> 等待 searchable -> 等待 indexed | 写入可见性 |
| CloudPayload | payload-aware search | output fields / serialization / network |
| CloudMultiTenant | tenant-aware search | namespace / tenant routing |
| CloudColdLatency | search-only cold run -> warm comparison | cold start |
| FTS | text load -> text index -> BM25 search | text index / BM25 recall |

### 16.1 Capacity runner 的特征

Capacity runner 不追求固定查询 QPS。它更像一个“容量探测器”：

1. 不断向数据库写入。
2. 每轮写入后检查是否成功。
3. 如果失败次数超过阈值或后端明确拒绝，停止。
4. 输出最大成功写入量。

这意味着 Capacity 结果通常不是一个稳定吞吐值，而是一个边界值。

### 16.2 Performance runner 的特征

Performance runner 是最标准的路径：

1. load data。
2. optimize/index。
3. serial search。
4. concurrent search。

它会同时关心：

- 数据进库要多久。
- 索引准备要多久。
- 单查询是否准确。
- 多并发能扛多少。

### 16.3 Filter runner 的特征

Filter runner 和 performance runner 很像，但 search request 会携带 predicate。

差异在于：

- 插入阶段需要额外写入 scalar / label 字段。
- ground truth 必须是 filter 后的 ground truth。
- 搜索阶段会调用带 filter 的接口。
- 结果要同时解释 ANN 和 scalar filter 的成本。

### 16.4 Streaming runner 的特征

Streaming runner 会把写入和搜索重叠起来：

1. insert runner 按目标速率写入。
2. search runner 在指定写入阶段启动。
3. 两者同时争抢 CPU、内存、IO、网络和后台任务资源。

这类 case 的结果更像生产系统，而不是离线 benchmark。

### 16.5 Cloud runner 的特征

Cloud runner 关注托管服务的体验细节。

它不只问“查得快不快”，还问：

- insert 返回以后，数据是否真的可搜。
- 搜索返回字段变大以后，服务端是否还能稳定。
- tenant 维度隔离以后，查询是否变慢。
- collection 冷态时，首查是否很慢。

## 17. 数据文件和字段粒度

VectorDBBench 的数据准备通常围绕三类文件：训练数据、查询数据、ground truth。

### 17.1 向量搜索数据

常见文件逻辑如下。

| 数据文件 | 常见字段 | 作用 |
| --- | --- | --- |
| train data | id、embedding/vector | 被插入数据库的主数据 |
| test/query data | query id、embedding/vector | 查询向量 |
| neighbors / ground truth | query id、neighbor ids | recall 计算基准 |
| scalar labels | id、label 或 scalar value | filter case 使用 |

字段名和文件名以具体 dataset manager 为准，但逻辑上必须满足：

- train vector 维度和 case 定义一致。
- query vector 维度和 train vector 一致。
- metric type 和 ground truth 生成方式一致。
- filter case 的 ground truth 必须考虑 filter 条件。

### 17.2 Text / FTS 数据

FTS case 不使用 dense vector 作为核心检索对象。

常见文件逻辑如下。

| 数据文件 | 常见字段 | 作用 |
| --- | --- | --- |
| text corpus | doc id、text | 被插入的文本文档 |
| text query | query id、query text | BM25 查询 |
| BM25 ground truth | query id、doc ids | BM25 recall 基准 |

FTS 的 recall 语义和向量 recall 不同：

- 向量 recall 看 ANN 结果和向量 ground truth 的一致性。
- FTS recall 看 BM25 结果和 BM25 ground truth 的一致性。

### 17.3 Custom Dataset 最容易出错的字段

自定义数据集常见错误集中在这些地方：

| 错误 | 后果 |
| --- | --- |
| train 和 query 维度不同 | load 或 search 直接失败 |
| metric type 和 ground truth 不一致 | recall 无意义 |
| id 类型和 backend schema 不一致 | 插入或查询失败 |
| parquet 字段名不符合 dataset manager 预期 | 数据读取失败 |
| ground truth 没有按 filter 重新生成 | filter recall 错误 |
| query 数量太少 | QPS 和 p99 不稳定 |

## 18. 每个阶段具体在压什么

### 18.1 Load 阶段

Load 阶段通常不只是调用 insert API。

它可能包含：

- collection 创建。
- schema 创建。
- batch insert。
- flush。
- 等待 row count 可见。
- 等待 backend 持久化。
- 对部分后端执行预加载。

它主要压：

- 客户端批量写入效率。
- 服务端写入队列。
- WAL / message queue。
- segment 分配。
- 对象存储或本地磁盘写入。
- schema 和字段序列化。

### 18.2 Optimize 阶段

Optimize 阶段是各数据库差异最大的部分。

它可能包含：

- 创建向量索引。
- 构建 scalar index。
- flush sealed segment。
- load collection。
- compaction。
- 等待 index build finished。
- 等待 collection ready。

它主要压：

- index builder。
- CPU。
- 内存峰值。
- 临时文件和对象存储。
- 后台任务调度。

### 18.3 Serial Search 阶段

Serial search 主要用于检查基础质量。

它压：

- 单查询执行路径。
- index search latency。
- topK reduce。
- filter expression evaluation。
- response serialization。

它适合发现：

- recall 异常。
- 单查询过慢。
- filter 语义错误。
- metric 配置错误。

### 18.4 Concurrent Search 阶段

Concurrent search 用来测吞吐和稳定性。

它压：

- query scheduler。
- worker pool。
- CPU 并行度。
- segment 并发访问。
- network。
- response serialization。
- client-side concurrency。

它适合发现：

- QPS 上不去。
- p99 飙升。
- timeout。
- 服务端排队。
- 查询失败率增加。

### 18.5 Recall 计算阶段

Recall 阶段不是性能阶段，但很关键。

需要确认：

- topK 和 ground truth 的 K 对齐。
- metric type 对齐。
- filter case 使用 filtered ground truth。
- FTS case 使用 BM25 ground truth。
- 查询向量是否需要归一化。

如果 recall 低，不要第一时间认为数据库性能差。先检查数据和参数是否一致。

## 19. 更细的 case-by-case 对照

### 19.1 Capacity case 对照

| CaseType | 核心问题 | 主要瓶颈 | 适合比较 |
| --- | --- | --- | --- |
| `CapacityDim960` | 大维向量最多能装多少 | 内存、索引体积、单条向量大小 | 高维数据承载能力 |
| `CapacityDim128` | 小维向量最多能装多少条 | 行数、segment 数、元数据、主键索引 | 高条目数承载能力 |

### 19.2 Search performance case 对照

| CaseType | 数据含义 | 主要压力 | 看结果时重点比较 |
| --- | --- | --- | --- |
| `Performance768D100M` | 超大规模 LAION | 分布式索引、内存、IO、调度 | 100M 级 search 能力 |
| `Performance768D10M` | 10M Cohere embedding | 生产级 semantic search | 和 1M 的扩展性差异 |
| `Performance768D1M` | 1M Cohere embedding | 常规中等规模 | 基线和参数调优 |
| `Performance1536D5M` | 5M OpenAI embedding | 高维向量计算和索引 | OpenAI 类业务成本 |
| `Performance1536D500K` | 500K OpenAI embedding | 中规模高维 | 高维基础延迟 |
| `Performance1536D50K` | 50K OpenAI embedding | 流程验证 | 本地 smoke test |
| `Performance1024D10M` | 10M Bioasq embedding | 1024D 大规模 | 介于 768D 和 1536D 的维度成本 |
| `Performance1024D1M` | 1M Bioasq embedding | 1024D 中规模 | 1024D 基线 |

### 19.3 Filter case 对照

| CaseType | 数据集 | 过滤强度 | 核心问题 |
| --- | --- | --- | --- |
| `Performance768D10M1P` | 10M / 768D | 严格过滤 | 大规模下小候选集搜索是否高效 |
| `Performance768D10M99P` | 10M / 768D | 宽松过滤 | filter 判断本身开销多大 |
| `Performance768D1M1P` | 1M / 768D | 严格过滤 | 中规模下 filter pushdown |
| `Performance768D1M99P` | 1M / 768D | 宽松过滤 | 中规模 filter overhead |
| `Performance1536D5M1P` | 5M / 1536D | 严格过滤 | 高维大规模小候选集 |
| `Performance1536D5M99P` | 5M / 1536D | 宽松过滤 | 高维大规模 filter overhead |
| `Performance1536D500K1P` | 500K / 1536D | 严格过滤 | 高维中规模 filter pushdown |
| `Performance1536D500K99P` | 500K / 1536D | 宽松过滤 | 高维中规模 filter overhead |
| `NewIntFilterPerformanceCase` | 参数化 | 参数化 | 用参数精确控制 int filter |
| `LabelFilterPerformanceCase` | 参数化 | 参数化 | 用标签/类别字段模拟 metadata filter |

### 19.4 Streaming / Cloud / FTS case 对照

| CaseType | 核心问题 | 最关键指标 |
| --- | --- | --- |
| `StreamingPerformanceCase` | 持续写入时搜索是否稳定 | 写入期间 QPS、p99、错误数 |
| `StreamingCustomDataset` | 真实数据持续写入时是否稳定 | 自定义数据下的读写干扰 |
| `PerformanceCustomDataset` | 真实业务数据搜索表现如何 | 自定义 recall、latency、QPS |
| `CloudInsertCase` | insert 成功后多久可搜、多久索引好 | searchable delay、indexed delay |
| `CloudPayloadSearchCase` | 返回字段变大后性能掉多少 | QPS drop、payload latency |
| `CloudMultiTenantSearchCase` | 多租户隔离下查询是否稳定 | tenant-aware QPS、p99 |
| `CloudColdLatencyCase` | 冷态首查慢不慢 | first query latency、cold p99 |
| `FTSBm25Performance` | BM25 文本检索能力如何 | BM25 recall、text search QPS |

## 20. 参数对结果的影响

### 20.1 `k`

`k` 控制返回 topK 数量。

影响：

- k 越大，搜索和 topK merge 成本越高。
- k 越大，payload 返回越大。
- recall 计算也必须和 k 对齐。

常见误区：

- 用不同 k 的 QPS 直接比较。
- benchmark k 和业务实际 k 差异太大。

### 20.2 `num_per_batch`

`num_per_batch` 控制每批插入条数。

影响：

- batch 太小，客户端和 RPC overhead 高。
- batch 太大，单次请求内存峰值高，容易超时。
- 对 streaming insert rate 也会产生影响。

### 20.3 `concurrency`

concurrency 控制并发查询量。

影响：

- 低 concurrency 看不到系统吞吐上限。
- 高 concurrency 会放大排队和尾延迟。
- 客户端机器也可能成为瓶颈。

解读建议：

- 同时看 QPS 和 p99。
- 如果 QPS 不涨但 p99 涨，说明系统已经过饱和。

### 20.4 `concurrency_duration`

duration 太短会导致结果波动。

尤其是：

- 大数据集。
- 云服务。
- cold latency。
- streaming case。

建议至少保证每个并发档位有足够 query 数，避免 p99 只有很少样本。

### 20.5 `filter_rate` / `label_percentage`

这两个参数决定候选集大小。

影响：

- 候选集越小，filter pushdown 越重要。
- 候选集越大，filter overhead 越重要。
- 过滤比例改变以后，不能只比较 QPS，还要看 recall。

### 20.6 `payload_profile`

payload_profile 控制返回内容。

影响：

- ids only 最轻。
- scalar metadata 中等。
- text payload 更重。
- vector payload 最重。

如果 vector payload case QPS 下降明显，不一定是搜索慢，也可能是字段读取、序列化或网络传输慢。

### 20.7 `insert_rate`

insert_rate 只在 streaming 类 case 里最关键。

影响：

- insert_rate 太低，看不到读写争抢。
- insert_rate 太高，系统可能主要在写入失败或排队。
- 合理 insert_rate 应该接近业务真实写入速率。

## 21. 从结果反推瓶颈

下面是更实用的诊断表。

| 现象 | 优先怀疑 | 需要继续看 |
| --- | --- | --- |
| Load duration 很长 | insert、flush、WAL、批量大小、网络 | insert rows/s、服务端写入日志 |
| Optimize duration 很长 | index build、load collection、compaction | index task、CPU、内存、对象存储 |
| Serial recall 低 | metric 错、索引参数、ground truth 错 | metric type、normalize、topK、filter GT |
| Serial latency 高 | 单查询执行慢、索引不合适 | index search 参数、segment 数量 |
| Concurrent QPS 低 | 并发调度、CPU、客户端瓶颈 | CPU 利用率、client QPS、server queue |
| QPS 不涨但 p99 涨 | 已经过载 | concurrency 曲线、timeout |
| 1% filter 很慢 | filter 无法下推、小候选集路径差 | scalar index、query plan |
| 99% filter 比无 filter 慢很多 | filter 判断开销大 | filter executor、bitmap 构建 |
| Label filter 慢 | 字符串/类别索引弱 | label cardinality、表达式路径 |
| Streaming 写入期间搜索抖动 | 读写资源争抢 | compaction、flush、index build |
| Cloud searchable delay 高 | 写入可见性滞后 | consistency、flush、load |
| Cloud indexed delay 高 | 索引构建滞后 | index queue、后台任务 |
| Payload case QPS 掉很多 | output fields、序列化、网络 | 响应体大小、字段读取 |
| MultiTenant p99 高 | tenant 路由或数据分布不均 | tenant cardinality、segment 分布 |
| Cold latency 高 | 冷加载、缓存缺失、远端存储 | first query log、load path |
| FTS recall 低 | tokenizer、BM25 参数、GT 不匹配 | analyzer、text normalization |

## 22. 针对 Milvus 的解读方式

这部分不是 VectorDBBench 的通用定义，而是把 case 结果映射到 Milvus 研发排查时常看的模块。

### 22.1 Capacity case 对 Milvus 的含义

重点看：

- DataNode 写入吞吐。
- segment 分配和 sealing。
- MinIO / S3 对象存储写入。
- IndexNode 构建吞吐。
- QueryNode load 后内存占用。

如果 CapacityDim960 差，优先看高维向量带来的内存和索引体积。

如果 CapacityDim128 差，优先看行数、segment 数、元数据和小对象数量。

### 22.2 Search performance 对 Milvus 的含义

重点看：

- QueryCoord 是否均衡调度。
- QueryNode 是否均衡加载 segment。
- sealed segment 数量是否过多。
- Knowhere index 参数是否合适。
- reduce 和 topK merge 是否成为瓶颈。

如果 serial latency 高，先看单 query 的 QueryNode 执行时间。

如果 concurrent QPS 低，先看 QueryNode CPU、队列和 segment 分布。

### 22.3 Filter case 对 Milvus 的含义

重点看：

- plan parser 生成的表达式是否合理。
- scalar index 是否命中。
- bitset 构建是否过慢。
- filter 是否在 ANN 前有效裁剪。
- JSON / varchar / int 字段走的执行路径是否不同。

1% filter 慢时，重点看 filter pushdown。

99% filter 慢时，重点看 filter evaluator 的额外开销。

### 22.4 Streaming case 对 Milvus 的含义

重点看：

- insert channel 是否积压。
- flush 是否频繁。
- compaction 是否影响 search。
- growing segment search 是否拖慢。
- sealed segment load 是否频繁变化。

如果写入期间 p99 抖动，通常要同时看 DataNode、QueryNode、QueryCoord 和对象存储。

### 22.5 CloudInsertCase 对 Milvus 的含义

重点看三个时间差：

- insert 返回。
- query 可见。
- index ready。

如果 insert 很快但 searchable 慢，重点看 flush、consistency、load。

如果 searchable 很快但 indexed 慢，重点看 IndexNode 和 index queue。

### 22.6 Payload case 对 Milvus 的含义

重点看：

- output fields 是否触发额外字段读取。
- vector 字段返回是否造成大对象传输。
- QueryNode 到 Proxy 的结果序列化。
- Proxy 到 client 的网络传输。

如果 ids only 很快，但 vector payload 很慢，不应简单归因于 ANN 搜索。

### 22.7 FTS case 对 Milvus 的含义

重点看：

- Tantivy / text index 构建。
- tokenizer 和 analyzer 配置。
- 文本字段存储和返回。
- BM25 查询执行路径。
- hybrid search 中 sparse/text 路径和 dense vector 路径的差异。

## 23. 写 benchmark 结论时建议包含的信息

一份可复现、可解释的 VectorDBBench 结论，至少应该写清楚：

- VectorDBBench 版本或 commit。
- 数据库版本。
- case_type。
- 数据集名称、规模、维度、metric。
- k。
- index 类型和参数。
- search 参数。
- batch size。
- concurrency 列表。
- 每个 concurrency 的 duration。
- 是否 drop old。
- 是否跳过 load。
- 是否带 filter。
- filter rate 或 label percentage。
- payload profile。
- 是否 cloud cold。
- 是否 streaming。
- 机器规格。
- client 和 server 是否同机。
- 网络环境。
- recall、p95、p99、QPS、load duration、optimize duration。

缺少这些上下文时，benchmark 结果很难比较。

## 24. 把 case 写成测试任务单的模板

如果要把某个 VectorDBBench case 变成正式测试任务，可以按下面模板写。

### 24.1 基本信息

| 字段 | 示例 |
| --- | --- |
| 测试名称 | Milvus `Performance768D10M` 搜索性能测试 |
| CaseType | `Performance768D10M` |
| Case 类别 | Search Performance |
| 数据库版本 | Milvus commit / release tag |
| VectorDBBench 版本 | commit / pip version |
| 数据集 | Cohere 10M |
| 数据规模 | 10M vectors |
| 向量维度 | 768D |
| Metric | Cosine |
| TopK | 100 |
| Index 类型 | IVF / HNSW / DiskANN / AUTOINDEX 等 |
| Search 参数 | nprobe / ef / beam width 等 |
| 并发配置 | 1, 5, 10, 20, 50 |
| 返回字段 | ids only / scalar / vector / text |

### 24.2 测试目标

需要明确写成可判断的问题，例如：

- 在 10M / 768D / Cosine 数据集上，Milvus 能否达到预期 recall。
- 在 recall 达标的前提下，最大稳定 QPS 是多少。
- p99 latency 是否满足业务要求。
- load 和 optimize 是否在可接受时间内完成。

不要只写“测试性能”。要写清楚性能指什么。

### 24.3 前置条件

至少确认：

- 数据集已下载。
- ground truth 已准备。
- 数据库集群状态健康。
- collection 不存在，或明确设置 drop_old。
- server 和 client 的机器规格已记录。
- 网络环境已记录。
- index/search 参数已固定。
- 运行期间没有其他干扰任务。

### 24.4 执行步骤

标准步骤可以写成：

1. 启动数据库。
2. 确认集群健康。
3. 启动 VectorDBBench。
4. 选择目标 DB client。
5. 设置连接参数。
6. 选择 case_type。
7. 设置 batch size、topK、concurrency。
8. 执行 load。
9. 等待 optimize / index ready。
10. 执行 serial search。
11. 检查 recall。
12. 执行 concurrent search。
13. 导出结果。
14. 收集数据库日志和系统指标。

### 24.5 指标记录

建议至少记录：

| 指标 | 是否必须 | 原因 |
| --- | --- | --- |
| Load duration | 必须 | 判断数据导入成本 |
| Optimize duration | 必须 | 判断索引准备成本 |
| Recall | 必须 | 判断结果是否可信 |
| Serial p95 / p99 | 必须 | 判断单查询延迟 |
| Concurrent QPS | 必须 | 判断吞吐 |
| Concurrent p95 / p99 | 必须 | 判断并发尾延迟 |
| Failed requests | 必须 | 判断稳定性 |
| CPU / memory | 建议 | 定位瓶颈 |
| Disk / object storage IO | 建议 | 定位 load/index 问题 |
| Network throughput | 建议 | 定位 payload 问题 |

### 24.6 结论模板

可以按这个格式写结论：

```text
Case: Performance768D10M
Dataset: Cohere 10M, 768D, Cosine
TopK: 100
Index/Search params: ...

Result:
- Load duration: ...
- Optimize duration: ...
- Serial recall: ...
- Serial p99 latency: ...
- Max stable QPS: ...
- Concurrent p99 latency at max QPS: ...
- Failed requests: ...

Interpretation:
- Recall 是否达标:
- 吞吐瓶颈:
- 尾延迟瓶颈:
- 与上一轮相比变化:
- 后续需要验证:
```

### 24.7 不同 case 的任务单差异

| Case 类别 | 任务单必须额外写清楚 |
| --- | --- |
| Capacity | 停止条件、失败阈值、最大装载数量 |
| Filter | filter 表达式、filter rate、filtered ground truth |
| Streaming | insert_rate、search_stages、写入期间指标 |
| Custom Dataset | parquet schema、metric、ground truth 生成方式 |
| CloudInsert | insert completion、searchable delay、indexed delay |
| CloudPayload | payload_profile、返回字段大小 |
| CloudMultiTenant | tenant_count、tenant 分配方式 |
| CloudColdLatency | collection 初始状态、cold/warm 区分 |
| FTS | analyzer/tokenizer、BM25 ground truth、text payload |

## 25. 一句话总结

- Capacity Case 测能装多少。
- Search Performance Case 测固定规模下能搜多快、搜多准。
- Filtering Case 测加业务条件后还能不能高效搜索。
- Streaming Case 测持续写入时搜索是否稳定。
- Custom Dataset Case 测你的真实数据，而不是公开样本。
- Cloud Case 测云服务的生产体验细节。
- FTS Case 测 BM25 文本检索能力。

VectorDBBench 的价值不是给一个单一分数，而是把向量数据库的不同能力面拆开，让容量、写入、索引、搜索、过滤、payload、多租户、冷启动和文本检索分别暴露出来。
