# Milvus 写入后立即读取最新 Record 的流程说明

- 日期：2026-09-03
- 源码基线：当前工作区 `tr_v2`，`v2.4.5-3-g6e35813d56`
- 适用场景：客户端写入一个 record 后，立即读取该 record，并要求读取结果包含刚刚写入的数据。

## 1. 结论

如果客户端等待写入 RPC 成功返回，再使用 Strong consistency 发起读取，Milvus 的预期流程是：

1. Proxy 为写入任务分配逻辑时间戳 `T_w`。
2. Proxy 将 Insert DML 成功 Produce 到消息流。
3. QueryNode 异步消费这条 Insert 消息。
4. QueryNode 将 record 写入本地 growing segment。
5. QueryNode 完成相应 DML batch 后推进该 channel 的 `tSafe`。
6. 后续 Strong Search/Query 在 QueryNode 等待 `tSafe >= G`。
7. `G` 是本次读请求要求的最低新鲜度时间戳；Strong 模式下通常由读任务时间戳 `T_r` 得到，即 `G = T_r`。
8. 如果没有显式指定 `MvccTimestamp`，QueryNode 使用等待结束时实际取得的 `tSafe` 作为读取快照时间。
9. 读请求默认同时读取 sealed/historical segment 和 growing/streaming segment，因此 record 不需要等待 flush 或 seal 才能首次被读到。

所以，最准确的描述是：

> 写入成功后，Strong 读可能短暂等待 QueryNode 将 DML 应用到 growing segment；当 QueryNode 的 `tSafe` 追上读请求的保证时间戳后，该 record 对本次读取可见。

这不是 Proxy 直接读取写缓存，也不是等待对象存储 flush 完成。

## 2. “马上读”分两种情况

### 2.1 写入和读取并发发出

```text
Insert RPC ----------------------------->
Query RPC -------->
```

如果两个 RPC 并发发出，Query 可能先在 Proxy 获得读任务时间戳并到达 QueryNode，而 Insert DML 尚未被 QueryNode 消费。此时读请求可能读不到该 record。

因此，应用侧必须形成以下顺序：

```text
insert(record) 返回成功
        |
        v
query(record primary key, Strong)
```

### 2.2 等待写入成功后再读取

```text
Client
  |
  | Insert(record)
  v
Proxy
  | 申请 T_w、分配 RowID/SegmentID、构造 DML
  | stream.Produce(DML)
  v
DML channel / MQ
  |
  v
QueryNode consumer
  | 消费 Insert
  v
本地 growing segment
  | 应用 record
  v
tSafe 推进
  |
  v
Strong Query 等待 tSafe >= G
  |
  v
读取 growing + sealed segment
```

## 3. 写入阶段发生什么

### 3.1 Proxy 处理 Insert

Proxy 在 Insert 任务中会：

- 校验 collection schema 和字段数据；
- 分配 RowID；
- 为 record/row 填充任务时间戳；
- 获取 vchannel；
- 分配 SegmentID；
- 按 channel 和 segment 重打包 InsertMsg；
- 调用 `stream.Produce(msgPack)`。

源码：

- `internal/proxy/task_insert.go:97-216`
- `internal/proxy/task_insert.go:219-286`
- `internal/proxy/task_insert.go:272-275`

写 RPC 的成功点主要是 Proxy 已成功生产消息。它不等价于：

- QueryNode 已经消费消息；
- record 已经写入 growing segment；
- record 已经对读取可见；
- DataNode 已经写入对象存储；
- flush 已完成；
- index 已完成。

### 3.2 QueryNode 写入 growing segment

QueryNode 消费 Insert DML 后，`insertNode` 会按 segment 合并 insert data，然后调用 `delegator.ProcessInsert`。

`ProcessInsert` 的主要动作是：

1. 根据 SegmentID 查找现有 growing segment；
2. 如果不存在，创建 `SegmentTypeGrowing`；
3. 调用 `growing.Insert(...)` 写入 RowID、timestamp 和字段数据；
4. 更新 primary-key bloom filter；
5. 将 growing segment 注册到 `segmentManager` 和 delegator distribution。

源码：

- `internal/querynodev2/pipeline/insert_node.go:44-116`
- `internal/querynodev2/delegator/delegator_data.go:88-159`
- `internal/querynodev2/segments/segment.go:754-806`

这里是新写入进入 QueryNode 实时查询数据面的关键位置。

## 4. QueryNode 如何保证读取到最新数据

### 4.1 `G` 和 `M` 的含义

本文使用两个时间戳符号：

- `G`：`GuaranteeTimestamp`，读请求要求 QueryNode 至少处理到的时间戳；
- `M`：`MvccTimestamp`，实际用于读取 MVCC 快照的时间戳。

二者不是同一个概念：

```text
先等待：tSafe >= G
再读取：使用 M
```

如果请求没有显式传入 `M`，当前 QueryNode 实现会执行：

```text
M = 等待结束时实际返回的 tSafe
```

因此默认情况下通常有：

```text
M >= G
```

### 4.2 Strong 读的流程

```text
Client 发起 Strong Query
        |
        v
Proxy 计算 GuaranteeTimestamp
        |
        | Strong: G = T_r
        v
QueryNode shard delegator
        |
        | waitTSafe(G)
        v
等待 latestTSafe >= G
        |
        v
若 M == 0，则 M = 实际返回的 tSafe
        |
        v
Pin readable segments
        |
        +--> sealed / historical segments
        |
        +--> growing / streaming segments
        |
        v
按 M 执行 Search 或 Query
```

源码：

- Proxy 的一致性时间戳映射：`internal/proxy/util.go:779-800`
- Search 的 guarantee timestamp：`internal/proxy/task_search.go:223-240`
- Query 的 guarantee timestamp：`internal/proxy/task_query.go:415-431`
- Search 等待 `tSafe`：`internal/querynodev2/delegator/delegator.go:266-285`
- QueryNode 等待 `tSafe`：`internal/querynodev2/delegator/delegator.go:454-473`
- `waitTSafe` 具体行为：`internal/querynodev2/delegator/delegator.go:672-724`

### 4.3 为什么 `tSafe >= G` 说明 record 已经越过处理边界

消息流的 TimeTick 批处理只释放满足时间边界的消息，普通消息要求：

```text
message.EndTs <= current TimeTick
```

QueryNode 的 pipeline 处理 insert/delete 后，才将该 batch 的最大时间戳设置为 `tSafe`。

源码：

- TimeTick 批处理：`pkg/mq/msgstream/mq_msgstream.go:650-735`
- QueryNode insert/delete pipeline：`internal/querynodev2/pipeline/pipeline.go:41-60`
- insert 应用：`internal/querynodev2/pipeline/insert_node.go:44-116`
- delete 后推进 `tSafe`：`internal/querynodev2/pipeline/delete_node.go:65-87`

因此，在以下前提成立时：

```text
T_w < T_r
Strong 读的 G = T_r
tSafe >= T_r
```

写入时间为 `T_w` 的 record 已经满足 QueryNode 的时间处理边界，并且可以进入本次读的 MVCC 可见范围。

## 5. growing segment 为什么重要

Milvus 的新写入通常先存在 QueryNode 的 growing segment 中，而不是立即变成 sealed segment。

当前 QueryNode delegator 的 distribution 同时保存：

- sealed segment；
- growing segment。

普通读请求会把它们拆成不同的数据范围：

- sealed segment -> `DataScope_Historical`
- growing segment -> `DataScope_Streaming`

源码：

- distribution 同时维护两类 segment：`internal/querynodev2/delegator/distribution.go:57-120`
- growing segment 加入 distribution：`internal/querynodev2/delegator/distribution.go:220-305`
- 读子任务拆分：`internal/querynodev2/delegator/delegator.go:590-612`
- Search streaming 分支：`internal/querynodev2/segments/search.go:206-231`
- Query/Retrieve streaming 分支：`internal/querynodev2/segments/retrieve.go:125-160`

所以：

```text
写入 -> QueryNode 消费 -> growing segment
     -> Strong 等待 tSafe
     -> Search/Query 读取 growing segment
```

不需要经过：

```text
写入 -> flush -> seal -> index -> load sealed
```

后者主要属于持久化、segment 生命周期和查询优化流程。

## 6. 推荐的应用写法

### 6.1 按主键验证刚写入的 record

如果目标是验证 record 是否已经可见，应优先使用按主键的 Query，而不是用向量 Search 判断。

```text
insert(record)
    |
    | 等待 RPC 成功
    v
query(primary_key,
      consistency = Strong,
      mvcc_timestamp = 0,
      ignore_growing = false)
```

原因：

- Query 是按条件/主键取回 record；
- Search 还受向量距离、metric、topK 等影响；
- Search 没有返回该 record，不一定表示 record 不可见，也可能是它没有进入 topK。

### 6.2 使用 Session consistency

如果 SDK 能正确传播本次写入相关的时间戳，可以使用 Session consistency，让后续读携带足够新的 guarantee timestamp。

但必须确认 SDK 的 session 时间戳传播逻辑。仅靠服务端当前映射函数，不能证明所有 SDK 版本的传播细节。

### 6.3 不要忽略 growing

不要设置：

```text
ignore_growing = true
```

该参数会显式排除 growing segment，而刚写入的 record 通常正位于 growing segment。

源码：

- Search 参数解析：`internal/proxy/task_search.go:178-190`
- Query 参数解析：`internal/proxy/task_query.go:321-332`
- QueryNode 排除 growing：`internal/querynodev2/delegator/delegator.go:201-206`、`407-408`、`474-476`

## 7. 失败和延迟情况

即使使用 Strong，读取也可能失败或等待，原因包括：

- collection/partition 没有加载；
- QueryCoord 没有可用 shard leader；
- QueryNode DML consumer 停滞；
- TimeTick 没有推进；
- `tSafe` 与请求 `G` 的时间差超过 `MaxTimestampLag`；
- 请求 deadline 到期；
- channel 或 delegator 不再 serviceable。

`waitTSafe` 不是无限等待。它会在上下文取消、channel 不可服务或时间滞后超过限制时返回错误。

因此 Strong 的语义是：

```text
满足 freshness barrier 后返回最新可服务快照
```

而不是：

```text
无条件等待直到一定成功
```

## 8. 最终判断

对于“写一个 record 后马上读取，并要求读到最新 record”的场景，推荐采用：

```text
等待写入成功
    +
Strong 或正确传播的 Session consistency
    +
读取时不忽略 growing segment
    +
使用主键 Query 验证
```

完整语义可以概括为：

```text
write success
  -> DML enters MQ
  -> QueryNode consumes DML
  -> record enters growing segment
  -> QueryNode advances tSafe
  -> Strong read waits tSafe >= GuaranteeTimestamp
  -> MVCC Query reads growing + sealed data
```

这保证的是**写后读的新鲜度和可见性**，不等同于：

- 写入已经 flush 到对象存储；
- segment 已经 sealed；
- 索引已构建；
- 所有副本在同一时刻完成更新；
- 多个写读操作构成完整 ACID 事务。
