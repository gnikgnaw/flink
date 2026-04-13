# Apache Spark vs Apache Flink 对比分析

## 1. 核心设计哲学

| 维度 | Spark | Flink |
|------|-------|-------|
| 计算模型 | 微批处理（Micro-batch） | 原生流处理（True Streaming） |
| 批流关系 | 流是批的特例 | 批是流的特例（有限流） |
| 核心抽象 | RDD / DataFrame / Dataset | DataStream / DataSet |

---

## 2. 流处理延迟

- **Spark Streaming**：基于 DStream，将流切分为小批次（秒级），延迟通常在 **秒级**
- **Structured Streaming**：延迟可降至 100ms 左右，但仍是微批
- **Flink**：逐条处理事件，延迟可达 **毫秒级**，适合对实时性要求极高的场景

---

## 3. 状态管理

### Spark
- 状态管理相对简单，通过 `updateStateByKey` / `mapGroupsWithState`
- 状态恢复依赖 Checkpoint（较重量级）

### Flink
- 原生支持丰富的状态类型：`ValueState`、`ListState`、`MapState`、`ReducingState`
- 状态后端可选：Memory、RocksDB（适合超大状态）
- 细粒度增量 Checkpoint，恢复速度更快

---

## 4. 容错机制

| 机制 | Spark | Flink |
|------|-------|-------|
| 核心原理 | RDD 血缘重算（Lineage） | 分布式快照（Chandy-Lamport 算法） |
| Exactly-Once | Structured Streaming 支持 | 原生支持，含两阶段提交 Sink |
| 恢复粒度 | 整个 Stage 重算 | 从最近 Checkpoint 精确恢复 |

---

## 5. 时间语义

- **Spark**：处理时间为主，事件时间支持有限
- **Flink**：三种时间语义完整支持
  - **Event Time**（事件时间）：基于数据本身的时间戳
  - **Processing Time**（处理时间）：算子处理时的系统时间
  - **Ingestion Time**（摄入时间）：数据进入 Flink 的时间
- Flink 的 **Watermark 机制**是处理乱序事件的业界标准方案

---

## 6. 生态与适用场景

| 场景 | 推荐 | 原因 |
|------|------|------|
| 大规模批处理 / ETL | **Spark** | 成熟的 SQL、DataFrame API，Shuffle 优化好 |
| 机器学习 / 图计算 | **Spark** | MLlib、GraphX 生态丰富 |
| 低延迟流处理 | **Flink** | 毫秒级延迟，精确状态管理 |
| 复杂事件处理（CEP） | **Flink** | 内置 FlinkCEP 库 |
| 实时数仓 / 流批一体 | **Flink** | SQL 流批统一，与 Hive 集成好 |

---

## 7. 一句话总结

> **Spark** 是批处理起家、兼顾流处理的通用大数据引擎，生态最成熟；
> **Flink** 是流处理原生设计、兼顾批处理的实时计算引擎，延迟最低、状态最强。
