# Flink SQL 专题（数据平台架构师视角）

> **面向岗位**：数据平台架构师
> **核心价值**：SQL执行原理、查询优化、实时数仓SQL开发能力
> **文档版本**：v1.0
> **基于版本**：Flink 1.18

---

## 第十一部分：Flink SQL 专题

### Q1: Flink SQL 执行流程

**【一句话总结】**
Flink SQL 执行流程为：SQL文本 → Calcite解析(AST) → 验证(Validator) → 逻辑计划(RelNode) → 优化(Optimizer) → 物理计划(ExecNode) → Transformation → StreamGraph → 执行。

**【详细解析】**

#### 1. 完整执行流程

```
SQL Statement → CalciteParser → SqlNode AST
                                    │
                                    ▼
SqlNode AST → FlinkPlanner(Validator) → RelNode(Logical Plan)
                                            │
                                            ▼
Logical Plan → Optimizer(Rule-Based) → Physical Plan
                                            │
                                            ▼
Physical Plan → ExecNodeGraphGenerator → Transformation DAG
                                              │
                                              ▼
Transformation DAG → Executor → StreamGraph → 执行
```

#### 2. 核心组件源码

**ParserImpl - SQL解析入口**：
```java
// 文件：flink-table/flink-table-planner/.../ParserImpl.java（第91-108行）
@Override
public List<Operation> parse(String statement) {
    CalciteParser parser = calciteParserSupplier.get();
    FlinkPlannerImpl planner = validatorSupplier.get();

    // 1. 尝试解析扩展命令(SHOW/SET等)
    Optional<Operation> command = EXTENDED_PARSER.parse(statement);
    if (command.isPresent()) {
        return Collections.singletonList(command.get());
    }

    // 2. 使用Calcite解析SQL为SqlNode AST
    SqlNodeList sqlNodeList = parser.parseSqlList(statement);

    // 3. 将SqlNode转换为Operation
    return translate(sqlNodeList, planner);
}
```

**PlannerBase - 核心翻译流程**：
```scala
// 文件：flink-table/flink-table-planner/.../PlannerBase.scala（第174-187行）
def translate(modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
    // 1. 转换为RelNode（逻辑计划）
    val relNodes = modifyOperations.map(translateToRel)

    // 2. 优化（应用优化规则）
    val optimizedRelNodes = optimize(relNodes)

    // 3. 转换为ExecNode图
    val execGraph = translateToExecNodeGraph(optimizedRelNodes)

    // 4. 转换为Transformation
    translateToPlan(execGraph)
}
```

#### 3. 各阶段说明

| 阶段 | 输入 | 输出 | 核心类 |
|------|------|------|--------|
| 解析 | SQL文本 | SqlNode AST | `CalciteParser` |
| 验证 | SqlNode | 验证后的SqlNode | `FlinkPlannerImpl` |
| 逻辑优化 | RelNode | 优化后的RelNode | `Optimizer` |
| 物理转换 | RelNode | FlinkPhysicalRel | `Optimizer` |
| 执行图生成 | PhysicalRel | ExecNodeGraph | `ExecNodeGraphGenerator` |

**【架构思考】**

**为什么使用 Calcite？**
- 成熟的 SQL 解析器和优化器框架
- 支持标准 SQL 语法
- 可扩展的架构

---

### Q2: Catalog 与 Metadata 管理

**【一句话总结】**
Catalog 是 Flink SQL 元数据的顶层容器，管理 Database → Table → Column 的层级关系，支持多种 Catalog（内存、Hive、Iceberg）。

**【详细解析】**

#### 1. 元数据层级

```
CatalogManager
├── default_catalog (GenericInMemoryCatalog)
│   └── default_db
│       └── my_table
└── hive_catalog (HiveCatalog)
    └── my_database
        └── hive_table
```

#### 2. 常用 Catalog 类型

| Catalog 类型 | 说明 | 使用场景 |
|-------------|------|---------|
| `GenericInMemoryCatalog` | 内存存储 | 开发测试 |
| `HiveCatalog` | Hive Metastore | 与 Hive 集成 |
| `JdbcCatalog` | 关系型数据库 | 与 MySQL/PG 集成 |
| `IcebergCatalog` | Iceberg 元数据 | 数据湖场景 |

#### 3. SQL 操作示例

```sql
-- 创建 Hive Catalog
CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'hive-conf-dir' = '/etc/hive/conf'
);

-- 切换 Catalog
USE CATALOG hive_catalog;
USE my_database;

-- 查看表
SHOW TABLES;
DESCRIBE my_table;
```

---

### Q3: SQL 查询优化器原理

**【一句话总结】**
Flink SQL 优化器基于 Calcite，采用 Rule-Based + Cost-Based 混合优化策略，流处理还有特殊的 Changelog 处理规则。

**【常见优化规则】**

| 规则类型 | 规则名称 | 作用 |
|---------|---------|------|
| **逻辑优化** | `ProjectMergeRule` | 合并连续的投影 |
| | `FilterPushDownRule` | 谓词下推 |
| | `JoinReorderRule` | JOIN 重排序 |
| **物理优化** | `StreamPhysicalHashAggRule` | 选择 Hash 聚合 |
| | `StreamPhysicalLookupJoinRule` | 选择 Lookup JOIN |
| **流处理特有** | `ChangelogNormalizeRule` | Changelog 标准化 |

**优化配置**：
```sql
SET 'table.optimizer.join-reorder-enabled' = 'true';
SET 'table.optimizer.multiple-input-enabled' = 'true';

-- 查看执行计划
EXPLAIN SELECT * FROM orders JOIN users ON orders.user_id = users.id;
```

---

### Q4: 动态表与流表转换

**【一句话总结】**
动态表是 Flink SQL 对流数据的抽象，流通过 Append/Retract/Upsert 三种模式转换为动态表。

**【ChangelogMode 模式】**

| 模式 | 包含的 RowKind | 说明 | 典型场景 |
|------|---------------|------|---------|
| `INSERT_ONLY` | +I | 仅追加 | 日志、Kafka |
| `UPSERT` | +I, +U, -D | 基于主键更新 | CDC |
| `ALL` | +I, -U, +U, -D | 完整变更日志 | 需要回撤的聚合 |

**示例**：
```sql
-- 聚合查询产生 Retract 流
SELECT
    TUMBLE_START(log_time, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS cnt
FROM logs
GROUP BY TUMBLE(log_time, INTERVAL '1' MINUTE);
-- 迟到数据会产生: -U(old_result), +U(new_result)
```

---

### Q5: SQL 维表 JOIN 实现原理

**【一句话总结】**
维表 JOIN（Lookup Join）是流表与外部维表的关联，通过 LookupTableSource + LookupJoinRunner 实现，支持同步/异步查询、缓存优化。

**【SQL 示例】**

```sql
-- 定义维表（JDBC）
CREATE TABLE dim_user (
    user_id BIGINT,
    user_name STRING,
    user_level STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/dim',
    'table-name' = 'user',
    'lookup.cache.max-rows' = '10000',
    'lookup.cache.ttl' = '1h',
    'lookup.max-retries' = '3'
);

-- Lookup Join（注意 FOR SYSTEM_TIME AS OF 语法）
SELECT
    o.order_id,
    o.user_id,
    o.amount,
    u.user_name,
    u.user_level
FROM orders AS o
LEFT JOIN dim_user FOR SYSTEM_TIME AS OF o.proc_time AS u
    ON o.user_id = u.user_id;
```

**【优化配置】**

| 配置项 | 说明 | 建议值 |
|--------|------|--------|
| `lookup.cache.max-rows` | 缓存最大行数 | 根据维表大小 |
| `lookup.cache.ttl` | 缓存过期时间 | 根据更新频率 |
| `lookup.max-retries` | 查询重试次数 | 3 |
| `lookup.async` | 是否异步查询 | true |

**【架构思考】**

**同步 vs 异步 Lookup**：
- 同步：简单，吞吐受限于查询延迟
- 异步：复杂，吞吐更高（并发查询）

**缓存策略**：
- 全量缓存：维表小，更新少
- LRU 缓存：维表大，热点集中
- 无缓存：维表更新频繁

---

## 小结：Flink SQL 核心知识点

### SQL 执行流程
```
SQL → Parse → Validate → Optimize → Physical Plan → Transformation → Execute
```

### 关键优化点
1. **谓词下推**：减少扫描数据量
2. **JOIN 优化**：选择合适的 JOIN 策略
3. **Changelog 处理**：正确处理 Upsert/Retract

### 常见问题
| 问题 | 原因 | 解决方案 |
|------|------|---------|
| SQL 执行慢 | 没有谓词下推 | 优化 WHERE 条件 |
| 维表 JOIN 超时 | 无缓存/同步查询 | 启用缓存+异步 |
| 数据不一致 | ChangelogMode 不匹配 | 检查源表模式 |
