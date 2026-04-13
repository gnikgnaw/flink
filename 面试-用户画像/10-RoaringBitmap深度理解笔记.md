# RoaringBitmap 深度理解笔记

> 本文整理了学习 RoaringBitmap 过程中的核心问题与解答，涵盖基础原理、画像平台应用、Iceberg 中的应用，以及与数据集成的关联。

---

## 一、什么是 Bitmap

### 1.1 核心思想

用一个超长的 01 串表示集合，**每一位的位置代表一个元素 ID，值为 1 代表"存在"，0 代表"不存在"**。

```
用户ID:        0  1  2  3  4  5  6  7  8  9
"北京用户":     0  1  1  0  0  1  0  0  1  0
"高消费用户":   0  1  0  0  0  1  0  1  0  0
```

要找"北京 AND 高消费"的用户？直接做**位与（AND）运算**：

```
"北京用户":     0  1  1  0  0  1  0  0  1  0
"高消费用户":   0  1  0  0  0  1  0  1  0  0
————————————  AND  ——————————————————————————
结果:           0  1  0  0  0  1  0  0  0  0
→ 用户 1 和 5 同时是北京+高消费
```

### 1.2 为什么快

传统方案做两个集合的交集运算需要遍历比较，Bitmap 直接用 CPU 的**位运算指令**，一条指令同时处理 64 个元素（64 位 CPU）：

| 方案 | 1 亿用户求交集 | 内存占用 |
|------|--------------|---------|
| HashSet | ~10 秒 | ~2 GB |
| Bitmap AND | ~100 ms | ~12.5 MB（1 亿 bit） |

### 1.3 普通 Bitmap 的问题

如果用户 ID 范围是 0~100 亿，但实际只有 1 亿用户：

```
普通 Bitmap 需要 100 亿 bit = 1.25 GB → 大量空间浪费在值为 0 的位上
```

---

## 二、RoaringBitmap：压缩 Bitmap

### 2.1 核心思想：分桶 + 自适应容器

将 32 位整数拆分为两段：

```
32 位用户 ID
├── 高 16 位 → 桶号（最多 65536 个桶）
└── 低 16 位 → 桶内的值
```

每个桶根据数据密度**自动选择**最优存储方式：

| 容器类型 | 适用场景 | 原理 | 空间 |
|---------|---------|------|------|
| ArrayContainer | 元素少（< 4096 个） | 排序的 short 数组 | 2 × n 字节 |
| BitmapContainer | 元素多（≥ 4096 个） | 65536 bit 的位图 | 固定 8 KB |
| RunContainer | 连续区间多 | (start, length) 对 | 4 × runs 字节 |

### 2.2 为什么阈值是 4096

```
ArrayContainer 存 n 个元素 = 2n 字节（每个 short 占 2 字节）
BitmapContainer 固定 = 65536 bit = 8192 字节 = 8 KB

当 n < 4096 时：ArrayContainer（2n < 8192）更省空间
当 n ≥ 4096 时：BitmapContainer（固定 8KB）更省空间

4096 就是两者的交叉点：2 × 4096 = 8192 = 8KB
```

### 2.3 压缩效果对比

以 1 亿用户、ID 范围 0~100 亿为例：

| 方案 | 空间占用 |
|------|---------|
| 普通 Bitmap | 100 亿 bit = **1.25 GB** |
| RoaringBitmap | 只存有用户的桶，约 **5 MB** |
| HashSet\<Integer\> | 约 **2 GB**（对象头 + 哈希表开销） |

---

## 三、RoaringBitmap 在画像平台中的应用

### 3.1 Bitmap 怎么"存储"用户 ID

**Bitmap 不是把用户 ID 当数据存储，而是把用户 ID 当作"地址"——第 ID 号位置上放 1 表示"有这个用户"，放 0 表示"没有"。**

```
gender=男 的 Bitmap：

位置:  0  1  2  3  4  5  6  7  8  9
bit:   0  1  0  1  0  1  0  1  0  1
          ↑     ↑     ↑     ↑     ↑
        ID=1  ID=3  ID=5  ID=7  ID=9  这些用户是男性
```

`RoaringBitmap{1, 3, 5, 7, 9}` 的含义是"**第 1、3、5、7、9 位被设置为 1**"，不是存了一个数组。

### 3.2 预构建过程（每天凌晨 Spark 任务）

Spark 扫描 HBase/Hive 标签宽表，**逐行处理**：

```
第 1 行：user_id=1, gender=男, city=北京, consume_level=高
  → bitmap("gender=男").add(1)          // 把第 1 位设为 1
  → bitmap("city=北京").add(1)          // 把第 1 位设为 1
  → bitmap("consume_level=高").add(1)   // 把第 1 位设为 1

第 2 行：user_id=2, gender=女, city=北京, consume_level=低
  → bitmap("gender=女").add(2)          // 把第 2 位设为 1
  → bitmap("city=北京").add(2)          // 把第 2 位设为 1
  → bitmap("consume_level=低").add(2)   // 把第 2 位设为 1

第 3 行：user_id=3, gender=男, city=上海, consume_level=高
  → bitmap("gender=男").add(3)
  → bitmap("city=上海").add(3)
  → bitmap("consume_level=高").add(3)

...扫描完全部 1 亿行
```

Java 代码表达：

```java
// 每个 "标签=值" 对应一个 RoaringBitmap
Map<String, RoaringBitmap> tagBitmaps = new HashMap<>();

// Spark 扫描标签宽表，每读到一行：
void processRow(long userId, Map<String, String> tags) {
    for (Map.Entry<String, String> tag : tags.entrySet()) {
        String key = tag.getKey() + "=" + tag.getValue();  // 如 "city=北京"
        tagBitmaps
            .computeIfAbsent(key, k -> new RoaringBitmap())
            .add((int) userId);   // 把第 userId 位设为 1
    }
}
```

构建完成后序列化存入 Redis（或本地文件），key 就是 `"标签名=标签值"`。

**规模估算**（1 亿用户）：

- 50 个枚举标签 × 平均 5 个取值 = **250 个 Bitmap**
- 每个 RoaringBitmap 压缩后约 **5 MB**
- 总计 250 × 5 MB = **1.25 GB**，可以全部驻留内存

### 3.3 在线圈选（毫秒级位运算）

运营选了 **"北京 AND 高消费 AND 女性"**，系统执行三步位运算：

```
步骤1：取出 bitmap("city=北京")
步骤2：AND bitmap("consume_level=高")   → 得到"北京且高消费"的用户集合
步骤3：AND bitmap("gender=女")          → 得到"北京且高消费且女性"的用户集合

result.getCardinality() → 精确人数，返回给前端
```

1 亿用户、3 个 AND 条件，耗时约 **50ms**。

### 3.4 三种逻辑运算的对应关系

| 圈选逻辑 | Bitmap 运算 | 含义 | 示例 |
|---------|-----------|------|------|
| A **AND** B | `a.and(b)` | 交集 | 北京 **且** 高消费 |
| A **OR** B | `a.or(b)` | 并集 | 北京 **或** 上海 |
| A **AND NOT** B | `a.andNot(b)` | 差集 | 高消费 **但非** 会员 |
| **IN** (值1, 值2) | 多个 Bitmap 先 OR 再参与 AND | 枚举并集 | city IN (北京, 上海) |

IN 条件的处理代码：

```java
// IN 条件 → 先把多个值的 Bitmap 做 OR 并集
if (cond.getOperator() == Operator.IN) {
    RoaringBitmap union = new RoaringBitmap();
    for (String val : cond.getValues()) {
        String key = cond.getTagCode() + "=" + val;   // 如 "city=北京"
        RoaringBitmap bm = tagBitmaps.get(key);
        if (bm != null) {
            union.or(bm);   // 北京 OR 上海 = 两个城市的用户并集
        }
    }
    return union;
}
```

### 3.5 从结果 Bitmap 还原用户 ID

位运算得到结果 Bitmap 后，**位置为 1 的那些位，就是用户 ID**：

```java
RoaringBitmap result = bitmap("city=北京").clone();
result.and(bitmap("consume_level=高"));   // 北京 AND 高消费

// 方式1：获取人数
long count = result.getLongCardinality();  // 有多少个 1 → 人群数量

// 方式2：遍历出所有用户 ID
result.forEach((int userId) -> {
    System.out.println(userId);  // 位置为 1 的每个位 → 就是用户 ID
});
```

### 3.6 Bitmap 的局限与混合方案

```
                    能用 Bitmap                        不能用 Bitmap
                ┌──────────────────┐            ┌──────────────────────┐
                │ 枚举标签精确匹配   │            │ 数值范围查询          │
                │ city = '北京'     │            │ amount > 5000        │
                │ gender = '女'     │            │ （值域太大，无法枚举）  │
                ├──────────────────┤            ├──────────────────────┤
                │ 低基数标签        │            │ 高基数标签            │
                │ 城市（~300种）    │            │ 手机号（几亿种）       │
                │ 消费等级（3-5种） │            │ Bitmap 数量爆炸        │
                └──────────────────┘            └──────────────────────┘
```

画像平台的实际混合方案：

```
前端输入条件
     │
     ▼
条件分析器：拆分为枚举条件 + 数值条件
     │
     ├─── 全部枚举 ──→ Bitmap AND/OR（精确，< 100ms）
     │
     ├─── 全部数值 ──→ ClickHouse SQL（精确，< 1s）
     │
     └─── 混合条件 ──→ Bitmap 先缩小候选集 → 再用 ClickHouse 精确筛选
                        例如：先用 Bitmap 算出"北京+女性"= 200万人
                        再让 ClickHouse 在这 200万人中筛选 amount > 5000
```

### 3.7 人群包的存储与使用

圈选得到的 RoaringBitmap 就是**人群包**，按规模选择存储方式：

| 人群规模 | 存储方式 | 在线判断 | 空间 |
|---------|---------|---------|------|
| < 100 万 | Redis Set | `SISMEMBER` O(1) | 100 万 × 8B = 8 MB |
| 100 万~1000 万 | Redis Bitmap | `GETBIT` O(1) | 12.5 MB（覆盖 1 亿 ID 空间） |
| > 1000 万 | RoaringBitmap 序列化存 Redis + HDFS 文件 | 反序列化后 `contains()` | 10-20 MB（压缩后） |

更新策略：

| 策略 | 适用场景 | 实现 |
|------|---------|------|
| 手动更新 | 一次性活动人群 | 运营手动触发 |
| 每天 T+1 | 持续运营人群（如"高价值用户"） | 凌晨 Spark 任务重算 |
| 实时更新 | 实时营销 | Flink 监听标签变化，实时增删 Bitmap 中的用户 ID |

### 3.8 隐含前提：ID-Mapping 与 One ID 体系

Bitmap 能工作的前提是**用户 ID 必须是整数**。但现实中同一个用户在不同渠道有不同的 ID，而且很多不是整数：

```
同一个人，分散在不同系统中：
  App 登录 UID:     100001        （整数）
  手机号:           13800001234   （字符串）
  设备 ID (IDFA):   abc-def-123   （字符串）
  微信 OpenID:      wx_xxxx       （字符串）
  Cookie ID:        cookie_yyyy   （字符串）

问题：
  ① 这些 ID 怎么知道是同一个人？
  ② 字符串 ID 怎么放进 Bitmap？
```

#### 什么是 ID-Mapping

ID-Mapping 就是**把同一个自然人在不同系统中的多个 ID 关联到一个统一的整数 ID（OneID）上**。

```
手机号 138xxxx    ──┐
设备ID abc-def    ──├──→ OneID: 42（全局唯一整数）
微信openid wx_xx  ──┤
App UID 100001    ──┘

这个 OneID = 42 才是 Bitmap 中的"位置"
所有标签、行为数据都挂在 OneID 下面
```

#### 两种匹配方式

| 方式 | 原理 | 准确率 | 适用场景 |
|------|------|--------|---------|
| **确定性匹配** | 用户登录/注册行为建立绑定 | ~100% | 主要方式 |
| **概率性匹配** | 设备指纹、IP、行为特征推断 | 80-90% | 补充手段 |

确定性匹配示例：
```
用户用手机号 138xxx 在 App 登录 → UID=100001 与手机号 138xxx 绑定
同一手机号在微信小程序登录     → OpenID=wx_xxxx 与手机号 138xxx 绑定
→ UID 和 OpenID 通过手机号这个"桥梁"关联到同一个 OneID
```

#### 数据模型

```sql
-- ID 映射表
CREATE TABLE id_mapping (
    one_id      BIGINT      COMMENT '全局唯一用户ID（整数，用于 Bitmap）',
    id_type     VARCHAR(20) COMMENT 'PHONE / UID / DEVICE_ID / OPENID / COOKIE',
    id_value    VARCHAR(128) COMMENT 'ID 值',
    confidence  DOUBLE      COMMENT '置信度 0-1（确定性=1.0，概率性<1.0）',
    bind_time   DATETIME    COMMENT '绑定时间',
    source      VARCHAR(50) COMMENT '绑定来源：LOGIN / REGISTER / INFER',
    PRIMARY KEY (id_type, id_value),
    INDEX idx_one_id (one_id)
);

-- 查询：通过任意 ID 找到 OneID
SELECT one_id FROM id_mapping
WHERE id_type = 'OPENID' AND id_value = 'wx_xxxx';
-- 返回 one_id = 42 → 就是 Bitmap 中第 42 位
```

#### 设计准则

**1. 确定性优先，概率性补充**

```
核心 ID 的信任度排序：
  App 登录 UID > 手机号 > 设备 ID > Cookie ID > IP

以确定性匹配（登录绑定）为主要手段，准确率接近 100%。
概率性匹配（设备指纹推断）仅作为补充，且要标记置信度。
```

**2. 选择一个"锚点 ID"作为关联桥梁**

```
通常选手机号作为核心 ID（锚点）：
  - 用户注册/登录时几乎都要手机号
  - 跨端跨应用的唯一稳定标识
  - 手机号 → 生成 OneID

不选设备 ID 的原因：
  - 一台设备可能多人使用（如家庭 iPad）
  - 换手机后设备 ID 变了，但手机号不变

不选身份证号的原因（三个层面）：

  法律层面（最大阻碍）：
    身份证号属于《个人信息保护法》第28条定义的"敏感个人信息"
    处理需要：① 取得用户"单独同意" ② 必须有"特定目的和充分必要性" ③ 影响评估并报备
    画像平台目的是"精准营销、人群圈选"→ 很难论证"为了推送优惠券需要采集身份证号"
    → 合规风险极高

  覆盖率层面（实际拿不到）：
    手机号覆盖率：几乎 100%（注册/登录都要）
    身份证号覆盖率：可能只有 10-30%
    能拿到的场景：金融类（实名认证）、出行类（买机票火车票）、政务类
    拿不到的场景：电商（买外卖不需要身份证）、社交、内容类 App
    大部分用户没有身份证号 → ID-Mapping 覆盖率崩塌

  技术层面（也没优势）：
    身份证号 330102199001011234（18位字符串，最后一位可能是 X）
    → 不是整数，不能直接放 Bitmap，仍需映射成 OneID
    → 相比手机号（11位纯数字）没有额外优势

  什么场景下会用身份证号：
    金融风控和政务场景 — 法律要求实名认证，身份证号是必须采集的
    但即使金融场景，锚点优先级通常也是：手机号（高频、100%覆盖）> 身份证号（低频但权威）
    实际做法：手机号做日常关联桥梁，身份证号做最终身份确认

  总结：身份证号"权威性最高但可用性最低"
        手机号是"权威性和可用性之间的最佳平衡点"
```

**3. OneID 必须是整数且尽量连续**

```
这是 Bitmap 能高效工作的前提。

OneID 生成策略：
  方案 A：数据库自增 ID（简单，天然连续）
  方案 B：雪花算法（分布式，但值域大，Bitmap 空间利用率低）
  方案 C：自增序列 + 分布式协调（推荐，兼顾连续性和分布式）

如果 OneID 不连续（如用雪花算法），会导致 RoaringBitmap 中
大量桶只有稀疏的 ArrayContainer，压缩效果变差。
```

**4. 标签合并规则要明确**

```
两个 OneID 发现是同一人时，需要合并标签。
不同类型的标签合并策略不同：

  事实标签（性别、注册时间）→ 取最新值
  统计标签（订单数、消费金额）→ 求和
  算法标签（购买意向分）→ 丢弃旧值，重新计算
```

#### 注意事项与踩坑点

**1. 手机号换绑 — ID-Mapping 中最棘手的问题**

```
场景：用户 A 换了手机号
  旧手机号 138xxx → OneID=42
  新手机号 139yyy → 绑定到 OneID=42

问题：如果 139yyy 之前已经绑过另一个 OneID=99，怎么办？

处理：
  ① 旧手机号 138xxx 与 OneID=42 的绑定标记为"已失效"（保留历史）
  ② 新手机号 139yyy 绑定到 OneID=42
  ③ 如果 139yyy 之前绑了 OneID=99 → 触发合并流程
  ④ 合并原则：UID（App 登录 ID）权重最高
     因为手机号可以换、设备可以换，但 App 账号通常稳定

Bitmap 层面的影响：
  OneID=99 对应的位要从所有 Bitmap 中清除
  OneID=42 对应的位要更新（可能需要合并标签后重算）
```

**2. 一设备多用户 — 不能用设备 ID 做主 ID**

```
家庭 iPad 上爸爸和孩子都在用同一个 App：
  设备 ID = device_001
  爸爸 UID = 100001
  孩子 UID = 100002

如果用设备 ID 做主 ID → 爸爸和孩子被误判为同一人
→ 标签混乱（消费能力、兴趣偏好全部混在一起）

正确做法：以登录 UID 为主，设备 ID 只作为辅助关联
```

**3. 合并风暴 — 大量 OneID 的级联合并**

```
极端场景：
  OneID=1 和 OneID=2 因手机号发现是同一人 → 合并
  合并后的 OneID=1 又和 OneID=3 因设备 ID 关联 → 再合并
  → 级联效应，可能把不相关的用户错误合并

防护措施：
  ① 设置单个 OneID 绑定 ID 数量上限（如最多 20 个）
  ② 合并前校验：如果合并后的用户画像出现矛盾
    （如一个人同时是男和女），触发人工审核
  ③ 低置信度的概率性匹配不参与合并决策
```

**4. ID-Mapping 表的查询性能**

```
id_mapping 表是高频查询表（每次标签查询都要先查 OneID）

优化：
  ① Redis 缓存热点映射关系（TTL 24h）
  ② 主键设计：(id_type, id_value) 联合主键，精确查询 O(1)
  ③ 二级索引 idx_one_id：支持反向查询（通过 OneID 查所有关联 ID）
```

**5. 隐私合规 — 最容易被忽视的风险**

```
ID-Mapping 表本质上是一张"用户身份关联表"，敏感度极高。

合规要求：
  ① 手机号等 PII（个人可识别信息）必须加密存储
  ② 第三方数据（运营商数据、征信数据）接入需要用户授权
  ③ 用户注销时，id_mapping 中的关联关系必须物理删除
  ④ GDPR/个人信息保护法要求：用户有权要求导出和删除所有关联数据
```

#### 与 Bitmap 的关系串联

```
完整链路：

用户行为/属性数据
    │
    ▼
ID-Mapping 层：把各种 ID 统一为 OneID（整数）
    │
    ▼
标签计算层：基于 OneID 计算标签（Spark/Flink）
    │
    ▼
Bitmap 预构建：bitmap("city=北京").add(oneId)
    │                                  ↑
    │                       这里的 oneId 就是整数
    ▼
在线圈选：Bitmap AND/OR 运算 → 秒级返回人数
```

ID-Mapping 是 Bitmap 方案的**上游基础设施**。没有 One ID 体系：
- 字符串 ID 无法放进 Bitmap → Bitmap 圈选不可用
- 多端数据无法合并 → 标签不完整 → 圈选结果不准
- 同一用户被算作多人 → 人群数量虚高

---

## 四、RoaringBitmap 在 Iceberg V3 中的应用

### 4.1 解决什么问题

Iceberg V2 的行级删除有性能问题：

```
V2 做法（Position Delete Files）：
  删除 data file 中的第 3、7、100 行
  → 生成一个 Parquet 文件：[(file_path, 3), (file_path, 7), (file_path, 100)]

问题：
  ① 频繁 UPDATE/DELETE 产生大量小 delete file
  ② 查询时要 join delete files 和 data file，开销大
  ③ 1 亿条删除记录 = 1 亿个 int64 = 800 MB 内存
```

### 4.2 V3 的 Deletion Vectors

**每个 data file 关联一个 RoaringBitmap，第 i 位为 1 表示第 i 行被删除**：

```
Data File: orders_001.parquet（10000 行）

删除了第 3、7、100 行：

Deletion Vector（RoaringBitmap）：
  第 3 位 = 1
  第 7 位 = 1
  第 100 位 = 1
  其余 = 0

查询时：扫描 data file，每读一行检查 bitmap → O(1) 跳过被删除的行
```

Bitmap 序列化后存储在 **Puffin 文件**（Iceberg 专用的二进制辅助文件格式）中。

### 4.3 Iceberg 的 64 位支持

Iceberg 的行位置是 64 位整数，但 RoaringBitmap 原生只支持 32 位。Iceberg 的实现（`RoaringPositionBitmap`）：

```
64 位 position 拆分为：
  高 32 位 → key（桶号）
  低 32 位 → sub-position

每个 key 对应一个 32 位的 RoaringBitmap
```

### 4.4 V2 vs V3 对比

| 对比维度 | V2 Position Delete Files | V3 Deletion Vectors (RoaringBitmap) |
|---------|------------------------|-------------------------------------|
| 存储格式 | Parquet 文件（逐行枚举被删位置） | Puffin 文件中的压缩 Bitmap |
| 每个 data file | 可能关联多个 delete file | **最多一个** deletion vector |
| 查询开销 | join delete files 和 data file | 按位检查 O(1) |
| 1 亿条删除 | ~800 MB | 压缩后可能只有几 MB |
| 小文件问题 | 频繁删除产生大量小文件 | 无此问题 |

### 4.5 画像 vs Iceberg：方向相反，思想一致

| 维度 | 画像平台 | Iceberg |
|------|---------|---------|
| bit=1 的含义 | 用户**属于**某个人群 | 某行**被删除了** |
| 用途 | 圈选（inclusion） | 过滤（exclusion） |
| 共同抽象 | 用一个 bit 高效表示某个 ID/位置的布尔状态 | 同左 |

---

## 五、与数据集成平台的关联

### 5.1 增量同步中的 Bitmap

数据集成平台做增量同步时，可以用 Bitmap 标记"已同步的记录 ID"：

```
已同步 Bitmap：RoaringBitmap{1, 2, 3, 5, 8, ...}
全量 ID Bitmap：RoaringBitmap{1, 2, 3, 4, 5, 6, 7, 8, ...}

未同步 = 全量.andNot(已同步)
       = RoaringBitmap{4, 6, 7, ...}
```

避免每次全量比对，效率远高于逐条查询。

### 5.2 集成 Iceberg 表时的注意点

如果数据集成平台以 Iceberg 表作为数据源，读取时需要正确处理 Deletion Vector——**不能只读 data file，还要读对应的 Puffin 文件中的 RoaringBitmap，跳过被标记删除的行**。

---

## 六、面试完整回答模板

### 问：画像平台如何实现亿级用户秒级圈选？

> 核心方案是 **RoaringBitmap 预计算 + ClickHouse 兜底**。
>
> 每天凌晨 Spark 任务扫描标签宽表，为每个枚举标签值（如 city=北京）构建 RoaringBitmap——把属于该标签值的所有用户 ID 对应的位设为 1。1 亿用户的全部 Bitmap 压缩后约 1.25 GB，完全可以驻留内存。
>
> 在线圈选时，将 AND/OR/NOT 条件转换为 Bitmap 的交集/并集/差集运算，3 个条件的圈选只需约 50ms。对于 Bitmap 无法处理的数值范围查询（如 amount > 5000），降级到 ClickHouse 列式查询。混合条件时先用 Bitmap 缩小候选集，再交给 ClickHouse 精确筛选。
>
> 选 RoaringBitmap 而非普通 Bitmap，是因为用户 ID 不连续——普通 Bitmap 对 100 亿 ID 空间需要 1.25 GB，RoaringBitmap 通过分桶 + 自适应容器（ArrayContainer/BitmapContainer/RunContainer），压缩到只有几 MB。

### 问：RoaringBitmap 的 Bitmap 里是怎么存用户 ID 的？

> Bitmap 不是"存"用户 ID，而是**用位置表示用户 ID**。第 i 位为 1 代表"用户 ID=i 属于这个集合"。构建时对标签宽表逐行扫描，执行 `bitmap.add(userId)` 把对应位设为 1。查询结果也一样——结果 Bitmap 中哪些位是 1，那些位的位置就是被圈选中的用户 ID。
>
> 这有一个隐含前提：用户 ID 必须是整数。如果是 UUID 等字符串 ID，需要先通过 ID-Mapping 层映射成连续整数。
