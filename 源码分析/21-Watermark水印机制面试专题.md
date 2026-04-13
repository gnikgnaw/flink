# Watermark 水印机制面试专题

> 基于 Apache Flink 1.18 源码，从底层原理到生产应用的完整专题

---

## 一、底层原理：Watermark 到底是什么

### 1.1 Watermark 的本质

Watermark 是一个**单调递增的时间戳 T**，它嵌入在数据流中随数据一起流动，向系统声明一个语义承诺：

> **不会再有事件时间 ≤ T 的数据到来**

这就是 Watermark 的全部本质。一切关于窗口触发、迟到数据处理、多流对齐的复杂行为，都建立在这一条简单承诺之上。

**源码定义**（`flink-core/.../eventtime/Watermark.java`）：

```java
public final class Watermark implements Serializable {
    public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);  // 流结束标记
    public static final Watermark UNINITIALIZED = new Watermark(Long.MIN_VALUE);  // 初始状态

    private final long timestamp;  // 毫秒时间戳

    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }
}
```

在运行时传输层，Watermark 是 `StreamElement` 的子类型，和 `StreamRecord`（数据记录）、`WatermarkStatus`（空闲/活跃状态标记）并列，在网络中以相同的序列化框架传输：

```
StreamElement（抽象基类）
├── StreamRecord<T>      ← 数据记录（携带 value + timestamp）
├── Watermark            ← 水印（携带 timestamp）
├── WatermarkStatus      ← 空闲/活跃状态（IDLE / ACTIVE）
└── LatencyMarker        ← 延迟测量标记
```

### 1.2 为什么需要 Watermark

流处理面临的根本挑战：**无法知道数据是否已经"到齐"**。

| 场景 | 不用 Watermark 的后果 |
|------|----------------------|
| 窗口 [10:00, 10:05) 何时触发？ | 无法判断，永远等下去或随意触发 |
| 10:03 的数据在 10:06 才到达 | 要么丢弃，要么无限期缓存所有窗口 |
| Kafka 某个 Partition 5 分钟没有数据 | 整个作业的事件时间进度卡住 |

Watermark 的解决思路：**用可容忍的不精确性换取系统的可操作性**。通过引入一个允许的乱序上界，在"数据完整性"和"处理及时性"之间取得平衡。

### 1.3 三种时间语义对比

| 时间语义 | 定义 | 是否需要 Watermark | 适用场景 |
|----------|------|-------------------|----------|
| Event Time | 事件真实发生的时间 | 需要 | 大多数业务场景（准确性要求高） |
| Processing Time | 事件被处理的时间 | 不需要 | 对准确性要求低、追求最低延迟 |
| Ingestion Time | 事件进入 Flink 的时间 | 不需要 | 折中方案（已弃用） |

---

## 二、水印生成机制：源码级剖析

### 2.1 核心接口体系

水印机制的接口设计分为三层：

```
WatermarkStrategy<T>（顶层策略，用户入口）
├── createWatermarkGenerator() → WatermarkGenerator<T>（水印生成逻辑）
├── createTimestampAssigner()  → TimestampAssigner<T>（时间戳提取逻辑）
├── withIdleness()             → 包装空闲检测能力
└── withWatermarkAlignment()   → 包装跨源对齐能力
```

#### WatermarkStrategy —— 策略工厂

**文件**：`flink-core/.../eventtime/WatermarkStrategy.java`

```java
@Public
public interface WatermarkStrategy<T>
        extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T> {

    // ====== 必须实现 ======
    WatermarkGenerator<T> createWatermarkGenerator(Context context);

    // ====== 可选重写（默认使用记录自带时间戳）======
    default TimestampAssigner<T> createTimestampAssigner(Context context) {
        return new RecordTimestampAssigner<>();
    }

    // ====== 可选重写（水印对齐，默认禁用）======
    default WatermarkAlignmentParams getAlignmentParameters() {
        return WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED;
    }

    // ====== 内置工厂方法 ======
    static <T> WatermarkStrategy<T> forMonotonousTimestamps() { ... }
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) { ... }
    static <T> WatermarkStrategy<T> noWatermarks() { ... }

    // ====== 装饰器方法 ======
    default WatermarkStrategy<T> withTimestampAssigner(...) { ... }
    default WatermarkStrategy<T> withIdleness(Duration idleTimeout) { ... }
    default WatermarkStrategy<T> withWatermarkAlignment(...) { ... }
}
```

#### WatermarkGenerator —— 生成器核心

**文件**：`flink-core/.../eventtime/WatermarkGenerator.java`

```java
@Public
public interface WatermarkGenerator<T> {
    /**
     * 每条数据到达时调用。
     * 生成器可以在此记录时间戳，或立即发射水印（Punctuated 模式）
     */
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);

    /**
     * 周期性调用（间隔由 ExecutionConfig.getAutoWatermarkInterval() 决定，默认 200ms）。
     * 在此决定是否发射新水印（Periodic 模式）
     */
    void onPeriodicEmit(WatermarkOutput output);
}
```

#### TimestampAssigner —— 时间戳提取器

**文件**：`flink-core/.../eventtime/TimestampAssigner.java`

```java
@FunctionalInterface
public interface TimestampAssigner<T> {
    long NO_TIMESTAMP = Long.MIN_VALUE;

    /**
     * 从元素中提取事件时间戳（毫秒）
     * @param element 数据元素
     * @param recordTimestamp 记录可能已携带的时间戳（如 Kafka 记录时间戳）
     */
    long extractTimestamp(T element, long recordTimestamp);
}
```

### 2.2 内置水印生成器

#### (1) BoundedOutOfOrdernessWatermarks —— 有界乱序（最常用）

**文件**：`flink-core/.../eventtime/BoundedOutOfOrdernessWatermarks.java`

```java
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {
    private long maxTimestamp;
    private final long outOfOrdernessMillis;

    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        // 初始化使得初始水印为 Long.MIN_VALUE
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);  // 只记录最大时间戳
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 水印 = 最大时间戳 - 最大乱序度 - 1
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}
```

**为什么要减 1？** 因为 Watermark T 的语义是"不会再有时间戳 **≤ T** 的数据"。如果 `maxTimestamp = 100s`，`outOfOrderness = 5s`，那么水印应该是 `94s`，表示 `≤ 94s` 的数据已经全部到齐，但 `95s` 的数据可能还没到。

**数值推演**：

```
事件到达序列: 10s, 12s, 8s, 15s, 9s, 20s（maxOutOfOrderness = 5s）

事件  maxTimestamp  Watermark = max - 5 - 1
10s   10s          4s
12s   12s          6s
8s    12s          6s    ← maxTimestamp 不变，水印不变
15s   15s          9s
9s    15s          9s    ← 9s > 水印(9s) 不成立，9s 不算迟到
20s   20s          14s

说明：8s 到达时水印是 6s（8 > 6），所以 8s 不是迟到数据。
```

#### (2) AscendingTimestampsWatermarks —— 单调递增

**文件**：`flink-core/.../eventtime/AscendingTimestampsWatermarks.java`

```java
public class AscendingTimestampsWatermarks<T> extends BoundedOutOfOrdernessWatermarks<T> {
    public AscendingTimestampsWatermarks() {
        super(Duration.ofMillis(0));  // 乱序度 = 0，等于 BoundedOutOfOrderness 的特例
    }
}
```

本质上就是 `BoundedOutOfOrdernessWatermarks(0)`，水印 = `maxTimestamp - 1`。

#### (3) WatermarksWithIdleness —— 空闲检测包装器

**文件**：`flink-core/.../eventtime/WatermarksWithIdleness.java`

```java
public class WatermarksWithIdleness<T> implements WatermarkGenerator<T> {
    private final WatermarkGenerator<T> watermarks;  // 被包装的生成器
    private final IdlenessTimer idlenessTimer;
    private boolean isIdleNow = false;

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        watermarks.onEvent(event, eventTimestamp, output);
        idlenessTimer.activity();   // 标记有活动
        isIdleNow = false;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (idlenessTimer.checkIfIdle()) {
            if (!isIdleNow) {
                output.markIdle();  // 超时无数据，标记为空闲
                isIdleNow = true;
            }
            // 空闲后不再发射水印
        } else {
            watermarks.onPeriodicEmit(output);  // 正常发射水印
        }
    }
}
```

**IdlenessTimer 的三阶段检测逻辑**：

```java
static final class IdlenessTimer {
    private long counter;              // 活动计数器
    private long lastCounter;          // 上次检查时的计数器值
    private long startOfInactivityNanos;
    private final long maxIdleTimeNanos;

    public void activity() { counter++; }  // 有事件时调用

    public boolean checkIfIdle() {
        if (counter != lastCounter) {
            // 阶段1：有新活动，重置
            lastCounter = counter;
            startOfInactivityNanos = 0L;
            return false;
        } else if (startOfInactivityNanos == 0L) {
            // 阶段2：首次发现无活动，开始计时
            startOfInactivityNanos = clock.relativeTimeNanos();
            return false;
        } else {
            // 阶段3：检查是否超过空闲阈值
            return clock.relativeTimeNanos() - startOfInactivityNanos > maxIdleTimeNanos;
        }
    }
}
```

### 2.3 两种水印生成模式

| 模式 | 触发方式 | 实现方式 | 适用场景 |
|------|----------|----------|----------|
| **Periodic**（周期性） | Processing Time 定时器触发 | 在 `onPeriodicEmit()` 中发射 | 绝大多数场景（默认） |
| **Punctuated**（标点式） | 每条事件触发 | 在 `onEvent()` 中直接发射 | 事件自身携带水印标记 |

**Periodic 模式**是通过 `ProcessingTimeCallback` 驱动的（源码 `TimestampsAndWatermarksOperator.java:108-113`）：

```java
@Override
public void onProcessingTime(long timestamp) throws Exception {
    watermarkGenerator.onPeriodicEmit(wmOutput);   // 调用生成器
    final long now = getProcessingTimeService().getCurrentProcessingTime();
    getProcessingTimeService().registerTimer(now + watermarkInterval, this);  // 注册下一次
}
```

**Punctuated 模式**示例——自定义实现：

```java
public class PunctuatedWatermarkGenerator implements WatermarkGenerator<MyEvent> {
    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
        if (event.isWatermarkMarker()) {
            // 数据中包含特殊的水印标记事件，直接发射
            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Punctuated 模式不在周期性方法中发射
    }
}
```

---

## 三、水印在哪个算子中生成

### 3.1 算子定位

水印生成发生在 **`TimestampsAndWatermarksOperator`** 中，这是一个专门的单输入算子，负责两件事：

1. **提取时间戳**：调用 `TimestampAssigner.extractTimestamp()` 为每条记录赋予事件时间
2. **生成水印**：调用 `WatermarkGenerator` 生成并发射水印

**文件**：`flink-streaming-java/.../runtime/operators/TimestampsAndWatermarksOperator.java`

```java
public class TimestampsAndWatermarksOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {

    private final WatermarkStrategy<T> watermarkStrategy;
    private transient TimestampAssigner<T> timestampAssigner;
    private transient WatermarkGenerator<T> watermarkGenerator;
    private transient WatermarkOutput wmOutput;
    private transient long watermarkInterval;
```

### 3.2 完整处理流程

当用户调用 `stream.assignTimestampsAndWatermarks(strategy)` 时，Flink 会在作业图中插入 `TimestampsAndWatermarksOperator`：

```
用户代码:
env.addSource(kafkaSource)
   .assignTimestampsAndWatermarks(strategy)  ← 插入 TimestampsAndWatermarksOperator
   .keyBy(...)
   .window(...)

生成的算子链:
[Source] → [TimestampsAndWatermarksOperator] → [KeyBy/Window] → [Sink]
                    ↑
              水印在这里生成
```

**open() 阶段**——初始化（`TimestampsAndWatermarksOperator.java:77-93`）：

```java
@Override
public void open() throws Exception {
    super.open();
    // 1. 创建时间戳分配器
    timestampAssigner = watermarkStrategy.createTimestampAssigner(this::getMetricGroup);
    // 2. 创建水印生成器
    watermarkGenerator = emitProgressiveWatermarks
            ? watermarkStrategy.createWatermarkGenerator(this::getMetricGroup)
            : new NoWatermarksGenerator<>();
    // 3. 创建水印发射器
    wmOutput = new WatermarkEmitter(output);
    // 4. 注册周期性定时器
    watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
    if (watermarkInterval > 0 && emitProgressiveWatermarks) {
        final long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + watermarkInterval, this);
    }
}
```

**processElement()——每条数据的处理**（`TimestampsAndWatermarksOperator.java:96-105`）：

```java
@Override
public void processElement(final StreamRecord<T> element) throws Exception {
    final T event = element.getValue();
    final long previousTimestamp =
            element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE;

    // Step 1: 提取时间戳
    final long newTimestamp = timestampAssigner.extractTimestamp(event, previousTimestamp);
    // Step 2: 将时间戳设置到 StreamRecord 上
    element.setTimestamp(newTimestamp);
    // Step 3: 向下游发送数据
    output.collect(element);
    // Step 4: 通知水印生成器（更新内部状态，如 maxTimestamp）
    watermarkGenerator.onEvent(event, newTimestamp, wmOutput);
}
```

**关键顺序**：先输出数据（`output.collect`），再调用生成器（`onEvent`）。这确保数据不会因为水印逻辑的异常而丢失。

**processWatermark()——对上游水印的处理**（`TimestampsAndWatermarksOperator.java:119-127`）：

```java
@Override
public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark)
        throws Exception {
    // 忽略所有上游水印！只有 MAX_WATERMARK 除外（表示流结束）
    if (mark.getTimestamp() == Long.MAX_VALUE) {
        wmOutput.emitWatermark(Watermark.MAX_WATERMARK);
    }
}
```

**这非常重要**：`TimestampsAndWatermarksOperator` **完全忽略上游传来的水印**，只使用自己生成的水印。这意味着一旦你插入了这个算子，上游的水印语义就被"覆盖"了。

### 3.3 WatermarkEmitter —— 水印发射器

**文件**：`TimestampsAndWatermarksOperator.java:145-188`（内部类）

```java
public static final class WatermarkEmitter implements WatermarkOutput {
    private final Output<?> output;
    private long currentWatermark;
    private boolean idle;

    public WatermarkEmitter(Output<?> output) {
        this.output = output;
        this.currentWatermark = Long.MIN_VALUE;  // 初始水印
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        final long ts = watermark.getTimestamp();
        if (ts <= currentWatermark) {
            return;  // 关键：水印只能前进，忽略回退的水印
        }
        currentWatermark = ts;
        markActive();
        // 转换为运行时 Watermark 并发射
        output.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(ts));
    }

    @Override
    public void markIdle() {
        if (!idle) {
            idle = true;
            output.emitWatermarkStatus(WatermarkStatus.IDLE);
        }
    }

    @Override
    public void markActive() {
        if (idle) {
            idle = false;
            output.emitWatermarkStatus(WatermarkStatus.ACTIVE);
        }
    }
}
```

**水印单调性保证**：`if (ts <= currentWatermark) return;` 确保了发射出去的水印永远是递增的。

### 3.4 FLIP-27 新 Source 架构中的水印生成

在 FLIP-27 新 Source API 中，水印生成被集成到了 `SourceOperator` 内部，不再需要单独的 `assignTimestampsAndWatermarks()` 调用：

```java
// 新 API 用法——水印策略直接传给 Source
KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("...")
        .setTopics("topic")
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

env.fromSource(
    source,
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),  // 直接传入
    "Kafka Source"
);
```

**内部实现**（`SourceOperator.java`）：

```
SourceOperator
├── eventTimeLogic: TimestampsAndWatermarks<T>
│   ├── ProgressiveTimestampsAndWatermarks  ← 生成渐进式水印
│   │   ├── 为每个 Split 创建独立的 WatermarkGenerator
│   │   ├── 通过 WatermarkOutputMultiplexer 合并多 Split 的水印
│   │   └── 周期性定时器驱动 onPeriodicEmit()
│   └── NoOpTimestampsAndWatermarks         ← 不生成水印（批模式）
└── watermarkAlignmentParams                ← 水印对齐参数
```

**每个 Split 有独立的 WatermarkGenerator 实例**，这解决了旧 API 中多 Partition 共享一个生成器的问题：

```java
// ProgressiveTimestampsAndWatermarks.SplitLocalOutputs
SourceOutput<T> createOutputForSplit(String splitId) {
    watermarkMultiplexer.registerNewOutput(splitId, ...);

    // 每个 Split 独立的水印输出
    final WatermarkOutput onEventOutput = watermarkMultiplexer.getImmediateOutput(splitId);
    final WatermarkOutput periodicOutput = watermarkMultiplexer.getDeferredOutput(splitId);

    // 每个 Split 独立的水印生成器
    final WatermarkGenerator<T> watermarks = watermarksFactory.createWatermarkGenerator(...);

    return SourceOutputWithWatermarks.createWithSeparateOutputs(
            recordOutput, onEventOutput, periodicOutput, timestampAssigner, watermarks);
}
```

---

## 四、水印传播机制

### 4.1 单流传播

水印在算子间的传播是**广播式**的——一个算子发射的水印会被发送到所有下游 Channel：

```
[Operator A] ──水印10s──→ [Operator B, Subtask 0]
              ──水印10s──→ [Operator B, Subtask 1]
              ──水印10s──→ [Operator B, Subtask 2]
```

### 4.2 多输入合并 —— StatusWatermarkValve

当一个算子有多个输入 Channel 时（如 KeyBy 后的算子接收来自所有上游 Subtask 的数据），需要将多个 Channel 的水印合并为一个。

**核心组件**：`StatusWatermarkValve`

**文件**：`flink-streaming-java/.../watermarkstatus/StatusWatermarkValve.java`

```java
public class StatusWatermarkValve {
    private final InputChannelStatus[] channelStatuses;      // 每个输入通道的状态
    private long lastOutputWatermark;                        // 最后输出的水印
    private WatermarkStatus lastOutputWatermarkStatus;       // 最后输出的状态
    // 堆优先队列：快速找到对齐通道中的最小水印
    private final HeapPriorityQueue<InputChannelStatus> alignedChannelStatuses;
```

**合并规则**：

```
输出水印 = min(所有"对齐"通道的水印)

对齐通道 = 满足以下全部条件的通道：
  1. 状态为 ACTIVE（非空闲）
  2. 水印 ≥ lastOutputWatermark（已追上整体进度）
```

**inputWatermark() 处理流程**（`StatusWatermarkValve.java:93-117`）：

```java
public void inputWatermark(Watermark watermark, int channelIndex, DataOutput<?> output) {
    // 前置检查：整体非空闲 且 通道非空闲
    if (lastOutputWatermarkStatus.isActive()
            && channelStatuses[channelIndex].watermarkStatus.isActive()) {

        long watermarkMillis = watermark.getTimestamp();

        // 忽略递减的水印
        if (watermarkMillis > channelStatuses[channelIndex].watermark) {
            channelStatuses[channelIndex].watermark = watermarkMillis;

            // 维护对齐状态
            if (channelStatuses[channelIndex].isWatermarkAligned) {
                adjustAlignedChannelStatuses(channelStatuses[channelIndex]);
            } else if (watermarkMillis >= lastOutputWatermark) {
                markWatermarkAligned(channelStatuses[channelIndex]);
            }

            // 尝试推进整体水印
            findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
        }
    }
}
```

**推进水印的核心方法**：

```java
private void findAndOutputNewMinWatermarkAcrossAlignedChannels(DataOutput<?> output) {
    boolean hasAlignedChannels = !alignedChannelStatuses.isEmpty();
    // 堆顶即为最小值（O(1) 查找）
    if (hasAlignedChannels && alignedChannelStatuses.peek().watermark > lastOutputWatermark) {
        lastOutputWatermark = alignedChannelStatuses.peek().watermark;
        output.emitWatermark(new Watermark(lastOutputWatermark));
    }
}
```

**数值推演**（3 个输入通道）：

```
初始: ch0=MIN, ch1=MIN, ch2=MIN → 输出: MIN

ch0 收到水印 10s → alignedQueue: [ch0=10, ch1=MIN, ch2=MIN]
   min = MIN → 不输出（MIN 没变）

ch1 收到水印 15s → alignedQueue: [ch0=10, ch1=15, ch2=MIN]
   min = MIN → 不输出

ch2 收到水印 12s → alignedQueue: [ch0=10, ch2=12, ch1=15]
   min = 10 → 输出水印 10s

ch0 收到水印 20s → alignedQueue: [ch2=12, ch1=15, ch0=20]
   min = 12 → 输出水印 12s

ch2 标记为 IDLE → ch2 从对齐队列移除
   alignedQueue: [ch1=15, ch0=20]
   min = 15 → 输出水印 15s  ← 空闲通道不再阻挡进度
```

### 4.3 多分片合并 —— WatermarkOutputMultiplexer

在 FLIP-27 新 Source 中，一个 SourceOperator 可能读取多个 Split（如 Kafka 的多个 Partition），每个 Split 有独立的 WatermarkGenerator。`WatermarkOutputMultiplexer` 负责合并这些 Split 的水印。

**文件**：`flink-core/.../eventtime/WatermarkOutputMultiplexer.java`

```java
public class WatermarkOutputMultiplexer {
    private final WatermarkOutput underlyingOutput;              // 底层输出
    private final Map<String, PartialWatermark> watermarkPerOutputId;  // 每个 Split 的水印
    private final CombinedWatermarkStatus combinedWatermarkStatus;     // 合并逻辑
```

**两种输出模式**：

| 模式 | 类 | 特点 | 使用场景 |
|------|-----|------|----------|
| **Immediate** | `ImmediateOutput` | 更新后立即尝试合并 | `onEvent()` 中（Punctuated 水印需要立即生效） |
| **Deferred** | `DeferredOutput` | 只更新内部状态，等周期性合并 | `onPeriodicEmit()` 中（避免频繁合并） |

```java
// ImmediateOutput: 更新后立即检查是否需要推进合并水印
@Override
public void emitWatermark(Watermark watermark) {
    long timestamp = watermark.getTimestamp();
    boolean wasUpdated = state.setWatermark(timestamp);
    if (wasUpdated && timestamp > combinedWatermarkStatus.getCombinedWatermark()) {
        updateCombinedWatermark();  // 立即合并
    }
}

// DeferredOutput: 只更新状态
@Override
public void emitWatermark(Watermark watermark) {
    state.setWatermark(watermark.getTimestamp());  // 仅更新，不合并
}
```

### 4.4 合并水印的计算 —— CombinedWatermarkStatus

**合并算法**：取所有非空闲分片的最小水印

```java
// CombinedWatermarkStatus.updateCombinedWatermark()
public boolean updateCombinedWatermark() {
    long minimumOverAllOutputs = Long.MAX_VALUE;
    boolean allIdle = true;

    for (PartialWatermark partialWatermark : partialWatermarks) {
        if (!partialWatermark.isIdle()) {
            minimumOverAllOutputs =
                Math.min(minimumOverAllOutputs, partialWatermark.getWatermark());
            allIdle = false;
        }
    }
    this.idle = allIdle;

    // 只有合并水印增大时才返回 true
    if (!allIdle && minimumOverAllOutputs > combinedWatermark) {
        combinedWatermark = minimumOverAllOutputs;
        return true;
    }
    return false;
}
```

---

## 五、水印与窗口的关系

### 5.1 窗口触发规则

```
当 Watermark >= 窗口结束时间 时，触发窗口计算
```

**推演示例**：

```
窗口: TumblingEventTimeWindow(5分钟)
WatermarkStrategy: BoundedOutOfOrderness(10秒)

数据流:
10:02:30  →  分配到窗口 [10:00, 10:05)
10:04:50  →  分配到窗口 [10:00, 10:05)
10:05:10  →  maxTimestamp = 10:05:10
             水印 = 10:05:10 - 10s - 1ms = 10:04:59.999
             水印(10:04:59.999) < 窗口结束(10:05:00) → 不触发

10:05:12  →  maxTimestamp = 10:05:12
             水印 = 10:05:12 - 10s - 1ms = 10:05:01.999
             水印(10:05:01.999) >= 窗口结束(10:05:00) → 触发！
```

### 5.2 迟到数据处理

```java
stream.keyBy(...)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowedLateness(Time.minutes(1))        // 允许 1 分钟迟到
    .sideOutputLateData(lateOutputTag)       // 超过迟到限制的数据输出到侧输出流
    .process(new MyWindowFunction());
```

**迟到判定**：

```
窗口结束时间 = 10:05:00
允许迟到 = 1 分钟

情况1: 水印 = 10:05:30，收到 10:04:50 的数据
  → 10:04:50 < 窗口结束(10:05:00) 且 水印(10:05:30) < 窗口清理时间(10:06:00)
  → 窗口已触发但未清理 → 重新计算窗口结果（late firing）

情况2: 水印 = 10:06:30，收到 10:04:50 的数据
  → 水印(10:06:30) >= 窗口清理时间(10:06:00)
  → 窗口已清理 → 输出到侧输出流
```

---

## 六、水印对齐（Watermark Alignment）

### 6.1 问题场景

当多个 Source 的数据速率差异巨大时（如 Kafka 某个 Partition 数据远超其他），快速的 Source 水印会远远领先慢速的 Source。这导致：

- 窗口状态膨胀（快速源的数据被缓存等待慢速源追上）
- 内存压力增大

### 6.2 解决方案

```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withWatermarkAlignment(
        "alignment-group-1",        // 对齐组名称
        Duration.ofSeconds(20),     // 最大允许偏移
        Duration.ofSeconds(1)       // 上报间隔
    );
```

**工作原理**：

```
Source A 水印: 100s    ┐
Source B 水印: 80s     ├→ 组内最小水印: 80s
Source C 水印: 90s     ┘

maxAllowedDrift = 20s
maxDesiredWatermark = 80s + 20s = 100s

Source A 水印(100s) >= maxDesiredWatermark(100s)
→ 暂停 Source A 的消费，等待其他 Source 追上
```

**源码**（`WatermarkAlignmentParams.java`）：

```java
public final class WatermarkAlignmentParams implements Serializable {
    public static final WatermarkAlignmentParams WATERMARK_ALIGNMENT_DISABLED =
            new WatermarkAlignmentParams(Long.MAX_VALUE, "", 0);

    private final long maxAllowedWatermarkDrift;
    private final String watermarkGroup;
    private final long updateInterval;

    public boolean isEnabled() {
        return maxAllowedWatermarkDrift < Long.MAX_VALUE;
    }
}
```

---

## 七、手动实现水印生成器

### 7.1 自定义周期性水印生成器

```java
/**
 * 自定义水印生成器：基于数据中的多个时间字段取最小值
 * 适用场景：一条数据包含多个时间戳，需要以最早的为准
 */
public class MultiFieldWatermarkGenerator implements WatermarkGenerator<OrderEvent> {
    private long maxTimestamp = Long.MIN_VALUE;
    private final long maxOutOfOrderness;

    public MultiFieldWatermarkGenerator(Duration maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness.toMillis();
    }

    @Override
    public void onEvent(OrderEvent event, long eventTimestamp, WatermarkOutput output) {
        // 取订单创建时间和支付时间中较早的
        long earliestTime = Math.min(event.getCreateTime(), event.getPayTime());
        maxTimestamp = Math.max(maxTimestamp, earliestTime);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - maxOutOfOrderness - 1));
    }
}

// 使用
WatermarkStrategy<OrderEvent> strategy = WatermarkStrategy
    .forGenerator(ctx -> new MultiFieldWatermarkGenerator(Duration.ofSeconds(10)))
    .withTimestampAssigner((event, ts) -> Math.min(event.getCreateTime(), event.getPayTime()));
```

### 7.2 自定义 Punctuated 水印生成器

```java
/**
 * 基于事件标记的水印生成器
 * 适用场景：数据流中包含特殊的"水印事件"（如 CDC 中的心跳事件）
 */
public class HeartbeatWatermarkGenerator implements WatermarkGenerator<CDCEvent> {
    @Override
    public void onEvent(CDCEvent event, long eventTimestamp, WatermarkOutput output) {
        if (event.getType() == CDCEvent.Type.HEARTBEAT) {
            // 心跳事件携带的时间戳作为水印
            output.emitWatermark(new Watermark(event.getTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Punctuated 模式：不在周期性方法中发射
    }
}
```

### 7.3 自定义混合模式水印生成器

```java
/**
 * 混合模式：大多数时候用 Periodic，遇到特殊事件立即推进
 * 适用场景：数据流中偶尔有明确的"时间进度标记"
 */
public class HybridWatermarkGenerator implements WatermarkGenerator<TradeEvent> {
    private long maxTimestamp = Long.MIN_VALUE;
    private final long outOfOrdernessMillis;

    public HybridWatermarkGenerator(Duration maxOutOfOrderness) {
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
    }

    @Override
    public void onEvent(TradeEvent event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);

        // 收盘事件：立即推进水印到收盘时间（不再等待）
        if (event.isMarketClose()) {
            output.emitWatermark(new Watermark(event.getTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 周期性发射常规水印
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}
```

### 7.4 自定义 WatermarkStrategy（完整实现）

```java
/**
 * 完整的自定义 WatermarkStrategy：带空闲检测 + 自定义生成器 + 自定义时间戳提取
 */
public class CustomWatermarkStrategy implements WatermarkStrategy<SensorReading> {

    @Override
    public WatermarkGenerator<SensorReading> createWatermarkGenerator(Context context) {
        return new SensorWatermarkGenerator(Duration.ofSeconds(3));
    }

    @Override
    public TimestampAssigner<SensorReading> createTimestampAssigner(
            TimestampAssignerSupplier.Context context) {
        return (reading, previousTimestamp) -> reading.getEventTime();
    }

    // 使用
    // DataStream<SensorReading> stream = env.addSource(...)
    //     .assignTimestampsAndWatermarks(
    //         new CustomWatermarkStrategy()
    //             .withIdleness(Duration.ofMinutes(2))
    //     );
}
```

---

## 八、水印相关指标与监控

### 8.1 关键指标

**文件**：`MetricNames.java`

```java
public static final String IO_CURRENT_INPUT_WATERMARK = "currentInputWatermark";
public static final String IO_CURRENT_OUTPUT_WATERMARK = "currentOutputWatermark";
public static final String WATERMARK_LAG = "watermarkLag";
public static final String WATERMARK_ALIGNMENT_DRIFT = "watermarkAlignmentDrift";
```

| 指标 | 含义 | 关注点 |
|------|------|--------|
| `currentInputWatermark` | 当前算子的输入水印 | 如果远低于当前时间，说明有数据延迟或空闲源 |
| `currentOutputWatermark` | 当前算子的输出水印 | 多个算子间水印的差异反映处理延迟 |
| `watermarkLag` | Source 水印与当前时间的差距 | 差距持续增大说明处理跟不上数据产生速度 |
| `watermarkAlignmentDrift` | 水印对齐组内的偏移量 | 偏移过大说明源之间速率严重不均 |

### 8.2 Prometheus 查询

```promql
# 查看所有算子的当前水印
flink_taskmanager_job_task_operator_currentOutputWatermark

# 水印延迟（当前时间 - 水印时间）
time() * 1000 - flink_taskmanager_job_task_operator_currentOutputWatermark

# Source 的水印 Lag
flink_taskmanager_job_task_operator_watermarkLag

# 同一算子各 Subtask 的水印差异（发现慢分区）
max(flink_taskmanager_job_task_operator_currentOutputWatermark{task_name="Source"})
- min(flink_taskmanager_job_task_operator_currentOutputWatermark{task_name="Source"})
```

---

## 九、面试高频问题与深度回答

### Q1: Watermark 是什么？为什么需要它？

Watermark 是 Flink 事件时间处理的核心机制。它是一个单调递增的时间戳 T，语义为"不会再有事件时间 ≤ T 的数据到来"。

**需要它的原因**：流处理中数据是无界的，系统永远无法确定某个时间范围的数据是否已经到齐。Watermark 通过引入一个可容忍的乱序上界，让系统能够在"准确性"和"及时性"之间取得平衡——窗口依靠水印来决定何时触发计算。

**源码级回答**：Watermark 类定义在 `flink-core/.../eventtime/Watermark.java`，核心就是一个 `long timestamp` 字段。它在运行时作为 `StreamElement` 的子类型在算子间传播，与 `StreamRecord` 共享网络传输通道。

---

### Q2: Watermark 在哪个算子中生成？

**两个位置**：

1. **`TimestampsAndWatermarksOperator`**（传统 API）：当用户调用 `assignTimestampsAndWatermarks()` 时插入。该算子的 `processElement()` 负责提取时间戳，`onProcessingTime()` 负责周期性发射水印。

2. **`SourceOperator`**（FLIP-27 新 Source API）：当用户调用 `env.fromSource(source, watermarkStrategy, ...)` 时，水印生成逻辑内嵌在 SourceOperator 中，通过 `ProgressiveTimestampsAndWatermarks` 为每个 Split 创建独立的 `WatermarkGenerator`。

**关键区别**：新 API 中每个 Split 有独立的生成器实例，通过 `WatermarkOutputMultiplexer` 合并，这比旧 API 更精确。

---

### Q3: 水印的生成模式有哪些？

**两种**：

| 模式 | 方法 | 触发方式 | 典型实现 |
|------|------|----------|----------|
| Periodic | `onPeriodicEmit()` | Processing Time 定时器，默认 200ms | `BoundedOutOfOrdernessWatermarks` |
| Punctuated | `onEvent()` | 每条数据到达时 | 自定义（如心跳事件触发） |

**Periodic 是默认模式**。通过 `env.getConfig().setAutoWatermarkInterval(200)` 控制调用间隔。如果设置为 0，则禁用周期性水印。

---

### Q4: 多个输入流的水印如何合并？

通过 `StatusWatermarkValve`（算子间合并）和 `WatermarkOutputMultiplexer`（Source 内多 Split 合并）。

**核心规则**：`输出水印 = min(所有活跃且对齐通道的水印)`

**"对齐"的定义**：一个通道如果是活跃的（非 IDLE），且其水印 ≥ 上一次的输出水印，就被认为是"对齐"的。未追上整体进度的通道暂时不参与最小值计算（否则会把整体水印拖回去）。

**源码关键**：`StatusWatermarkValve` 使用 `HeapPriorityQueue`（堆优先队列）来 O(1) 获取最小水印，O(log n) 更新，保证高效合并。

---

### Q5: 空闲源（Idle Source）如何处理？

**问题**：Kafka 20 个 Partition 中有 3 个长时间没数据，它们的水印停留在很久以前，导致整体水印无法推进，窗口无法触发。

**解决方案**：

```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withIdleness(Duration.ofMinutes(1));  // 1 分钟无数据则标记为空闲
```

**工作原理**（`WatermarksWithIdleness.java`）：
1. 每次 `onEvent()` 调用 `idlenessTimer.activity()` 递增计数器
2. `onPeriodicEmit()` 中 `idlenessTimer.checkIfIdle()` 检查：计数器是否在上次检查后有变化，如果持续无变化超过阈值，标记为 IDLE
3. IDLE 的分片在 `CombinedWatermarkStatus.updateCombinedWatermark()` 中被跳过，不参与最小值计算

---

### Q6: BoundedOutOfOrderness 的水印计算公式为什么要减 1？

**公式**：`Watermark = maxTimestamp - outOfOrdernessMillis - 1`

**原因**：水印 T 的语义是"不会再有时间戳 **≤ T** 的数据到来"（闭区间）。如果不减 1，那么 `maxTimestamp - outOfOrderness` 这个时间点本身的数据就被声明为"已经全部到齐"，但实际上可能还有该时间戳的数据在路上。减 1 确保了 `maxTimestamp - outOfOrderness` 时间点的数据仍然有效。

**源码**：`BoundedOutOfOrdernessWatermarks.java` 中 `onPeriodicEmit()` 明确 `maxTimestamp - outOfOrdernessMillis - 1`。

---

### Q7: assignTimestampsAndWatermarks 算子会处理上游传来的水印吗？

**不会**。源码 `TimestampsAndWatermarksOperator.java:119-127`：

```java
public void processWatermark(Watermark mark) throws Exception {
    if (mark.getTimestamp() == Long.MAX_VALUE) {
        wmOutput.emitWatermark(Watermark.MAX_WATERMARK);  // 只转发流结束标记
    }
    // 其他水印被忽略
}
```

它甚至也忽略上游的 `WatermarkStatus`（`processWatermarkStatus` 是空方法）。这意味着该算子是水印的"重置点"——所有水印语义由它重新建立。

---

### Q8: 水印发送间隔（autoWatermarkInterval）设置多少合适？

**默认 200ms**，适合大多数场景。

| 场景 | 建议值 | 理由 |
|------|--------|------|
| 通用 | 200ms | 默认值，平衡延迟和开销 |
| 低延迟 | 50~100ms | 水印更新更频繁，窗口触发更及时 |
| 高吞吐 | 500ms~1s | 减少水印发射频率，降低网络开销 |
| 设为 0 | 禁用周期性水印 | 仅 Punctuated 模式有效 |

**注意**：间隔越小，水印发射越频繁，但水印本身是很轻量的（仅一个 long 值），通常不会成为性能瓶颈。真正影响的是窗口触发的及时性。

---

### Q9: Watermark 能保证数据不丢失吗？

**不能**。Watermark 是一个启发式的进度指标，它做出的是"有限容忍度"的承诺。

- **乱序度设小了**：超出乱序容忍范围的数据被判定为"迟到"
- **迟到数据的处理**取决于用户配置：
  - `allowedLateness`：窗口触发后仍保留一段时间，接受迟到数据并重新计算
  - `sideOutputLateData`：超过允许延迟的数据输出到侧输出流
  - 默认：直接丢弃

**生产建议**：乱序度设置应基于实际数据的延迟分布（P99 或 P99.9），而不是"猜测"。

---

### Q10: 新旧 Source API 中水印生成的区别？

| 特性 | 旧 API (`assignTimestampsAndWatermarks`) | 新 API (FLIP-27 `fromSource`) |
|------|----------------------------------------|-------------------------------|
| 水印生成位置 | 独立算子 `TimestampsAndWatermarksOperator` | 内嵌在 `SourceOperator` 中 |
| 生成器粒度 | 整个 Subtask 共享一个 `WatermarkGenerator` | 每个 Split 独立的 `WatermarkGenerator` |
| 多分片合并 | 无（多个分片混在一起） | `WatermarkOutputMultiplexer` 精确合并 |
| 水印对齐 | 不支持 | 支持 `withWatermarkAlignment()` |
| Split 级别水印追踪 | 不支持 | 支持（`splitCurrentWatermarks`） |

**新 API 的优势**：每个 Split 独立跟踪水印，然后取最小值合并，比旧 API 中多个 Partition 混合计算 `maxTimestamp` 更精确。

---

## 十、生产实践与常见陷阱

### 10.1 最佳实践

#### 选择合适的水印策略

```java
// 场景1：Kafka 数据，已知最大延迟 30 秒
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((event, ts) -> event.getEventTime())
    .withIdleness(Duration.ofMinutes(2));  // Kafka 分区可能空闲

// 场景2：数据已按时间排序（如读取已排序的文件）
WatermarkStrategy.forMonotonousTimestamps()
    .withTimestampAssigner((event, ts) -> event.getTimestamp());

// 场景3：多源对齐（防止快源远超慢源）
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withWatermarkAlignment("my-group", Duration.ofMinutes(1))
    .withIdleness(Duration.ofMinutes(5));
```

#### 乱序度的确定方法

```
步骤1：采集一段时间的数据，记录每条数据的 事件时间 和 处理时间
步骤2：计算延迟 = 处理时间 - 事件时间
步骤3：取 P99 延迟作为 maxOutOfOrderness 的参考值
步骤4：在 P99 基础上适当增加余量（如 20%）
步骤5：配合 allowedLateness 处理超出范围的数据
```

### 10.2 常见陷阱

| 陷阱 | 表现 | 原因 | 解决方案 |
|------|------|------|----------|
| **窗口永远不触发** | 结果迟迟不输出 | 某个分区空闲，水印卡住 | 配置 `withIdleness()` |
| **大量数据被判定为迟到** | 侧输出流数据多 | `maxOutOfOrderness` 设置过小 | 增大乱序容忍度 |
| **窗口延迟太高** | 结果输出晚 | `maxOutOfOrderness` 设置过大 | 减小乱序容忍度或加 `allowedLateness` |
| **时间戳提取异常** | 水印为负值或异常 | `extractTimestamp` 返回错误值 | 检查时间戳提取逻辑和时区问题 |
| **水印回退** | 无明显影响但逻辑错误 | 自定义生成器未保证单调性 | `WatermarkEmitter` 会自动过滤回退值 |
| **Checkpoint 后水印跳变** | 恢复后窗口行为异常 | 水印生成器状态未正确恢复 | 内置生成器已处理，自定义需注意 |

### 10.3 调试水印问题的步骤

```
1. 检查水印指标
   → Flink Web UI → 算子 → Metrics → currentInputWatermark / currentOutputWatermark
   → 如果水印为 Long.MIN_VALUE，说明生成器未工作或数据未到达

2. 检查时间戳提取
   → 在 TimestampAssigner 中打印日志或指标
   → 确认提取的时间戳是毫秒级且值合理

3. 检查空闲分区
   → 查看各 Subtask 的水印是否差异巨大
   → 最小水印的 Subtask 可能是空闲分区

4. 验证乱序度
   → 通过指标对比 maxTimestamp 和 watermark 的差值
   → 确认 outOfOrderness 设置合理

5. 检查水印传播
   → 对比上下游算子的 currentInputWatermark
   → 如果下游水印远低于上游，检查中间是否有阻塞
```

---

## 十一、核心源码文件索引

| 功能 | 核心文件 | 包路径 |
|------|----------|--------|
| Watermark 定义 | `Watermark.java` | `flink-core/.../eventtime/` |
| 策略接口 | `WatermarkStrategy.java` | `flink-core/.../eventtime/` |
| 生成器接口 | `WatermarkGenerator.java` | `flink-core/.../eventtime/` |
| 时间戳提取器 | `TimestampAssigner.java` | `flink-core/.../eventtime/` |
| 有界乱序生成器 | `BoundedOutOfOrdernessWatermarks.java` | `flink-core/.../eventtime/` |
| 递增时间戳生成器 | `AscendingTimestampsWatermarks.java` | `flink-core/.../eventtime/` |
| 空闲检测包装器 | `WatermarksWithIdleness.java` | `flink-core/.../eventtime/` |
| 水印输出接口 | `WatermarkOutput.java` | `flink-core/.../eventtime/` |
| 多分片合并器 | `WatermarkOutputMultiplexer.java` | `flink-core/.../eventtime/` |
| 合并水印计算 | `CombinedWatermarkStatus.java` | `flink-core/.../eventtime/` |
| 水印对齐参数 | `WatermarkAlignmentParams.java` | `flink-core/.../eventtime/` |
| 水印生成算子 | `TimestampsAndWatermarksOperator.java` | `flink-streaming-java/.../operators/` |
| 多输入水印合并 | `StatusWatermarkValve.java` | `flink-streaming-java/.../watermarkstatus/` |
| 新 Source 水印逻辑 | `ProgressiveTimestampsAndWatermarks.java` | `flink-streaming-java/.../operators/source/` |
| SourceOperator | `SourceOperator.java` | `flink-streaming-java/.../operators/` |
| 运行时水印 | `Watermark.java`（streaming 版） | `flink-streaming-java/.../api/watermark/` |
| 流元素基类 | `StreamElement.java` | `flink-streaming-java/.../streamrecord/` |
| 水印状态 | `WatermarkStatus.java` | `flink-streaming-java/.../watermarkstatus/` |

---

## 十二、面试口述速背手册

> 以下内容按 STAR 法则（Situation-Task-Action-Result）组织，面试时可直接口述，每段控制在 1~2 分钟。

### 话题一：面试官问"说说 Flink 的 Watermark 机制"

> **S（背景）**：在流处理中使用事件时间语义时，面临一个根本问题——数据是无界的，而且由于网络延迟、分布式处理等原因，数据到达的顺序和事件实际发生的顺序往往不一致，也就是所谓的"乱序"。这导致系统无法判断某个时间窗口的数据是否已经到齐，窗口不知道什么时候该触发。
>
> **T（目标）**：需要一种机制来跟踪事件时间的进度，让系统能够在"准确性"和"处理及时性"之间取得平衡。
>
> **A（方案）**：Flink 引入了 Watermark 机制。Watermark 本质上是一个单调递增的时间戳 T，它的语义是"不会再有事件时间小于等于 T 的数据到来"。最常用的生成策略是 `BoundedOutOfOrderness`，计算公式是 `Watermark = maxTimestamp - maxOutOfOrderness - 1`，其中 maxTimestamp 是目前见过的最大事件时间戳，maxOutOfOrderness 是允许的最大乱序度。减 1 是因为水印的语义是闭区间的"小于等于"。
>
> Watermark 的生成分两种模式：Periodic 模式由 Processing Time 定时器驱动，默认每 200ms 调用一次 `onPeriodicEmit()` 方法发射水印；Punctuated 模式则在 `onEvent()` 中根据特殊事件直接发射。实际生产中 Periodic 模式是主流。
>
> **R（效果）**：有了 Watermark，窗口就有了触发依据——当水印超过窗口结束时间时触发计算。同时通过 `allowedLateness` 和侧输出流，还能优雅地处理迟到数据，做到既不过度等待又不丢失数据。

---

### 话题二：面试官问"Watermark 在哪个算子中生成的"

> Watermark 的生成位置取决于使用哪套 API：
>
> **传统 API**：当调用 `stream.assignTimestampsAndWatermarks(strategy)` 时，Flink 会在作业图中插入一个 `TimestampsAndWatermarksOperator`。这个算子做两件事——在 `processElement()` 中调用 `TimestampAssigner.extractTimestamp()` 为每条记录赋予事件时间，然后调用 `WatermarkGenerator.onEvent()` 更新内部状态。同时有一个 Processing Time 定时器每隔 200ms 调用 `onPeriodicEmit()` 来发射水印。
>
> 有一个很重要的设计细节：这个算子会**完全忽略上游传来的水印**，只有流结束标记 `MAX_WATERMARK` 除外。也就是说它是水印语义的"重置点"。
>
> **FLIP-27 新 Source API**：调用 `env.fromSource(source, watermarkStrategy, ...)` 时，水印生成被内嵌到 `SourceOperator` 中。关键改进是每个 Split（比如 Kafka 的每个 Partition）有独立的 `WatermarkGenerator` 实例，通过 `WatermarkOutputMultiplexer` 合并多个 Split 的水印再输出，比旧 API 中多分区混合计算更精确。

---

### 话题三：面试官问"多个输入流的水印怎么合并"

> 这要分两个层面：
>
> **算子间合并**——由 `StatusWatermarkValve` 负责。当一个算子有多个输入 Channel 时（比如 KeyBy 后的算子接收来自所有上游 Subtask 的数据），合并规则是取所有"对齐"通道中水印的最小值。这里"对齐"是指通道处于活跃状态，且其水印已经追上了上次的输出水印。内部用堆优先队列实现 O(1) 取最小值。
>
> **Source 内多 Split 合并**——由 `WatermarkOutputMultiplexer` 和 `CombinedWatermarkStatus` 负责。算法类似，也是取所有非空闲 Split 的最小水印。
>
> 两者的共同设计原则是：**空闲的通道/分片不参与最小值计算**，避免一个长时间没数据的分区卡住整条链路的水印进度。这就是为什么我们在使用多分区数据源时要配置 `withIdleness()`。

---

### 话题四：面试官问"如何处理空闲源问题"

> **S（场景）**：我们消费一个 Kafka Topic，有 20 个 Partition，但某些 Partition 在非高峰期可能几分钟都没有数据。因为水印合并取最小值，这些空闲 Partition 的水印停在很久以前，导致整个作业的水印无法推进，窗口一直不触发。
>
> **T（任务）**：需要让空闲分区不阻挡整体进度。
>
> **A（方案）**：在 WatermarkStrategy 上配置 `withIdleness(Duration.ofMinutes(2))`。底层实现是 `WatermarksWithIdleness`，它用一个 `IdlenessTimer` 做三阶段检测：每次有数据就递增计数器；周期性检查时如果计数器没变化就开始计时；计时超过阈值就调用 `output.markIdle()` 将该分片标记为 IDLE。被标记为 IDLE 的分片在水印合并时会被跳过。当新数据到来时，调用 `markActive()` 恢复。
>
> **R（结果）**：配置后窗口能正常触发了。要注意 idleTimeout 不能设太短，否则正常的流量波动也会频繁触发空闲/恢复切换。

---

### 话题五：面试官问"如何自定义 Watermark 生成器"

> 实现 `WatermarkGenerator` 接口就行，核心是两个方法：`onEvent()` 在每条数据到达时被调用，用来记录和更新状态（比如追踪最大时间戳）；`onPeriodicEmit()` 被定时器周期性调用，在这里决定是否发射新水印。
>
> 比如我做过一个场景，一条订单数据包含创建时间和支付时间两个时间字段，业务要求以较早的为准。我在 `onEvent()` 中取两个时间的最小值来更新 maxTimestamp，然后在 `onPeriodicEmit()` 中按 `maxTimestamp - outOfOrderness - 1` 发射水印。
>
> 需要注意的是，`WatermarkEmitter` 内部有单调性保护——如果你发射的水印比上一次还小，它会自动忽略，所以自定义实现不用太担心回退问题。但最佳实践还是自己保证单调性。

---

### 话题六：面试官问"Watermark 和窗口的关系"

> 一句话：**Watermark 决定窗口何时触发**。规则就是当 Watermark >= 窗口结束时间时触发窗口计算。
>
> 举个例子：5 分钟滚动窗口 [10:00, 10:05)，配 10 秒的乱序度。当来了一条 10:05:12 的数据，maxTimestamp 更新为 10:05:12，水印推进到 10:05:12 - 10s - 1ms = 10:05:01.999，这个值 >= 窗口结束时间 10:05:00，窗口触发。
>
> 触发后如果还配了 `allowedLateness(1分钟)`，窗口状态会保留到 10:06:00。在这之前到达的属于该窗口的数据会触发重新计算（late firing）。超过 10:06:00 后到达的才被丢弃或输出到侧输出流。

---

### 话题七：面试官问"水印对齐是什么"

> **S（场景）**：多个 Source 的数据速率差异很大。比如 Kafka Source A 的数据量远大于 Source B，A 的水印已经推进到 100 秒了，B 才到 80 秒。这导致算子要缓存 A 中 80~100 秒这 20 秒的数据等待 B 追上，窗口状态膨胀严重。
>
> **T（任务）**：限制不同 Source 之间的水印偏移，避免快源远超慢源。
>
> **A（方案）**：Flink 1.15 引入了水印对齐。配置方式是 `withWatermarkAlignment("group", Duration.ofSeconds(20))`，表示同一组内的 Source，水印偏移不能超过 20 秒。底层实现是 `SourceOperator` 周期性地向协调器上报自己的水印，协调器计算组内最小水印，然后告诉所有 Source "最大允许水印 = 最小水印 + maxDrift"。超过这个值的 Source 会被暂停消费。
>
> **R（结果）**：窗口状态内存大幅减少，系统更稳定。适用于多异构数据源（如 Kafka + File）的场景。

---

**文档版本**：v1.0
**基于 Flink 版本**：Apache Flink 1.18（release-1.18 分支）
**最后更新**：2026-03-21
