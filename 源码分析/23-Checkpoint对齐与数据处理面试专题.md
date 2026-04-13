# Checkpoint 对齐与数据处理面试专题

> 对齐检查点和非对齐检查点在 Barrier 处理期间如何对待数据？源码级深度剖析

---

## 一、直接回答核心问题

| 问题 | 对齐检查点（Aligned） | 非对齐检查点（Unaligned） |
|------|----------------------|--------------------------|
| Barrier 到达后还会处理数据吗？ | **部分通道停止处理** | **所有通道继续处理** |
| 具体怎么做的？ | 收到 Barrier 的通道被**阻塞**（blockChannel），数据被缓冲；未收到 Barrier 的通道继续处理 | 不阻塞任何通道，Barrier 之后的数据继续处理，但 Barrier 之前还在网络中飞行的数据（in-flight data）被**写入 Checkpoint 状态** |
| 对吞吐的影响 | 高（等待最慢的通道对齐） | 低（几乎无等待） |
| 对 Checkpoint 大小的影响 | 小（不额外存储数据） | 大（需要存储 in-flight 数据） |

---

## 二、对齐检查点：Barrier 到达时发生了什么

### 2.1 核心机制：阻塞快速通道

一个算子有多个输入通道时，Barrier 不会同时到达。对齐模式的核心就是**让快的通道等慢的通道**：

```
Input Channel 0: ─[数据]─[数据]─[Barrier]─[数据]─[数据]─→
                                    │
                          收到Barrier → 阻塞通道0！
                          通道0的后续数据被缓冲，不处理

Input Channel 1: ─[数据]─[数据]─[数据]─[数据]─[Barrier]─→
                                                   │
                                         通道1继续处理数据
                                         直到通道1的Barrier也到达
                                                   │
                          全部Barrier到齐 → 触发快照 → 解除阻塞
```

### 2.2 源码级流程

**状态机**（`BarrierHandlerState` 接口的实现）：

```
                    ┌──────────────────────────┐
                    │ WaitingForFirstBarrier   │
                    │ （等待第一个 Barrier）     │
                    └────────────┬─────────────┘
                                 │ 收到第一个 Barrier
                                 │ → blockChannel(channelInfo)
                                 ▼
                    ┌──────────────────────────┐
                    │ CollectingBarriers       │
                    │ （收集剩余 Barrier）      │
                    │ 快速通道被阻塞，慢通道    │
                    │ 继续处理数据              │
                    └────────────┬─────────────┘
                                 │ 所有 Barrier 到齐
                                 │ → triggerGlobalCheckpoint()
                                 │ → unblockAllChannels()
                                 ▼
                    ┌──────────────────────────┐
                    │ WaitingForFirstBarrier   │
                    │ （进入下一轮等待）        │
                    └──────────────────────────┘
```

**阻塞通道的源码**（`AbstractAlignedBarrierHandlerState.java`）：

```java
@Override
public final BarrierHandlerState barrierReceived(
        Controller controller,
        InputChannelInfo channelInfo,
        CheckpointBarrier checkpointBarrier,
        boolean markChannelBlocked)
        throws IOException, CheckpointException {
    checkState(!checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint());

    if (markChannelBlocked) {
        state.blockChannel(channelInfo);  // ← 关键：阻塞收到 Barrier 的通道
    }

    if (controller.allBarriersReceived()) {
        return triggerGlobalCheckpoint(controller, checkpointBarrier);  // 全部到齐 → 快照
    }

    return convertAfterBarrierReceived(state);  // 继续等待其余 Barrier
}
```

**`blockChannel` 的实现**（`ChannelState.java`）：

```java
public void blockChannel(InputChannelInfo channelInfo) {
    // 调用 InputGate 阻塞该通道的数据消费
    inputs[channelInfo.getGateIdx()].blockConsumption(channelInfo);
    blockedChannels.add(channelInfo);
}

// 所有 Barrier 到齐后解除阻塞
public void unblockAllChannels() throws IOException {
    for (InputChannelInfo blockedChannel : blockedChannels) {
        inputs[blockedChannel.getGateIdx()].resumeConsumption(blockedChannel);
    }
    blockedChannels.clear();
}
```

### 2.3 数据推演：对齐过程中算子在干什么

```
时间线 →

通道0: [A1] [A2] [Barrier] [A3] [A4] [A5]
通道1: [B1] [B2] [B3] [B4] [Barrier] [B5]

算子的实际处理行为：

t1: 处理 A1 ✓
t2: 处理 B1 ✓
t3: 处理 A2 ✓
t4: 处理 B2 ✓
t5: 收到通道0的 Barrier → 阻塞通道0
    A3, A4, A5 被缓冲在 InputGate 中，不被处理
t6: 处理 B3 ✓  ← 通道1还没收到 Barrier，继续正常处理
t7: 处理 B4 ✓
t8: 收到通道1的 Barrier → 全部对齐！
    → 触发 Checkpoint（此时状态包含 A1,A2,B1,B2,B3,B4 的处理结果）
    → 解除通道0的阻塞
t9: 处理 A3 ✓  ← 开始处理之前缓冲的数据
t10: 处理 A4 ✓
t11: 处理 B5 ✓
t12: 处理 A5 ✓
```

**关键点**：
- **Barrier 之前的数据全部被处理**（体现在快照的状态中）
- **Barrier 之后的数据被缓冲**（快速通道的数据等到对齐后才处理）
- **慢通道在对齐期间正常处理数据**（它的 Barrier 还没到）
- 快照的状态是一个**逻辑上一致的切面**——所有通道恰好在 Barrier 位置切分

---

## 三、非对齐检查点：Barrier 到达时发生了什么

### 3.1 核心机制：不阻塞，但保存 in-flight 数据

非对齐模式的核心思想是：**Barrier 到哪就立刻开始快照，不等其他通道，但把网络中还在飞行的数据也保存到 Checkpoint 中**。

```
Input Channel 0: ─[数据]─[数据]─[Barrier]─[数据]─[数据]─→
                                    │
                          收到Barrier → 不阻塞！继续处理数据
                          但把通道1中 Barrier 前的 in-flight 数据
                          写入 Checkpoint 状态

Input Channel 1: ─[数据]─[数据]─[数据]─[数据]─[Barrier]─→
                   └──┬──┘ └──┬──┘
                      │      │
                  这些数据在通道0收到Barrier时还在网络中飞行
                  → 被 ChannelStatePersister 写入 Checkpoint
```

### 3.2 源码级流程

**状态机**：

```
                    ┌───────────────────────────────────┐
                    │ AlternatingWaitingForFirstBarrier │
                    │ Unaligned                         │
                    └────────────────┬──────────────────┘
                                     │ 收到第一个 Barrier
                                     │ → 不阻塞通道
                                     │ → checkpointStarted()
                                     │ → 开始记录 in-flight 数据
                                     │ → 立即触发 Checkpoint!
                                     ▼
                    ┌───────────────────────────────────┐
                    │ AlternatingCollectingBarriers     │
                    │ Unaligned                         │
                    │ 所有通道继续处理数据               │
                    │ 同时持续保存 in-flight 数据         │
                    └────────────────┬──────────────────┘
                                     │ 所有 Barrier 到齐
                                     │ → 停止记录 in-flight
                                     ▼
                    ┌───────────────────────────────────┐
                    │ AlternatingWaitingForFirstBarrier │
                    │ （进入下一轮）                     │
                    └───────────────────────────────────┘
```

**in-flight 数据保存的源码**（`ChannelStatePersister.java`）：

```java
// 非对齐模式下，收到 Barrier 通知后开始持久化
protected void startPersisting(long barrierId, List<Buffer> knownBuffers)
        throws CheckpointException {
    if (lastSeenBarrier < barrierId) {
        checkpointStatus = CheckpointStatus.BARRIER_PENDING;
        lastSeenBarrier = barrierId;
    }
    if (knownBuffers.size() > 0) {
        // 把已知的缓冲区数据写入 Checkpoint 状态
        channelStateWriter.addInputData(
                barrierId,
                channelInfo,
                ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                CloseableIterator.fromList(knownBuffers, Buffer::recycleBuffer));
    }
}

// Barrier 到达之前，每个 Buffer 都可能被持久化
protected void maybePersist(Buffer buffer) {
    if (checkpointStatus == CheckpointStatus.BARRIER_PENDING && buffer.isBuffer()) {
        // Barrier 还没到这个通道，但 Checkpoint 已经开始了
        // → 这个 Buffer 是 in-flight 数据，写入 Checkpoint
        channelStateWriter.addInputData(
                lastSeenBarrier,
                channelInfo,
                ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                CloseableIterator.ofElement(buffer.retainBuffer(), Buffer::recycleBuffer));
    }
}
```

### 3.3 数据推演：非对齐过程中算子在干什么

```
时间线 →

通道0: [A1] [A2] [Barrier] [A3] [A4]
通道1: [B1] [B2] [B3] [B4] [Barrier]

算子的实际处理行为：

t1: 处理 A1 ✓
t2: 处理 B1 ✓
t3: 处理 A2 ✓
t4: 收到通道0的 Barrier
    → 不阻塞！
    → 立即触发 Checkpoint
    → 通道1中还在飞的 B2, B3, B4 → 写入 Checkpoint 的 in-flight 状态
t5: 处理 B2 ✓  ← 数据被正常处理，同时也被保存到 in-flight 状态
t6: 处理 A3 ✓  ← 通道0 Barrier 后的数据正常处理
t7: 处理 B3 ✓
t8: 处理 A4 ✓
t9: 处理 B4 ✓
t10: 收到通道1的 Barrier → 停止记录 in-flight

Checkpoint 保存的内容：
├─ 算子状态：包含 A1, A2, B1 的处理结果（Barrier 之前所有通道已处理的）
├─ 通道1的 in-flight 数据：B2, B3, B4（通道0的 Barrier 到达时，通道1中尚未被处理的）
└─ 输出缓冲区的 in-flight 数据：算子已输出但下游还没收到的数据
```

**恢复时**：先恢复算子状态，再把 in-flight 数据**重新注入**到对应的通道中，从 Barrier 之后的位置继续处理。效果等价于"所有通道都在 Barrier 位置切分"。

---

## 四、对齐超时自动切换（Aligned → Unaligned）

Flink 支持配置 `aligned-checkpoint-timeout`，当对齐等待超过阈值时自动切换为非对齐模式：

```java
// 配置方式
env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(30));
```

### 4.1 切换流程

```
对齐模式开始
    │
    ├─ 收到第一个 Barrier → blockChannel → 开始对齐
    │
    ├─ 等待中... 其他通道的 Barrier 还没到
    │
    ├─ 30秒超时！
    │   │
    │   ▼
    │  alignedCheckpointTimeout() 被触发
    │   │
    │   ├─ 解除所有被阻塞的通道
    │   ├─ 把之前被缓冲的数据作为 in-flight 数据写入 Checkpoint
    │   ├─ Barrier 类型转换：checkpointBarrier.asUnaligned()
    │   ├─ 对所有输入调用 checkpointStarted() 开始记录 in-flight
    │   └─ 立即触发 Checkpoint（不再等待）
    │
    └─ 继续在非对齐模式下等待剩余 Barrier
```

**源码**（`AlternatingCollectingBarriers.java`）：

```java
@Override
public BarrierHandlerState alignedCheckpointTimeout(
        Controller controller, CheckpointBarrier checkpointBarrier)
        throws IOException, CheckpointException {
    // 把所有 Barrier 通知优先处理
    state.prioritizeAllAnnouncements();
    // 转换为非对齐 Barrier
    CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
    // 初始化输入通道的 Checkpoint（开始记录 in-flight 数据）
    controller.initInputsCheckpoint(unalignedBarrier);
    for (CheckpointableInput input : state.getInputs()) {
        input.checkpointStarted(unalignedBarrier);
    }
    // 立即触发 Checkpoint
    controller.triggerGlobalCheckpoint(unalignedBarrier);
    // 状态转移到非对齐收集模式
    return new AlternatingCollectingBarriersUnaligned(true, state);
}
```

**超时定时器的注册**（`SingleCheckpointBarrierHandler.java`）：

```java
private void registerAlignmentTimer(CheckpointBarrier announcedBarrier) {
    long timerDelay = BarrierAlignmentUtil.getTimerDelay(getClock(), announcedBarrier);
    this.currentAlignmentTimer = registerTimer.registerTask(() -> {
        long barrierId = announcedBarrier.getId();
        if (currentCheckpointId == barrierId
                && !getAllBarriersReceivedFuture(barrierId).isDone()) {
            // 还没收齐 → 超时 → 切换
            currentState = currentState.alignedCheckpointTimeout(context, announcedBarrier);
        }
        return null;
    }, Duration.ofMillis(timerDelay));
}
```

---

## 五、AT_LEAST_ONCE 模式：完全不阻塞

除了 Aligned 和 Unaligned，还有 `AT_LEAST_ONCE` 模式，使用 `CheckpointBarrierTracker`：

```java
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
```

| 特性 | AT_LEAST_ONCE |
|------|--------------|
| 阻塞通道 | 不阻塞 |
| 保存 in-flight 数据 | 不保存 |
| 触发快照时机 | 所有 Barrier 到齐时 |
| 数据一致性 | 可能有重复 |

**为什么会重复？** 因为慢通道的 Barrier 到达之前，快通道 Barrier 之后的数据已经被处理了。如果故障恢复，这些数据会被重新处理，导致重复。

```
通道0: [A1] [A2] [Barrier] [A3] [A4]
通道1: [B1] [B2] [B3] [Barrier]

AT_LEAST_ONCE 行为：
t1-t4: A1, B1, A2, B2 正常处理
t5: 收到通道0的 Barrier → 不阻塞
t6: 处理 A3 ✓  ← Barrier 后的数据直接处理了！
t7: 处理 B3 ✓
t8: 处理 A4 ✓
t9: 收到通道1的 Barrier → 全部到齐 → 触发快照

此时快照包含了 A3, A4 的处理结果（它们在 Barrier 之后）
恢复时 Source 从 Barrier 位置重放 → A3, A4 会被再次处理 → 重复
```

---

## 六、三种模式全景对比

### 6.1 行为对比

```
三个输入通道，Barrier 到达时间不同：

通道0: ──[数据]──[Barrier]──[数据]──[数据]──[数据]──→
通道1: ──[数据]──[数据]──[数据]──[Barrier]──[数据]──→
通道2: ──[数据]──[数据]──[数据]──[数据]──[Barrier]──→

═══ Aligned（对齐）═════════════════════════════════
通道0: ──[处理]──[Barrier]──[缓冲]──[缓冲]──[缓冲]──→
通道1: ──[处理]──[处理]──[处理]──[Barrier]──[缓冲]──→  ← 通道1未收到Barrier前正常处理
通道2: ──[处理]──[处理]──[处理]──[处理]──[Barrier]──→  ← 最慢的通道一直在处理
                                              ↑
                                        全部到齐，触发快照
                                        释放缓冲的数据
状态一致性：✓ 精确  |  吞吐影响：高  |  快照大小：小

═══ Unaligned（非对齐）══════════════════════════════
通道0: ──[处理]──[Barrier]──[处理]──[处理]──[处理]──→
通道1: ──[处理]──[处理]──[处理]──[Barrier]──[处理]──→  ← 全程不阻塞
通道2: ──[处理]──[处理]──[处理]──[处理]──[Barrier]──→  ← 全程不阻塞
                    ↑
              通道0 Barrier 到达，立即触发快照
              同时把通道1、2中 Barrier 前的数据保存为 in-flight

状态一致性：✓ 精确  |  吞吐影响：低  |  快照大小：大（含 in-flight）

═══ AT_LEAST_ONCE ═══════════════════════════════════
通道0: ──[处理]──[Barrier]──[处理]──[处理]──[处理]──→
通道1: ──[处理]──[处理]──[处理]──[Barrier]──[处理]──→  ← 全程不阻塞
通道2: ──[处理]──[处理]──[处理]──[处理]──[Barrier]──→  ← 全程不阻塞
                                              ↑
                                        全部到齐才触发快照
                                        但快照可能包含 Barrier 后数据

状态一致性：✗ 可能重复  |  吞吐影响：低  |  快照大小：小
```

### 6.2 特性对比表

| 特性 | Aligned | Unaligned | AT_LEAST_ONCE |
|------|---------|-----------|---------------|
| **语义保证** | Exactly-Once | Exactly-Once | At-Least-Once |
| **Barrier 到达后阻塞通道** | 是 | 否 | 否 |
| **数据被缓冲** | 是（快速通道的数据） | 否 | 否 |
| **in-flight 数据存入 Checkpoint** | 否 | 是 | 否 |
| **触发快照时机** | 所有 Barrier 到齐 | 第一个 Barrier 到达 | 所有 Barrier 到齐 |
| **对齐等待时间** | 可能很长 | 接近 0 | 无 |
| **Checkpoint 大小** | 小 | 大 | 小 |
| **反压场景表现** | 差（对齐等待加剧反压） | 好 | 好 |
| **适用场景** | 低延迟、少反压 | 高反压、大规模作业 | 容忍重复的场景 |

### 6.3 Handler 创建逻辑

**源码**（`InputProcessorUtil.java`）：

```java
private static SingleCheckpointBarrierHandler createBarrierHandler(...) {
    if (config.isUnalignedCheckpointsEnabled()) {
        // 启用了非对齐 → 使用 alternating 模式（先对齐，超时切非对齐）
        return SingleCheckpointBarrierHandler.alternating(
                taskName, toNotifyOnCheckpoint, checkpointCoordinator,
                clock, numberOfChannels, registerTimerCallback,
                enableCheckpointAfterTasksFinished, inputs);
    } else {
        // 未启用非对齐 → 纯对齐模式
        return SingleCheckpointBarrierHandler.aligned(
                taskName, toNotifyOnCheckpoint, clock, numberOfChannels,
                registerTimerCallback, enableCheckpointAfterTasksFinished, inputs);
    }
}
```

注意：`isUnalignedCheckpointsEnabled = true` 时，实际创建的是 **alternating** 模式（先尝试对齐，超时再切），而不是一开始就非对齐。

---

## 七、恢复时 in-flight 数据如何处理

非对齐 Checkpoint 恢复时，需要把之前保存的 in-flight 数据重新注入到通道中：

```
正常运行时保存的 Checkpoint：
├─ 算子状态：已处理数据的状态
├─ 输入通道 in-flight 数据：
│   ├─ Channel 1: [B2, B3, B4]  ← Barrier 前在通道1中飞行的数据
│   └─ Channel 2: [C1, C2]     ← Barrier 前在通道2中飞行的数据
└─ 输出缓冲区 in-flight 数据：
    └─ SubPartition 0: [X1, X2] ← 已发出但下游还没收到的数据

恢复时：
1. 恢复算子状态
2. 把 [B2, B3, B4] 重新注入到 Channel 1 的输入缓冲区
3. 把 [C1, C2] 重新注入到 Channel 2 的输入缓冲区
4. 把 [X1, X2] 重新注入到输出缓冲区
5. 然后再开始从 Source 消费新数据

效果：等价于所有通道都在 Barrier 处精确切分
```

---

## 八、面试口述速背手册

### 话题一：面试官问"对齐 Checkpoint 时还会处理数据吗"

> 对齐 Checkpoint 时，**不是所有通道都停止处理，而是先到 Barrier 的通道停、后到的继续处理**。
>
> 具体过程是：当一个算子有多个输入通道时，某个通道率先收到 Barrier，这个通道就被阻塞（`blockChannel`），它后续的数据会被缓冲在 InputGate 中不被处理。但其他还没收到 Barrier 的通道仍然正常处理数据。一直等到所有通道的 Barrier 都到齐，才触发状态快照，然后解除所有通道的阻塞，被缓冲的数据才开始被处理。
>
> 这样做的目的是保证快照的**一致性切面**——Barrier 之前的数据全部被处理并体现在状态中，Barrier 之后的数据一条都没被处理。恢复时从 Barrier 位置重放就不会丢也不会重复。

---

### 话题二：面试官问"非对齐 Checkpoint 时还会处理数据吗"

> 非对齐模式下**所有通道全程不阻塞，数据一直在被处理**。
>
> 当第一个通道的 Barrier 到达时，就立即触发 Checkpoint，不等其他通道。但问题是：其他通道的 Barrier 还没到，它们的 Barrier 之前还有一些数据在网络中飞行（in-flight data），这些数据属于"Barrier 之前"但还没被处理。
>
> Flink 的做法是：通过 `ChannelStatePersister` 把这些 in-flight 数据也**写入 Checkpoint 状态**。恢复时先恢复算子状态，再把 in-flight 数据重新注入到对应通道的输入缓冲区，然后再从 Source 消费新数据。这样逻辑上等价于所有通道都在 Barrier 处精确切分。
>
> 代价是 Checkpoint 更大（多了 in-flight 数据），好处是完全不阻塞、不影响吞吐，特别适合反压严重的场景。

---

### 话题三：面试官问"对齐和非对齐有什么区别，怎么选"

> 核心区别就是**阻塞 vs 不阻塞**。
>
> 对齐模式阻塞快速通道等待慢通道，优点是 Checkpoint 小（不存 in-flight 数据），缺点是对齐期间吞吐下降，反压场景下 Barrier 传播慢导致对齐时间很长，甚至 Checkpoint 超时失败。
>
> 非对齐模式不阻塞任何通道，优点是 Checkpoint 速度快、不受反压影响，缺点是 Checkpoint 更大（要存储 in-flight 数据），恢复时间也更长。
>
> **生产中推荐的做法**是配置 `aligned-checkpoint-timeout`，比如设为 30 秒。一开始走对齐模式，如果 30 秒内没对齐完就自动切换为非对齐模式。这样正常情况下用对齐（Checkpoint 小），反压时自动降级为非对齐（保证 Checkpoint 不超时）。源码中这个功能是通过 `AlternatingCollectingBarriers` 状态类实现的。

---

### 话题四：面试官问"AT_LEAST_ONCE 为什么会有重复数据"

> AT_LEAST_ONCE 模式使用 `CheckpointBarrierTracker`，它既不阻塞通道也不保存 in-flight 数据。它等所有 Barrier 到齐才触发快照，但在等待期间，快速通道 Barrier 之后的数据已经被处理了。
>
> 这意味着快照中的状态**包含了部分 Barrier 之后的数据的处理结果**。恢复时 Source 从 Barrier 位置重放，这些数据会被再次处理，导致重复。
>
> 举个例子：通道0的 Barrier 先到，通道0 Barrier 后面的数据 A3 被处理了，此时通道1的 Barrier 才到，触发快照。快照里包含 A3 的处理结果。恢复后 Source 从通道0的 Barrier 位置重放，A3 又被处理一次——重复了。

---

### 话题五：面试官问"对齐超时切换的原理"

> 配置 `aligned-checkpoint-timeout` 后，Flink 在收到第一个 Barrier 时会注册一个定时器。如果在超时时间内所有 Barrier 到齐了，正常走对齐模式。如果超时了还没收齐，定时器触发 `alignedCheckpointTimeout()` 方法：
>
> 1. 解除之前被阻塞的通道
> 2. 把之前被缓冲的数据重新标记为 in-flight 数据写入 Checkpoint
> 3. 把当前 Barrier 从 aligned 类型转换为 unaligned 类型
> 4. 对所有输入通道开始记录 in-flight 数据
> 5. 立即触发 Checkpoint 快照
>
> 从这一刻开始，就进入非对齐模式继续收集剩余的 Barrier。这是一种优雅的降级策略——正常走对齐，反压时自动降级。

---

## 九、核心源码文件索引

| 功能 | 核心文件 | 包路径 |
|------|----------|--------|
| Barrier 处理基类 | `CheckpointBarrierHandler.java` | `streaming/.../io/checkpointing/` |
| EXACTLY_ONCE Handler | `SingleCheckpointBarrierHandler.java` | 同上 |
| AT_LEAST_ONCE Handler | `CheckpointBarrierTracker.java` | 同上 |
| 状态机接口 | `BarrierHandlerState.java` | 同上 |
| 对齐模式基类 | `AbstractAlignedBarrierHandlerState.java` | 同上 |
| 交替模式基类 | `AbstractAlternatingAlignedBarrierHandlerState.java` | 同上 |
| 交替模式-等待 Barrier | `AlternatingWaitingForFirstBarrier.java` | 同上 |
| 交替模式-收集 Barrier | `AlternatingCollectingBarriers.java` | 同上 |
| 交替模式-非对齐收集 | `AlternatingCollectingBarriersUnaligned.java` | 同上 |
| 通道状态管理 | `ChannelState.java` | 同上 |
| InputGate Barrier 拦截 | `CheckpointedInputGate.java` | 同上 |
| Handler 创建工厂 | `InputProcessorUtil.java` | 同上 |
| 超时计算 | `BarrierAlignmentUtil.java` | 同上 |
| in-flight 数据持久化 | `ChannelStatePersister.java` | `runtime/.../partition/consumer/` |
| Channel State Writer | `ChannelStateWriter.java` | `runtime/.../checkpoint/channel/` |
| Checkpoint 选项 | `CheckpointOptions.java` | `runtime/.../checkpoint/` |

---

**文档版本**：v1.0
**基于 Flink 版本**：Apache Flink 1.18（release-1.18 分支）
**最后更新**：2026-03-22
