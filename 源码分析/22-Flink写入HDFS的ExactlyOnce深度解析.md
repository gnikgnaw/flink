# Flink 写入 HDFS 的 Exactly-Once 语义深度解析

> 以 FileSink 写入 HDFS 为例，深度剖析文件系统场景下的"两阶段提交"变体、三种故障场景的精确一次保证

---

## 一、核心问题：HDFS 用的是两阶段提交吗？

**严格来说，不是传统意义上的两阶段提交，而是一种基于文件状态机的"类两阶段提交"协议。**

| 对比维度 | 传统两阶段提交（如 Kafka Sink） | 文件系统的"类两阶段提交"（如 HDFS Sink） |
|----------|-------------------------------|---------------------------------------|
| 事务机制 | 依赖外部系统的事务支持（Kafka Transaction） | 利用文件状态转换（in-progress → pending → finished） |
| 预提交 | 调用外部系统的 `preCommit()`（如 Kafka flush） | 关闭文件 + 持久化文件元数据到 Checkpoint |
| 最终提交 | 调用外部系统的 `commit()`（如 Kafka commitTransaction） | 文件重命名（原子操作） |
| 回滚 | 调用外部系统的 `abort()`（如 Kafka abortTransaction） | 删除 in-progress 临时文件 |
| 幂等保证 | 由外部系统保证 | `commitAfterRecovery()` 幂等重命名 |

**但在 Flink 新 Sink API 的抽象层面，FileSink 确实实现了 `TwoPhaseCommittingSink` 接口**：

```java
// FileSink.java
public class FileSink<IN>
        implements StatefulSink<IN, FileWriterBucketState>,
                   TwoPhaseCommittingSink<IN, FileSinkCommittable>,  // ← 实现了两阶段提交接口
                   ...
```

它的"两阶段"对应关系是：
- **第一阶段（prepareCommit）**：关闭 in-progress 文件 → 转为 pending 状态 → 将 `FileSinkCommittable` 写入 Checkpoint
- **第二阶段（commit）**：`FileCommitter.commit()` 将 pending 文件重命名为最终文件名

---

## 二、文件状态机：三态转换

这是理解整个机制的关键——HDFS Sink 通过**文件的三种状态**来模拟事务：

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│  in-progress │  close  │   pending    │  rename  │  finished    │
│  (正在写入)   │ ──────→ │  (待提交)    │ ──────→  │  (已提交)    │
│              │         │              │         │              │
│ .part-0-0   │         │ .part-0-0    │         │ part-0-0     │
│ .inprogress │         │              │         │              │
└──────────────┘         └──────────────┘         └──────────────┘
      │                                                │
      │ 故障恢复时                                      │
      └─── 删除或恢复继续写入                            └─── 对下游可见
```

**文件命名规则**：
- **in-progress**：`.part-{subtaskIndex}-{partCounter}.inprogress`（带 `.` 前缀，对用户不可见）
- **pending**：文件已关闭，等待重命名
- **finished**：`part-{subtaskIndex}-{partCounter}`（无 `.` 前缀，对用户可见）

**核心源码**（`FileWriterBucket.java`）——关闭文件转为 pending：

```java
// FileWriterBucket.java
private void closePartFile() throws IOException {
    if (inProgressPart != null) {
        // 关闭文件，获取 pending 文件句柄
        InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable =
                inProgressPart.closeForCommit();
        pendingFiles.add(pendingFileRecoverable);   // 加入待提交列表
        inProgressPart = null;
    }
}
```

**核心源码**（`FileCommitter.java`）——提交 pending 文件：

```java
// FileCommitter.java
@Override
public void commit(Collection<CommitRequest<FileSinkCommittable>> requests) throws IOException {
    for (CommitRequest<FileSinkCommittable> request : requests) {
        FileSinkCommittable committable = request.getCommittable();

        if (committable.hasPendingFile()) {
            // 恢复 pending 文件并执行幂等提交（重命名）
            bucketWriter.recoverPendingFile(committable.getPendingFile())
                        .commitAfterRecovery();  // ← 幂等操作：已重命名则跳过
        }

        if (committable.hasInProgressFileToCleanup()) {
            // 清理未完成的 in-progress 文件（删除）
            bucketWriter.cleanupInProgressFileRecoverable(
                    committable.getInProgressFileToCleanup());
        }
    }
}
```

---

## 三、完整的 Checkpoint 生命周期

### 3.1 正常流程时序

```
时间轴 →

[持续写入数据到 in-progress 文件]
         │
         ▼
═══ Checkpoint Barrier 到达 ═══════════════════════════════════════
         │
         ▼
[Step 1] FileWriter.prepareCommit()        ← 第一阶段
         │
         ├─ 根据 RollingPolicy 判断是否关闭文件
         │  ├─ shouldRollOnCheckpoint = true → closePartFile()
         │  │   └─ in-progress 文件关闭 → 转为 pending
         │  └─ shouldRollOnCheckpoint = false → 文件继续写入
         │
         ├─ 收集所有 pending 文件 → 封装为 FileSinkCommittable
         └─ 返回 List<FileSinkCommittable>
         │
         ▼
[Step 2] FileWriter.snapshotState()        ← 状态持久化
         │
         ├─ 如果有 in-progress 文件（未被关闭的）：
         │   └─ 调用 inProgressPart.persist() → 持久化写入位置
         │       └─ 产生 InProgressFileRecoverable（包含文件路径、偏移量）
         │
         └─ 返回 FileWriterBucketState（包含 bucket 信息 + in-progress 状态）
         │
         ▼
[Step 3] Checkpoint 完成（所有算子的快照都成功）
         │
         ▼
[Step 4] FileCommitter.commit()            ← 第二阶段
         │
         ├─ 遍历所有 FileSinkCommittable
         │   ├─ pending 文件 → commitAfterRecovery() → 重命名为最终文件
         │   └─ in-progress 清理 → 删除旧的临时文件
         │
         └─ 文件对下游可见！
```

### 3.2 源码级流程

**prepareCommit**（`FileWriterBucket.java`）：

```java
List<FileSinkCommittable> prepareCommit(boolean endOfInput) throws IOException {
    // 1. 判断是否需要关闭当前 in-progress 文件
    if (inProgressPart != null
            && (rollingPolicy.shouldRollOnCheckpoint(inProgressPart) || endOfInput)) {
        closePartFile();  // in-progress → pending
    }

    List<FileSinkCommittable> committables = new ArrayList<>();

    // 2. 所有 pending 文件封装为 Committable
    pendingFiles.forEach(
            pendingFile -> committables.add(new FileSinkCommittable(bucketId, pendingFile)));
    pendingFiles.clear();

    // 3. 如果有需要清理的旧 in-progress 文件，也返回
    if (inProgressFileToCleanup != null) {
        committables.add(new FileSinkCommittable(bucketId, inProgressFileToCleanup));
        inProgressFileToCleanup = null;
    }

    return committables;
}
```

**snapshotState**（`FileWriterBucket.java`）：

```java
FileWriterBucketState snapshotState() throws IOException {
    InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
    long inProgressFileCreationTime = Long.MAX_VALUE;

    if (inProgressPart != null) {
        // 持久化 in-progress 文件的写入位置（不关闭文件）
        inProgressFileRecoverable = inProgressPart.persist();
        // 标记为待清理（恢复时如果发现这个文件没被正确提交，需要清理）
        inProgressFileToCleanup = inProgressFileRecoverable;
        inProgressFileCreationTime = inProgressPart.getCreationTime();
    }

    return new FileWriterBucketState(
            bucketId, bucketPath, inProgressFileCreationTime, inProgressFileRecoverable);
}
```

---

## 四、三种故障场景深度分析

### 4.1 故障场景一：Checkpoint 提交前故障（Barrier 到达之前或快照过程中）

**时间点**：数据正在写入 in-progress 文件，Checkpoint 尚未完成

```
正常写入 ──→ [数据 A, B, C 写入 in-progress 文件]
                                    │
                              ╳ 故障发生！
                                    │
                     Checkpoint 未完成，无有效快照
```

**系统行为**：

```
1. 回滚到上一个成功的 Checkpoint（假设 CP-N）
2. CP-N 之后写入的所有数据（A, B, C）都"不存在"了
   ├─ 因为 in-progress 文件的状态未被持久化
   └─ 即使 HDFS 上存在这个文件，也会被丢弃

3. 恢复流程：
   ├─ Source 从 CP-N 记录的 offset 重新消费数据
   ├─ FileWriter.initializeState() 从 CP-N 的状态恢复
   │   ├─ 恢复 CP-N 时的 in-progress 文件（如果有）
   │   └─ 恢复 CP-N 时的 pending 文件（如果有）
   └─ 开始重新处理 A, B, C
```

**Exactly-Once 保证分析**：

| 数据 | 是否丢失 | 是否重复 | 原因 |
|------|---------|---------|------|
| CP-N 之前的数据 | 不丢失 | 不重复 | 已通过之前的 Checkpoint 提交完成 |
| A, B, C | 不丢失 | 不重复 | 虽然写入了 in-progress 但未持久化状态，恢复后 Source 会重发，重新写入新的 in-progress 文件 |

**关键问题：HDFS 上遗留的旧 in-progress 文件怎么办？**

```java
// FileWriterBucket 恢复时的处理
private void restoreInProgressFile(FileWriterBucketState state) throws IOException {
    if (!state.hasInProgressFileRecoverable()) {
        return;  // CP-N 没有 in-progress 状态 → 不恢复
    }

    InProgressFileRecoverable inProgressFileRecoverable =
            state.getInProgressFileRecoverable();

    if (bucketWriter.getProperties().supportsResume()) {
        // HDFS 支持 resume → 从 CP-N 的写入位置继续写入
        // 效果：CP-N 之后写的数据被"截断"，从那个位置重新写
        inProgressPart = bucketWriter.resumeInProgressFileFrom(
                bucketId, inProgressFileRecoverable, state.getInProgressFileCreationTime());
    } else {
        // 不支持 resume → 转为 pending 文件提交 CP-N 之前的部分
        pendingFiles.add(inProgressFileRecoverable);
    }
}
```

**HDFS 的 `RecoverableWriter.recover()` 会将文件截断到 Checkpoint 时 `persist()` 记录的位置**，因此故障后多写的数据被自动丢弃。

---

### 4.2 故障场景二：Checkpoint 提交中故障（快照完成但 commit 未执行）

**时间点**：`prepareCommit()` 和 `snapshotState()` 已完成，但 `FileCommitter.commit()` 还没来得及执行

```
Checkpoint Barrier ──→ prepareCommit() ──→ snapshotState() ──→ Checkpoint 成功
     │                                                               │
     │  文件状态：                                                    │
     │  ├─ 部分文件已从 in-progress 转为 pending                      │
     │  └─ pending 文件信息已持久化到 Checkpoint 状态                  │
     │                                                               │
     │                                              FileCommitter.commit()
     │                                                    ╳ 还没执行，故障了！
     │
     └─ pending 文件存在于 HDFS，但未被重命名为最终文件
```

**系统行为**：

```
1. 从刚完成的 CP-(N+1) 恢复（因为快照已成功）

2. 恢复流程：
   FileWriter.initializeState(recoveredState)
       │
       ├─ 恢复 in-progress 文件状态
       │   └─ 如果 CP-(N+1) 快照时有 in-progress 文件 → resume 继续写入
       │
       └─ 恢复 pending 文件
           └─ cacheRecoveredPendingFiles()
               └─ 将 CP-(N+1) 中记录的 pending 文件加入待提交列表

3. 下一次 prepareCommit() 时：
   └─ 这些恢复的 pending 文件被封装为 FileSinkCommittable
       └─ 发送给 FileCommitter

4. FileCommitter.commit():
   └─ bucketWriter.recoverPendingFile(pendingFile).commitAfterRecovery()
       └─ 幂等地重命名文件（如果已重命名则跳过）
```

**Exactly-Once 保证分析**：

| 数据 | 是否丢失 | 是否重复 | 原因 |
|------|---------|---------|------|
| pending 文件中的数据 | 不丢失 | 不重复 | pending 文件信息已在 Checkpoint 中持久化，恢复后重新提交 |
| in-progress 文件中的数据 | 不丢失 | 不重复 | in-progress 状态已持久化，恢复后从 persist 位置继续写入 |

**关键保障：`commitAfterRecovery()` 的幂等性**

```java
// RecoverableFsDataOutputStream.Committer 接口
interface Committer {
    void commit() throws IOException;           // 非幂等，首次提交用
    void commitAfterRecovery() throws IOException;  // 幂等，恢复时用
}

// HDFS 实现的 commitAfterRecovery() 逻辑（伪代码）：
void commitAfterRecovery() {
    if (targetFile.exists()) {
        // 目标文件已存在 → 说明之前已经提交过了 → 跳过
        return;
    }
    if (tempFile.exists()) {
        // 临时文件存在 → 执行重命名
        fs.rename(tempFile, targetFile);
    }
    // 两者都不存在 → 异常情况，抛错
}
```

---

### 4.3 故障场景三：Checkpoint 提交后故障（commit 执行后）

**时间点**：`FileCommitter.commit()` 已执行完毕，pending 文件已被重命名为最终文件

```
... ──→ FileCommitter.commit() ──→ 文件已重命名 ──→ 继续处理新数据
                                                         │
                                               新数据写入 in-progress
                                                         │
                                                    ╳ 故障发生！
```

**系统行为**：

```
1. 从 CP-(N+1) 恢复（commit 之前的 Checkpoint）

2. 恢复流程：
   ├─ CP-(N+1) 的 pending 文件 → 再次调用 commitAfterRecovery()
   │   └─ 文件已经重命名 → commitAfterRecovery() 检测到目标文件已存在 → 跳过
   │       └─ 幂等保证：不会产生重复文件
   │
   ├─ CP-(N+1) 的 in-progress 文件 → resume 继续写入
   │   └─ 文件被截断到 CP-(N+1) 时 persist() 的位置
   │   └─ commit 后新写入的数据被丢弃
   │
   └─ Source 从 CP-(N+1) 的 offset 重新消费
       └─ 重新处理 commit 后的新数据
```

**Exactly-Once 保证分析**：

| 数据 | 是否丢失 | 是否重复 | 原因 |
|------|---------|---------|------|
| 已 commit 的数据 | 不丢失 | 不重复 | `commitAfterRecovery()` 幂等跳过 |
| commit 后新写入的数据 | 不丢失 | 不重复 | in-progress 截断 + Source 重放 |

---

## 五、全景故障恢复流程图

```
                         ┌─────────────────────────────┐
                         │      故障发生，开始恢复       │
                         └──────────────┬──────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │  从最后成功的 Checkpoint 恢复  │
                         │  Source 回退到保存的 offset    │
                         └──────────────┬──────────────┘
                                        │
                      ┌─────────────────┼─────────────────┐
                      ▼                 ▼                 ▼
          ┌───────────────────┐ ┌──────────────┐ ┌──────────────────┐
          │ 恢复 in-progress  │ │ 恢复 pending │ │ 清理残留文件      │
          │ 文件              │ │ 文件         │ │                  │
          └────────┬──────────┘ └──────┬───────┘ └────────┬─────────┘
                   │                   │                  │
          ┌────────▼──────────┐        │         ┌────────▼─────────┐
          │ 支持 resume？     │        │         │ 删除旧的         │
          ├─ 是: 截断到       │        │         │ in-progress 文件 │
          │   persist 位置    │        │         │ (cleanup)        │
          │   继续写入        │        │         └──────────────────┘
          │                   │        │
          └─ 否: 转为 pending │        │
             等待提交         │        │
                   │          │        │
                   ▼          ▼        ▼
          ┌─────────────────────────────────────┐
          │    下一次 prepareCommit()             │
          │    收集所有恢复的 pending 文件         │
          │    → 封装为 FileSinkCommittable       │
          └──────────────────┬──────────────────┘
                             │
                             ▼
          ┌─────────────────────────────────────┐
          │    FileCommitter.commit()            │
          │    commitAfterRecovery()（幂等）      │
          │    ├─ 文件已存在 → 跳过              │
          │    └─ 文件不存在 → 重命名            │
          └─────────────────────────────────────┘
```

---

## 六、in-progress 脏文件的清理机制

故障恢复后，HDFS 上可能残留"不属于任何有效 Checkpoint"的 in-progress 文件——这就是**脏数据**。Flink 有一套完整的清理链路来处理它们。

### 6.1 脏文件是怎么产生的

```
场景1：Checkpoint 之间故障
  数据 A → 写入 .part-0-5.inprogress
  数据 B → 追加写入 .part-0-5.inprogress
  ╳ 故障！Checkpoint 未完成
  →  恢复时系统回退到上一个 CP，不知道 .part-0-5.inprogress 的存在
  →  .part-0-5.inprogress 成为孤儿文件（脏数据）

场景2：resume 后旧 Checkpoint 的恢复元数据过期
  CP-N 时 persist() 记录了 .part-0-3.inprogress 在 offset=1024 的位置
  CP-(N+1) 时该文件被关闭 → 转为 pending → 提交为 finished
  此时 CP-N 中记录的 in-progress 恢复信息已无用
  → 对应的 ResumeRecoverable 元数据需要被清理

场景3：不支持 resume 的文件系统（如 S3）
  S3 使用 Multipart Upload，persist() 会创建临时上传对象
  如果这个 in-progress 文件后来被关闭或废弃
  → 临时上传对象成为脏数据，占用存储空间
```

### 6.2 Flink 自动清理机制（源码级）

Flink 通过**三层清理机制**处理 in-progress 脏文件：

#### 第一层：snapshotState 时标记待清理

```java
// FileWriterBucket.java - snapshotState()
FileWriterBucketState snapshotState() throws IOException {
    if (inProgressPart != null) {
        inProgressFileRecoverable = inProgressPart.persist();
        // 关键：将本次 persist 的文件标记为"待清理"
        // 含义：如果下一次 Checkpoint 中该文件被成功处理了，
        // 这个旧的恢复元数据就可以清理掉了
        inProgressFileToCleanup = inProgressFileRecoverable;
    }
    // ...
}
```

#### 第二层：prepareCommit 时将清理请求封装为 Committable

```java
// FileWriterBucket.java - prepareCommit()
List<FileSinkCommittable> prepareCommit(boolean endOfInput) throws IOException {
    // ... 处理 pending 文件 ...

    // 将上一次 snapshotState 标记的 in-progress 文件加入清理队列
    if (inProgressFileToCleanup != null) {
        committables.add(new FileSinkCommittable(bucketId, inProgressFileToCleanup));
        inProgressFileToCleanup = null;
    }
    return committables;
}
```

#### 第三层：FileCommitter 执行实际清理

```java
// FileCommitter.java - commit()
if (committable.hasInProgressFileToCleanup()) {
    // 调用 BucketWriter 清理 in-progress 文件的恢复状态
    bucketWriter.cleanupInProgressFileRecoverable(
            committable.getInProgressFileToCleanup());
}
```

**底层实现**（`OutputStreamBasedPartFileWriter.java:176-181`）：

```java
@Override
public boolean cleanupInProgressFileRecoverable(
        InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
    final RecoverableWriter.ResumeRecoverable resumeRecoverable =
            ((OutputStreamBasedInProgressFileRecoverable) inProgressFileRecoverable)
                    .getResumeRecoverable();
    // 委托给文件系统的 RecoverableWriter 执行清理
    return recoverableWriter.cleanupRecoverableState(resumeRecoverable);
}
```

**RecoverableWriter.cleanupRecoverableState()** 在不同文件系统的行为：

| 文件系统 | cleanupRecoverableState() 做了什么 | requiresCleanup |
|----------|-----------------------------------|-----------------|
| **HDFS** | 删除 `.part-x-x.inprogress` 临时文件 | true |
| **S3** | 中止 Multipart Upload，删除已上传的 Parts | true |
| **本地文件系统** | 删除临时文件 | true |
| **某些文件系统** | 无需额外清理 | false |

### 6.3 Flink 无法自动清理的情况

有些脏文件 Flink **无法自动清理**，需要人工或外部脚本处理：

| 场景 | 原因 | 表现 |
|------|------|------|
| **作业被 cancel 而非从 savepoint 停止** | 没有触发最终的 cleanup 流程 | HDFS 上残留 `.inprogress` 文件 |
| **作业 OOM 或被 kill -9** | 进程异常终止，cleanup 代码未执行 | 同上 |
| **Checkpoint 存储损坏** | 无法恢复，元数据丢失 | 无法知道哪些文件需要清理 |
| **更换 Checkpoint 重新启动** | 旧 Checkpoint 的文件信息丢失 | 旧的 in-progress 文件无人认领 |

### 6.4 生产环境的脏文件清理方案

#### 方案一：定期脚本清理（推荐）

```bash
#!/bin/bash
# 清理 HDFS 上超过 24 小时的 in-progress 文件
# 原理：正常的 in-progress 文件生命周期不会超过几个 Checkpoint 周期

HDFS_OUTPUT_PATH="/output"
MAX_AGE_HOURS=24

# 查找所有 .inprogress 文件
hdfs dfs -find ${HDFS_OUTPUT_PATH} -name "*.inprogress" | while read file; do
    # 获取文件修改时间
    mod_time=$(hdfs dfs -stat "%Y" "$file" 2>/dev/null)
    current_time=$(date +%s%3N)
    age_hours=$(( (current_time - mod_time) / 3600000 ))

    if [ "$age_hours" -gt "$MAX_AGE_HOURS" ]; then
        echo "Deleting stale in-progress file: $file (age: ${age_hours}h)"
        hdfs dfs -rm "$file"
    fi
done
```

#### 方案二：使用 OnCheckpointRollingPolicy 减少残留

```java
// 每次 Checkpoint 都关闭文件 → in-progress 文件存活时间最短
FileSink.forRowFormat(path, encoder)
    .withRollingPolicy(OnCheckpointRollingPolicy.build())
    .build();
// 效果：每个 Checkpoint 后都没有 in-progress 文件
// 脏文件只可能在 Checkpoint 间隙产生
```

#### 方案三：优雅停止作业

```bash
# 使用 savepoint 停止（会触发最终的 flush + commit）
flink stop --savepointPath hdfs:///savepoints <jobId>

# 而不是直接 cancel（cancel 不会清理 in-progress 文件）
# flink cancel <jobId>  ← 避免使用
```

### 6.5 清理流程总结

```
in-progress 脏文件的生命周期：
═══════════════════════════════════════════════════

产生 → [正常写入时创建 .inprogress 文件]
  │
  ├─ 正常关闭 → closeForCommit() → 转为 pending → commit → finished
  │              └─ 文件被重命名，.inprogress 消失 ✓
  │
  ├─ Checkpoint 时 persist() → 标记为 inProgressFileToCleanup
  │   └─ 下一次 prepareCommit() → FileCommitter 清理旧恢复元数据 ✓
  │
  ├─ 故障恢复 → resume 继续写入（截断到 persist 位置）
  │   └─ 旧数据被覆盖，不产生脏文件 ✓
  │
  └─ 异常终止（cancel/kill/OOM）
      └─ .inprogress 文件残留在 HDFS 上 ✗
          └─ 需要外部脚本定期清理
```

---

## 七、HDFS 特有的机制：RecoverableWriter

HDFS 能实现上述机制的基础是 `RecoverableWriter` 接口——它让文件系统支持"可恢复的写入"。

**文件**：`flink-core/.../fs/RecoverableWriter.java`

```java
public interface RecoverableWriter {
    // 打开新的可恢复输出流
    RecoverableFsDataOutputStream open(Path path) throws IOException;

    // 从持久化的恢复点恢复流（继续追加写入）
    RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException;

    // 为最终提交恢复流（不继续写入，只做提交）
    RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable)
            throws IOException;

    // 是否支持 resume（HDFS 支持，S3 不支持）
    boolean supportsResume();
}
```

**RecoverableFsDataOutputStream**——可恢复的文件输出流：

```java
public abstract class RecoverableFsDataOutputStream extends FSDataOutputStream {
    // 持久化当前写入进度（不关闭文件，可继续写入）
    // 返回 ResumeRecoverable：记录了文件路径 + 写入偏移量
    public abstract ResumeRecoverable persist() throws IOException;

    // 关闭文件以准备提交
    // 返回 Committer：用于执行最终的重命名操作
    public abstract Committer closeForCommit() throws IOException;
}
```

**不同文件系统对 `commitAfterRecovery()` 的实现差异**：

| 文件系统 | 提交机制 | resume 支持 | 幂等保证方式 |
|----------|----------|------------|-------------|
| **HDFS** | `rename()` 原子重命名 | 支持（truncate + append） | 检查目标文件是否存在 |
| **S3** | Multipart Upload Complete | 不支持 | 检查 Object 是否存在 |
| **本地文件系统** | `File.renameTo()` | 支持 | 检查目标文件是否存在 |
| **GCS** | Compose + Delete | 不支持 | 检查 Blob 是否存在 |

---

## 八、RollingPolicy 对 Exactly-Once 的影响

`RollingPolicy` 决定了何时关闭 in-progress 文件，直接影响 Checkpoint 时的文件状态。

### 8.1 三种 Rolling 策略

```java
// 策略1：默认策略——基于大小和时间滚动
DefaultRollingPolicy.builder()
    .withRolloverInterval(Duration.ofMinutes(15))  // 每 15 分钟滚动
    .withInactivityInterval(Duration.ofMinutes(5))  // 5 分钟无数据滚动
    .withMaxPartSize(MemorySize.ofMebiBytes(128))   // 128MB 滚动
    .build();

// 策略2：Checkpoint 时滚动（最严格的 Exactly-Once）
OnCheckpointRollingPolicy.build();
// 每次 Checkpoint 都关闭文件 → 每个文件对应一个 Checkpoint 周期
// 优点：文件边界与 Checkpoint 完全对齐，最安全
// 缺点：Checkpoint 频繁时产生大量小文件

// 策略3：不滚动（仅在任务结束时关闭）
// 适用于有限数据源（批处理模式）
```

### 8.2 对故障恢复的影响

```
使用 OnCheckpointRollingPolicy：
  每次 Checkpoint → 文件关闭 → 转为 pending → 提交
  恢复时：没有 in-progress 文件需要处理（已全部转为 pending）
  特点：最简单，最安全，但小文件多

使用 DefaultRollingPolicy：
  Checkpoint 时文件可能未满足滚动条件 → 继续写入
  恢复时：有 in-progress 文件 → 需要 resume 或截断
  特点：文件更大，但恢复逻辑更复杂
```

---

## 九、Pending 文件的恢复处理与文件 UUID 机制

### 9.1 Pending 文件在恢复时如何处理

你提到了 in-progress 文件恢复时 Flink 会截断重写，那 **已经处于 pending 状态的文件**呢？它们的恢复逻辑完全不同——**pending 文件不会被截断，而是被重新提交**。

**核心区别**：

| 文件状态 | 恢复时的处理方式 | 原因 |
|----------|----------------|------|
| **in-progress** | 截断到 `persist()` 位置，继续写入 | 文件内容可能包含 Checkpoint 之后的"脏数据"，需要截断 |
| **pending** | 原封不动，重新走 commit 流程 | 文件已关闭，内容完整且正确，只是还没被重命名 |

**源码流程**（`FileWriterBucket.java:143-150`）：

```java
private void cacheRecoveredPendingFiles(FileWriterBucketState state) {
    // 遍历 Checkpoint 状态中保存的所有 pending 文件
    // （按 checkpointId 分组存储）
    for (List<InProgressFileWriter.PendingFileRecoverable> restoredPendingRecoverables :
            state.getPendingFileRecoverablesPerCheckpoint().values()) {
        // 直接加入 pendingFiles 列表，等待下一次 prepareCommit 发送给 Committer
        pendingFiles.addAll(restoredPendingRecoverables);
    }
}
```

**完整恢复时序**：

```
故障恢复
    │
    ▼
FileWriterBucket 从 Checkpoint 状态恢复
    │
    ├─[1] restoreInProgressFile()
    │     └─ in-progress 文件 → resume（截断到 persist 位置）或转 pending
    │
    ├─[2] cacheRecoveredPendingFiles()
    │     └─ 所有 pending 文件 → 加入 pendingFiles 列表（不做任何修改）
    │
    ▼
第一次 prepareCommit() 调用
    │
    ├─ pendingFiles 中包含：
    │   ├─ 恢复的 pending 文件（来自上一个 Checkpoint）
    │   ├─ in-progress 转换来的 pending 文件（不支持 resume 时）
    │   └─ 新写入后关闭的 pending 文件
    │
    └─ 全部封装为 FileSinkCommittable → 发送给 FileCommitter
         │
         ▼
    FileCommitter.commit()
         │
         └─ commitAfterRecovery()（幂等）
              ├─ 文件已被重命名 → 跳过（之前的 commit 其实成功了）
              └─ 文件未被重命名 → 执行重命名（之前的 commit 没执行到这个文件）
```

**为什么 pending 文件不需要截断？**

因为 pending 文件是通过 `closeForCommit()` 产生的——文件已经被正确关闭，写入的数据量是确定的（记录在 `PendingFileRecoverable.getSize()` 中）。它只是"等待重命名"而已，文件内容本身没有任何问题。

**RollingPolicy 的影响**：

```
使用 OnCheckpointRollingPolicy 时：
  每次 Checkpoint → 所有文件关闭 → 全部变为 pending
  恢复时：只有 pending 文件，没有 in-progress 文件
  → 恢复最简单，只需要重新提交 pending 文件

使用 DefaultRollingPolicy 时：
  Checkpoint 时部分文件未满足滚动条件 → 仍为 in-progress
  恢复时：既有 pending 文件又有 in-progress 文件
  → pending 重新提交 + in-progress 截断恢复
  → 逻辑更复杂，但文件更大、数量更少
```

### 9.2 文件 UUID 的生成机制与作用

FileSink 生成的文件名中包含一个 **UUID**，这不是随意设计，而是保证 Exactly-Once 的关键一环。

**文件名格式**：

```
{partPrefix}-{uniqueId}-{partCounter}{partSuffix}

示例：
part-4b5c6d7e-8f9a-1234-abcd-ef0123456789-0        ← 第一个文件
part-4b5c6d7e-8f9a-1234-abcd-ef0123456789-1        ← 第二个文件
part-4b5c6d7e-8f9a-1234-abcd-ef0123456789-2.txt    ← 带后缀

写入中：
.part-4b5c6d7e-8f9a-1234-abcd-ef0123456789-0.inprogress  ← 临时文件
```

**源码**（`FileWriterBucket.java:264-275`）：

```java
/** 构造新的文件路径并递增 partCounter */
private Path assembleNewPartPath() {
    long currentPartCounter = partCounter++;
    return new Path(
            bucketPath,
            outputFileConfig.getPartPrefix()    // 默认 "part"
                    + '-'
                    + uniqueId                   // UUID
                    + '-'
                    + currentPartCounter         // 递增计数器
                    + outputFileConfig.getPartSuffix());  // 默认 ""
}
```

**UUID 的生成时机**（`FileWriterBucket.java:97`）：

```java
// 新建 Bucket 时生成
private FileWriterBucket(...) {
    // ...
    this.uniqueId = UUID.randomUUID().toString();  // 每个 Bucket 实例一个 UUID
    this.partCounter = 0;
}
```

### 9.3 UUID 的三个核心作用

#### 作用一：防止并行 Subtask 之间的文件名冲突

```
Subtask 0 → bucket="2024-01-01" → uniqueId = "aaa-111"
  生成文件：part-aaa-111-0, part-aaa-111-1, ...

Subtask 1 → bucket="2024-01-01" → uniqueId = "bbb-222"
  生成文件：part-bbb-222-0, part-bbb-222-1, ...

虽然两个 Subtask 写入同一个 bucket 目录，但 UUID 不同 → 文件名不冲突
```

如果没有 UUID，仅靠 `subtaskIndex + partCounter`，在**并行度变更**（rescale）时可能产生冲突。

#### 作用二：故障恢复后防止新旧文件名冲突

```
正常运行：
  uniqueId = "aaa-111"
  写了 part-aaa-111-0, part-aaa-111-1, part-aaa-111-2

故障 → 从 Checkpoint 恢复：
  创建新的 FileWriterBucket → 新 uniqueId = "ccc-333"（重新生成）
  新文件：part-ccc-333-0, part-ccc-333-1, ...

旧的 part-aaa-111-2 如果是 pending 状态 → 通过 commitAfterRecovery() 提交
新的 part-ccc-333-0 → 正常写入

UUID 不同 → 恢复后的新文件绝对不会和旧文件重名
```

#### 作用三：支持并发执行尝试（Speculative Execution）

```
FileSink 实现了 SupportsConcurrentExecutionAttempts 接口
多个执行尝试可能同时写入同一个 bucket

Attempt 0 → uniqueId = "xxx-000"
Attempt 1 → uniqueId = "yyy-111"（投机执行）

即使两个 Attempt 并发运行，UUID 保证文件名不冲突
最终只有成功的 Attempt 的文件被 commit
```

### 9.4 partCounter 的恢复机制

`partCounter` 在恢复时也有特殊处理——确保不会重用已有的计数值：

```
正常运行：
  partCounter = 0 → 写 part-xxx-0
  partCounter = 1 → 写 part-xxx-1
  partCounter = 2 → 写 part-xxx-2（in-progress）

  Checkpoint 保存：partCounter = 2

故障恢复：
  恢复 → partCounter 从 Checkpoint 中恢复为 2
  但因为 UUID 是重新生成的（新 Bucket 实例）
  所以 partCounter 从 0 开始也不会冲突
```

---

## 十、完整使用示例

```java
// 创建 FileSink 写入 HDFS
FileSink<String> sink = FileSink
    .forRowFormat(
        new Path("hdfs://namenode:8020/output"),
        new SimpleStringEncoder<String>("UTF-8"))
    .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
    .withRollingPolicy(
        DefaultRollingPolicy.builder()
            .withRolloverInterval(Duration.ofMinutes(15))
            .withInactivityInterval(Duration.ofMinutes(5))
            .withMaxPartSize(MemorySize.ofMebiBytes(128))
            .build())
    .withOutputFileConfig(
        OutputFileConfig.builder()
            .withPartPrefix("data")
            .withPartSuffix(".txt")
            .build())
    .build();

// 配置 Checkpoint（必须开启）
env.enableCheckpointing(60_000);  // 60 秒
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);

// 使用
stream.sinkTo(sink);
```

---

## 十一、面试口述速背手册

> 以下内容按 STAR 法则组织，面试时可直接口述。

### 话题一：面试官问"Flink 写 HDFS 怎么保证 Exactly-Once"

> **S（背景）**：Flink 的 FileSink 写入 HDFS 时，面临一个挑战——HDFS 不像 Kafka 那样有原生的事务支持，不能简单地 beginTransaction / commitTransaction。但又需要保证每条数据精确写入一次，不丢不重。
>
> **T（目标）**：利用文件系统的特性，实现一种"类两阶段提交"协议。
>
> **A（方案）**：FileSink 实现了 `TwoPhaseCommittingSink` 接口，但底层用的是**文件三态转换**来模拟事务。核心是三个文件状态：
>
> - **in-progress**：正在写入的临时文件，文件名带 `.inprogress` 后缀，对下游不可见
> - **pending**：文件已关闭但未提交，等待最终重命名
> - **finished**：通过原子重命名操作变为最终文件名，对下游可见
>
> Checkpoint 来的时候，第一阶段 `prepareCommit()` 根据 RollingPolicy 决定是否关闭文件（转为 pending），然后把所有 pending 文件的元数据（路径、大小、恢复句柄）持久化到 Checkpoint 状态中。第二阶段 `FileCommitter.commit()` 在 Checkpoint 完成后执行，把 pending 文件原子重命名为最终文件名。
>
> 如果文件在 Checkpoint 时没被关闭（比如 DefaultRollingPolicy 下文件还没满），那就调用 `persist()` 记录当前写入位置到 Checkpoint，恢复时可以从这个位置继续写入。
>
> **R（效果）**：通过文件状态机 + Checkpoint 持久化 + `commitAfterRecovery()` 幂等提交，实现了端到端的 Exactly-Once。任何时刻故障都不会导致数据丢失或重复。

---

### 话题二：面试官问"Checkpoint 提交前故障怎么办"

> 如果故障发生在 Checkpoint 完成之前，说明当前这轮快照没有成功。系统会回退到上一个成功的 Checkpoint，Source 从上一个 Checkpoint 保存的 offset 重新消费数据。
>
> 此时 HDFS 上可能存在一个写了一半的 in-progress 文件，但因为它的状态没有被持久化到任何成功的 Checkpoint 中，恢复时 FileWriter 根本不知道它的存在。系统会根据上一个 Checkpoint 保存的 in-progress 文件状态来恢复——调用 `RecoverableWriter.recover()` 从上次 `persist()` 的位置恢复，文件会被自动截断到那个位置。然后 Source 重放的数据会重新写入，不丢不重。

---

### 话题三：面试官问"Checkpoint 提交中故障怎么办（快照成功但 commit 没执行）"

> 这是最关键的场景。Checkpoint 快照已经成功了，pending 文件的信息已经持久化到了 Checkpoint 状态中。但 `FileCommitter.commit()` 还没来得及执行，也就是 pending 文件还没被重命名。
>
> 恢复时，系统从这个成功的 Checkpoint 恢复。`FileWriter.initializeState()` 会调用 `cacheRecoveredPendingFiles()` 把 Checkpoint 中记录的 pending 文件加入待提交列表。然后在下一次 `prepareCommit()` 时，这些 pending 文件会被发送给 `FileCommitter`，执行 `commitAfterRecovery()` 进行幂等提交。
>
> `commitAfterRecovery()` 是幂等的——如果目标文件已经存在（说明之前的 commit 其实执行了一部分），就跳过。这样无论 commit 执行到哪一步故障，恢复后都能安全重试。

---

### 话题四：面试官问"Checkpoint 提交后故障怎么办"

> commit 已经执行完了，pending 文件已经成功重命名为最终文件。之后故障的话，系统会从执行 commit 之前的那个 Checkpoint 恢复。
>
> 恢复后，`FileCommitter` 会再次尝试 `commitAfterRecovery()`，但因为文件已经被成功重命名了，`commitAfterRecovery()` 检测到目标文件已存在，直接跳过。所以不会产生重复文件。
>
> 同时，commit 之后新写入的 in-progress 文件中的数据会被丢弃——因为恢复时 `RecoverableWriter.recover()` 会把文件截断到 Checkpoint 时 `persist()` 的位置。而 Source 会从 Checkpoint 的 offset 重新消费数据重新写入。

---

### 话题五：面试官问"和 Kafka Sink 的两阶段提交有什么区别"

> 本质区别是**有没有外部事务系统**。
>
> Kafka Sink 的两阶段提交是利用 Kafka 原生的事务机制——`beginTransaction()`、`commitTransaction()`、`abortTransaction()`，数据在事务提交前对消费者不可见。
>
> HDFS Sink 没有事务系统可用，它是通过**文件状态转换**来模拟事务的——利用文件系统的原子重命名操作作为"提交"，利用文件名前缀（`.inprogress`）来控制可见性，利用 `RecoverableWriter` 的 `persist() / recover()` 来实现写入位置的持久化和恢复。
>
> 两者在 Flink 新 Sink API 中都统一实现了 `TwoPhaseCommittingSink` 接口，但底层机制完全不同。HDFS 方案的核心依赖是 `commitAfterRecovery()` 的幂等性和文件系统 `rename()` 的原子性。

---

**文档版本**：v1.0
**基于 Flink 版本**：Apache Flink 1.18（release-1.18 分支）
**最后更新**：2026-03-21
