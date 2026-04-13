# Mailbox 模型源码深度解析

## 一、为什么需要 Mailbox 模型

### 1.1 旧模型的问题：Checkpoint Lock

在 Flink 早期（SourceFunction 时代），Task 线程和 Checkpoint 线程之间通过 `synchronized(checkpointLock)` 来保证互斥：

```java
// 用户代码中必须手动加锁
synchronized (ctx.getCheckpointLock()) {
    ctx.collect(element);
    offset++;
}
```

这种方式存在严重问题：

| 问题 | 说明 |
|------|------|
| **用户负担重** | 用户必须正确使用 checkpoint lock，忘记加锁就会导致状态不一致 |
| **死锁风险** | 多把锁交叉持有时容易产生死锁 |
| **性能瓶颈** | synchronized 是重量级锁，竞争激烈时性能下降 |
| **难以扩展** | 定时器、异步回调等操作都需要争抢同一把锁 |

### 1.2 Mailbox 模型的核心思想

Mailbox 模型借鉴了 **Actor 模型**的思想：

```
所有操作（数据处理、Checkpoint、定时器、异步回调）
都以 Mail 的形式投递到一个队列中，
由单一的 Mailbox 线程按顺序串行执行。
```

**从"多线程 + 锁"变为"单线程 + 消息队列"**，从根本上消除了并发竞争。

## 二、核心架构

### 2.1 整体结构

```
                    外部线程（Checkpoint Coordinator、Timer 等）
                           │
                           │ put(Mail)
                           ▼
                    ┌──────────────┐
                    │  TaskMailbox  │  ← 线程安全的消息队列
                    │  (队列)       │     多个写入者，单个读取者
                    └──────┬───────┘
                           │ take() / tryTakeFromBatch()
                           ▼
                    ┌──────────────────┐
                    │ MailboxProcessor  │  ← 核心调度循环
                    │                  │
                    │  while(running) { │
                    │    processMail()  │  ← 处理队列中的 Mail
                    │    defaultAction()│  ← 处理数据（读 InputGate）
                    │  }               │
                    └──────────────────┘
                           │
                           ▼
                    Mailbox Thread（唯一的执行线程）
```

### 2.2 三个核心组件

| 组件 | 类名 | 职责 |
|------|------|------|
| **邮件** | `Mail` | 封装一个待执行的操作（Runnable + 优先级 + 描述） |
| **邮箱** | `TaskMailboxImpl` | 线程安全的双端队列，多写单读 |
| **处理器** | `MailboxProcessor` | 调度循环，交替处理 Mail 和默认动作（数据处理） |

## 三、源码解析

### 3.1 Mail —— 最小执行单元

> 源码位置：`flink-streaming-java/.../tasks/mailbox/Mail.java`

```java
public class Mail {
    private final ThrowingRunnable<? extends Exception> runnable; // 实际操作
    private final int priority;                                    // 优先级
    private final StreamTaskActionExecutor actionExecutor;         // 执行装饰器
    private final String descriptionFormat;                        // 描述（用于调试）

    public void run() throws Exception {
        actionExecutor.runThrowing(runnable);  // 通过 executor 执行
    }
}
```

**关键设计：**
- `priority` 用于区分上下游 Mail，避免死锁（下游算子不会处理上游投递的低优先级 Mail）
- `actionExecutor` 对于旧版 SourceStreamTask 是 `SynchronizedStreamTaskActionExecutor`（加锁执行），对于新版 Task 是 `IMMEDIATE`（直接执行，因为已经是单线程）

### 3.2 TaskMailboxImpl —— 线程安全的邮箱

> 源码位置：`flink-streaming-java/.../tasks/mailbox/TaskMailboxImpl.java`

```java
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
    private final ReentrantLock lock = new ReentrantLock();
    private final Deque<Mail> queue = new ArrayDeque<>();     // 主队列（加锁访问）
    private final Condition notEmpty = lock.newCondition();
    private final Deque<Mail> batch = new ArrayDeque<>();     // 批处理缓存（无锁访问）
    private volatile boolean hasNewMail = false;              // 快速检查标志
    private final Thread taskMailboxThread;                    // 绑定的 Mailbox 线程
}
```

#### 双队列设计（性能优化的关键）

```
外部线程写入：                         Mailbox 线程读取：

  put(mail)                            1. 先从 batch 读（无锁）
    │                                     │
    ▼                                     ▼
  lock → queue.addLast(mail) → unlock    batch 空了？
         hasNewMail = true                  │
         notEmpty.signal()                  ▼
                                       2. lock → 把 queue 全部转移到 batch → unlock
                                          （createBatch）
                                           │
                                           ▼
                                       3. 从 batch 逐个取出执行（无锁）
```

**为什么要分 queue 和 batch？**

```
热路径优化：大部分时间 Mailbox 线程从 batch 读取，不需要加锁。
只有 batch 耗尽时，才加一次锁把 queue 批量转移到 batch。
```

核心方法：

```java
// 外部线程投递 Mail（需要加锁）
public void put(@Nonnull Mail mail) {
    lock.lock();
    try {
        queue.addLast(mail);
        hasNewMail = true;
        notEmpty.signal();       // 唤醒可能阻塞的 Mailbox 线程
    } finally {
        lock.unlock();
    }
}

// 创建批次：把 queue 中的所有 Mail 转移到 batch（减少锁竞争）
public boolean createBatch() {
    if (!hasNewMail) {
        return !batch.isEmpty();  // 快速路径：volatile 读
    }
    lock.lock();
    try {
        Mail mail;
        while ((mail = queue.pollFirst()) != null) {
            batch.addLast(mail);
        }
        hasNewMail = false;
        return !batch.isEmpty();
    } finally {
        lock.unlock();
    }
}

// 从 batch 取 Mail（无锁，只有 Mailbox 线程访问）
public Optional<Mail> tryTakeFromBatch() {
    return Optional.ofNullable(batch.pollFirst());
}
```

#### 优先投递（putFirst）

```java
public void putFirst(@Nonnull Mail mail) {
    if (isMailboxThread()) {
        batch.addFirst(mail);      // Mailbox 线程自己投递，直接放 batch 头部（无锁）
    } else {
        lock.lock();
        try {
            queue.addFirst(mail);  // 外部线程投递，放 queue 头部
            hasNewMail = true;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }
}
```

### 3.3 MailboxProcessor —— 核心调度循环

> 源码位置：`flink-streaming-java/.../tasks/mailbox/MailboxProcessor.java`

```java
public class MailboxProcessor implements Closeable {
    protected final TaskMailbox mailbox;
    protected final MailboxDefaultAction mailboxDefaultAction;  // 默认动作（处理数据）
    private boolean mailboxLoopRunning;
    private boolean suspended;
    private final StreamTaskActionExecutor actionExecutor;
}
```

#### 主循环 runMailboxLoop()

这是 Flink Task 执行的核心入口：

```java
public void runMailboxLoop() throws Exception {
    suspended = !mailboxLoopRunning;
    final TaskMailbox localMailbox = mailbox;

    checkState(localMailbox.isMailboxThread(),
        "Method must be executed by declared mailbox thread!");

    final MailboxController mailboxController = new MailboxController(this);

    while (isNextLoopPossible()) {
        // 1. 先处理所有待处理的 Mail（Checkpoint、Timer 等）
        processMail(localMailbox, false);

        // 2. 再执行默认动作（从 InputGate 读数据并处理）
        if (isNextLoopPossible()) {
            mailboxDefaultAction.runDefaultAction(mailboxController);
        }
    }
}
```

**核心节奏：处理 Mail → 处理数据 → 处理 Mail → 处理数据 → ...**

```
┌──────────────────────────────────────────────────┐
│                  Mailbox Loop                     │
│                                                   │
│  ┌─────────────┐    ┌──────────────────────────┐ │
│  │ processMail │───▶│ defaultAction            │ │
│  │             │    │ (处理 InputGate 中的数据)  │ │
│  │ - Checkpoint│    │                          │ │
│  │ - Timer     │    │ - 读取 StreamRecord      │ │
│  │ - 异步回调   │    │ - 调用算子处理           │ │
│  │ - 控制命令   │    │ - 发送到下游             │ │
│  └─────────────┘    └──────────────────────────┘ │
│         ▲                      │                  │
│         └──────────────────────┘                  │
│                  循环                              │
└──────────────────────────────────────────────────┘
```

#### processMail() 的分层处理

```java
private boolean processMail(TaskMailbox mailbox, boolean singleStep) throws Exception {
    // 优化：先用 volatile 读检查是否有新 Mail
    boolean isBatchAvailable = mailbox.createBatch();

    // 非阻塞地处理当前批次的所有 Mail
    boolean processed = isBatchAvailable && processMailsNonBlocking(singleStep);

    if (singleStep) {
        return processed;
    }

    // 如果默认动作不可用（如 InputGate 没数据），
    // 阻塞等待 Mail 到来
    processed |= processMailsWhenDefaultActionUnavailable();
    return processed;
}
```

## 四、Checkpoint 如何通过 Mailbox 执行

### 4.1 触发链路

```
CheckpointCoordinator (JobManager)
    │
    │ RPC 调用
    ▼
TaskManager 收到 triggerCheckpoint
    │
    ▼
StreamTask.triggerCheckpointAsync()
    │
    │ mainMailboxExecutor.execute(...)  ← 投递到 Mailbox
    ▼
Mailbox Queue: [..., checkpoint-mail, ...]
    │
    │ Mailbox 线程按顺序取出执行
    ▼
triggerCheckpointAsyncInMailbox()
    │
    ▼
performCheckpoint() → snapshotState()
```

### 4.2 源码关键路径

> 源码位置：`flink-streaming-java/.../tasks/StreamTask.java`

```java
@Override
public CompletableFuture<Boolean> triggerCheckpointAsync(
        CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {

    CompletableFuture<Boolean> result = new CompletableFuture<>();

    // 关键：把 Checkpoint 操作作为 Mail 投递到 Mailbox
    mainMailboxExecutor.execute(
            () -> {
                result.complete(
                    triggerCheckpointAsyncInMailbox(checkpointMetaData, checkpointOptions));
            },
            "checkpoint %s with %s",
            checkpointMetaData, checkpointOptions);

    return result;
}
```

**Checkpoint 不再需要抢锁！它以 Mail 的形式排队，等 Mailbox 线程处理完当前数据后自然执行。**

### 4.3 对比旧模型

```
旧模型（SourceFunction + checkpoint lock）：
  Source 线程：  synchronized(lock) { collect + updateState }
  Checkpoint 线程：synchronized(lock) { snapshotState }
  → 两个线程抢同一把锁

新模型（Mailbox）：
  Mailbox 线程：processMail() → 执行 checkpoint
  Mailbox 线程：defaultAction() → 处理数据
  → 同一个线程，串行执行，不需要锁
```

## 五、定时器与异步回调如何通过 Mailbox 执行

### 5.1 定时器触发

```java
// 定时器到期时，回调会被封装为 Mail 投递
processingTimeService.registerTimer(timestamp, (time) -> {
    // 这个回调会被包装成 Mail 投递到 Mailbox
    // 由 Mailbox 线程执行，天然与数据处理串行
    fireTimer(time);
});
```

### 5.2 异步 IO 回调

```java
// AsyncWaitOperator 的异步结果就绪时
// 回调通过 Mailbox 投递，确保与数据处理串行
mailboxExecutor.execute(
    () -> outputCompletedElement(),
    "async-io callback"
);
```

## 六、Mailbox 的状态生命周期

```
    OPEN ──────────────► QUIESCED ──────────────► CLOSED
    (正常运行)            (静默期)                  (关闭)

    - 可以 put           - 不能 put              - 不能 put
    - 可以 take          - 可以 take             - 不能 take
                          (处理剩余 Mail)          (丢弃所有 Mail)
```

```java
// 准备关闭：停止接收新 Mail，但处理完已有的
public void quiesce() {
    if (state == OPEN) {
        state = QUIESCED;
    }
}

// 完全关闭：清空队列，唤醒所有等待线程
public List<Mail> close() {
    List<Mail> droppedMails = drain();
    state = CLOSED;
    notEmpty.signalAll();
    return droppedMails;
}
```

## 七、性能优化细节

### 7.1 volatile 快速检查

```java
// hasNewMail 是 volatile 变量
// 热路径中只做一次 volatile 读，避免加锁
public boolean hasMail() {
    return !batch.isEmpty() || hasNewMail;  // batch 检查无锁，hasNewMail 是 volatile
}
```

### 7.2 批量转移减少锁竞争

```
不用批量转移的情况：每次取一个 Mail 都要加锁
  lock → take → unlock → lock → take → unlock → ...

使用批量转移：一次加锁把所有 Mail 搬到 batch，后续无锁读取
  lock → 转移 N 个到 batch → unlock → 无锁取 N 次
```

### 7.3 Mailbox 线程自投递优化

```java
public void putFirst(@Nonnull Mail mail) {
    if (isMailboxThread()) {
        batch.addFirst(mail);   // Mailbox 线程自己投递，直接操作 batch，无锁
    } else {
        // 外部线程投递，需要加锁操作 queue
        lock.lock();
        ...
    }
}
```

## 八、Mailbox 与 SourceFunction checkpoint lock 的关系

### 8.1 SourceStreamTask 中的特殊处理

SourceStreamTask 是旧版 API 的遗留产物，它同时使用了 Mailbox 和 checkpoint lock：

```java
// SourceStreamTask.java
public SourceStreamTask(Environment env) throws Exception {
    this(env, new Object());  // 创建 lock
}

private SourceStreamTask(Environment env, Object lock) throws Exception {
    super(env, null, FatalExitExceptionHandler.INSTANCE,
        StreamTaskActionExecutor.synchronizedExecutor(lock));  // Mailbox 的 Mail 执行时也加锁
    this.lock = lock;  // 同一把锁传给 SourceContext
}
```

**对于 SourceStreamTask：Mailbox 中的 Mail 执行时，也会通过 `SynchronizedStreamTaskActionExecutor` 获取 lock，保证与 Source 线程互斥。**

### 8.2 新版 Source（FLIP-27）

新版 Source API 完全基于 Mailbox 模型：
- `SourceReader` 的数据读取作为 defaultAction 在 Mailbox 线程执行
- Checkpoint 作为 Mail 在同一线程串行执行
- **完全不需要 checkpoint lock**

## 九、总结

### 9.1 Mailbox 解决了什么

| 旧模型问题 | Mailbox 方案 |
|-----------|-------------|
| 用户必须手动加 checkpoint lock | 框架保证单线程串行，用户无感 |
| 多线程竞争导致死锁 | 单线程消息队列，不存在竞争 |
| synchronized 性能开销 | 只在 Mail 入队时加锁，执行时无锁 |
| 定时器、异步回调的线程安全问题 | 统一通过 Mailbox 投递，串行执行 |

### 9.2 核心设计要点

```
1. 单线程执行模型：所有操作在 Mailbox 线程串行执行，消除并发问题
2. 双队列优化：queue（加锁写入）+ batch（无锁读取），减少锁竞争
3. 优先级机制：避免上下游 Mail 混合导致的死锁
4. 批量转移：一次加锁搬运所有 Mail，热路径几乎无锁
5. 兼容旧 API：SourceStreamTask 通过 SynchronizedStreamTaskActionExecutor 桥接
```

### 9.3 关键源码文件

| 文件 | 路径 |
|------|------|
| Mail | `flink-streaming-java/.../runtime/tasks/mailbox/Mail.java` |
| TaskMailboxImpl | `flink-streaming-java/.../runtime/tasks/mailbox/TaskMailboxImpl.java` |
| MailboxProcessor | `flink-streaming-java/.../runtime/tasks/mailbox/MailboxProcessor.java` |
| StreamTask | `flink-streaming-java/.../runtime/tasks/StreamTask.java` |
| SourceStreamTask | `flink-streaming-java/.../runtime/tasks/SourceStreamTask.java` |
| StreamTaskActionExecutor | `flink-streaming-java/.../runtime/tasks/StreamTaskActionExecutor.java` |