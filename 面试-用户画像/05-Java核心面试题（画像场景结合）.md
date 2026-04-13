# Java 核心面试题（画像场景结合）

> 本文档将 Java 核心知识体系与美团用户画像平台的实际工程场景深度结合。每个知识点不仅阐述原理，更强调"我在画像项目中实际遇到过这些问题、做过哪些决策、踩过哪些坑"，体现真正的工程经验而非八股背诵。

---

## 一、JVM 篇

### 1.1 内存模型

#### 基础知识：JVM 内存区域划分

| 区域 | 线程共享 | 存储内容 | OOM 风险 |
|------|---------|---------|---------|
| 堆（Heap） | 是 | 对象实例、数组 | java.lang.OutOfMemoryError: Java heap space |
| 虚拟机栈（VM Stack） | 否 | 局部变量、操作数栈、栈帧 | StackOverflowError |
| 方法区（Metaspace） | 是 | 类信息、常量池、静态变量 | OutOfMemoryError: Metaspace |
| 本地方法栈 | 否 | Native 方法调用 | StackOverflowError |
| 程序计数器 | 否 | 当前执行字节码地址 | 唯一不会 OOM 的区域 |
| 直接内存（Direct Memory） | 是 | NIO Buffer、Netty ByteBuf | OutOfMemoryError: Direct buffer memory |

堆内存进一步划分为新生代（Eden + S0 + S1）和老年代。JDK 8 以后方法区实现从永久代改为 Metaspace，使用本地内存而非堆内存。

#### 画像场景结合

**场景一：Bitmap 对象大量占用老年代**

在画像平台中，RoaringBitmap 是核心数据结构，用于存储人群包。一个千万级别的人群包，对应的 Bitmap 对象可能占用几十 MB。这些对象有以下特点：

- 生命周期长：人群包一旦加载到内存，在整个服务生命周期内都存在
- 体积大：单个 Bitmap 可达 10-100 MB
- 数量多：同时在线的人群包可能有数百个

这直接导致这些对象在 Young GC 时无法被回收，快速晋升到老年代。如果老年代空间不足，会触发 Full GC 甚至 OOM。

我们在画像 Bitmap 服务中的内存配置策略：

```
# 堆内存 16G，老年代占比调大
-Xms16g -Xmx16g
-XX:NewRatio=1          # 新生代:老年代 = 1:1（默认是 1:2）
# 但后来我们发现 Bitmap 对象太多，调整为
-XX:NewRatio=3          # 新生代:老年代 = 1:3，给老年代更多空间
```

更进一步，我们引入了 Bitmap 的分层存储：热门人群包常驻内存，冷门人群包使用 LRU 策略淘汰，通过堆外内存（MappedByteBuffer）做二级缓存，减少堆内存压力。

**场景二：Netty Direct Memory 用于网络传输**

画像查询服务使用 Netty 作为 RPC 底层框架。Netty 默认使用 Direct Memory（堆外内存）做数据传输缓冲区，这样可以避免 JVM 堆内存到内核空间的数据拷贝（零拷贝）。

```
# 直接内存限制
-XX:MaxDirectMemorySize=4g
-Dio.netty.maxDirectMemory=4g
```

我们遇到过一次直接内存泄漏：一个查询接口返回的 ByteBuf 没有正确 release，在高并发下导致直接内存耗尽。排查过程：

1. 监控发现 Direct Memory 持续增长不释放
2. 开启 Netty 的内存泄漏检测：`-Dio.netty.leakDetectionLevel=PARANOID`
3. 日志中发现泄漏的 ByteBuf 分配位置
4. 修复：确保在 ChannelHandler 中使用 try-finally 释放 ByteBuf

**场景三：大量 String 对象的 intern 优化**

画像系统中标签值大量重复。例如"城市"标签，全国就几百个值，但可能有上亿条用户画像记录都包含这些值。如果每条记录都创建新的 String 对象，内存浪费严重。

```java
// 优化前：每次反序列化都创建新 String
String city = new String(bytes, "UTF-8");

// 优化后：使用 intern 复用常量池中的 String
String city = new String(bytes, "UTF-8").intern();

// 更优方案：使用本地缓存做字符串去重
private static final ConcurrentHashMap<String, String> STRING_POOL = new ConcurrentHashMap<>();
String city = STRING_POOL.computeIfAbsent(rawCity, k -> k);
```

注意：`String.intern()` 在 JDK 6 中存储在永久代，容易导致 PermGen OOM。JDK 7+ 移到了堆中。但大量调用 intern() 会导致 StringTable 变大，GC 时扫描耗时增加。所以我们最终选择了自建 ConcurrentHashMap 做字符串池，可控性更强。

---

### 1.2 垃圾回收

#### 基础知识：主流 GC 的对比

| 特性 | CMS | G1 | ZGC |
|------|-----|----|----|
| 算法 | 标记-清除 | 标记-整理 + 分区 | 着色指针 + 读屏障 |
| 停顿时间 | 不可控（碎片导致 Full GC） | 可控（-XX:MaxGCPauseMillis） | 亚毫秒级（< 1ms） |
| 堆大小适用 | < 8G | 4G - 64G | 8G - 16TB |
| 碎片问题 | 严重 | 轻微（Region 间 compact） | 无（并发整理） |
| JDK 版本 | 8（JDK 14 移除） | 8+（JDK 9 默认） | 11+（JDK 15 正式） |
| 并发度 | 部分并发 | 大部分并发 | 几乎完全并发 |

#### 画像场景结合

**画像查询服务为什么选 G1？**

画像查询服务的特点：
- 堆内存 8-16G
- 延迟敏感：P99 要求 < 50ms
- 对象分布：大量短生命周期的查询结果对象 + 少量长生命周期的缓存对象

选择 G1 的理由：
1. 可以通过 `-XX:MaxGCPauseMillis=20` 控制停顿目标
2. 混合回收（Mixed GC）可以逐步回收老年代，避免 Full GC
3. 大对象直接分配在 Humongous Region，不影响正常 Young GC

```
-XX:+UseG1GC
-XX:MaxGCPauseMillis=20
-XX:G1HeapRegionSize=8m       # 大于等于 Bitmap 的一半
-XX:InitiatingHeapOccupancyPercent=45
-XX:G1MixedGCCountTarget=8
-XX:G1HeapWastePercent=5
```

**Bitmap 服务为什么考虑 ZGC？**

Bitmap 服务的特点不同：
- 堆内存 32-64G（大量 Bitmap 常驻内存）
- 对象生命周期极长（人群包不频繁更新）
- Full GC 一次可能停顿数秒，严重影响可用性

G1 在大堆场景下的问题：
- Region 数量过多，记忆集（Remembered Set）占用大量内存
- Mixed GC 回收效率不够，老年代增长快于回收速度
- 最终触发 Full GC，停顿时间不可控

ZGC 的优势：
- 停顿时间 < 1ms，与堆大小无关
- 支持 TB 级堆内存
- 并发整理，无碎片问题

```
-XX:+UseZGC
-Xms48g -Xmx48g
-XX:SoftMaxHeapSize=40g      # 软上限，触发更积极的回收
-XX:ZCollectionInterval=120   # 定期触发 GC 间隔（秒）
```

**实际遇到的 GC 问题与调优过程**

问题现象：画像查询服务上线后，每天定时出现几分钟的延迟抖动，P99 从 30ms 飙升到 500ms+。

排查过程：

第一步，开启 GC 日志：
```
-Xlog:gc*:file=/var/log/gc.log:time,uptime,level,tags:filecount=5,filesize=100m
```

第二步，分析 GC 日志，发现问题：
```
[2024-03-15T14:00:01] GC(1234) Pause Young (G1 Evacuation Pause) 12G->10G(16G) 45.123ms
[2024-03-15T14:00:02] GC(1235) Pause Young (G1 Humongous Allocation) 10G->10G(16G) 120.456ms
```

发现 Humongous Allocation 频繁触发，停顿时间远超正常 Young GC。原因是批量查询接口一次返回大量数据，序列化为 byte[] 时超过了 Region 大小的一半（4M），被当作 Humongous 对象。

第三步，解决方案：
1. 增大 Region 大小到 16M：`-XX:G1HeapRegionSize=16m`
2. 批量查询结果分批序列化，单次不超过 8M
3. 增加 `-XX:G1ReservePercent=15` 预留更多空间

#### GC 日志分析方法

关键指标解读：
```
# G1 GC 日志关键行
[gc,start] GC(1234) Pause Young (Normal) (G1 Evacuation Pause)
[gc,heap] GC(1234) Eden regions: 200->0(180)          # Eden 回收情况
[gc,heap] GC(1234) Survivor regions: 20->25(30)       # Survivor 变化
[gc,heap] GC(1234) Old regions: 450->455               # 老年代增长
[gc,heap] GC(1234) Humongous regions: 10->8            # 大对象区域
[gc] GC(1234) Pause Young (Normal) 12G->10G(16G) 15.234ms  # 总耗时
```

分析重点：
- Young GC 频率和耗时是否稳定
- 老年代增长速度是否异常（内存泄漏信号）
- Mixed GC 是否频繁触发
- Humongous Allocation 是否频繁
- To-space exhausted 是否出现（空间不足，触发 Full GC 前兆）

#### 常用 JVM 参数

```
# 基础参数
-Xms8g -Xmx8g                  # 堆大小（建议 min=max 避免动态扩缩）
-XX:MaxDirectMemorySize=2g     # 直接内存上限
-XX:MetaspaceSize=256m         # Metaspace 初始大小
-XX:MaxMetaspaceSize=512m      # Metaspace 上限

# G1 参数
-XX:+UseG1GC
-XX:MaxGCPauseMillis=20        # 目标停顿时间
-XX:G1HeapRegionSize=8m        # Region 大小（1-32M，2的幂）
-XX:InitiatingHeapOccupancyPercent=45  # 触发并发标记的阈值
-XX:ParallelGCThreads=8        # 并行 GC 线程数
-XX:ConcGCThreads=4            # 并发 GC 线程数

# 故障排查参数
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/var/log/heapdump.hprof
-XX:ErrorFile=/var/log/hs_err_%p.log
```

---

### 1.3 类加载

#### 基础知识：双亲委派模型

类加载器层次：
```
Bootstrap ClassLoader  （加载 rt.jar）
    ↑
Extension ClassLoader  （加载 ext 目录）
    ↑
Application ClassLoader（加载 classpath）
    ↑
Custom ClassLoader     （自定义加载）
```

双亲委派流程：收到类加载请求时，先委派给父加载器，父加载器无法完成时，自己才尝试加载。这保证了核心类库的安全性（如 java.lang.String 只能由 Bootstrap 加载）。

#### 画像场景结合

**场景一：规则引擎的自定义类加载（动态加载 UDF）**

画像平台的标签计算支持用户自定义函数（UDF）。业务方可以上传 jar 包，定义自己的标签计算逻辑。这就需要动态加载外部类。

```java
public class UdfClassLoader extends URLClassLoader {

    public UdfClassLoader(URL[] urls) {
        // 父加载器设为 AppClassLoader
        super(urls, Thread.currentThread().getContextClassLoader());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // 对 UDF 包下的类打破双亲委派，优先自己加载
        if (name.startsWith("com.meituan.profile.udf.")) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                c = findClass(name);
            }
            if (resolve) resolveClass(c);
            return c;
        }
        // 其他类走双亲委派
        return super.loadClass(name, resolve);
    }
}
```

为什么要打破双亲委派？因为不同业务方可能上传同名但不同版本的 UDF，如果走双亲委派，第一个加载的版本会被缓存，后续都用同一个版本，导致冲突。每个 UDF jar 使用独立的 ClassLoader 实现类隔离。

**场景二：标签计算插件的热加载**

标签计算引擎支持不停机更新计算逻辑。核心思路是卸载旧的 ClassLoader，创建新的 ClassLoader 加载新版本的类。

```java
public class PluginManager {
    // volatile 保证可见性
    private volatile Map<String, TagComputePlugin> plugins;
    private volatile UdfClassLoader currentLoader;

    public void hotReload(String jarPath) {
        // 1. 创建新的类加载器
        UdfClassLoader newLoader = new UdfClassLoader(new URL[]{new URL(jarPath)});

        // 2. 用新加载器加载插件
        Map<String, TagComputePlugin> newPlugins = loadPlugins(newLoader);

        // 3. 原子切换（volatile 写）
        this.plugins = newPlugins;

        // 4. 旧的 ClassLoader 延迟关闭（等待正在执行的请求完成）
        UdfClassLoader oldLoader = this.currentLoader;
        this.currentLoader = newLoader;

        scheduler.schedule(() -> {
            try { oldLoader.close(); } catch (IOException e) { /* log */ }
        }, 60, TimeUnit.SECONDS);
    }
}
```

踩过的坑：ClassLoader 泄漏。旧的 ClassLoader 没有被 GC 回收，因为加载的类中有静态变量持有了 AppClassLoader 中的对象引用，导致整个 ClassLoader 及其加载的所有类都无法被回收。最终引发 Metaspace OOM。解决方案是确保 UDF 类不持有外部 ClassLoader 的引用，并在卸载时手动清理所有静态字段。

---

### 1.4 内存溢出排查

#### 画像场景结合

**场景一：堆 OOM -- 批量查询结果未分页**

现象：画像查询服务在某天突然 OOM 重启。

排查过程：
```bash
# 1. 查看 OOM 时的堆转储（启动参数已配置 HeapDumpOnOutOfMemoryError）
# 转储文件：/var/log/heapdump.hprof，大小 12GB

# 2. 使用 MAT 分析
# 发现 Leak Suspects：一个 ArrayList 持有了 800 万个 UserProfile 对象

# 3. 追踪到代码
# 某个内部接口调用时传了 limit=0，后端把整个人群包的所有用户画像一次性加载到 List
```

修复方案：
1. 所有查询接口强制分页，limit 最大值限制为 1000
2. 增加查询结果大小的监控告警
3. 在 Gateway 层增加请求参数校验

**场景二：直接内存 OOM -- HBase Client 连接池泄漏**

现象：服务运行一段时间后报 `OutOfMemoryError: Direct buffer memory`。

排查过程：
```bash
# 1. 使用 Arthas 查看直接内存使用情况
[arthas@1234]$ memory

# 2. 发现 direct memory 持续增长
# 初始：200MB，运行 2 小时后：3.8GB（限制 4GB）

# 3. 使用 NMT（Native Memory Tracking）定位
-XX:NativeMemoryTracking=detail
jcmd <pid> VM.native_memory detail

# 4. 发现 HBase AsyncConnection 的 ByteBuf 没有释放
```

根因：HBase 异步客户端在查询超时后，回调函数中没有释放网络响应的 ByteBuf。超时的请求其实后来还是返回了数据，但回调已经不处理了，ByteBuf 就泄漏了。

修复方案：在超时处理逻辑中增加 ByteBuf 的释放，同时设置 HBase 客户端的 `hbase.rpc.timeout` 和 `hbase.client.operation.timeout` 合理超时时间。

**场景三：Metaspace OOM -- 动态生成规则类过多**

现象：画像圈选服务在运行数天后报 `OutOfMemoryError: Metaspace`。

排查过程：
```bash
# 1. jmap 查看类信息
jmap -clstats <pid>
# 发现加载了 50000+ 个类，远超正常水平

# 2. 分析发现大量类名包含 "Generated" 和 "Rule"
# 原因：规则引擎每次执行都用 Javassist 动态生成一个新的规则类
```

根因：圈选条件编译器每次接收到圈选请求时，都用 Javassist 生成一个新的过滤器类。这些类的 ClassLoader 不会被回收（被缓存持有），导致 Metaspace 持续增长。

修复方案：
1. 对相同的圈选条件做缓存，相同条件复用同一个编译后的类
2. 使用 LRU 缓存限制最大编译类数量（10000）
3. 增加 Metaspace 监控和告警
4. 设置 `-XX:MaxMetaspaceSize=512m` 防止无限增长

#### 排查工具箱

```bash
# jmap：堆内存分析
jmap -heap <pid>                  # 查看堆内存概览
jmap -histo:live <pid>            # 查看存活对象统计
jmap -dump:live,format=b,file=dump.hprof <pid>  # 导出堆转储

# jstack：线程分析
jstack <pid> > thread_dump.txt    # 导出线程栈
jstack -l <pid>                   # 包含锁信息

# Arthas：在线诊断
dashboard                         # 实时面板
thread -n 5                       # CPU 最高的 5 个线程
heapdump /tmp/dump.hprof          # 在线导出堆转储
trace com.meituan.profile.* *     # 方法调用链路追踪
watch com.meituan.profile.service.QueryService query returnObj  # 观察返回值

# MAT（Memory Analyzer Tool）
# 1. 打开 hprof 文件
# 2. Leak Suspects Report 自动分析泄漏点
# 3. Dominator Tree 查看对象持有关系
# 4. OQL 查询特定对象
```

---

## 二、并发编程篇

### 2.1 线程池

#### 基础知识：ThreadPoolExecutor 七大参数

```java
public ThreadPoolExecutor(
    int corePoolSize,         // 核心线程数（即使空闲也不回收）
    int maximumPoolSize,      // 最大线程数
    long keepAliveTime,       // 非核心线程空闲存活时间
    TimeUnit unit,            // 存活时间单位
    BlockingQueue<Runnable> workQueue,  // 任务队列
    ThreadFactory threadFactory,         // 线程工厂
    RejectedExecutionHandler handler    // 拒绝策略
)
```

执行流程：任务提交 -> 核心线程 -> 队列 -> 最大线程 -> 拒绝策略

#### 画像场景结合

**查询服务的线程池配置**

画像查询服务有三种不同的查询模式，我们为每种模式配置独立的线程池：

```java
// 1. 单查线程池：查单个用户的画像，RT 要求 < 10ms
ThreadPoolExecutor singleQueryPool = new ThreadPoolExecutor(
    32,                           // 核心线程数 = CPU 核数 * 2（IO 密集型）
    32,                           // 最大线程数 = 核心线程数（避免线程频繁创建销毁）
    0L, TimeUnit.SECONDS,
    new LinkedBlockingQueue<>(5000),  // 队列大小根据 QPS 和 RT 计算：QPS 50000 * RT 0.01s * 10倍缓冲
    new NamedThreadFactory("single-query"),
    new CallerRunsPolicy()        // 拒绝策略：调用者执行，起到反压作用
);

// 2. 批量查询线程池：批量查询多个用户的画像，RT 要求 < 200ms
ThreadPoolExecutor batchQueryPool = new ThreadPoolExecutor(
    16, 32,                       // 批量查询单次耗时更长，核心线程数适当减少
    60L, TimeUnit.SECONDS,
    new LinkedBlockingQueue<>(500),
    new NamedThreadFactory("batch-query"),
    new AbortPolicy()             // 直接拒绝并返回错误，防止积压
);

// 3. 圈选线程池：人群圈选，RT 要求 < 5s
ThreadPoolExecutor selectionPool = new ThreadPoolExecutor(
    8, 16,                        // 圈选是重计算任务，线程数不宜过多
    120L, TimeUnit.SECONDS,
    new LinkedBlockingQueue<>(100),
    new NamedThreadFactory("selection"),
    new CustomRejectHandler()     // 自定义：写入消息队列异步处理
);
```

**为什么要隔离线程池？**

真实案例：上线初期我们所有查询共用一个线程池。某天一个大客户发起了一次全量圈选（涉及 2 亿用户），圈选任务占满了线程池，导致单查和批量查询全部排队等待。线上 P99 从 10ms 飙升到 30s，多个下游服务超时报警。

隔离后的效果：
- 单查线程池专注低延迟请求，保证 P99 < 10ms
- 批量查询独立，不影响单查
- 圈选任务单独隔离，满了就异步化，不拖垮在线服务

**线程池满了怎么降级？**

```java
public class ProfileQueryDegradeHandler implements RejectedExecutionHandler {
    private final MeterRegistry metrics;
    private final KafkaTemplate<String, String> kafka;

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        metrics.counter("threadpool.rejected", "pool", executor.toString()).increment();

        if (r instanceof ProfileQueryTask) {
            ProfileQueryTask task = (ProfileQueryTask) r;
            switch (task.getPriority()) {
                case HIGH:
                    // 高优先级：同步执行（CallerRuns）
                    r.run();
                    break;
                case MEDIUM:
                    // 中优先级：写入 Kafka 异步处理
                    kafka.send("profile-query-async", JSON.toJSONString(task));
                    task.getFuture().complete(DegradeResult.ASYNC);
                    break;
                case LOW:
                    // 低优先级：直接拒绝
                    task.getFuture().completeExceptionally(
                        new ServiceDegradeException("系统繁忙，请稍后重试"));
                    break;
            }
        }
    }
}
```

---

### 2.2 锁机制

#### 基础知识对比

| 特性 | synchronized | ReentrantLock | CAS |
|------|-------------|---------------|-----|
| 实现层面 | JVM 内置 | JDK API | CPU 指令 |
| 是否可中断 | 否 | 是（lockInterruptibly） | N/A |
| 是否公平 | 非公平 | 可选 | N/A |
| 条件变量 | 1 个（wait/notify） | 多个（Condition） | N/A |
| 性能（低竞争） | 偏向锁优化后接近 | 略差 | 最优 |
| 性能（高竞争） | 重量级锁退化 | 与 synchronized 接近 | 自旋消耗 CPU |

#### 画像场景结合

**场景一：本地缓存更新用 CAS**

标签元数据缓存更新频率低（分钟级），但读取频率极高（每次请求都要读）。使用 CAS 操作可以避免加锁：

```java
public class TagMetadataCache {
    // AtomicReference 保证引用的原子更新
    private final AtomicReference<Map<String, TagMetadata>> cache =
        new AtomicReference<>(Collections.emptyMap());

    // 读操作：无锁，直接读取当前引用
    public TagMetadata getTag(String tagId) {
        return cache.get().get(tagId);
    }

    // 写操作：CAS 更新（Copy-On-Write 思想）
    public void refresh(Map<String, TagMetadata> newData) {
        // 创建新的不可变 Map
        Map<String, TagMetadata> newCache = Collections.unmodifiableMap(new HashMap<>(newData));
        // CAS 替换引用
        cache.set(newCache);  // 对于引用替换，set 就够了，不需要 compareAndSet
    }
}
```

**场景二：Bitmap 双 Buffer 切换用 ReadWriteLock**

人群包 Bitmap 需要定期全量更新。更新期间不能阻塞查询请求。使用双 Buffer + ReadWriteLock：

```java
public class BitmapDoubleBuffer {
    private volatile RoaringBitmap currentBitmap;  // 当前生效的 Bitmap
    private RoaringBitmap backBitmap;              // 后台准备中的 Bitmap
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // 查询：读锁，多个查询可以并发执行
    public boolean contains(long userId) {
        rwLock.readLock().lock();
        try {
            return currentBitmap.contains((int) userId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // 更新步骤1：在后台 Buffer 构建新 Bitmap（不需要锁）
    public void prepareUpdate(RoaringBitmap newBitmap) {
        this.backBitmap = newBitmap;
    }

    // 更新步骤2：切换 Buffer（写锁，会等待所有读操作完成）
    public void switchBuffer() {
        rwLock.writeLock().lock();
        try {
            RoaringBitmap temp = currentBitmap;
            currentBitmap = backBitmap;
            backBitmap = temp;  // 旧的 Bitmap 变为后台 Buffer，下次更新时覆盖
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
```

**场景三：分布式锁用于人群包创建去重**

多个节点可能同时收到创建同一个人群包的请求，需要分布式锁保证只有一个节点执行：

```java
public class CrowdPackService {
    @Autowired
    private RedissonClient redisson;

    public CrowdPack createCrowdPack(CrowdPackRequest request) {
        String lockKey = "crowd:create:" + request.getDigest();  // 基于条件摘要去重
        RLock lock = redisson.getLock(lockKey);

        try {
            // 尝试获取锁，等待 5 秒，持有 300 秒（人群包创建可能耗时较长）
            boolean acquired = lock.tryLock(5, 300, TimeUnit.SECONDS);
            if (!acquired) {
                // 获取锁失败，说明其他节点正在创建，查询已有结果返回
                return waitAndGetExistingPack(request.getDigest());
            }

            // 双重检查：获取锁后再检查是否已存在
            CrowdPack existing = crowdPackDao.findByDigest(request.getDigest());
            if (existing != null) return existing;

            // 执行创建
            return doCreateCrowdPack(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceException("创建人群包被中断");
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
```

---

### 2.3 并发工具

#### 画像场景结合

**CompletableFuture 并行查询多个标签源并合并**

用户画像由多个标签源组成：基础属性在 MySQL，实时标签在 Redis，行为特征在 HBase。查询一个用户的完整画像需要并行查询多个数据源：

```java
public class ProfileQueryService {

    public UserProfile queryFullProfile(long userId) {
        // 并行查询多个标签源
        CompletableFuture<Map<String, Object>> basicFuture =
            CompletableFuture.supplyAsync(
                () -> mysqlTagSource.query(userId), singleQueryPool);

        CompletableFuture<Map<String, Object>> realtimeFuture =
            CompletableFuture.supplyAsync(
                () -> redisTagSource.query(userId), singleQueryPool);

        CompletableFuture<Map<String, Object>> behaviorFuture =
            CompletableFuture.supplyAsync(
                () -> hbaseTagSource.query(userId), singleQueryPool);

        // 合并结果，任何一个源超时不影响其他结果
        try {
            CompletableFuture.allOf(basicFuture, realtimeFuture, behaviorFuture)
                .get(50, TimeUnit.MILLISECONDS);  // 总超时 50ms
        } catch (TimeoutException e) {
            // 超时了，取已完成的结果
            log.warn("部分标签源查询超时, userId={}", userId);
        }

        UserProfile profile = new UserProfile(userId);
        // 逐个合并已完成的结果
        mergeIfDone(profile, basicFuture, "basic");
        mergeIfDone(profile, realtimeFuture, "realtime");
        mergeIfDone(profile, behaviorFuture, "behavior");

        return profile;
    }

    private void mergeIfDone(UserProfile profile,
                              CompletableFuture<Map<String, Object>> future,
                              String source) {
        if (future.isDone() && !future.isCompletedExceptionally()) {
            try {
                profile.merge(future.getNow(Collections.emptyMap()));
            } catch (Exception e) {
                log.error("合并标签源[{}]结果失败", source, e);
            }
        }
    }
}
```

**Semaphore 控制对 HBase 的并发查询数**

HBase 集群的并发处理能力有限，需要在客户端做并发控制：

```java
public class HBaseTagSource {
    // 限制对 HBase 的并发查询数为 100
    private final Semaphore hbaseSemaphore = new Semaphore(100);

    public Map<String, Object> query(long userId) {
        boolean acquired = false;
        try {
            acquired = hbaseSemaphore.tryAcquire(10, TimeUnit.MILLISECONDS);
            if (!acquired) {
                // 获取信号量超时，走降级逻辑
                metrics.counter("hbase.semaphore.timeout").increment();
                return queryFromLocalCache(userId);  // 从本地缓存查
            }
            return doHBaseQuery(userId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyMap();
        } finally {
            if (acquired) hbaseSemaphore.release();
        }
    }
}
```

**CountDownLatch 等待多个 Bitmap 分片计算完成**

大型人群圈选时，将 Bitmap 计算分片并行执行：

```java
public class BitmapSelectionEngine {

    public RoaringBitmap select(SelectionCondition condition) {
        // 将条件拆分为多个分片（按 userId 范围分片）
        List<Range> shards = splitToShards(condition, 16);  // 分 16 个分片

        CountDownLatch latch = new CountDownLatch(shards.size());
        RoaringBitmap[] results = new RoaringBitmap[shards.size()];
        AtomicReference<Exception> error = new AtomicReference<>();

        for (int i = 0; i < shards.size(); i++) {
            final int index = i;
            selectionPool.submit(() -> {
                try {
                    results[index] = computeShard(shards.get(index), condition);
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有分片完成，最长等待 30 秒
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        if (!completed) throw new TimeoutException("圈选计算超时");
        if (error.get() != null) throw new ComputeException("分片计算失败", error.get());

        // 合并所有分片结果
        RoaringBitmap merged = new RoaringBitmap();
        for (RoaringBitmap shard : results) {
            if (shard != null) merged.or(shard);
        }
        return merged;
    }
}
```

---

### 2.4 volatile 与 happens-before

#### 画像场景结合

**Bitmap 双 Buffer 切换的可见性保证**

在 2.2 节的双 Buffer 方案中，`currentBitmap` 必须声明为 `volatile`。原因：

```java
// 线程 A（更新线程）
backBitmap.add(userId);          // step 1：向后台 Buffer 写入数据
currentBitmap = backBitmap;      // step 2：切换引用（volatile 写）

// 线程 B（查询线程）
RoaringBitmap bmp = currentBitmap;  // step 3：读取引用（volatile 读）
bmp.contains(userId);               // step 4：使用 Bitmap 查询
```

volatile 写保证了 happens-before 关系：step 1 happens-before step 2（程序顺序规则），step 2 happens-before step 3（volatile 规则），因此 step 1 happens-before step 4。这保证了查询线程看到的是完整更新后的 Bitmap 数据。

如果不加 volatile，编译器和 CPU 可能重排序 step 1 和 step 2，导致查询线程看到一个不完整的 Bitmap。

**规则引擎的规则列表热更新**

```java
public class RuleEngine {
    // volatile 引用 + 不可变列表 = 安全的无锁发布
    private volatile List<Rule> rules = Collections.emptyList();

    // 查询线程：直接读取，无需加锁
    public List<Rule> getRules() {
        return rules;  // volatile 读
    }

    // 更新线程：创建新列表替换（Copy-On-Write）
    public void updateRules(List<Rule> newRules) {
        // 创建不可变副本
        this.rules = Collections.unmodifiableList(new ArrayList<>(newRules));
        // volatile 写，保证新列表中所有 Rule 对象的字段对读线程可见
    }
}
```

关键点：volatile 只保证引用的可见性，不保证引用指向的对象内部状态的可见性。所以我们使用不可变对象（Collections.unmodifiableList + Rule 是不可变类），确保一旦发布，对象内部状态不会再变。

---

### 2.5 AQS 原理

#### 核心原理

AbstractQueuedSynchronizer 是 java.util.concurrent 包的基石，核心思想：

1. **state 变量**：用 `volatile int state` 表示同步状态
2. **CLH 队列**：获取锁失败的线程入队等待（双向链表）
3. **模板方法**：子类实现 `tryAcquire`/`tryRelease`（独占模式）或 `tryAcquireShared`/`tryReleaseShared`（共享模式）

```
CLH 队列结构：
head -> [Thread-1|SIGNAL] <-> [Thread-2|WAITING] <-> [Thread-3|WAITING] <- tail
```

获取锁流程：
1. 调用 `tryAcquire()`（子类实现），成功则直接返回
2. 失败则创建 Node 入队（`addWaiter`）
3. 在队列中自旋尝试获取（前驱节点是 head 时 `tryAcquire`）
4. 获取失败则 `park` 当前线程

释放锁流程：
1. 调用 `tryRelease()`，更新 state
2. 唤醒后继节点（`unparkSuccessor`）

#### 基于 AQS 的实现

**ReentrantLock**：
- state 表示重入次数，0 为未锁定
- tryAcquire：CAS 将 state 从 0 改为 1，如果当前线程已持有锁则 state++
- tryRelease：state--，减到 0 时释放锁

**Semaphore**：
- state 表示可用许可数
- tryAcquireShared：CAS 将 state 减 1，state >= 0 时成功
- tryReleaseShared：CAS 将 state 加 1

**CountDownLatch**：
- state 表示计数
- tryAcquireShared：state == 0 时返回成功（所有 countDown 完成）
- tryReleaseShared：CAS 将 state 减 1，减到 0 时唤醒所有等待线程

---

## 三、集合框架篇

### 3.1 HashMap

#### 基础知识

底层结构：数组 + 链表 + 红黑树（JDK 8+）

```
table[0] -> null
table[1] -> Node(key1,val1) -> Node(key2,val2) -> null
table[2] -> TreeNode(key3,val3)  // 链表长度 >= 8 且 table 长度 >= 64 时转红黑树
...
```

关键参数：
- 初始容量：16（必须是 2 的幂）
- 负载因子：0.75
- 树化阈值：8（链表长度 >= 8 转红黑树）
- 退化阈值：6（红黑树节点 <= 6 退化为链表）

扩容机制：
1. 当元素数量 > 容量 * 负载因子时触发扩容
2. 容量翻倍（<< 1）
3. 重新计算每个元素的位置（hash & (newCap - 1)）
4. JDK 8 优化：元素要么在原位置，要么在原位置 + 旧容量

线程安全问题：
- JDK 7：并发扩容可能导致链表成环（死循环）
- JDK 8：虽然不会成环，但并发 put 可能丢数据

#### 画像场景结合

**标签元数据缓存用 ConcurrentHashMap（而非 HashMap）**

```java
// 错误做法：使用 HashMap 做共享缓存
private Map<String, TagMetadata> metadataCache = new HashMap<>();  // 多线程读写不安全！

// 正确做法：使用 ConcurrentHashMap
private Map<String, TagMetadata> metadataCache = new ConcurrentHashMap<>(256);
// 初始容量设为预估大小 / 负载因子，避免扩容：200 个标签 / 0.75 ≈ 267 -> 取 256（2的幂）
```

**用户 ID -> Bitmap 位映射用 HashMap（只读场景）**

Bitmap 使用整数位置来表示用户，需要一个 userId 到 bitmapIndex 的映射。这个映射在初始化后不再修改，因此可以安全使用 HashMap：

```java
public class UserIdMapping {
    // 只读映射，初始化后不再修改，HashMap 是安全的
    private final Map<Long, Integer> userIdToBitmapIndex;

    public UserIdMapping(List<Long> allUserIds) {
        Map<Long, Integer> map = new HashMap<>(
            (int)(allUserIds.size() / 0.75) + 1);  // 预分配避免扩容
        for (int i = 0; i < allUserIds.size(); i++) {
            map.put(allUserIds.get(i), i);
        }
        this.userIdToBitmapIndex = Collections.unmodifiableMap(map);
    }
}
```

---

### 3.2 ConcurrentHashMap

#### JDK 7 vs JDK 8

**JDK 7 -- 分段锁（Segment）**：
- 将数据分为 16 个 Segment，每个 Segment 独立加锁
- 最大并发度 = Segment 数量（默认 16）
- 读操作大部分无锁（volatile 读）

**JDK 8 -- CAS + synchronized**：
- 废弃 Segment，直接对 table[i] 加锁
- 空桶使用 CAS 写入
- 非空桶使用 synchronized 锁住链表头节点
- 并发度 = table 长度（远大于 16）

#### 画像场景结合

```java
public class TagLocalCache {
    // 两级缓存：L1 = ConcurrentHashMap, L2 = Redis
    private final ConcurrentHashMap<String, CacheEntry> l1Cache;

    public TagLocalCache(int expectedSize) {
        // concurrencyLevel 参数在 JDK 8 中只影响初始容量
        this.l1Cache = new ConcurrentHashMap<>(expectedSize, 0.75f,
            Runtime.getRuntime().availableProcessors());
    }

    public Object getTag(String userId, String tagName) {
        String key = userId + ":" + tagName;

        // computeIfAbsent：原子性的"查缓存，不存在则加载"
        CacheEntry entry = l1Cache.computeIfAbsent(key, k -> {
            Object value = redisClient.get(k);  // L2 缓存
            if (value == null) {
                value = hbaseClient.get(userId, tagName);  // 数据库
            }
            return new CacheEntry(value, System.currentTimeMillis());
        });

        // 检查过期
        if (entry.isExpired()) {
            l1Cache.remove(key);
            return getTag(userId, tagName);  // 重新加载
        }

        return entry.getValue();
    }
}
```

注意 `computeIfAbsent` 的坑：在 JDK 8 中，如果计算函数中访问了同一个 ConcurrentHashMap 的同一个桶，会死锁。我们踩过这个坑：加载标签时递归依赖了另一个标签，而两个标签 key 恰好落在同一个桶中。解决方案是将加载逻辑外提，先 get 判断，再 putIfAbsent。

---

### 3.3 其他集合

**ArrayList vs LinkedList**

标签列表统一使用 ArrayList：
- 标签数量有限（通常 < 100），随机访问频繁（按 index 获取第 N 个标签）
- ArrayList 内存连续，CPU 缓存友好
- LinkedList 的优势（O(1) 插入删除）在小集合中不明显，反而因为 Node 对象开销更大

**TreeMap：有序标签排序**

标签按优先级排序展示时使用 TreeMap：

```java
// 标签按 priority 排序，相同优先级按名称排序
TreeMap<TagPriority, List<Tag>> sortedTags = new TreeMap<>(
    Comparator.comparingInt(TagPriority::getLevel)
              .thenComparing(TagPriority::getName));
```

**BitSet vs RoaringBitmap**

Java 原生 BitSet 的局限性：
- 内存占用 = maxValue / 8 字节。如果用户 ID 范围是 0-10亿，BitSet 需要 125MB
- 不支持序列化（Serializable 效率低）
- 不支持分布式存储

RoaringBitmap 优势：
- 压缩存储：稀疏数据只占用实际需要的空间
- 高效的交集/并集/差集运算（SIMD 优化）
- 支持高效序列化/反序列化

```java
// BitSet：存储 10 亿范围内的 1000 万个用户
BitSet bitSet = new BitSet(1_000_000_000);  // 约 125MB 内存！

// RoaringBitmap：同样的数据
RoaringBitmap bitmap = new RoaringBitmap();
// 批量添加 1000 万个 ID 后，通常只占 10-30MB
```

---

## 四、Spring 篇

### 4.1 IoC 与 AOP

#### Bean 生命周期

```
实例化 -> 属性填充 -> Aware 接口回调 -> BeanPostProcessor#postProcessBeforeInitialization
-> InitializingBean#afterPropertiesSet -> init-method
-> BeanPostProcessor#postProcessAfterInitialization -> 使用
-> DisposableBean#destroy -> destroy-method
```

#### AOP 实现原理

- JDK 动态代理：目标类实现了接口时使用，基于 `java.lang.reflect.Proxy`
- CGLIB：目标类没有接口时使用，基于字节码生成子类
- Spring Boot 2.x 默认使用 CGLIB（`spring.aop.proxy-target-class=true`）

#### 画像场景结合

**AOP 实现接口鉴权与限流**

```java
@Aspect
@Component
public class ProfileApiAspect {
    @Autowired
    private AuthService authService;
    @Autowired
    private RateLimiter rateLimiter;

    // 鉴权切面：所有 Controller 方法
    @Around("@within(org.springframework.web.bind.annotation.RestController)")
    public Object authAndRateLimit(ProceedingJoinPoint pjp) throws Throwable {
        HttpServletRequest request = getRequest();
        String appKey = request.getHeader("X-App-Key");

        // 1. 鉴权
        AppInfo app = authService.authenticate(appKey);
        if (app == null) {
            throw new UnauthorizedException("无效的 appKey");
        }

        // 2. 限流（基于 appKey 的令牌桶）
        if (!rateLimiter.tryAcquire(appKey, app.getQpsLimit())) {
            throw new RateLimitException("超出 QPS 限制：" + app.getQpsLimit());
        }

        // 3. 执行业务逻辑
        return pjp.proceed();
    }

    // 日志与耗时统计切面
    @Around("execution(* com.meituan.profile.service..*.*(..))")
    public Object logAndMetrics(ProceedingJoinPoint pjp) throws Throwable {
        String method = pjp.getSignature().toShortString();
        long start = System.nanoTime();

        try {
            Object result = pjp.proceed();
            long cost = (System.nanoTime() - start) / 1_000_000;
            metrics.timer("profile.method.rt", "method", method)
                   .record(cost, TimeUnit.MILLISECONDS);
            if (cost > 100) {
                log.warn("慢方法: {} 耗时 {}ms, 参数: {}", method, cost,
                    Arrays.toString(pjp.getArgs()));
            }
            return result;
        } catch (Exception e) {
            metrics.counter("profile.method.error", "method", method).increment();
            throw e;
        }
    }
}
```

**自定义 BeanPostProcessor 注册标签源插件**

```java
@Component
public class TagSourceRegistrar implements BeanPostProcessor {

    @Autowired
    private TagSourceRegistry registry;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        // 扫描所有标注了 @TagSource 注解的 Bean
        if (bean.getClass().isAnnotationPresent(TagSource.class)) {
            TagSource annotation = bean.getClass().getAnnotation(TagSource.class);
            String sourceName = annotation.name();

            if (bean instanceof TagQuerySource) {
                registry.register(sourceName, (TagQuerySource) bean);
                log.info("注册标签源: {} -> {}", sourceName, bean.getClass().getName());
            }
        }
        return bean;
    }
}

// 使用：业务方只需加注解，自动注册
@TagSource(name = "hbase-behavior")
@Component
public class HBaseBehaviorTagSource implements TagQuerySource {
    // ...
}
```

---

### 4.2 Spring Boot 自动配置

#### 原理

`@EnableAutoConfiguration` 触发加载 `META-INF/spring.factories`（或 Spring Boot 3.x 的 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`）中配置的自动配置类。

配置类通过条件注解（`@ConditionalOnClass`、`@ConditionalOnProperty` 等）决定是否生效。

#### 画像场景结合

**画像 SDK 的自动配置**

我们封装了画像查询 SDK，业务方只需引入 maven 依赖即可使用：

```java
@Configuration
@ConditionalOnClass(ProfileClient.class)
@EnableConfigurationProperties(ProfileProperties.class)
public class ProfileAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ProfileClient profileClient(ProfileProperties props) {
        return ProfileClient.builder()
            .serverAddress(props.getServerAddress())
            .timeout(props.getTimeout())
            .retryTimes(props.getRetryTimes())
            .build();
    }

    // 根据配置选择不同的标签源实现
    @Bean
    @ConditionalOnProperty(name = "profile.tag-source.type", havingValue = "hbase")
    public TagQuerySource hbaseTagSource(ProfileProperties props) {
        return new HBaseTagSource(props.getHbase());
    }

    @Bean
    @ConditionalOnProperty(name = "profile.tag-source.type", havingValue = "redis")
    public TagQuerySource redisTagSource(ProfileProperties props) {
        return new RedisTagSource(props.getRedis());
    }

    @Bean
    @ConditionalOnProperty(name = "profile.tag-source.type", havingValue = "clickhouse")
    public TagQuerySource clickHouseTagSource(ProfileProperties props) {
        return new ClickHouseTagSource(props.getClickhouse());
    }
}
```

业务方使用：
```yaml
# application.yml
profile:
  server-address: profile.meituan.com:8080
  timeout: 50ms
  tag-source:
    type: redis  # 自动选择 Redis 标签源
```

---

### 4.3 事务管理

#### @Transactional 常见坑

1. 自调用失效：同一个类中方法 A 调用方法 B，B 上的 @Transactional 不生效（因为没走代理）
2. 异常类型：默认只对 RuntimeException 回滚，checked 异常不回滚
3. 传播行为：`REQUIRED`（默认，加入当前事务）vs `REQUIRES_NEW`（新建事务）

#### 画像场景结合

**标签元数据变更的事务一致性**

标签元数据修改涉及多张表：标签定义表、标签关联关系表、标签版本表。需要保证原子性：

```java
@Service
public class TagMetadataService {

    @Transactional(rollbackFor = Exception.class)
    public void updateTag(TagUpdateRequest request) {
        // 1. 更新标签定义
        tagDefinitionDao.update(request.toDefinition());

        // 2. 更新关联关系
        tagRelationDao.deleteByTagId(request.getTagId());
        tagRelationDao.batchInsert(request.getRelations());

        // 3. 记录版本（使用 REQUIRES_NEW 单独事务，确保版本记录不受主事务影响）
        tagVersionService.recordVersion(request.getTagId(), request.getOperator());

        // 4. 发送变更通知（异步，在事务提交后执行）
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    eventPublisher.publish(new TagChangedEvent(request.getTagId()));
                }
            });
    }
}
```

**人群包创建的幂等设计**

```java
@Transactional(rollbackFor = Exception.class)
public CrowdPack createCrowdPack(CreateRequest request) {
    // 幂等键：基于圈选条件的摘要
    String idempotentKey = DigestUtils.md5Hex(JSON.toJSONString(request.getConditions()));

    // 1. 幂等检查（SELECT FOR UPDATE 防止并发插入）
    CrowdPack existing = crowdPackDao.selectByIdempotentKeyForUpdate(idempotentKey);
    if (existing != null) {
        return existing;  // 已存在，直接返回
    }

    // 2. 创建人群包记录（状态：创建中）
    CrowdPack pack = new CrowdPack();
    pack.setIdempotentKey(idempotentKey);
    pack.setStatus(CrowdPackStatus.CREATING);
    crowdPackDao.insert(pack);

    // 3. 异步触发圈选计算
    crowdPackComputeService.asyncCompute(pack.getId(), request.getConditions());

    return pack;
}
```

---

## 五、网络与 IO 篇

### 5.1 Netty

#### Reactor 模型

Netty 采用主从 Reactor 多线程模型：

```
BossGroup (1个线程) -- 负责 Accept 连接
    ↓
WorkerGroup (N个线程) -- 负责 Read/Write IO 操作
    ↓
BusinessGroup (M个线程) -- 负责业务逻辑处理（可选）
```

#### 零拷贝

Netty 的零拷贝包含多个层面：
1. OS 层：`FileChannel.transferTo()` 避免内核态到用户态的数据拷贝
2. 应用层：`CompositeByteBuf` 组合多个 Buffer 逻辑上为一个，避免内存拷贝
3. 应用层：`slice()`/`duplicate()` 共享底层 Buffer，避免拷贝

#### 画像场景结合

**画像查询服务基于 Netty 自建 RPC**

为什么不直接用 HTTP 或现成 RPC 框架（如 Dubbo）？
1. 画像查询 QPS 极高（单机 10 万+），需要极致性能
2. 协议简单，不需要复杂的服务治理功能
3. 自建协议可以针对 Bitmap 传输做专门优化

```java
public class ProfileRpcServer {
    public void start(int port) {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(
            Runtime.getRuntime().availableProcessors());

        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)     // 关闭 Nagle 算法，降低延迟
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)  // 池化 Buffer
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new ProfileProtocolDecoder())    // 自定义协议解码
                        .addLast(new ProfileProtocolEncoder())    // 自定义协议编码
                        .addLast(new IdleStateHandler(60, 0, 0)) // 心跳检测
                        .addLast(businessGroup, new ProfileQueryHandler());  // 业务处理
                }
            });

        bootstrap.bind(port).sync();
    }
}
```

**粘包拆包处理**

自定义协议格式：
```
+--------+--------+--------+--------+--------+
| Magic  | Version| Type   | Length | Body   |
| 2 bytes| 1 byte | 1 byte | 4 bytes| N bytes|
+--------+--------+--------+--------+--------+
```

```java
public class ProfileProtocolDecoder extends LengthFieldBasedFrameDecoder {
    public ProfileProtocolDecoder() {
        // maxFrameLength=16MB, lengthFieldOffset=4, lengthFieldLength=4
        // lengthAdjustment=0, initialBytesToStrip=0
        super(16 * 1024 * 1024, 4, 4, 0, 0);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) return null;

        try {
            short magic = frame.readShort();
            if (magic != MAGIC_NUMBER) {
                throw new ProtocolException("非法协议头");
            }
            byte version = frame.readByte();
            byte type = frame.readByte();
            int length = frame.readInt();
            byte[] body = new byte[length];
            frame.readBytes(body);

            return new ProfileMessage(version, type, body);
        } finally {
            frame.release();  // 必须释放！
        }
    }
}
```

---

### 5.2 序列化

#### 画像场景结合

| 场景 | 序列化方案 | 选择理由 |
|------|-----------|---------|
| 内部 RPC 通信 | Protobuf | 二进制格式，体积小（比 JSON 小 3-5 倍），序列化快 |
| 对外 HTTP API | JSON (Jackson) | 可读性好，调试方便，兼容性强 |
| Bitmap 网络传输 | 自定义二进制 | RoaringBitmap 内置序列化，直接转字节数组 |
| Redis 缓存 | Protobuf | 减少 Redis 内存占用和网络带宽 |
| Kafka 消息 | Protobuf | 支持 Schema Evolution，兼容性好 |

**Protobuf 定义示例**

```protobuf
syntax = "proto3";

message UserProfileRequest {
    int64 user_id = 1;
    repeated string tag_names = 2;
}

message UserProfileResponse {
    int64 user_id = 1;
    map<string, TagValue> tags = 2;
    int32 code = 3;
    string message = 4;
}

message TagValue {
    oneof value {
        string string_value = 1;
        int64 int_value = 2;
        double double_value = 3;
        StringList list_value = 4;
    }
}
```

**Bitmap 序列化优化**

```java
public class BitmapSerializer {

    // 序列化：Bitmap -> byte[]
    public static byte[] serialize(RoaringBitmap bitmap) {
        bitmap.runOptimize();  // 先优化压缩
        byte[] bytes = new byte[bitmap.serializedSizeInBytes()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bitmap.serialize(buffer);
        return bytes;
    }

    // 反序列化：byte[] -> Bitmap（零拷贝）
    public static ImmutableRoaringBitmap deserializeImmutable(byte[] bytes) {
        // ImmutableRoaringBitmap 直接在 byte[] 上操作，无需拷贝
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        return new ImmutableRoaringBitmap(buffer);
    }
}
```

---

### 5.3 网络协议

#### 画像场景结合

**服务间通信为什么用 gRPC**

画像平台内部微服务间通信选择 gRPC 而非 HTTP/1.1 的理由：

1. **HTTP/2 多路复用**：单个 TCP 连接上并发多个请求/响应，避免 HTTP/1.1 的队头阻塞
2. **二进制帧传输**：头部和数据都是二进制，解析效率比 HTTP/1.1 的文本格式高
3. **Protobuf 集成**：IDL 定义接口，自动生成客户端/服务端代码，类型安全
4. **双向流**：支持 Server-Side Streaming，适合 Bitmap 分批传输

性能对比（画像服务实测）：
```
HTTP/1.1 + JSON：QPS 8000, P99 25ms
gRPC + Protobuf：QPS 30000, P99 8ms
```

**长连接复用减少握手开销**

画像查询服务每秒处理大量请求，如果每次都建立新的 TCP 连接：
- 三次握手：至少 1.5 RTT（约 1-3ms 同机房）
- TLS 握手（如果有）：额外 1-2 RTT
- TIME_WAIT 状态积累导致端口耗尽

我们的连接池配置：
```java
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("profile-service", 8080)
    .usePlaintext()               // 内网不需要 TLS
    .maxInboundMessageSize(64 * 1024 * 1024)   // 最大消息 64MB（Bitmap 可能很大）
    .keepAliveTime(30, TimeUnit.SECONDS)        // 30 秒发一次 keepalive ping
    .keepAliveTimeout(5, TimeUnit.SECONDS)      // 5 秒无响应认为连接断开
    .idleTimeout(5, TimeUnit.MINUTES)           // 空闲 5 分钟关闭连接
    .build();
```

---

## 六、设计模式篇（结合画像场景实际应用）

### 6.1 策略模式

不同标签存储后端有不同的查询逻辑，使用策略模式统一抽象：

```java
// 策略接口
public interface TagQueryStrategy {
    Map<String, Object> query(String userId, List<String> tagNames);
    boolean supports(TagStorageType type);
}

// HBase 策略
@Component
public class HBaseQueryStrategy implements TagQueryStrategy {
    @Override
    public Map<String, Object> query(String userId, List<String> tagNames) {
        // HBase Get 操作，按列族+列限定符查询
        Get get = new Get(Bytes.toBytes(userId));
        tagNames.forEach(tag -> get.addColumn(CF_TAG, Bytes.toBytes(tag)));
        Result result = table.get(get);
        return parseResult(result);
    }

    @Override
    public boolean supports(TagStorageType type) {
        return type == TagStorageType.HBASE;
    }
}

// Redis 策略
@Component
public class RedisQueryStrategy implements TagQueryStrategy {
    @Override
    public Map<String, Object> query(String userId, List<String> tagNames) {
        // Redis HMGET 批量获取
        String key = "profile:" + userId;
        List<String> values = redis.hmget(key, tagNames.toArray(new String[0]));
        return zipToMap(tagNames, values);
    }

    @Override
    public boolean supports(TagStorageType type) {
        return type == TagStorageType.REDIS;
    }
}

// ClickHouse 策略
@Component
public class ClickHouseQueryStrategy implements TagQueryStrategy {
    @Override
    public Map<String, Object> query(String userId, List<String> tagNames) {
        // SQL 查询
        String columns = String.join(",", tagNames);
        String sql = "SELECT " + columns + " FROM user_profile WHERE user_id = ?";
        return jdbcTemplate.queryForMap(sql, userId);
    }

    @Override
    public boolean supports(TagStorageType type) {
        return type == TagStorageType.CLICKHOUSE;
    }
}

// 策略选择器
@Component
public class TagQueryStrategySelector {
    @Autowired
    private List<TagQueryStrategy> strategies;

    public TagQueryStrategy select(TagStorageType type) {
        return strategies.stream()
            .filter(s -> s.supports(type))
            .findFirst()
            .orElseThrow(() -> new UnsupportedOperationException("不支持的存储类型：" + type));
    }
}
```

### 6.2 工厂模式

根据标签类型创建不同的查询客户端：

```java
public class TagSourceFactory {
    private final Map<String, TagSourceCreator> creators = new HashMap<>();

    @PostConstruct
    public void init() {
        creators.put("hbase", config -> new HBaseTagSource(config));
        creators.put("redis", config -> new RedisTagSource(config));
        creators.put("clickhouse", config -> new ClickHouseTagSource(config));
        creators.put("elasticsearch", config -> new ESTagSource(config));
    }

    public TagQuerySource create(String type, TagSourceConfig config) {
        TagSourceCreator creator = creators.get(type.toLowerCase());
        if (creator == null) {
            throw new IllegalArgumentException("未知的标签源类型: " + type);
        }
        TagQuerySource source = creator.create(config);
        source.init();  // 初始化连接池等资源
        return source;
    }

    @FunctionalInterface
    interface TagSourceCreator {
        TagQuerySource create(TagSourceConfig config);
    }
}
```

### 6.3 观察者模式

标签更新后通知下游缓存刷新：

```java
// 事件定义
public class TagChangedEvent {
    private final String tagId;
    private final ChangeType changeType;  // CREATE, UPDATE, DELETE
    private final long timestamp;
}

// 发布者
@Component
public class TagChangePublisher {
    @Autowired
    private ApplicationEventPublisher eventPublisher;

    public void publishChange(String tagId, ChangeType type) {
        eventPublisher.publishEvent(new TagChangedEvent(tagId, type, System.currentTimeMillis()));
    }
}

// 观察者1：本地缓存刷新
@Component
public class LocalCacheRefreshListener {
    @EventListener
    @Async  // 异步处理，不阻塞主流程
    public void onTagChanged(TagChangedEvent event) {
        tagLocalCache.invalidate(event.getTagId());
        log.info("本地缓存已刷新: tagId={}", event.getTagId());
    }
}

// 观察者2：通知其他节点
@Component
public class ClusterNotifyListener {
    @EventListener
    @Async
    public void onTagChanged(TagChangedEvent event) {
        // 通过 Redis Pub/Sub 通知集群其他节点
        redis.publish("tag:change", JSON.toJSONString(event));
    }
}

// 观察者3：更新搜索索引
@Component
public class SearchIndexUpdateListener {
    @EventListener
    @Async
    public void onTagChanged(TagChangedEvent event) {
        if (event.getChangeType() == ChangeType.DELETE) {
            esClient.delete("tag_index", event.getTagId());
        } else {
            TagMetadata metadata = tagMetadataService.getById(event.getTagId());
            esClient.index("tag_index", event.getTagId(), metadata);
        }
    }
}
```

### 6.4 装饰器模式

在基础查询上逐层叠加缓存、限流、监控能力：

```java
// 基础接口
public interface TagQueryService {
    Map<String, Object> query(String userId, List<String> tagNames);
}

// 基础实现
public class BasicTagQueryService implements TagQueryService {
    public Map<String, Object> query(String userId, List<String> tagNames) {
        return tagSource.query(userId, tagNames);
    }
}

// 缓存装饰器
public class CachedTagQueryService implements TagQueryService {
    private final TagQueryService delegate;
    private final Cache<String, Map<String, Object>> cache;

    @Override
    public Map<String, Object> query(String userId, List<String> tagNames) {
        String cacheKey = userId + ":" + String.join(",", tagNames);
        return cache.get(cacheKey, key -> delegate.query(userId, tagNames));
    }
}

// 限流装饰器
public class RateLimitedTagQueryService implements TagQueryService {
    private final TagQueryService delegate;
    private final RateLimiter rateLimiter;

    @Override
    public Map<String, Object> query(String userId, List<String> tagNames) {
        if (!rateLimiter.tryAcquire(10, TimeUnit.MILLISECONDS)) {
            throw new RateLimitException("查询限流");
        }
        return delegate.query(userId, tagNames);
    }
}

// 监控装饰器
public class MonitoredTagQueryService implements TagQueryService {
    private final TagQueryService delegate;

    @Override
    public Map<String, Object> query(String userId, List<String> tagNames) {
        Timer.Sample sample = Timer.start();
        try {
            Map<String, Object> result = delegate.query(userId, tagNames);
            sample.stop(Timer.builder("tag.query.rt").tag("status", "success").register(registry));
            return result;
        } catch (Exception e) {
            sample.stop(Timer.builder("tag.query.rt").tag("status", "error").register(registry));
            throw e;
        }
    }
}

// 组装：基础服务 -> 缓存 -> 限流 -> 监控
TagQueryService service = new MonitoredTagQueryService(
    new RateLimitedTagQueryService(
        new CachedTagQueryService(
            new BasicTagQueryService(tagSource),
            caffeineCache),
        rateLimiter),
    meterRegistry);
```

### 6.5 模板方法模式

标签计算流水线定义骨架，子类实现具体计算逻辑：

```java
public abstract class TagComputePipeline<T> {

    // 模板方法：定义计算骨架
    public final TagComputeResult compute(TagComputeContext context) {
        // 1. 参数校验
        validate(context);

        // 2. 数据加载
        T rawData = loadData(context);

        // 3. 数据清洗
        T cleanData = cleanData(rawData, context);

        // 4. 标签计算（子类实现核心逻辑）
        Map<String, Object> tagValues = doCompute(cleanData, context);

        // 5. 结果校验
        validateResult(tagValues);

        // 6. 结果写入
        writeResult(context.getUserId(), tagValues);

        return TagComputeResult.success(tagValues);
    }

    // 钩子方法：子类可选覆盖
    protected void validate(TagComputeContext context) {
        Preconditions.checkNotNull(context.getUserId(), "userId 不能为空");
    }

    // 抽象方法：子类必须实现
    protected abstract T loadData(TagComputeContext context);
    protected abstract T cleanData(T rawData, TagComputeContext context);
    protected abstract Map<String, Object> doCompute(T data, TagComputeContext context);

    protected void validateResult(Map<String, Object> tagValues) { /* 默认不校验 */ }
    protected void writeResult(String userId, Map<String, Object> tagValues) { /* 默认写 HBase */ }
}

// 子类：用户消费能力标签计算
public class ConsumptionTagPipeline extends TagComputePipeline<List<OrderRecord>> {

    @Override
    protected List<OrderRecord> loadData(TagComputeContext context) {
        return orderDao.queryByUserId(context.getUserId(), context.getTimeRange());
    }

    @Override
    protected List<OrderRecord> cleanData(List<OrderRecord> data, TagComputeContext context) {
        return data.stream()
            .filter(o -> o.getStatus() == OrderStatus.COMPLETED)
            .filter(o -> o.getAmount() > 0)
            .collect(Collectors.toList());
    }

    @Override
    protected Map<String, Object> doCompute(List<OrderRecord> data, TagComputeContext context) {
        Map<String, Object> tags = new HashMap<>();
        tags.put("total_amount", data.stream().mapToDouble(OrderRecord::getAmount).sum());
        tags.put("order_count", data.size());
        tags.put("avg_amount", tags.get("total_amount") / Math.max(data.size(), 1));
        tags.put("consumption_level", classifyLevel((Double) tags.get("avg_amount")));
        return tags;
    }
}
```

### 6.6 责任链模式

圈选条件校验链：

```java
public interface SelectionValidator {
    void validate(SelectionRequest request) throws ValidationException;
    int getOrder();  // 执行顺序
}

// 参数校验
@Component
public class ParamValidator implements SelectionValidator {
    @Override
    public void validate(SelectionRequest request) throws ValidationException {
        if (request.getConditions() == null || request.getConditions().isEmpty()) {
            throw new ValidationException("圈选条件不能为空");
        }
        if (request.getConditions().size() > 20) {
            throw new ValidationException("圈选条件不能超过 20 个");
        }
    }
    @Override
    public int getOrder() { return 1; }
}

// 权限校验
@Component
public class PermissionValidator implements SelectionValidator {
    @Override
    public void validate(SelectionRequest request) throws ValidationException {
        AppInfo app = authService.getAppInfo(request.getAppKey());
        for (Condition condition : request.getConditions()) {
            if (!app.hasPermission(condition.getTagName())) {
                throw new ValidationException("无权限访问标签: " + condition.getTagName());
            }
        }
    }
    @Override
    public int getOrder() { return 2; }
}

// 标签存在性校验
@Component
public class TagExistenceValidator implements SelectionValidator {
    @Override
    public void validate(SelectionRequest request) throws ValidationException {
        for (Condition condition : request.getConditions()) {
            TagMetadata metadata = tagMetadataCache.get(condition.getTagName());
            if (metadata == null) {
                throw new ValidationException("标签不存在: " + condition.getTagName());
            }
            if (metadata.getStatus() != TagStatus.ACTIVE) {
                throw new ValidationException("标签已下线: " + condition.getTagName());
            }
        }
    }
    @Override
    public int getOrder() { return 3; }
}

// 责任链执行器
@Component
public class SelectionValidationChain {
    private final List<SelectionValidator> validators;

    @Autowired
    public SelectionValidationChain(List<SelectionValidator> validators) {
        this.validators = validators.stream()
            .sorted(Comparator.comparingInt(SelectionValidator::getOrder))
            .collect(Collectors.toList());
    }

    public void validate(SelectionRequest request) throws ValidationException {
        for (SelectionValidator validator : validators) {
            validator.validate(request);  // 任何一个校验失败都会抛异常中断链
        }
    }
}
```

---

## 七、中间件篇

### 7.1 Redis

#### 数据结构与画像场景

| 数据结构 | 画像应用场景 | 示例 |
|---------|------------|------|
| String | 简单标签缓存 | `SET profile:123:age "25"` |
| Hash | 用户完整画像 | `HSET profile:123 age 25 city beijing` |
| Set | 人群包判定 | `SISMEMBER crowd:vip 123` |
| Bitmap | 大规模人群包 | `GETBIT crowd:active 123` |
| Sorted Set | 标签排行榜 | `ZRANGEBYSCORE tag:consumption 100 200` |
| HyperLogLog | 标签覆盖人数估算 | `PFCOUNT tag:city:beijing` |

#### 集群方案选择

画像平台的 Redis 使用 Cluster 模式而非 Sentinel：
- 数据量大（TB 级），需要水平分片
- QPS 高（百万级），单节点扛不住
- 自动故障转移，无需人工介入

#### 画像场景结合

**实时标签存储（Hash 结构）**

```java
public class RedisTagSource implements TagQuerySource {

    // 查询用户画像
    public Map<String, Object> query(String userId, List<String> tagNames) {
        String key = "profile:" + userId;

        if (tagNames == null || tagNames.isEmpty()) {
            // 全量查询
            return redis.hgetAll(key);
        }

        // 指定标签查询（HMGET 批量获取，一次网络往返）
        List<String> values = redis.hmget(key, tagNames.toArray(new String[0]));
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < tagNames.size(); i++) {
            if (values.get(i) != null) {
                result.put(tagNames.get(i), deserializeValue(values.get(i)));
            }
        }
        return result;
    }

    // 更新标签（Pipeline 批量写入，减少网络往返）
    public void batchUpdate(Map<String, Map<String, Object>> userTags) {
        try (Pipeline pipeline = redis.pipelined()) {
            userTags.forEach((userId, tags) -> {
                String key = "profile:" + userId;
                pipeline.hmset(key, serializeValues(tags));
                pipeline.expire(key, 7 * 86400);  // 7 天过期
            });
            pipeline.sync();
        }
    }
}
```

**人群包 Bitmap 判定**

```java
public class RedisBitmapCrowdPack {

    // 判断用户是否在人群包中
    public boolean contains(String crowdId, long userId) {
        String key = "crowd:bitmap:" + crowdId;
        return redis.getbit(key, userId);
    }

    // 人群包交集、并集运算
    public long intersectionCount(String crowdId1, String crowdId2) {
        String tempKey = "crowd:temp:" + UUID.randomUUID();
        try {
            redis.bitop(BitOP.AND, tempKey,
                "crowd:bitmap:" + crowdId1, "crowd:bitmap:" + crowdId2);
            return redis.bitcount(tempKey);
        } finally {
            redis.del(tempKey);
        }
    }
}
```

**分布式锁的高可用**

```java
// 使用 Redisson 的 RedLock 实现（多节点加锁，避免单点故障）
RLock lock1 = redisson1.getLock("lock:crowd:" + crowdId);
RLock lock2 = redisson2.getLock("lock:crowd:" + crowdId);
RLock lock3 = redisson3.getLock("lock:crowd:" + crowdId);

RedissonRedLock redLock = new RedissonRedLock(lock1, lock2, lock3);
try {
    boolean acquired = redLock.tryLock(5, 60, TimeUnit.SECONDS);
    if (acquired) {
        // 执行业务逻辑
    }
} finally {
    redLock.unlock();
}
```

---

### 7.2 Kafka

#### 画像场景结合

**标签变更事件通知**

```java
// 生产者：标签计算完成后发送变更事件
@Component
public class TagChangeProducer {
    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public void sendTagChange(String userId, Map<String, Object> changedTags) {
        TagChangeEvent event = TagChangeEvent.newBuilder()
            .setUserId(userId)
            .putAllTags(changedTags)
            .setTimestamp(System.currentTimeMillis())
            .build();

        // 按 userId 分区，保证同一用户的事件有序
        kafkaTemplate.send("tag-change-event", userId, event.toByteArray());
    }
}

// 消费者：接收变更事件，刷新缓存
@Component
public class TagChangeConsumer {

    @KafkaListener(
        topics = "tag-change-event",
        groupId = "profile-cache-refresh",
        concurrency = "8"  // 8 个消费线程
    )
    public void onTagChange(ConsumerRecord<String, byte[]> record) {
        TagChangeEvent event = TagChangeEvent.parseFrom(record.value());

        // 刷新本地缓存
        tagLocalCache.invalidate(event.getUserId());

        // 刷新 Redis 缓存
        redisTagSource.batchUpdate(Map.of(event.getUserId(), event.getTagsMap()));
    }
}
```

**实时行为数据采集**

```java
// 行为数据采集 -> Kafka -> Flink 实时计算 -> 实时标签更新
// Kafka 配置（高吞吐场景）
Properties props = new Properties();
props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
props.put("acks", "1");                           // 不要求所有副本确认（吞吐优先）
props.put("batch.size", 65536);                    // 64KB 批量发送
props.put("linger.ms", 10);                        // 最多等待 10ms 凑批
props.put("compression.type", "lz4");              // LZ4 压缩
props.put("buffer.memory", 134217728);             // 128MB 发送缓冲区
props.put("max.in.flight.requests.per.connection", 5);  // 最多 5 个未确认请求
```

**圈选任务异步处理**

```java
// 大型圈选任务不同步执行，写入 Kafka 异步处理
@Component
public class AsyncSelectionService {

    public String submitSelection(SelectionRequest request) {
        String taskId = UUID.randomUUID().toString();
        SelectionTask task = SelectionTask.newBuilder()
            .setTaskId(taskId)
            .setRequest(request)
            .setSubmitTime(System.currentTimeMillis())
            .build();

        kafkaTemplate.send("selection-task", taskId, task.toByteArray());
        return taskId;  // 返回任务 ID，客户端轮询查结果
    }

    @KafkaListener(topics = "selection-task", groupId = "selection-worker", concurrency = "4")
    public void processSelection(ConsumerRecord<String, byte[]> record) {
        SelectionTask task = SelectionTask.parseFrom(record.value());
        try {
            // 执行圈选
            RoaringBitmap result = selectionEngine.select(task.getRequest());

            // 保存结果
            crowdPackDao.saveResult(task.getTaskId(), result);
            crowdPackDao.updateStatus(task.getTaskId(), TaskStatus.COMPLETED);
        } catch (Exception e) {
            crowdPackDao.updateStatus(task.getTaskId(), TaskStatus.FAILED);
            log.error("圈选任务失败: taskId={}", task.getTaskId(), e);
        }
    }
}
```

---

### 7.3 MySQL

#### 画像场景结合

**标签元数据表设计**

```sql
CREATE TABLE tag_metadata (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    tag_id VARCHAR(64) NOT NULL COMMENT '标签ID',
    tag_name VARCHAR(128) NOT NULL COMMENT '标签名称',
    tag_type TINYINT NOT NULL COMMENT '标签类型：1-基础属性 2-实时行为 3-统计特征',
    data_type VARCHAR(32) NOT NULL COMMENT '数据类型：STRING/INT/DOUBLE/LIST',
    storage_type VARCHAR(32) NOT NULL COMMENT '存储类型：HBASE/REDIS/CLICKHOUSE',
    category_id BIGINT COMMENT '所属分类ID',
    description TEXT COMMENT '标签描述',
    compute_rule TEXT COMMENT '计算规则（JSON）',
    update_frequency VARCHAR(32) COMMENT '更新频率：REALTIME/DAILY/WEEKLY',
    status TINYINT DEFAULT 1 COMMENT '状态：0-下线 1-在线',
    owner VARCHAR(64) COMMENT '负责人',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_tag_id (tag_id),
    INDEX idx_category (category_id),
    INDEX idx_status_type (status, tag_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='标签元数据表';
```

**人群包管理表设计**

```sql
CREATE TABLE crowd_pack (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    crowd_id VARCHAR(64) NOT NULL COMMENT '人群包ID',
    crowd_name VARCHAR(256) NOT NULL COMMENT '人群包名称',
    conditions TEXT NOT NULL COMMENT '圈选条件（JSON）',
    idempotent_key VARCHAR(64) NOT NULL COMMENT '幂等键（条件摘要）',
    user_count BIGINT DEFAULT 0 COMMENT '人群包用户数',
    status TINYINT DEFAULT 0 COMMENT '状态：0-创建中 1-就绪 2-已过期 3-失败',
    storage_path VARCHAR(512) COMMENT 'Bitmap 存储路径',
    expire_time DATETIME COMMENT '过期时间',
    creator VARCHAR(64) COMMENT '创建人',
    app_key VARCHAR(64) COMMENT '所属应用',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_crowd_id (crowd_id),
    UNIQUE INDEX idx_idempotent_key (idempotent_key),
    INDEX idx_app_status (app_key, status),
    INDEX idx_expire_time (expire_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='人群包管理表';
```

**索引优化实践**

```sql
-- 慢查询1：按标签分类查询标签列表
-- 优化前：全表扫描
EXPLAIN SELECT * FROM tag_metadata WHERE category_id = 100 AND status = 1;
-- type: ALL, rows: 50000

-- 优化后：添加联合索引
ALTER TABLE tag_metadata ADD INDEX idx_category_status (category_id, status);
-- type: ref, rows: 50

-- 慢查询2：按创建时间范围查询人群包
-- 优化前：range 扫描全表
EXPLAIN SELECT * FROM crowd_pack WHERE app_key = 'app001' AND created_at > '2024-01-01';
-- 使用 idx_app_status 索引，但还需要回表过滤 created_at

-- 优化后：覆盖索引
ALTER TABLE crowd_pack ADD INDEX idx_app_created (app_key, created_at);
```

---

## 八、面试高频问题与参考回答

### 1. HashMap 底层原理？在你项目中哪里用到了？

**回答**：HashMap 底层是数组 + 链表 + 红黑树。put 时通过 key 的 hashCode 经过扰动函数 `(h = key.hashCode()) ^ (h >>> 16)` 计算数组下标。哈希冲突时形成链表，链表长度达到 8 且数组长度 >= 64 时转为红黑树。扩容时容量翻倍，JDK 8 利用 `hash & oldCap` 判断元素是否需要移动。

在画像项目中，我们在 Bitmap 服务的用户 ID 映射模块使用 HashMap。因为这个映射在初始化时一次性构建，之后只读不写，所以 HashMap 是安全的。初始化时我们会根据用户总数预计算容量（`size / 0.75 + 1`），避免扩容带来的性能开销和内存波动。

### 2. ConcurrentHashMap 怎么保证线程安全？

**回答**：JDK 8 的 ConcurrentHashMap 放弃了分段锁，采用 CAS + synchronized。对空桶使用 CAS 写入第一个节点，对非空桶使用 synchronized 锁住桶的头节点。读操作完全无锁（Node.val 和 Node.next 用 volatile 修饰）。扩容时多个线程可以协助迁移，通过 `transferIndex` 分配迁移任务。

在画像项目中，标签元数据本地缓存使用 ConcurrentHashMap。有一个坑需要注意：`computeIfAbsent` 在 JDK 8 中如果 mappingFunction 里访问同一个桶的 key 会死锁，JDK 9 才修复。我们曾经因为标签元数据互相引用触发了这个 bug，最终改用 get + putIfAbsent 的分步写法规避。

### 3. 线程池参数怎么设置的？为什么这么设？

**回答**：画像查询服务使用了三个隔离的线程池。以单查线程池为例：核心线程数 32（CPU 核数的 2 倍，因为是 IO 密集型任务，线程大部分时间在等待 HBase/Redis 响应），最大线程数也是 32（避免突发流量创建大量线程），队列容量 5000（根据 QPS 50000 * RT 0.01s * 10 倍缓冲计算），拒绝策略用 CallerRunsPolicy（起到反压作用，不会丢弃请求）。

之所以要隔离线程池，是因为上线初期所有查询共用一个池子，一次大型圈选任务耗尽了所有线程，导致简单的单查请求也超时。隔离后，圈选任务即使积压也不影响在线查询。

### 4. 遇到过 OOM 吗？怎么排查的？

**回答**：遇到过三次。印象最深的是一次堆 OOM：某个内部调用方传了 `limit=0`，我们的代码没有做默认值处理，导致一次性把整个人群包（800 万条记录）全部加载到 ArrayList 中。

排查过程：首先通过 `-XX:+HeapDumpOnOutOfMemoryError` 自动生成了堆转储文件，用 MAT 打开后，Leak Suspects 报告直接指出一个 12GB 的 ArrayList。通过 Dominator Tree 追踪到持有者是 ProfileBatchQueryService.queryAll() 方法。检查代码发现是参数校验缺失。

修复措施：所有查询接口强制 limit 最大 1000，添加请求参数校验中间件，增加返回结果大小的监控告警。

### 5. G1 和 ZGC 怎么选择？

**回答**：在画像项目中，查询服务用 G1，Bitmap 服务考虑用 ZGC。核心区别在于堆大小和停顿时间需求。

查询服务堆 8-16G，G1 的 Mixed GC 可以很好地控制停顿在 20ms 以内。而 Bitmap 服务堆 32-64G，G1 在这个量级会因为 Remembered Set 过大、Mixed GC 跟不上老年代增长速度而偶发 Full GC，停顿几秒。ZGC 的停顿时间与堆大小无关，始终保持亚毫秒级，适合大堆场景。但 ZGC 需要 JDK 11+，且吞吐量比 G1 略低（约 5-10%），需要权衡。

### 6. CompletableFuture 怎么用的？

**回答**：画像查询需要并行访问多个数据源（MySQL 基础属性、Redis 实时标签、HBase 行为特征），用 CompletableFuture 实现并行查询。每个数据源的查询提交到线程池异步执行，然后用 `CompletableFuture.allOf()` 等待所有结果，设置总超时 50ms。如果某个数据源超时，其他已完成的结果照常返回，实现"降级"而非"阻塞"。

关键实现细节：不用 `join()` 或无超时的 `get()`，必须设超时；超时后用 `isDone()` 判断哪些完成了，逐个合并；异常处理用 `exceptionally()` 或 `handle()` 确保单个数据源异常不影响整体。

### 7. volatile 的作用？在项目中哪里用了？

**回答**：volatile 保证两点：可见性（写入立即刷新到主存，读取从主存获取）和有序性（禁止指令重排序）。但不保证原子性。

在画像项目中有两个典型使用场景。第一是 Bitmap 双 Buffer 切换，`currentBitmap` 引用声明为 volatile，保证更新线程切换引用后查询线程能立即看到新的 Bitmap。第二是规则引擎的热更新，规则列表引用声明为 volatile，配合不可变集合实现安全发布。关键理解：volatile 写 happens-before volatile 读，所以 volatile 写之前的所有写操作对后续 volatile 读之后的操作都可见。

### 8. AQS 原理？

**回答**：AQS 是 J.U.C 的基石，核心是 volatile int state + CLH 双向队列。子类通过实现 `tryAcquire`/`tryRelease`（独占模式）或 `tryAcquireShared`/`tryReleaseShared`（共享模式）定义同步语义。

ReentrantLock 的 state 表示重入次数，CAS 0->1 获取锁，同一线程重入则 state++。Semaphore 的 state 表示许可数，acquire 时 CAS 减 1，release 时 CAS 加 1。CountDownLatch 的 state 是初始计数，countDown 时 CAS 减 1 到 0 时唤醒所有 await 的线程。

在画像项目中虽然不直接使用 AQS，但理解它有助于正确使用 ReentrantLock（如 Bitmap 双 Buffer 的 ReadWriteLock）和 Semaphore（如 HBase 并发控制）。

### 9. Spring AOP 在你项目中怎么用的？

**回答**：画像平台用 AOP 实现了三个横切关注点。第一是接口鉴权和限流，@Around 切面拦截所有 Controller 方法，从请求头提取 appKey 做鉴权，然后基于 appKey 的令牌桶做限流。第二是方法耗时统计，记录每个 Service 方法的 RT 到 Prometheus，超过 100ms 的打 warn 日志。第三是查询日志审计，记录谁查了哪些用户的哪些标签，用于安全审计。

实现上使用 CGLIB 代理（Spring Boot 默认），因为我们的 Service 类不都实现接口。需要注意自调用问题：同类中方法 A 调方法 B，B 上的 AOP 不生效，因为 A 中的 this.B() 绕过了代理。

### 10. Redis 缓存一致性怎么保证？

**回答**：画像系统采用"更新数据库 + 删除缓存 + 延迟双删"策略。标签数据更新时，先更新 HBase/MySQL，再删除 Redis 缓存，最后通过 Kafka 消息延迟 500ms 再删一次（防止并发读请求在删除前读到旧值并写回缓存）。

对于实时性要求极高的场景（如实时标签），我们采用"写穿透"策略：Flink 计算出新标签后，同时写 HBase 和 Redis。对于人群包 Bitmap，采用全量刷新策略：定时任务重新计算完整 Bitmap，通过双 Buffer 切换原子替换，不做增量更新，保证一致性。

### 11. Kafka 消息丢失怎么防止？

**回答**：从三个环节防止。生产者端：`acks=all` 确保所有 ISR 副本确认，`retries` 设为 Integer.MAX_VALUE，`enable.idempotence=true` 开启幂等。Broker 端：`min.insync.replicas=2` 最少 2 个同步副本，`unclean.leader.election.enable=false` 禁止非 ISR 副本选举为 Leader。消费者端：手动提交 offset（`enable.auto.commit=false`），消费完成并处理成功后再 commit。

在画像项目中，标签变更事件用的是 `acks=all` 保证不丢失，而行为数据采集因为量大且容忍少量丢失，用 `acks=1` 换取更高吞吐。

### 12. MySQL 索引优化做过哪些？

**回答**：标签元数据表有一个慢查询：按分类查询在线标签列表。原本只有 category_id 的单列索引，但查询条件还带 status = 1，导致回表后需要额外过滤。优化为 `(category_id, status)` 联合索引后，从全表扫描 5 万行降到 ref 扫描 50 行。

另一个是人群包表的分页查询。`ORDER BY created_at DESC LIMIT 20 OFFSET 10000` 在数据量大时很慢，因为 MySQL 要扫描前 10020 行再丢弃前 10000 行。优化为游标分页：`WHERE id < #{lastId} ORDER BY id DESC LIMIT 20`，利用主键索引避免深分页问题。

### 13. Netty 的线程模型？

**回答**：画像查询服务使用主从 Reactor 模型。BossGroup 1 个线程专门处理连接建立（Accept），WorkerGroup 等于 CPU 核数的线程处理 IO 读写（Read/Write），业务逻辑提交到独立的 BusinessGroup 线程池执行（避免阻塞 IO 线程）。

关键配置：开启 TCP_NODELAY 禁用 Nagle 算法降低延迟；使用 PooledByteBufAllocator 池化内存分配减少 GC；通过 IdleStateHandler 做心跳检测，60 秒无读写事件则关闭连接。ByteBuf 的释放是最容易出问题的地方，我们规定每个 Handler 谁分配谁释放，并且用 Netty 的泄漏检测在测试环境排查。

### 14. Protobuf vs JSON 的选择？

**回答**：内部 RPC 用 Protobuf，对外 API 用 JSON。Protobuf 的优势是体积小（比 JSON 小 3-5 倍，节省网络带宽）、序列化速度快（比 Jackson 快 5-10 倍）、有 IDL 定义保证类型安全和前后向兼容。JSON 的优势是可读性好、调试方便、前端可以直接解析。

在画像项目中实测：一个包含 50 个标签的用户画像，JSON 序列化后约 2KB，Protobuf 约 400B。在日均数十亿次查询的场景下，这个差异对网络带宽和 Redis 存储成本影响巨大。所以 Redis 中的画像数据也用 Protobuf 存储。

### 15. 设计模式在你项目中的应用？

**回答**：画像平台大量使用设计模式。策略模式：不同标签存储后端（HBase/Redis/ClickHouse）的查询策略统一接口，通过标签元数据中的 storage_type 动态选择。工厂模式：根据配置创建不同的标签源客户端。装饰器模式：在基础查询上叠加缓存、限流、监控层，每一层都是一个装饰器，可以灵活组合。责任链模式：圈选请求依次经过参数校验、权限校验、标签存在性校验。模板方法模式：标签计算流水线定义了"加载-清洗-计算-写入"的骨架，不同标签类型实现各自的计算逻辑。

### 16. Spring Boot 自动配置原理？

**回答**：`@EnableAutoConfiguration` 通过 `AutoConfigurationImportSelector` 加载 `META-INF/spring.factories` 中所有自动配置类的全限定名。每个配置类通过条件注解（@ConditionalOnClass、@ConditionalOnProperty 等）判断是否生效。

在画像项目中，我们封装了画像查询 SDK，通过自动配置实现"引入依赖即可用"。SDK 的 `ProfileAutoConfiguration` 类根据 `profile.tag-source.type` 配置自动装配对应的标签源实现（HBase/Redis/ClickHouse），业务方零代码接入。通过 `@ConditionalOnMissingBean` 允许业务方自定义覆盖默认实现。

### 17. 分布式锁怎么实现的？

**回答**：使用 Redisson 的 RedLock 实现。典型场景是人群包创建去重：多个节点可能同时收到创建相同人群包的请求。Redisson 底层使用 Lua 脚本保证加锁的原子性（SET key value NX PX timeout），通过看门狗机制自动续期（默认每 10 秒续一次，过期时间 30 秒），避免业务未完成锁就过期。

关键实践：获取锁后做双重检查（DCL），因为在等待锁的过程中可能其他节点已经完成了创建。锁的超时时间要大于业务最大执行时间。finally 中确保释放锁，且要判断 `isHeldByCurrentThread()` 避免释放别人的锁。

### 18. 如何做接口限流？

**回答**：画像平台使用多级限流。第一级是 Gateway 层的全局限流（基于 Sentinel），保护整个集群不被突发流量击穿。第二级是应用层的 appKey 维度限流，每个接入方有独立的 QPS 配额，使用令牌桶算法实现。第三级是资源层的 Semaphore 限流，控制对 HBase 等下游资源的并发数。

令牌桶实现：使用 Guava 的 RateLimiter 做单机限流，分布式场景下使用 Redis + Lua 脚本实现。Lua 脚本保证原子性：获取当前桶中令牌数，根据距上次填充的时间差计算应填充的令牌数，如果令牌足够则扣减并返回成功。

### 19. TCP 连接池怎么管理？

**回答**：画像服务与 HBase、Redis、下游 RPC 服务之间都使用连接池。以 HBase 连接池为例：使用 Apache Commons Pool2 管理 HBase Connection 对象。池大小根据下游容量和本机线程数设置（通常等于线程池最大线程数）。

关键配置：maxTotal 控制最大连接数，maxIdle 控制最大空闲连接数，minIdle 保持最小空闲连接（避免冷启动），testOnBorrow 借出时检测连接有效性（可能因网络问题失效），evictionRunsMillis 定期清理失效连接。踩过的坑：maxWaitMillis 设置过大导致线程长时间阻塞等待连接，引发雪崩。应设为合理值（如 100ms）并在超时时走降级逻辑。

### 20. 类加载器在你项目中有什么应用？

**回答**：画像平台的标签计算引擎支持用户上传自定义 UDF jar 包。为了实现类隔离（不同业务方的同名 UDF 不冲突）和热加载（不停机更新 UDF），我们实现了自定义 ClassLoader。

每个 UDF jar 对应一个独立的 URLClassLoader 实例，对 UDF 包路径下的类打破双亲委派（优先自己加载），其他类仍走双亲委派保证基础类库的一致性。热加载通过"创建新 ClassLoader -> 加载新版本 -> volatile 引用切换 -> 延迟关闭旧 ClassLoader"实现。踩过一个严重的坑：ClassLoader 无法被 GC 回收导致 Metaspace OOM。原因是 UDF 类的静态字段引用了 AppClassLoader 加载的对象，形成了双向引用链。最终通过规范 UDF 开发指南（禁止静态字段持有外部引用）并在卸载时反射清理静态字段解决。

---

> **总结**：本文档将 Java 核心知识体系与用户画像平台的实际工程场景深度结合。面试时，每个知识点都能给出具体的项目实例，做到"知其然更知其所以然"。关键是不仅能回答"是什么"，还能回答"为什么这么做"和"踩过什么坑"，体现真实的工程经验和技术深度。
