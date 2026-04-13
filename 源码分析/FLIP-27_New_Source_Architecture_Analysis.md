# Flink FLIP-27 New Source Architecture -- Deep Source Code Analysis

> Based on Apache Flink release-1.18 source code
> Analysis date: 2026-02-26

---

## Table of Contents

1. [Overview and Design Philosophy](#1-overview-and-design-philosophy)
2. [Overall Architecture Diagram](#2-overall-architecture-diagram)
3. [Core Interface Analysis](#3-core-interface-analysis)
   - 3.1 [Source -- Top-Level Factory Interface](#31-source----top-level-factory-interface)
   - 3.2 [SourceSplit -- Split Abstraction](#32-sourcesplit----split-abstraction)
   - 3.3 [Boundedness -- Batch-Stream Unification Key](#33-boundedness----batch-stream-unification-key)
   - 3.4 [SplitEnumerator -- Split Discovery and Assignment](#34-splitenumerator----split-discovery-and-assignment)
   - 3.5 [SourceReader -- Data Reading](#35-sourcereader----data-reading)
   - 3.6 [SourceEvent -- Custom Communication Protocol](#36-sourceevent----custom-communication-protocol)
4. [Context Interface Analysis](#4-context-interface-analysis)
   - 4.1 [SplitEnumeratorContext](#41-splitenumeratorcontext)
   - 4.2 [SourceReaderContext](#42-sourcereadercontext)
5. [Base Implementation Framework (flink-connector-base)](#5-base-implementation-framework-flink-connector-base)
   - 5.1 [SourceReaderBase -- Core Reader Implementation](#51-sourcereaderbase----core-reader-implementation)
   - 5.2 [SplitReader -- I/O Layer Abstraction](#52-splitreader----io-layer-abstraction)
   - 5.3 [RecordEmitter -- Record Transformation Bridge](#53-recordemitter----record-transformation-bridge)
   - 5.4 [SplitFetcherManager and SplitFetcher -- Thread Management](#54-splitfetchermanager-and-splitfetcher----thread-management)
   - 5.5 [SingleThreadMultiplexSourceReaderBase](#55-singlethreadmultiplexsourcereaderbase)
   - 5.6 [RecordsWithSplitIds -- Data Transfer Container](#56-recordswithsplitids----data-transfer-container)
6. [Runtime Coordination Layer](#6-runtime-coordination-layer)
   - 6.1 [SourceCoordinator -- JobManager-Side Coordinator](#61-sourcecoordinator----jobmanager-side-coordinator)
   - 6.2 [SourceCoordinatorContext -- Coordinator Execution Environment](#62-sourcecoordinatorcontext----coordinator-execution-environment)
   - 6.3 [SourceOperator -- TaskManager-Side Operator](#63-sourceoperator----taskmanager-side-operator)
7. [Event Communication System](#7-event-communication-system)
8. [Checkpoint Flow -- Component Collaboration](#8-checkpoint-flow----component-collaboration)
9. [Batch-Stream Unification Mechanism](#9-batch-stream-unification-mechanism)
10. [Dynamic Split Discovery Mechanism](#10-dynamic-split-discovery-mechanism)
11. [Watermark Processing Mechanism](#11-watermark-processing-mechanism)
12. [Fault Tolerance and Recovery Mechanism](#12-fault-tolerance-and-recovery-mechanism)
13. [Thread Model Deep Dive](#13-thread-model-deep-dive)
14. [Design Patterns Analysis](#14-design-patterns-analysis)
15. [Engineering Highlights and Best Practices](#15-engineering-highlights-and-best-practices)
16. [Improvement Suggestions](#16-improvement-suggestions)
17. [Summary and Key Insights](#17-summary-and-key-insights)

---

## 1. Overview and Design Philosophy

### 1.1 Why FLIP-27?

The old `SourceFunction` interface had several fundamental problems:

1. **Batch and stream processing were separate code paths** -- There was no unified way to write a source that worked for both batch and streaming.
2. **Split discovery and data reading were entangled** -- The old `SourceFunction.run()` method mixed split enumeration with data reading in a single thread, making it impossible to implement efficient parallel reading patterns.
3. **No framework-level support for event time alignment** -- Watermark generation was ad-hoc and not integrated with the source framework.
4. **Checkpoint locking was error-prone** -- The `SourceFunction` required holding a checkpoint lock around emission, which was a common source of bugs.

FLIP-27 addresses all of these by introducing a clear separation of concerns:

- **Split Enumeration** runs on the JobManager (single instance) -- responsible for discovering and assigning work.
- **Data Reading** runs on TaskManagers (parallel instances) -- responsible for consuming data from assigned splits.
- The two communicate through an **asynchronous event-based protocol**.

### 1.2 Core Design Principles

| Principle | Realization |
|-----------|-------------|
| **Separation of concerns** | SplitEnumerator (control plane) vs SourceReader (data plane) |
| **Batch-stream unification** | Single `Boundedness` enum controls execution mode |
| **Non-blocking I/O model** | `pollNext()` is non-blocking; `isAvailable()` provides back-pressure via CompletableFuture |
| **Framework-managed threading** | Connector developers never create threads; SplitFetcherManager handles it |
| **Type-safe serialization** | `SimpleVersionedSerializer` for all state, supporting schema evolution |
| **Event-driven communication** | SourceEvent-based protocol between enumerator and readers |

---

## 2. Overall Architecture Diagram

```
+===========================================================================+
|                         JobManager (JM)                                    |
|                                                                            |
|  +-------------------------------------------------------------------+    |
|  |                    SourceCoordinator                               |    |
|  |  (implements OperatorCoordinator)                                  |    |
|  |                                                                    |    |
|  |  +-------------------------------------------------------------+  |    |
|  |  |              SplitEnumerator<SplitT, EnumChkT>              |  |    |
|  |  |                                                              |  |    |
|  |  |  - start()                                                   |  |    |
|  |  |  - handleSplitRequest(subtaskId, hostname)                   |  |    |
|  |  |  - addSplitsBack(splits, subtaskId)                          |  |    |
|  |  |  - addReader(subtaskId)                                      |  |    |
|  |  |  - snapshotState(checkpointId)                               |  |    |
|  |  |  - handleSourceEvent(subtaskId, event)                       |  |    |
|  |  +-------------------------------------------------------------+  |    |
|  |                          |                                         |    |
|  |              +-----------+-----------+                             |    |
|  |              |                       |                             |    |
|  |  +-----------------------+  +-----------------------------+        |    |
|  |  | SplitEnumeratorContext|  | SourceCoordinatorContext     |        |    |
|  |  | - assignSplits()      |  | - assignmentTracker          |        |    |
|  |  | - signalNoMoreSplits()|  | - subtaskGateways            |        |    |
|  |  | - callAsync()         |  | - coordinatorExecutor         |        |    |
|  |  | - runInCoordThread()  |  | - workerExecutor              |        |    |
|  |  +-----------------------+  +-----------------------------+        |    |
|  +-------------------------------------------------------------------+    |
|                          |                                                 |
+==========================|=================================================+
                           | OperatorEvent (RPC)
                           | - AddSplitEvent
                           | - NoMoreSplitsEvent
                           | - WatermarkAlignmentEvent
                           | - SourceEventWrapper
                           | - RequestSplitEvent
                           | - ReaderRegistrationEvent
                           | - ReportedWatermarkEvent
                           |
+==========================|=================================================+
|                    TaskManager (TM)  x N parallel                          |
|                          |                                                 |
|  +-------------------------------------------------------------------+    |
|  |                     SourceOperator                                 |    |
|  |  (implements OperatorEventHandler, PushingAsyncDataInput)          |    |
|  |                                                                    |    |
|  |  +-------------------------------------------------------------+  |    |
|  |  |             SourceReader<T, SplitT>                         |  |    |
|  |  |                                                              |  |    |
|  |  |  - start()                                                   |  |    |
|  |  |  - pollNext(ReaderOutput)    <-- non-blocking!               |  |    |
|  |  |  - addSplits(splits)                                         |  |    |
|  |  |  - snapshotState(checkpointId)                               |  |    |
|  |  |  - isAvailable()  --> CompletableFuture<Void>                |  |    |
|  |  |  - notifyNoMoreSplits()                                      |  |    |
|  |  |  - pauseOrResumeSplits()                                     |  |    |
|  |  +-------------------------------------------------------------+  |    |
|  |                          |                                         |    |
|  |  +-----------------------+---------------------------------+       |    |
|  |  |            SourceReaderBase (base impl)                  |       |    |
|  |  |                                                          |       |    |
|  |  |  +------------------+  +-----------------------------+   |       |    |
|  |  |  | SplitFetcherMgr  |  | FutureCompletingBlockQueue |   |       |    |
|  |  |  |   |              |  |   (elementsQueue)            |   |       |    |
|  |  |  |   v              |  +-----------------------------+   |       |    |
|  |  |  | SplitFetcher     |         ^                          |       |    |
|  |  |  |   |              |         |                          |       |    |
|  |  |  |   v              |    RecordsWithSplitIds             |       |    |
|  |  |  | SplitReader      |                                    |       |    |
|  |  |  | - fetch()        +-----> RecordEmitter               |       |    |
|  |  |  +------------------+       - emitRecord()               |       |    |
|  |  +---------------------------------------------------------+       |    |
|  |                                                                    |    |
|  |  +-------------------------------------------------------------+  |    |
|  |  |        TimestampsAndWatermarks                              |  |    |
|  |  |  - createMainOutput() -> ReaderOutput                       |  |    |
|  |  |  - per-split watermark tracking                              |  |    |
|  |  |  - periodic watermark emission                               |  |    |
|  |  +-------------------------------------------------------------+  |    |
|  +-------------------------------------------------------------------+    |
+===========================================================================+
```

---

## 3. Core Interface Analysis

### 3.1 Source -- Top-Level Factory Interface

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java`

```java
@Public
public interface Source<T, SplitT extends SourceSplit, EnumChkT>
        extends SourceReaderFactory<T, SplitT> {

    Boundedness getBoundedness();

    SplitEnumerator<SplitT, EnumChkT> createEnumerator(
            SplitEnumeratorContext<SplitT> enumContext) throws Exception;

    SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception;

    SimpleVersionedSerializer<SplitT> getSplitSerializer();

    SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer();
}
```

**Design Analysis**:

The `Source` interface serves as an **Abstract Factory** pattern. It does not read any data itself -- it creates the components that do:

| Type Parameter | Purpose | Example |
|----------------|---------|---------|
| `T` | Output record type | `String`, `RowData` |
| `SplitT` | Split type (must extend `SourceSplit`) | `KafkaPartitionSplit`, `FileSourceSplit` |
| `EnumChkT` | Enumerator checkpoint state type | `Collection<KafkaPartitionSplit>`, `PendingSplitsCheckpoint` |

Key design decisions:

1. **`extends SourceReaderFactory<T, SplitT>`** -- The Source inherits a `createReader(SourceReaderContext)` method from `SourceReaderFactory`. This parent interface is `Serializable`, ensuring the Source can be serialized and sent to TaskManagers.

2. **Separate creation vs restoration** -- `createEnumerator()` for fresh starts vs `restoreEnumerator()` for checkpoint recovery. This forces connector developers to explicitly handle recovery logic.

3. **`SimpleVersionedSerializer`** for both split and enumerator state -- This ensures forward/backward compatibility during upgrades, as the serializer carries a version number.

### 3.2 SourceSplit -- Split Abstraction

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/SourceSplit.java`

```java
@Public
public interface SourceSplit {
    String splitId();
}
```

This is intentionally minimal. A split is an opaque unit of work identified by a string ID. The actual content (e.g., file path + offset range, Kafka topic + partition + offset) is defined by the connector implementation.

**Why only `splitId()`?** The framework needs to track splits by ID for:
- Per-split watermark tracking in `SourceOperator.splitCurrentWatermarks`
- Per-split output management in `ReaderOutput.createOutputForSplit(splitId)`
- Split state management in `SourceReaderBase.splitStates`
- Pause/resume of individual splits for watermark alignment

### 3.3 Boundedness -- Batch-Stream Unification Key

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/Boundedness.java`

```java
@Public
public enum Boundedness {
    BOUNDED,              // Finite records (batch mode)
    CONTINUOUS_UNBOUNDED  // Infinite records (streaming mode)
}
```

This enum is the cornerstone of batch-stream unification. Source code commentary reveals important semantics:

- **BOUNDED**: "the source implementations may not have to keep track of event times or watermarks. Instead, a higher throughput would be preferred." This tells us that in batch mode, the runtime can skip watermark generation entirely.
- **CONTINUOUS_UNBOUNDED**: "Flink always assumes the sources are going to run forever." This means the runtime must set up watermark generation, periodic checkpointing, and other streaming infrastructure.

The `SourceOperator.open()` method uses this to decide between `ProgressiveTimestampsAndWatermarks` (streaming) and `NoOpTimestampsAndWatermarks` (batch) via the `emitProgressiveWatermarks` flag.

### 3.4 SplitEnumerator -- Split Discovery and Assignment

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumerator.java`

```java
@Public
public interface SplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        extends AutoCloseable, CheckpointListener {

    void start();
    void handleSplitRequest(int subtaskId, @Nullable String requesterHostname);
    void addSplitsBack(List<SplitT> splits, int subtaskId);
    void addReader(int subtaskId);
    CheckpointT snapshotState(long checkpointId) throws Exception;
    void close() throws IOException;

    // Default implementations:
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}
    default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}
}
```

**Method Semantics (Lifecycle Order)**:

| Method | When Called | Purpose |
|--------|------------|---------|
| `start()` | After coordinator start | Initialize discovery (e.g., list Kafka topics, scan HDFS directories) |
| `addReader(subtaskId)` | When a SourceReader registers | Track available readers for assignment decisions |
| `handleSplitRequest(subtaskId, hostname)` | When reader requests work | Pull-based split assignment (locality-aware via hostname) |
| `addSplitsBack(splits, subtaskId)` | After reader failure | Reclaim un-checkpointed splits for reassignment |
| `snapshotState(checkpointId)` | During checkpoint | Capture pending/unassigned splits and discovery state |
| `handleSourceEvent(subtaskId, event)` | Custom event from reader | Extensible protocol for connector-specific communication |
| `notifyCheckpointComplete(checkpointId)` | Checkpoint success | Commit external state (e.g., mark Kafka offsets) |
| `close()` | Coordinator shutdown | Release resources |

**Critical design detail from source comments**: `snapshotState()` should assume "all operations that happened before the snapshot have successfully completed". This means already-assigned splits do NOT need to be included -- they are tracked by the `SplitAssignmentTracker` in the runtime.

### 3.5 SourceReader -- Data Reading

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/SourceReader.java`

```java
@Public
public interface SourceReader<T, SplitT extends SourceSplit>
        extends AutoCloseable, CheckpointListener {

    void start();
    InputStatus pollNext(ReaderOutput<T> output) throws Exception;
    List<SplitT> snapshotState(long checkpointId);
    CompletableFuture<Void> isAvailable();
    void addSplits(List<SplitT> splits);
    void notifyNoMoreSplits();

    // Default implementations:
    default void handleSourceEvents(SourceEvent sourceEvent) {}
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}
    default void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        throw new UnsupportedOperationException(...);
    }
}
```

**The Non-Blocking Contract**:

The `pollNext()` method is the heart of the reader. The source code comment explicitly states: "The implementation must make sure this method is non-blocking." This is fundamentally different from the old `SourceFunction.run()` which was a blocking loop.

The return value `InputStatus` has three states:

| Status | Meaning | Runtime Behavior |
|--------|---------|-----------------|
| `MORE_AVAILABLE` | More data ready now | Immediately call `pollNext()` again |
| `NOTHING_AVAILABLE` | No data right now | Wait on `isAvailable()` future |
| `END_OF_INPUT` | All splits finished | Mark source as done |

**The Availability Protocol**:

```
                +---> pollNext() --> MORE_AVAILABLE --+
                |                                      |
    isAvailable() future completes                     |
                ^                                      |
                |                                      v
                +---> pollNext() --> NOTHING_AVAILABLE -+
                            |
                            v
                      pollNext() --> END_OF_INPUT (done)
```

This future-based back-pressure mechanism integrates naturally with Flink's mailbox-based task execution model. When no data is available, the task thread can process other mailbox events (e.g., checkpoint barriers, watermark alignment events) without blocking.

**Watermark Alignment Support**:

The `pauseOrResumeSplits()` method (marked `@PublicEvolving`) was added to support per-split watermark alignment. When a split's watermark advances too far ahead of others, the runtime can pause it. The default implementation throws `UnsupportedOperationException` -- connectors that do not support this will cause watermark alignment to fail unless `pipeline.watermark-alignment.allow-unaligned-source-splits` is set to `true`.

### 3.6 SourceEvent -- Custom Communication Protocol

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/SourceEvent.java`

```java
@Public
public interface SourceEvent extends Serializable {}
```

A marker interface for connector-specific events. This enables the extension of the communication protocol without modifying the framework. For example, a Kafka source might define a `PartitionDiscoveredEvent` to notify readers about new partitions.

---

## 4. Context Interface Analysis

### 4.1 SplitEnumeratorContext

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumeratorContext.java`

This is the enumerator's window into the runtime. It serves three purposes documented in the source comments:

1. **Information Provider** -- parallelism, registered readers
2. **Action Taker** -- assign splits, signal no-more-splits, send events
3. **Thread Model** -- `callAsync()`, `runInCoordinatorThread()`

**Complete Method Table**:

| Method | Category | Thread Safety |
|--------|----------|---------------|
| `metricGroup()` | Info | Thread-safe |
| `currentParallelism()` | Info | Thread-safe (dynamic, do not cache!) |
| `registeredReaders()` | Info | Returns snapshot |
| `registeredReadersOfAttempts()` | Info | Speculative execution support |
| `assignSplits(SplitsAssignment)` | Action | Delegates to coordinator thread |
| `assignSplit(SplitT, int)` | Action | Convenience method for single split |
| `signalNoMoreSplits(int)` | Action | Delegates to coordinator thread |
| `sendEventToSourceReader(int, SourceEvent)` | Action | Delegates to coordinator thread |
| `sendEventToSourceReader(int, int, SourceEvent)` | Action | Speculative execution support |
| `callAsync(Callable, BiConsumer)` | Threading | Runs callable in worker pool, handler in coordinator thread |
| `callAsync(Callable, BiConsumer, long, long)` | Threading | Periodic version for continuous discovery |
| `runInCoordinatorThread(Runnable)` | Threading | Execute on coordinator thread from external trigger |

**The Async Execution Model (from source comments)**:

```
                 Worker Thread Pool              Coordinator Thread
                 ================               ===================
callAsync() --> | callable.call() | -------->  | handler.accept()  |
                | (blocking I/O   |   result   | (state mutation)  |
                |  is OK here)    |            |                   |
                 =================              ===================
```

"It is important to make sure that the callable does not modify any shared state, especially the states that will be a part of the SplitEnumerator#snapshotState(long)."

This is a critical constraint -- the callables run concurrently in a thread pool, but the handlers are serialized on the coordinator thread, ensuring thread safety for state mutations.

The `runInCoordinatorThread(Runnable)` method is particularly interesting. The source comments give a specific use case: "Watermark from another source advanced and this source now be able to assign splits to awaiting readers." This enables cross-source coordination without locks.

### 4.2 SourceReaderContext

**File**: `flink-core/src/main/java/org/apache/flink/api/connector/source/SourceReaderContext.java`

```java
@Public
public interface SourceReaderContext {
    SourceReaderMetricGroup metricGroup();
    Configuration getConfiguration();
    String getLocalHostName();
    int getIndexOfSubtask();
    void sendSplitRequest();
    void sendSourceEventToCoordinator(SourceEvent sourceEvent);
    UserCodeClassLoader getUserCodeClassLoader();
    default int currentParallelism() { throw new UnsupportedOperationException(); }
}
```

The reader's context is simpler than the enumerator's. Key methods:

- `sendSplitRequest()` -- Triggers the pull-based split assignment protocol. The reader calls this when it needs more work. In `SourceOperator`, this translates to sending a `RequestSplitEvent` to the coordinator.
- `getLocalHostName()` -- Enables locality-aware split assignment. The hostname is forwarded to `handleSplitRequest()`.
- `sendSourceEventToCoordinator()` -- Sends a custom `SourceEvent` wrapped in a `SourceEventWrapper` operator event.

---

## 5. Base Implementation Framework (flink-connector-base)

### 5.1 SourceReaderBase -- Core Reader Implementation

**File**: `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/SourceReaderBase.java`

This abstract class provides the **core hand-over protocol** between I/O threads and the mailbox thread. It eliminates the need for connector developers to manage thread synchronization.

**Architecture**:

```
  [Mailbox Thread]                     [I/O Thread(s)]
        |                                    |
  pollNext()                           SplitReader.fetch()
        |                                    |
  elementsQueue.poll() <--- RecordsWithSplitIds --- elementsQueue.put()
        |                                    |
  recordEmitter.emitRecord()                 |
        |                                    |
  SourceOutput.collect()                     |
```

**Key Fields**:

```java
// The producer-consumer bridge between I/O and mailbox threads
private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

// Split states: maps split ID to mutable state + per-split output
private final Map<String, SplitContext<T, SplitStateT>> splitStates;

// Converts raw records to final output records
protected final RecordEmitter<E, T, SplitStateT> recordEmitter;

// Manages I/O fetcher threads
protected final SplitFetcherManager<E, SplitT> splitFetcherManager;
```

**The pollNext() Implementation (annotated)**:

```java
@Override
public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
    RecordsWithSplitIds<E> recordsWithSplitId = this.currentFetch;
    if (recordsWithSplitId == null) {
        // Try to get a new batch from the queue (non-blocking poll)
        recordsWithSplitId = getNextFetch(output);
        if (recordsWithSplitId == null) {
            // No data available -- check if we are done or should wait
            return trace(finishedOrAvailableLater());
        }
    }

    while (true) {
        final E record = recordsWithSplitId.nextRecordFromSplit();
        if (record != null) {
            numRecordsInCounter.inc(1);
            // Transform and emit via RecordEmitter
            recordEmitter.emitRecord(record, currentSplitOutput, currentSplitContext.state);
            // Optimistically return MORE_AVAILABLE (avoids per-record availability checks)
            return trace(InputStatus.MORE_AVAILABLE);
        } else if (!moveToNextSplit(recordsWithSplitId, output)) {
            // All splits in this fetch are consumed; recursively try next fetch
            return pollNext(output);
        }
    }
}
```

**Performance optimization insight**: The code returns `MORE_AVAILABLE` after every record emission, even without checking if more data is actually available. As the source comment explains: "We always emit MORE_AVAILABLE here... this saves us doing checks for every record. Ultimately, this is cheaper." The occasional false positive causes one extra `pollNext()` call, which is much cheaper than checking availability on each record.

**The `finishedOrAvailableLater()` Logic**:

```java
private InputStatus finishedOrAvailableLater() {
    final boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
    if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
        return InputStatus.NOTHING_AVAILABLE;
    }
    if (elementsQueue.isEmpty()) {
        splitFetcherManager.checkErrors();
        return InputStatus.END_OF_INPUT;  // Truly done
    } else {
        return InputStatus.MORE_AVAILABLE; // Race condition: more data arrived
    }
}
```

The reader finishes only when THREE conditions are met:
1. `noMoreSplitsAssignment == true` (received NoMoreSplitsEvent)
2. All fetchers have shut down (no active I/O)
3. The elements queue is empty (no buffered data)

**Abstract Methods Connector Developers Must Implement**:

```java
// Called when a split finishes reading -- e.g., request more splits
protected abstract void onSplitFinished(Map<String, SplitStateT> finishedSplitIds);

// Convert immutable split to mutable state (e.g., add offset tracking)
protected abstract SplitStateT initializedState(SplitT split);

// Convert mutable state back to immutable split (for checkpointing)
protected abstract SplitT toSplitType(String splitId, SplitStateT splitState);
```

The **Immutable Split vs Mutable State** pattern is a key design insight. The split (`SplitT`) is an immutable description of work (e.g., topic-partition-startOffset). The state (`SplitStateT`) is a mutable tracking object (e.g., topic-partition-currentOffset). This separation ensures that splits can be safely sent over the network while state is updated in-place during reading.

### 5.2 SplitReader -- I/O Layer Abstraction

**File**: `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/splitreader/SplitReader.java`

```java
@PublicEvolving
public interface SplitReader<E, SplitT extends SourceSplit> extends AutoCloseable {
    RecordsWithSplitIds<E> fetch() throws IOException;
    void handleSplitsChanges(SplitsChange<SplitT> splitsChanges);
    void wakeUp();
    default void pauseOrResumeSplits(
            Collection<SplitT> splitsToPause, Collection<SplitT> splitsToResume) {
        throw new UnsupportedOperationException(...);
    }
}
```

This is the interface that connector developers implement to interact with the external system:

- `fetch()` -- **Can be blocking**. This runs on the I/O thread(s), not the mailbox thread. It should return when data is available or when `wakeUp()` is called.
- `handleSplitsChanges()` -- Called (on the I/O thread) when splits are added or removed. Must be non-blocking.
- `wakeUp()` -- Interrupts a blocking `fetch()` call. Essential for shutdown and split changes.
- `pauseOrResumeSplits()` -- For watermark alignment. E.g., a Kafka SplitReader can pause/resume partition consumption.

### 5.3 RecordEmitter -- Record Transformation Bridge

**File**: `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/RecordEmitter.java`

```java
@PublicEvolving
public interface RecordEmitter<E, T, SplitStateT> {
    void emitRecord(E element, SourceOutput<T> output, SplitStateT splitState) throws Exception;
}
```

This interface bridges the gap between:
- `E` -- The raw element type from the I/O layer (e.g., `ConsumerRecord<byte[], byte[]>`)
- `T` -- The final output type (e.g., `RowData`, `String`)
- `SplitStateT` -- The mutable split state to update (e.g., update current offset)

**Usage pattern in a Kafka-like source**:

```java
// Example implementation
class KafkaRecordEmitter implements RecordEmitter<ConsumerRecord, String, KafkaSplitState> {
    @Override
    public void emitRecord(ConsumerRecord record, SourceOutput<String> output, KafkaSplitState state) {
        // 1. Update split state (advance offset)
        state.setCurrentOffset(record.offset() + 1);
        // 2. Deserialize and emit
        output.collect(deserializer.deserialize(record), record.timestamp());
    }
}
```

### 5.4 SplitFetcherManager and SplitFetcher -- Thread Management

**File**: `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/fetcher/SplitFetcherManager.java`
**File**: `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/fetcher/SplitFetcher.java`

**SplitFetcherManager** is the abstract thread manager. It provides:

```java
// Different threading models implemented by overriding addSplits():
// - SingleThreadFetcherManager: One thread reads all splits (e.g., Kafka, file source)
// - Custom: One thread per split (e.g., for sources requiring dedicated connections)

public abstract void addSplits(List<SplitT> splitsToAdd);
public abstract void removeSplits(List<SplitT> splitsToRemove);
```

**SplitFetcher** is the actual `Runnable` that executes on I/O threads. Its internal architecture is a **task queue** pattern:

```java
// The SplitFetcher main loop (simplified):
@Override
public void run() {
    while (runOnce()) {
        // nothing to do
    }
}

boolean runOnce() {
    lock.lock();
    try {
        task = getNextTaskUnsafe();  // May block waiting for tasks/splits
    } finally {
        lock.unlock();
    }

    taskFinished = task.run();  // Execute outside lock (can be woken up)

    lock.lock();
    try {
        processTaskResultUnsafe(task, taskFinished);
    } finally {
        lock.unlock();
    }
    return true;
}
```

**Task types in the queue**:
- `FetchTask` -- The default task when splits are assigned. Calls `SplitReader.fetch()`.
- `AddSplitsTask` -- Calls `SplitReader.handleSplitsChanges()` with new splits.
- `RemoveSplitsTask` -- Removes splits from the reader.
- `PauseOrResumeSplitsTask` -- For watermark alignment.

When there are no specific tasks in the queue and splits are assigned, the fetcher falls back to `FetchTask`. When there are no splits and no tasks, it blocks on a `Condition` until new work arrives.

**SingleThreadFetcherManager**:

```java
@Override
public void addSplits(List<SplitT> splitsToAdd) {
    SplitFetcher<E, SplitT> fetcher = getRunningFetcher();
    if (fetcher == null) {
        fetcher = createSplitFetcher();
        fetcher.addSplits(splitsToAdd);
        startFetcher(fetcher);  // Submit to ExecutorService
    } else {
        fetcher.addSplits(splitsToAdd);
    }
}
```

This maintains at most one fetcher thread that handles all splits via multiplexing (suitable for Kafka, where a single KafkaConsumer polls all assigned partitions).

### 5.5 SingleThreadMultiplexSourceReaderBase

**File**: `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/SingleThreadMultiplexSourceReaderBase.java`

This is a convenience base class that pre-configures `SourceReaderBase` with a `SingleThreadFetcherManager`:

```java
public abstract class SingleThreadMultiplexSourceReaderBase<E, T, SplitT, SplitStateT>
        extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    public SingleThreadMultiplexSourceReaderBase(
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        this(
            new FutureCompletingBlockingQueue<>(...),
            splitReaderSupplier,
            recordEmitter,
            config,
            context);
    }
}
```

Connector developers should extend this class for the common pattern where a single I/O thread reads from all splits. The threading model looks like:

```
[Mailbox Thread]                 [Single I/O Thread]
      |                                |
  pollNext()                    SplitReader.fetch()
      |                           reads from ALL splits
  elementsQueue.poll() <------- elementsQueue.put()
      |
  RecordEmitter.emitRecord()
```

### 5.6 RecordsWithSplitIds -- Data Transfer Container

**File**: `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/RecordsWithSplitIds.java`

```java
@PublicEvolving
public interface RecordsWithSplitIds<E> {
    @Nullable String nextSplit();
    @Nullable E nextRecordFromSplit();
    Set<String> finishedSplits();
    default void recycle() {}
}
```

This is the data transfer object between I/O threads and the mailbox thread. It provides an iterator-like interface organized by split:

```
RecordsWithSplitIds
  |
  +-- nextSplit() -> "split-1"
  |     +-- nextRecordFromSplit() -> record1
  |     +-- nextRecordFromSplit() -> record2
  |     +-- nextRecordFromSplit() -> null  (no more for split-1)
  |
  +-- nextSplit() -> "split-2"
  |     +-- nextRecordFromSplit() -> record3
  |     +-- nextRecordFromSplit() -> null
  |
  +-- nextSplit() -> null  (no more splits in this batch)
  |
  +-- finishedSplits() -> {"split-1"}  (split-1 reached EOF)
```

The `recycle()` method is a performance optimization for object reuse, avoiding GC pressure for high-throughput sources.

---

## 6. Runtime Coordination Layer

### 6.1 SourceCoordinator -- JobManager-Side Coordinator

**File**: `flink-runtime/src/main/java/org/apache/flink/runtime/source/coordinator/SourceCoordinator.java`

The `SourceCoordinator` implements `OperatorCoordinator` and bridges the Flink runtime with the user-provided `SplitEnumerator`. Its source code documentation states:

> "The SourceCoordinator provides an event loop style thread model to interact with the Flink runtime. The coordinator ensures that all the state manipulations are made by its event loop thread."

**Event Handling (from source code)**:

```java
@Override
public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
    runInEventLoop(() -> {
        if (event instanceof RequestSplitEvent) {
            handleRequestSplitEvent(subtask, attemptNumber, (RequestSplitEvent) event);
        } else if (event instanceof SourceEventWrapper) {
            handleSourceEvent(subtask, attemptNumber,
                    ((SourceEventWrapper) event).getSourceEvent());
        } else if (event instanceof ReaderRegistrationEvent) {
            handleReaderRegistrationEvent(subtask, attemptNumber,
                    (ReaderRegistrationEvent) event);
        } else if (event instanceof ReportedWatermarkEvent) {
            handleReportedWatermark(subtask,
                    new Watermark(((ReportedWatermarkEvent) event).getWatermark()));
        } else {
            throw new FlinkException("Unrecognized Operator Event: " + event);
        }
    }, ...);
}
```

**Start Sequence**:

```java
@Override
public void start() throws Exception {
    started = true;

    // Two creation paths:
    if (enumerator == null) {
        // Path 1: Fresh start -> Source.createEnumerator()
        enumerator = source.createEnumerator(context);
    }
    // Path 2: Restored from checkpoint -> enumerator already set by resetToCheckpoint()

    // Start the enumerator on the coordinator thread
    runInEventLoop(() -> enumerator.start(), "starting the SplitEnumerator.");

    // Set up watermark alignment if enabled
    if (watermarkAlignmentParams.isEnabled()) {
        coordinatorStore.putIfAbsent(
                watermarkAlignmentParams.getWatermarkGroup(), new WatermarkAggregator<>());
        context.schedulePeriodTask(
                this::announceCombinedWatermark,
                watermarkAlignmentParams.getUpdateInterval(), ...);
    }
}
```

**Checkpoint Protocol**:

```java
@Override
public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
    runInEventLoop(() -> {
        context.onCheckpoint(checkpointId);        // Track assignment boundaries
        result.complete(toBytes(checkpointId));     // Serialize enumerator state
    }, ...);
}

private byte[] toBytes(long checkpointId) throws Exception {
    return writeCheckpointBytes(
            enumerator.snapshotState(checkpointId),  // User code: snapshot state
            enumCheckpointSerializer);                // Framework: serialize
}
```

**The WatermarkAggregator** is an internal class that maintains the minimum watermark across all subtasks:

```java
static class WatermarkAggregator<T> {
    private final Map<T, WatermarkElement> watermarks = new HashMap<>();
    private final HeapPriorityQueue<WatermarkElement> orderedWatermarks = ...;

    public Optional<Watermark> aggregate(T key, Watermark watermark) {
        // Update the watermark for the given key
        // Return new aggregate if it changed, empty otherwise
    }

    public Watermark getAggregatedWatermark() {
        // Returns the minimum watermark across all keys
        return orderedWatermarks.peek();
    }
}
```

### 6.2 SourceCoordinatorContext -- Coordinator Execution Environment

**File**: `flink-runtime/src/main/java/org/apache/flink/runtime/source/coordinator/SourceCoordinatorContext.java`

This class implements `SplitEnumeratorContext` and manages:

**Thread Infrastructure**:
- `coordinatorExecutor` -- Single-threaded `ScheduledExecutorService` for all state mutations
- `workerExecutor` -- Multi-threaded pool for async callables
- `ExecutorNotifier` -- Bridges worker results to coordinator thread

**Reader Management**:
- `registeredReaders` -- `ConcurrentMap<subtaskId, ConcurrentMap<attemptNumber, ReaderInfo>>`
- Supports speculative execution with multiple concurrent attempts per subtask

**Split Assignment Tracking**:
- `SplitAssignmentTracker` -- Records which splits were assigned to which subtask and when
- Tracks uncheckpointed assignments for failure recovery

**The assignSplits() implementation**:

```java
@Override
public void assignSplits(SplitsAssignment<SplitT> assignment) {
    callInCoordinatorThread(() -> {
        // 1. Verify all target subtasks are registered
        assignment.assignment().forEach((id, splits) -> {
            if (!registeredReaders.containsKey(id)) {
                throw new IllegalArgumentException(...);
            }
        });
        // 2. Record assignment for checkpoint tracking
        assignmentTracker.recordSplitAssignment(assignment);
        // 3. Send AddSplitEvent to each target reader
        assignSplitsToAttempts(assignment);
        return null;
    }, ...);
}
```

### 6.3 SourceOperator -- TaskManager-Side Operator

**File**: `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/SourceOperator.java`

This is the operator that runs on each TaskManager. It bridges the `SourceReader` with the Flink task runtime.

**Operating Modes**:

```java
private enum OperatingMode {
    READING,                   // Normal data reading
    WAITING_FOR_ALIGNMENT,     // Paused due to watermark alignment
    OUTPUT_NOT_INITIALIZED,    // Before first emitNext() call
    SOURCE_DRAINED,            // Draining for stop-with-savepoint
    SOURCE_STOPPED,            // Hard stop
    DATA_FINISHED              // All data consumed
}
```

**The hot path -- `emitNext()`**:

```java
@Override
public DataInputStatus emitNext(DataOutput<OUT> output) throws Exception {
    // Fast path for normal reading (performance critical!)
    if (operatingMode != OperatingMode.READING) {
        return emitNextNotReading(output);
    }

    InputStatus status;
    do {
        status = sourceReader.pollNext(currentMainOutput);
    } while (status == InputStatus.MORE_AVAILABLE
            && canEmitBatchOfRecords.check()       // Batch emission optimization
            && !shouldWaitForAlignment());          // Watermark alignment check
    return convertToInternalStatus(status);
}
```

The loop inside `emitNext()` is a **batch emission optimization**: as long as records are available and the runtime allows batch processing, records are emitted in a tight loop without returning to the task thread. This significantly reduces overhead for high-throughput sources.

**Checkpoint Integration**:

```java
@Override
public void snapshotState(StateSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    // Ask the reader for current split states
    readerState.update(sourceReader.snapshotState(checkpointId));
}

@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    // Restore split state from checkpoint
    final ListState<byte[]> rawState =
            context.getOperatorStateStore().getListState(SPLITS_STATE_DESC);
    readerState = new SimpleVersionedListState<>(rawState, splitSerializer);
}
```

**Event Handling**:

```java
public void handleOperatorEvent(OperatorEvent event) {
    if (event instanceof WatermarkAlignmentEvent) {
        updateMaxDesiredWatermark((WatermarkAlignmentEvent) event);
        checkWatermarkAlignment();
        checkSplitWatermarkAlignment();
    } else if (event instanceof AddSplitEvent) {
        handleAddSplitsEvent(((AddSplitEvent<SplitT>) event));
    } else if (event instanceof SourceEventWrapper) {
        sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
    } else if (event instanceof NoMoreSplitsEvent) {
        sourceReader.notifyNoMoreSplits();
    }
}
```

---

## 7. Event Communication System

The FLIP-27 Source uses **OperatorEvents** for all communication between JobManager and TaskManagers. All events flow through the existing RPC infrastructure.

### Event Types and Data Flow

```
TaskManager -> JobManager (Upstream Events):
================================================
  RequestSplitEvent         Reader requests new split assignment
  ReaderRegistrationEvent   Reader announces itself (subtaskId + hostname)
  ReportedWatermarkEvent    Reader reports current watermark (for alignment)
  SourceEventWrapper        Custom connector event (wraps SourceEvent)

JobManager -> TaskManager (Downstream Events):
================================================
  AddSplitEvent<SplitT>     Coordinator assigns splits (serialized via splitSerializer)
  NoMoreSplitsEvent         No more splits will be assigned to this reader
  WatermarkAlignmentEvent   New max allowed watermark (for alignment)
  SourceEventWrapper        Custom connector event (wraps SourceEvent)
```

### Registration and Split Request Flow

```
Step 1: Reader Registration
  SourceOperator.open() -> registerReader()
    -> operatorEventGateway.sendEventToCoordinator(new ReaderRegistrationEvent(subtaskId, hostname))
      -> SourceCoordinator.handleReaderRegistrationEvent()
        -> context.registerSourceReader(subtaskId, attemptNumber, location)
        -> enumerator.addReader(subtaskId)

Step 2: Split Request (Pull-based)
  SourceReader.start() -> context.sendSplitRequest()
    -> operatorEventGateway.sendEventToCoordinator(new RequestSplitEvent(hostname))
      -> SourceCoordinator.handleRequestSplitEvent()
        -> enumerator.handleSplitRequest(subtaskId, hostname)

Step 3: Split Assignment
  SplitEnumerator.handleSplitRequest() -> context.assignSplits(assignment)
    -> SourceCoordinatorContext.assignSplits()
      -> assignmentTracker.recordSplitAssignment(assignment)
      -> gateway.sendEvent(new AddSplitEvent<>(splits, splitSerializer))
        -> SourceOperator.handleOperatorEvent(AddSplitEvent)
          -> sourceReader.addSplits(newSplits)
```

---

## 8. Checkpoint Flow -- Component Collaboration

The checkpoint process involves coordinated snapshots of both the enumerator (on JM) and readers (on TMs).

### Detailed Checkpoint Sequence

```
Phase 1: Trigger
===========================================================================
  CheckpointCoordinator (Flink Runtime)
    |
    |--- checkpointCoordinator(checkpointId, resultFuture)
    |      |
    |      +-> SourceCoordinator.checkpointCoordinator()
    |          |
    |          +-> runInEventLoop:
    |              |
    |              +-> context.onCheckpoint(checkpointId)
    |              |     // Records checkpoint boundary in SplitAssignmentTracker
    |              |     // Any splits assigned after this point are "uncheckpointed"
    |              |
    |              +-> enumerator.snapshotState(checkpointId)
    |              |     // Returns EnumChkT (e.g., list of unassigned splits)
    |              |
    |              +-> enumCheckpointSerializer.serialize(checkpoint)
    |              |     // Serializes to byte[]
    |              |
    |              +-> resultFuture.complete(bytes)
    |
    |--- Checkpoint barrier flows through network to SourceOperator
           |
           +-> SourceOperator.snapshotState(context)
               |
               +-> sourceReader.snapshotState(checkpointId)
               |     // SourceReaderBase: converts all splitStates to immutable splits
               |     // Each split contains the CURRENT reading position
               |     splitStates.forEach((id, ctx) -> splits.add(toSplitType(id, ctx.state)))
               |
               +-> readerState.update(splits)
                     // Stores serialized splits in ListState<byte[]>

Phase 2: Completion
===========================================================================
  CheckpointCoordinator
    |
    +-> SourceCoordinator.notifyCheckpointComplete(checkpointId)
    |     +-> runInEventLoop:
    |         +-> context.onCheckpointComplete(checkpointId)
    |         |     // Clears assignment tracking for this checkpoint
    |         |     // Splits assigned before this checkpoint are now "safe"
    |         +-> enumerator.notifyCheckpointComplete(checkpointId)
    |               // User code: e.g., commit Kafka offsets
    |
    +-> SourceOperator.notifyCheckpointComplete(checkpointId)
          +-> sourceReader.notifyCheckpointComplete(checkpointId)
                // User code: e.g., cleanup temporary state
```

### Assignment Tracking for Recovery

The `SplitAssignmentTracker` is crucial for correct recovery. It maintains:

```
  Checkpoint ID    |  Assigned Splits per Subtask
  ================+================================
  CP-10           |  subtask-0: [split-A, split-B]
                  |  subtask-1: [split-C]
  ----------------+--------------------------------
  CP-11 (pending) |  subtask-0: [split-D]  (assigned after CP-10)
                  |  subtask-1: [split-E]
```

If subtask-0 fails and recovers to CP-10:
- Splits A and B are in the checkpoint state (reader state)
- Split D was assigned after CP-10 but not yet checkpointed
- The coordinator calls `getAndRemoveUncheckpointedAssignment(subtask=0, checkpointId=10)` which returns `[split-D]`
- Then `enumerator.addSplitsBack([split-D], subtask=0)` is called to return split-D for reassignment

---

## 9. Batch-Stream Unification Mechanism

FLIP-27 achieves batch-stream unification through several coordinated mechanisms:

### 9.1 Mode Selection

```java
// In Source implementation:
@Override
public Boundedness getBoundedness() {
    // Kafka: can be either based on configuration
    if (stopOffsets != null) {
        return Boundedness.BOUNDED;
    }
    return Boundedness.CONTINUOUS_UNBOUNDED;
}
```

### 9.2 Runtime Behavior Differences

| Aspect | BOUNDED | CONTINUOUS_UNBOUNDED |
|--------|---------|---------------------|
| Watermark generation | `NoOpTimestampsAndWatermarks` (skip) | `ProgressiveTimestampsAndWatermarks` (active) |
| Checkpointing | Optional | Required for fault tolerance |
| Split discovery | Usually one-shot enumeration | Continuous periodic discovery |
| `END_OF_INPUT` handling | Expected normal termination | Unexpected (usually means error) |
| `emitProgressiveWatermarks` | `false` | `true` |

### 9.3 SourceOperator Watermark Mode Selection

```java
// From SourceOperator.open():
if (emitProgressiveWatermarks) {
    eventTimeLogic = TimestampsAndWatermarks.createProgressiveEventTimeLogic(
            watermarkStrategy, sourceMetricGroup,
            getProcessingTimeService(),
            getExecutionConfig().getAutoWatermarkInterval());
} else {
    eventTimeLogic = TimestampsAndWatermarks.createNoOpEventTimeLogic(
            watermarkStrategy, sourceMetricGroup);
}
```

### 9.4 How Bounded Sources Terminate

For bounded sources, the termination chain is:

```
1. SplitEnumerator discovers all splits (e.g., lists all files)
2. Assigns all splits to readers
3. Calls context.signalNoMoreSplits(subtask) for each subtask
   -> Sends NoMoreSplitsEvent -> SourceReader.notifyNoMoreSplits()
   -> Sets noMoreSplitsAssignment = true in SourceReaderBase
4. Each split finishes reading -> SplitReader returns finishedSplits()
5. SourceReaderBase: all fetchers idle + no more splits + queue empty
   -> Returns InputStatus.END_OF_INPUT
6. SourceOperator converts to DataInputStatus.END_OF_DATA
   -> Eventually DATA_FINISHED
```

---

## 10. Dynamic Split Discovery Mechanism

Dynamic split discovery is essential for streaming sources that need to detect new partitions, new files in a directory, etc.

### 10.1 Implementation via callAsync

The primary mechanism uses `SplitEnumeratorContext.callAsync()` with periodic scheduling:

```java
// Example: Periodic partition discovery in a Kafka-like source
public class KafkaEnumerator implements SplitEnumerator<KafkaSplit, EnumState> {
    @Override
    public void start() {
        // One-time initial discovery
        context.callAsync(this::discoverPartitions, this::handleDiscoveryResult);

        // Periodic discovery every 30 seconds
        context.callAsync(
                this::discoverPartitions,
                this::handleDiscoveryResult,
                0,          // initial delay
                30000       // period in ms
        );
    }

    private Set<TopicPartition> discoverPartitions() {
        // This runs on WORKER thread (can do blocking I/O)
        return kafkaAdmin.listPartitions(topics);
    }

    private void handleDiscoveryResult(Set<TopicPartition> partitions, Throwable error) {
        // This runs on COORDINATOR thread (safe to modify state)
        if (error != null) { ... }
        Set<TopicPartition> newPartitions = partitions - alreadyDiscovered;
        // Create splits for new partitions and assign them
        context.assignSplits(createAssignment(newPartitions));
    }
}
```

### 10.2 Thread Safety Guarantees

From the source code comments on `callAsync()`:

> "When this method is invoked multiple times, The Callables may be executed in a thread pool concurrently."

> "It is important to make sure that the callable does not modify any shared state, especially the states that will be a part of the SplitEnumerator.snapshotState(long)."

The execution model:
```
Worker Pool (concurrent)        Coordinator Thread (serial)
========================       ==========================
callable1() --result1--->     handler1(result1, null)
callable2() --result2--->     handler2(result2, null)
callable3() --error3---->     handler3(null, error3)
```

### 10.3 Pull vs Push Assignment Models

The framework supports both models:

**Pull Model** (reader-initiated):
```
Reader.start() -> context.sendSplitRequest()
  -> Enumerator.handleSplitRequest(subtaskId, hostname)
    -> context.assignSplit(split, subtaskId)
```

**Push Model** (enumerator-initiated):
```
Enumerator.start() -> discovers splits
  -> context.assignSplits(new SplitsAssignment(splits, subtaskIds))
```

Most practical sources use a hybrid: the enumerator proactively assigns splits when they are discovered, and readers request more when they finish their current splits.

---

## 11. Watermark Processing Mechanism

### 11.1 Per-Split Watermark Tracking

The `ReaderOutput` supports creating per-split outputs:

```java
// In SourceOperator.handleAddSplitsEvent():
private void createOutputForSplits(List<SplitT> newSplits) {
    for (SplitT split : newSplits) {
        currentMainOutput.createOutputForSplit(split.splitId());
    }
}
```

Each per-split output runs its own watermark generator (from `WatermarkStrategy`). The `TimestampsAndWatermarks.WatermarkUpdateListener` receives callbacks:

```java
// In SourceOperator:
@Override
public void updateCurrentSplitWatermark(String splitId, long watermark) {
    splitCurrentWatermarks.put(splitId, watermark);
    if (numSplits > 1 && watermark > currentMaxDesiredWatermark
            && !currentlyPausedSplits.contains(splitId)) {
        pauseOrResumeSplits(Collections.singletonList(splitId), Collections.emptyList());
        currentlyPausedSplits.add(splitId);
    }
}

@Override
public void updateCurrentEffectiveWatermark(long watermark) {
    latestWatermark = watermark;
    checkWatermarkAlignment();
}
```

### 11.2 Cross-Source Watermark Alignment

The alignment mechanism spans multiple levels:

```
Level 1: Per-Split Alignment (within a single SourceOperator)
================================================================
  SourceOperator tracks per-split watermarks in splitCurrentWatermarks.
  When a split's watermark exceeds currentMaxDesiredWatermark:
    -> pauseOrResumeSplits() pauses that split
  When global watermark catches up:
    -> checkSplitWatermarkAlignment() resumes paused splits

Level 2: Per-Subtask Alignment (across subtasks of same source)
================================================================
  SourceOperator periodically reports its latestWatermark to coordinator:
    -> emitLatestWatermark() sends ReportedWatermarkEvent
  SourceCoordinator aggregates across subtasks:
    -> WatermarkAggregator<Integer> combinedWatermark
  Computes maxAllowedWatermark = globalMinWatermark + maxDrift

Level 3: Cross-Source Alignment (across different sources)
================================================================
  SourceCoordinator publishes combined watermark to CoordinatorStore:
    -> coordinatorStore.computeIfPresent(watermarkGroup, aggregate)
  WatermarkAggregator<String> per watermark group aggregates across sources
  announceCombinedWatermark() distributes maxAllowedWatermark to all subtasks:
    -> WatermarkAlignmentEvent(maxAllowedWatermark) to each subtask
```

### 11.3 Alignment Event Flow

```
SourceOperator                 SourceCoordinator              CoordinatorStore
     |                              |                              |
     |-- ReportedWatermarkEvent --->|                              |
     |   (latestWatermark)          |                              |
     |                              |-- aggregate(subtask, wm) --->|
     |                              |                              |
     |                        [periodic task]                      |
     |                              |                              |
     |                              |<-- getAggregatedWatermark() -|
     |                              |                              |
     |                              |-- compute(group, aggregate)->|
     |                              |                              |
     |                        announceCombinedWatermark()          |
     |                              |                              |
     |                  globalWm = coordinatorStore.apply(group)   |
     |                  maxAllowed = globalWm + maxDrift            |
     |                              |                              |
     |<- WatermarkAlignmentEvent ---|                              |
     |   (maxAllowedWatermark)      |                              |
     |                              |                              |
   checkWatermarkAlignment()        |                              |
     |                              |                              |
   if (latestWatermark > maxAllowed):                              |
     operatingMode = WAITING_FOR_ALIGNMENT                         |
     (source pauses reading)                                       |
```

### 11.4 The shouldWaitForAlignment() Logic

```java
private boolean shouldWaitForAlignment() {
    return currentMaxDesiredWatermark < latestWatermark;
}

private void checkWatermarkAlignment() {
    if (operatingMode == OperatingMode.READING) {
        if (shouldWaitForAlignment()) {
            operatingMode = OperatingMode.WAITING_FOR_ALIGNMENT;
            waitingForAlignmentFuture = new CompletableFuture<>();
        }
    } else if (operatingMode == OperatingMode.WAITING_FOR_ALIGNMENT) {
        if (!shouldWaitForAlignment()) {
            operatingMode = OperatingMode.READING;
            waitingForAlignmentFuture.complete(null);
        }
    }
}
```

---

## 12. Fault Tolerance and Recovery Mechanism

### 12.1 Recovery Flow Overview

```
Subtask Failure
     |
     v
SourceCoordinator.executionAttemptFailed(subtaskId, attemptNumber)
     |
     +-> context.unregisterSourceReader(subtaskId, attemptNumber)
     +-> context.attemptFailed(subtaskId, attemptNumber)
          (removes subtask gateway)
     |
     v
SourceCoordinator.subtaskReset(subtaskId, checkpointId)
     |
     +-> context.subtaskReset(subtaskId)
     |     - Clear subtask gateway
     |     - Remove from registeredReaders
     |     - Reset subtaskHasNoMoreSplits[subtaskId] = false
     |
     +-> splitsToAddBack = context.getAndRemoveUncheckpointedAssignment(subtaskId, checkpointId)
     |     // Returns splits assigned AFTER the checkpoint being restored to
     |
     +-> enumerator.addSplitsBack(splitsToAddBack, subtaskId)
           // Enumerator can reassign these to available readers

New Task Starts
     |
     v
SourceOperator.initializeState(context)
     |
     +-> readerState = new SimpleVersionedListState<>(rawState, splitSerializer)
           // Deserializes split state from checkpoint
     |
     v
SourceOperator.open()
     |
     +-> splits = readerState.get()
     +-> sourceReader.addSplits(splits)    // Restore splits with checkpointed positions
     +-> registerReader()                   // Re-register with coordinator
     +-> sourceReader.start()               // Resume reading
```

### 12.2 Split Assignment Safety

The `SplitAssignmentTracker` ensures no splits are lost during failure:

```
Timeline:
  [CP-10 taken] --- [Split-D assigned] --- [CP-11 taken] --- [Subtask fails]
                                                                  |
  Recovery to CP-11:                                              |
    - Splits in CP-11 reader state: [A, B, D with current offsets]
    - Uncheckpointed after CP-11: [] (nothing)
    - Reader restores from state

  Recovery to CP-10:                                              |
    - Splits in CP-10 reader state: [A, B with older offsets]
    - Uncheckpointed after CP-10: [D] (assigned but not checkpointed)
    - Reader restores from state
    - Split D returned to enumerator via addSplitsBack()
    - Enumerator reassigns Split D
```

### 12.3 Coordinator State Recovery

```java
// SourceCoordinator.resetToCheckpoint():
@Override
public void resetToCheckpoint(final long checkpointId, @Nullable final byte[] checkpointData) {
    checkState(!started, "The coordinator can only be reset if it was not yet started");

    if (checkpointData == null) {
        return;  // No previous checkpoint; fresh start
    }

    // Deserialize enumerator checkpoint state
    final EnumChkT enumeratorCheckpoint = deserializeCheckpoint(checkpointData);
    // Create enumerator from checkpoint (not from scratch)
    enumerator = source.restoreEnumerator(context, enumeratorCheckpoint);
    // start() will be called later, which calls enumerator.start()
}
```

---

## 13. Thread Model Deep Dive

### 13.1 Complete Thread Architecture

```
================================================================
JobManager Process
================================================================

  [Coordinator Thread]  (ScheduledExecutorService, single thread)
    |
    +-- All state mutations for SplitEnumerator
    +-- handleEventFromOperator() dispatching
    +-- checkpointCoordinator() / notifyCheckpointComplete()
    +-- Periodic watermark alignment announcement
    +-- Handler callbacks from callAsync()
    |
  [Worker Thread Pool]  (ScheduledExecutorService, configurable threads)
    |
    +-- callAsync() callables (blocking I/O allowed)
    +-- e.g., Kafka admin.listPartitions(), HDFS.listFiles()

================================================================
TaskManager Process (per parallel subtask)
================================================================

  [Mailbox Thread]  (Task Thread, runs the operator chain)
    |
    +-- SourceOperator.emitNext()
    |     +-- sourceReader.pollNext() (non-blocking!)
    |     +-- RecordEmitter.emitRecord()
    |     +-- SourceOutput.collect()
    +-- SourceOperator.snapshotState()
    +-- SourceOperator.handleOperatorEvent()
    +-- Watermark emission
    +-- Checkpoint barrier processing
    |
  [I/O Thread(s)]  (Managed by SplitFetcherManager)
    |
    +-- SplitFetcher.run()
    |     +-- FetchTask: SplitReader.fetch() (blocking allowed!)
    |     +-- AddSplitsTask: SplitReader.handleSplitsChanges()
    |     +-- PauseOrResumeSplitsTask: SplitReader.pauseOrResumeSplits()
    |
  [Hand-Over Queue]  (FutureCompletingBlockingQueue)
    |
    +-- Producer: I/O thread puts RecordsWithSplitIds
    +-- Consumer: Mailbox thread polls RecordsWithSplitIds
    +-- CompletableFuture for availability notification
```

### 13.2 Thread Safety Analysis

| Component | Thread Context | Safety Mechanism |
|-----------|----------------|-----------------|
| `SplitEnumerator` methods | Coordinator thread only | Event-loop serialization |
| `SplitEnumerator.snapshotState()` | Coordinator thread | Called within runInEventLoop |
| `SourceReader.pollNext()` | Mailbox thread only | Single-threaded mailbox |
| `SourceReader.addSplits()` | Mailbox thread (via event) | Operator event dispatched to mailbox |
| `SplitReader.fetch()` | I/O thread | Isolated; communicates via queue |
| `FutureCompletingBlockingQueue` | Both threads | Thread-safe blocking queue with future |
| `SourceReaderBase.splitStates` | Mailbox thread only | Only accessed from pollNext/snapshotState |
| `SplitFetcher.taskQueue` | Multiple (via lock) | `ReentrantLock` with conditions |
| `SourceCoordinatorContext.callInCoordinatorThread()` | Any thread | Submits to coordinatorExecutor |

### 13.3 The FutureCompletingBlockingQueue

This custom data structure is the key to non-blocking integration with the mailbox:

```
I/O Thread:                           Mailbox Thread:
  queue.put(records)                    future = queue.getAvailabilityFuture()
    -> if was empty:                      -> returns CompletableFuture
       future.complete(null)                (completes when data arrives)
                                          -> mailbox schedules pollNext()
                                        records = queue.poll()
                                          -> returns data or null
```

---

## 14. Design Patterns Analysis

### 14.1 Abstract Factory Pattern

**Location**: `Source` interface
**Implementation**: `Source` creates `SplitEnumerator`, `SourceReader`, and serializers.

```java
Source<T, SplitT, EnumChkT>
  |-- createEnumerator(context) -> SplitEnumerator
  |-- restoreEnumerator(context, checkpoint) -> SplitEnumerator
  |-- createReader(readerContext) -> SourceReader (inherited from SourceReaderFactory)
  |-- getSplitSerializer() -> SimpleVersionedSerializer<SplitT>
  |-- getEnumeratorCheckpointSerializer() -> SimpleVersionedSerializer<EnumChkT>
```

**Design intent**: Encapsulate the creation of a family of related objects (enumerator, reader, serializers) that must be compatible.

### 14.2 Template Method Pattern

**Location**: `SourceReaderBase`
**Implementation**: `pollNext()`, `snapshotState()`, `addSplits()` define the algorithm skeleton. Subclasses implement `initializedState()`, `toSplitType()`, `onSplitFinished()`.

### 14.3 Strategy Pattern

**Location**: `SplitFetcherManager.addSplits()` is abstract, with `SingleThreadFetcherManager` providing the single-thread-multiplex strategy.
**Extension**: Custom implementations can provide one-thread-per-split or pool-based strategies.

### 14.4 Observer/Listener Pattern

**Location**: `TimestampsAndWatermarks.WatermarkUpdateListener`
**Implementation**: `SourceOperator` implements the listener, receives watermark updates from per-split outputs.

### 14.5 Producer-Consumer Pattern

**Location**: `FutureCompletingBlockingQueue` between `SplitFetcher` (producer) and `SourceReaderBase.pollNext()` (consumer).

### 14.6 Command Pattern

**Location**: `SplitFetcherTask` hierarchy (`FetchTask`, `AddSplitsTask`, `RemoveSplitsTask`, `PauseOrResumeSplitsTask`).
**Implementation**: Tasks are enqueued and executed by `SplitFetcher`, decoupling request from execution.

### 14.7 Event-Driven Architecture

**Location**: The entire communication between `SourceCoordinator` and `SourceOperator`.
**Implementation**: `OperatorEvent` subclasses carry commands and data across the JM-TM boundary.

---

## 15. Engineering Highlights and Best Practices

### 15.1 Performance Optimization: Optimistic Availability Reporting

In `SourceReaderBase.pollNext()`, after emitting a record, the code returns `MORE_AVAILABLE` without checking. The source comment explains the trade-off: "We always emit MORE_AVAILABLE here... this saves us doing checks for every record."

This is an example of **amortized cost reduction**: the occasional false positive (one extra `pollNext()` call) is O(1) and much cheaper than checking the queue on every record in the hot path.

### 15.2 Performance Optimization: Batch Emission Loop

In `SourceOperator.emitNext()`:
```java
do {
    status = sourceReader.pollNext(currentMainOutput);
} while (status == InputStatus.MORE_AVAILABLE
        && canEmitBatchOfRecords.check()
        && !shouldWaitForAlignment());
```

This tight loop avoids the overhead of returning to the mailbox between records when data is flowing continuously.

### 15.3 Lazy Initialization

`SourceOperator.initReader()` uses lazy initialization because metric groups are not available at construction time. The code comment explicitly documents this technical debt: "This code should move to the constructor once the metric groups are available at task setup time."

### 15.4 Object Recycling

`RecordsWithSplitIds.recycle()` enables object reuse for high-throughput scenarios, reducing GC pressure.

### 15.5 Thread-Safe Coordinator Access

`SourceCoordinatorContext.callInCoordinatorThread()` provides a safe way to ensure code runs on the coordinator thread regardless of the calling context:

```java
private <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
    if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
        return coordinatorExecutor.submit(callable).get();  // Block and wait
    }
    return callable.call();  // Already on coordinator thread
}
```

### 15.6 Graceful Degradation for Watermark Alignment

The `allowUnalignedSourceSplits` configuration provides a graceful degradation path when a source does not implement `pauseOrResumeSplits()`:

```java
private void pauseOrResumeSplits(...) {
    try {
        sourceReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
    } catch (UnsupportedOperationException e) {
        if (!allowUnalignedSourceSplits) {
            throw e;  // Fail fast by default
        }
        // Silently ignore if explicitly allowed
    }
}
```

---

## 16. Improvement Suggestions

### 16.1 Eager Reader Initialization

**Problem**: `SourceOperator.initReader()` exists because metric groups are lazily initialized. This causes the reader to be created later than ideal.

**Impact**: Adds complexity and an extra initialization path.

**Recommendation**: Migrate to the `StreamOperatorV2` interface which supports eager initialization, as noted in the source comment: "when we this one is migrated to the 'eager initialization' operator (StreamOperatorV2)..."

### 16.2 RecordEmitter Error Handling

**Problem**: The `RecordEmitter.emitRecord()` Javadoc is incomplete: "The implementation needs to make sure it reades" (truncated sentence in source code).

**Impact**: Incomplete documentation for an important contract.

**Recommendation**: Complete the documentation to specify exactly-once emission semantics when interrupted.

### 16.3 OperatingMode Complexity

**Problem**: The `SourceOperator` has 6 operating modes with a complex state machine. The `emitNext()` method splits into hot-path and non-hot-path methods for performance.

**Impact**: Difficult to reason about all possible state transitions.

**Recommendation**: Consider extracting the state machine into a separate class with explicit transition validation.

### 16.4 Watermark Alignment Thread Safety

**Problem**: `SourceOperator.splitCurrentWatermarks` is a regular `HashMap` accessed from the mailbox thread, but watermark update callbacks may come from timer threads.

**Impact**: Potential race condition if timer callbacks are not dispatched through the mailbox.

**Recommendation**: Verify that all `WatermarkUpdateListener` callbacks are guaranteed to execute on the mailbox thread, or use a concurrent map.

---

## 17. Summary and Key Insights

### Core Design Philosophy

FLIP-27 embodies the principle of **separation of concerns at the architectural level**:

1. **Control Plane vs Data Plane** -- The SplitEnumerator (control) and SourceReader (data) are physically separated across JM and TM, communicating via events. This mirrors patterns from distributed systems like Kubernetes (controller vs kubelet) and HDFS (NameNode vs DataNode).

2. **Non-blocking execution model** -- By making `pollNext()` non-blocking and using `CompletableFuture` for availability, the source integrates seamlessly with Flink's mailbox thread model without blocking checkpoint barriers or watermark processing.

3. **Framework-managed complexity** -- Threading, checkpoint coordination, watermark alignment, and fault recovery are handled by the framework (`SourceReaderBase`, `SourceCoordinator`, `SourceOperator`). Connector developers only implement the split-specific logic.

### Key Technical Highlights

| Aspect | Design Choice | Engineering Benefit |
|--------|---------------|-------------------|
| Thread model | Event loop (coordinator) + mailbox (reader) | No locks needed for state mutations |
| I/O isolation | Dedicated fetcher threads with hand-over queue | Blocking I/O does not block mailbox |
| Batch-stream unification | Single `Boundedness` enum | Same source code for both modes |
| Watermark alignment | Three-level hierarchy (split/subtask/source) | Fine-grained event time control |
| Fault tolerance | Assignment tracking + split-back protocol | No split loss during failures |
| Extensibility | `SourceEvent` custom protocol | Connector-specific communication |
| Serialization | `SimpleVersionedSerializer` everywhere | Forward/backward compatibility |

### Engineering Practice Insights

1. **The importance of clear interface boundaries** -- The 5 core interfaces (`Source`, `SplitEnumerator`, `SourceReader`, `SourceSplit`, `Boundedness`) form a minimal yet complete contract. Every method has a clear purpose and thread context.

2. **Layered abstraction** -- Three layers of abstraction (core interfaces -> base implementations -> runtime coordination) allow different connector developers to plug in at the appropriate level.

3. **Performance-aware design** -- The hot path (`emitNext` -> `pollNext`) is carefully optimized with batch emission loops, optimistic availability reporting, and fast-path short-circuits.

4. **Test-friendly architecture** -- The separation between interfaces and implementations, combined with context injection, makes it straightforward to test each component in isolation.

---

## Appendix: Source File Index

| File | Module | Role |
|------|--------|------|
| `Source.java` | flink-core | Top-level factory interface |
| `SourceSplit.java` | flink-core | Split identity abstraction |
| `Boundedness.java` | flink-core | Batch/stream mode enum |
| `SplitEnumerator.java` | flink-core | Split discovery and assignment interface |
| `SourceReader.java` | flink-core | Data reading interface |
| `SourceEvent.java` | flink-core | Custom event marker interface |
| `SplitEnumeratorContext.java` | flink-core | Enumerator runtime context |
| `SourceReaderContext.java` | flink-core | Reader runtime context |
| `ReaderOutput.java` | flink-core | Record emission with per-split support |
| `SourceReaderFactory.java` | flink-core | Reader factory (parent of Source) |
| `SourceReaderBase.java` | flink-connector-base | Abstract reader with thread hand-over |
| `SplitReader.java` | flink-connector-base | I/O layer interface |
| `RecordEmitter.java` | flink-connector-base | Record transformation interface |
| `SplitFetcherManager.java` | flink-connector-base | Thread management abstraction |
| `SplitFetcher.java` | flink-connector-base | I/O thread runnable with task queue |
| `SingleThreadFetcherManager.java` | flink-connector-base | Single-thread-multiplex manager |
| `SingleThreadMultiplexSourceReaderBase.java` | flink-connector-base | Convenience base class |
| `RecordsWithSplitIds.java` | flink-connector-base | Data transfer container |
| `SourceCoordinator.java` | flink-runtime | JM-side coordinator (OperatorCoordinator impl) |
| `SourceCoordinatorContext.java` | flink-runtime | Coordinator execution environment |
| `SourceOperator.java` | flink-streaming-java | TM-side operator (integrates with task runtime) |
| `TimestampsAndWatermarks.java` | flink-streaming-java | Watermark generation logic factory |

---

> This analysis is based on the Apache Flink release-1.18 branch source code. All code examples and descriptions are verified against the actual implementation.
