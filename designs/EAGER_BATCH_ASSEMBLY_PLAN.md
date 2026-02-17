# Eager Batch Assembly Design

## Problem Statement

Batch assembly in row readers was limited by the number of projected columns:

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Page Decoding Phase                          │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                │
│  │ PageCursor  │   │ PageCursor  │   │ PageCursor  │                │
│  │  Column 0   │   │  Column 1   │   │  Column 2   │                │
│  │ (prefetch)  │   │ (prefetch)  │   │ (prefetch)  │                │
│  └─────────────┘   └─────────────┘   └─────────────┘                │
│        ↓                 ↓                 ↓                         │
│   ForkJoinPool workers decode pages (parallel, all cores)            │
└──────────────────────────────────────────────────────────────────────┘
                              ↓
                    Pages wait in queues
                              ↓
┌──────────────────────────────────────────────────────────────────────┐
│                      Batch Assembly Phase                            │
│                                                                      │
│   loadNextBatch() {                                                  │
│     // Only N threads (N = column count)                             │
│     futures[0] = supplyAsync(() -> iterator[0].readBatch(size));    │
│     futures[1] = supplyAsync(() -> iterator[1].readBatch(size));    │
│     futures[2] = supplyAsync(() -> iterator[2].readBatch(size));    │
│     CompletableFuture.allOf(futures).join();  // BLOCKED             │
│   }                                                                  │
└──────────────────────────────────────────────────────────────────────┘
                              ↓
                          Consumer
```

With 3 columns and 16 cores:
- Page decoding uses all 16 cores (via PageCursor prefetch queue)
- Batch assembly uses only 3 cores
- Consumer thread is blocked during assembly

## Solution: Per-Column Eager Assembly

Move batch assembly into the decoding pipeline. Each column independently assembles pages into batches as they are decoded. The consumer finds batches pre-assembled.

## Architecture

```
        Column 0                    Column 1                    Column 2
        ────────                    ────────                    ────────
     ┌─────────────┐             ┌─────────────┐             ┌─────────────┐
     │ PageCursor  │             │ PageCursor  │             │ PageCursor  │
     │  (decode)   │             │  (decode)   │             │  (decode)   │
     └──────┬──────┘             └──────┬──────┘             └──────┬──────┘
            │                           │                           │
            │ Virtual Thread            │                           │
            │ (assembly)                │                           │
            ▼                           ▼                           ▼
     ┌─────────────┐             ┌─────────────┐             ┌─────────────┐
     │ColumnAssem- │             │ColumnAssem- │             │ColumnAssem- │
     │ blyBuffer   │             │ blyBuffer   │             │ blyBuffer   │
     │             │             │             │             │             │
     │ BlockingQ:  │             │ BlockingQ:  │             │ BlockingQ:  │
     │ [Batch][.] │             │ [Batch][.] │             │ [Batch][.] │
     │             │             │             │             │             │
     │ ArrayPool:  │             │ ArrayPool:  │             │ ArrayPool:  │
     │ [arr][arr]  │             │ [arr][arr]  │             │ [arr][arr]  │
     └──────┬──────┘             └──────┬──────┘             └──────┬──────┘
            │                           │                           │
            └───────────────┬───────────┴───────────────────────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │   Consumer    │
                    │ (await ready  │
                    │   batches)    │
                    └───────────────┘
```

## Implementation

### ColumnAssemblyBuffer

Uses `BlockingQueue` for producer-consumer coordination:

```java
public class ColumnAssemblyBuffer {

    private static final int QUEUE_CAPACITY = 2;

    // Blocking queue of ready batches
    private final BlockingQueue<TypedColumnData> readyBatches;

    // Pool of reusable arrays (producer takes, consumer returns)
    private final BlockingQueue<Object> arrayPool;

    // Working state for current batch being filled
    private Object currentValues;
    private BitSet currentNulls;  // Built incrementally during copyPageData
    private int rowsInCurrentBatch = 0;

    public ColumnAssemblyBuffer(ColumnSchema column, int batchCapacity) {
        this.readyBatches = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        // Pool: QUEUE_CAPACITY + 1 arrays (one being filled)
        this.arrayPool = new ArrayBlockingQueue<>(QUEUE_CAPACITY + 2);
        for (int i = 0; i < QUEUE_CAPACITY + 1; i++) {
            arrayPool.add(allocateArray(physicalType, batchCapacity));
        }

        this.currentValues = arrayPool.poll();
        this.currentNulls = maxDefinitionLevel > 0 ? new BitSet(batchCapacity) : null;
    }

    // Producer: append page data to current batch
    public void appendPage(Page page) {
        while (pagePosition < pageSize) {
            int toCopy = Math.min(spaceInBatch, pageSize - pagePosition);

            // Copy values and mark nulls incrementally
            copyPageData(page, pagePosition, rowsInCurrentBatch, toCopy);

            rowsInCurrentBatch += toCopy;
            pagePosition += toCopy;

            if (rowsInCurrentBatch >= batchCapacity) {
                publishCurrentBatch();
            }
        }
    }

    private void publishCurrentBatch() {
        // Clone nulls (since we reuse currentNulls)
        BitSet nulls = (currentNulls != null && !currentNulls.isEmpty())
                ? (BitSet) currentNulls.clone()
                : null;

        TypedColumnData data = createTypedColumnDataDirect(currentValues, recordCount, nulls);

        // Publish batch (blocks if queue full)
        readyBatches.put(data);

        // Get next array from pool (blocks if empty)
        currentValues = arrayPool.take();

        rowsInCurrentBatch = 0;
        if (currentNulls != null) currentNulls.clear();
    }

    // Consumer: get next batch, return previous array to pool
    public TypedColumnData awaitNextBatch() {
        // Return previous batch's array to pool
        if (previousBatch != null) {
            returnArrayToPool(previousBatch);
            previousBatch = null;
        }

        // Wait for next batch
        TypedColumnData data = readyBatches.poll(timeout);
        previousBatch = data;
        return data;
    }
}
```

### Key Design Decisions

1. **BlockingQueue synchronization**: Uses `ArrayBlockingQueue` for both ready batches and array pool. CPU-friendly blocking when waits are longer.

2. **Array pool for reuse**: Pre-allocated arrays cycle between producer and consumer:
   - Producer takes from pool, fills batch, publishes
   - Consumer gets batch, uses it, returns array to pool on next call
   - Avoids per-batch allocation after initialization

3. **Incremental null bitmap**: Nulls are marked during `copyPageData()` via `markNulls()`:
   ```java
   private void markNulls(int[] defLevels, int srcPos, int destPos, int length) {
       if (currentNulls != null && defLevels != null) {
           for (int i = 0; i < length; i++) {
               if (defLevels[srcPos + i] < maxDefinitionLevel) {
                   currentNulls.set(destPos + i);
               }
           }
       }
   }
   ```
   Null marking is spread across page copies rather than computed at batch publish time.

4. **BitSet cloning**: The `currentNulls` BitSet is cloned before publishing (since we reuse it). Empty BitSets are passed as null to avoid allocation.

5. **Virtual threads for assembly**: `PageCursor` starts a virtual thread that consumes decoded pages and calls `appendPage()`:
   ```java
   if (assemblyBuffer != null) {
       Thread.startVirtualThread(this::runAssemblyThread);
   }

   private void runAssemblyThread() {
       try {
           while (hasNext()) {
               Page page = nextPage();
               if (page != null) {
                   assemblyBuffer.appendPage(page);
               }
           }
       } finally {
           signalExhausted();
       }
   }
   ```
   Virtual threads are ideal because they block waiting for decoded pages (I/O-like wait).

### Files Modified

1. **`ColumnAssemblyBuffer.java`**
   - BlockingQueue-based producer/consumer
   - Array pool for reuse
   - Incremental null marking

2. **`PageCursor.java`**
   - Virtual thread runs assembly
   - Passes decoded pages to buffer

3. **`ColumnValueIterator.java`**
   - `readEagerBatch()` gets pre-assembled batches from buffer

4. **`SingleFileRowReader.java` / `MultiFileRowReader.java`**
   - Create `ColumnAssemblyBuffer` for flat schemas
   - Pass to `PageCursor` constructor

### Flat Schemas Only

Eager assembly is only enabled for flat schemas where one value = one row. Nested schemas use on-demand batch computation via `computeNestedBatch()`.

## Memory Layout

```
Per Column:
┌─────────────────────────────────────────────────────┐
│ readyBatches (BlockingQueue, capacity 2)            │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │ TypedColumn  │  │ TypedColumn  │                 │
│  │   Data #1    │  │   Data #2    │                 │
│  └──────────────┘  └──────────────┘                 │
├─────────────────────────────────────────────────────┤
│ arrayPool (BlockingQueue, capacity 4)               │
│  ┌────────┐  ┌────────┐  ┌────────┐                 │
│  │ long[] │  │ long[] │  │ long[] │  (recycled)     │
│  └────────┘  └────────┘  └────────┘                 │
├─────────────────────────────────────────────────────┤
│ currentValues: long[] (being filled)                │
│ currentNulls: BitSet (built incrementally)          │
└─────────────────────────────────────────────────────┘

Total arrays per column: QUEUE_CAPACITY + 1 = 3
Array size: batchCapacity elements (e.g., 8192 longs = 64KB)
```

## Data Flow

```
Decoder Thread              Assembly Thread              Consumer Thread
───────────────             ───────────────              ───────────────

decode page
    │
    ▼
prefetchQueue.add()
    │
    └──────────────────────► nextPage()
                                 │
                                 ▼
                            appendPage()
                                 │
                                 ├─► copyPageData()
                                 │   markNulls()
                                 │
                                 ▼
                            batch full?
                                 │
                                 ▼
                            readyBatches.put() ──────────► awaitNextBatch()
                                 │                              │
                                 ▼                              │
                            arrayPool.take() ◄─────────── returnArrayToPool()
                                 │                              │
                                 ▼                              ▼
                            fill next batch              use batch data
```

## Expected Performance

### Without Eager Assembly

```
Time ──────────────────────────────────────────────────────────────────▶

Batch 0:  │◀── Decode ──▶│◀── Assemble ──▶│◀── Consume ──▶│
Batch 1:                  │◀── Decode ──▶│◀── Assemble ──▶│◀── Consume ──▶│

Consumer blocked: ████████████████████████
                  (during assembly)
```

### With Eager Assembly

```
Time ──────────────────────────────────────────────────────────────────▶

Decode:   ████████████████████████████████████████████████████████████▶
Assemble: ════════════════════════════════════════════════════════════▶
          (pipelined with decoding)

Consume:       │◀─ Batch 0 ─▶│◀─ Batch 1 ─▶│◀─ Batch 2 ─▶│
               (batches ready immediately)

Consumer blocked: (minimal - only if decode/assemble slower than consume)
```

## Measured Performance

Throughput: ~155 million records/sec (651M rows across 119 files)

Benefits:
- Assembly pipelined with decoding
- Consumer finds batches pre-assembled
- CPU-friendly blocking via BlockingQueue
- No per-batch allocation after initialization

---

# Optional Future Improvements

## Nested Schema Eager Assembly

Currently, eager assembly only works for flat schemas. Extending it to nested schemas would require a different approach.

### Why It's Different

**Flat schemas (current implementation):**
- One value = one row
- Simple null handling (definition level 0 or max)
- No repetition levels
- Output: `FlatColumnData`

**Nested schemas:**
- Multiple values can belong to the same row (lists, maps, nested structs)
- Repetition levels indicate list/array boundaries
- Multi-level definition levels (e.g., a 3-level nested struct has def levels 0, 1, 2, or 3)
- Row boundaries determined by analyzing repetition levels
- Output: `NestedColumnData`

### Implementation Approach

A `NestedColumnAssemblyBuffer` would need to:

1. **Track repetition levels** to identify row boundaries
2. **Handle variable values per row** (lists can have 0 to N elements)
3. **Support multi-level definition levels** for nested null semantics
4. **Create `NestedColumnData`** instead of `FlatColumnData`

```java
class NestedColumnAssemblyBuffer {

    // Working arrays include repetition/definition levels
    private int[] currentRepLevels;
    private int[] currentDefLevels;
    private Object currentValues;

    // Row boundary tracking
    private int currentRowCount = 0;
    private int valuesInCurrentBatch = 0;

    void appendPage(Page page) {
        int[] repLevels = page.repetitionLevels();
        int[] defLevels = page.definitionLevels();

        for (int i = 0; i < page.size(); i++) {
            // rep_level == 0 indicates start of new row
            if (repLevels[i] == 0 && valuesInCurrentBatch > 0) {
                currentRowCount++;

                // Check if batch is full (by row count, not value count)
                if (currentRowCount >= batchCapacity) {
                    publishCurrentBatch();
                }
            }

            // Copy value and levels
            copyValue(page, i);
            currentRepLevels[valuesInCurrentBatch] = repLevels[i];
            currentDefLevels[valuesInCurrentBatch] = defLevels[i];
            valuesInCurrentBatch++;
        }
    }

    private TypedColumnData createNestedColumnData() {
        return new NestedColumnData(
            column,
            Arrays.copyOf(currentValues, valuesInCurrentBatch),
            Arrays.copyOf(currentRepLevels, valuesInCurrentBatch),
            Arrays.copyOf(currentDefLevels, valuesInCurrentBatch),
            currentRowCount
        );
    }
}
```

### Complexity Factors

1. **Variable batch sizes**: Batches are sized by row count, but the number of values per batch varies based on list lengths.

2. **Memory estimation**: Harder to pre-allocate arrays when values-per-row is unknown.

3. **Row boundary detection**: Must scan repetition levels to find row starts.

4. **Integration with existing NestedColumnData**: The assembly output must be compatible with the existing nested schema infrastructure.

### Priority

This is a **lower priority** optimization because:
- Flat schemas are the most common performance-critical use case (analytics workloads)
- Nested schema reading is typically I/O bound rather than CPU bound
- The complexity is significantly higher than flat schema assembly

Consider implementing this if profiling shows nested schema batch assembly as a bottleneck.
