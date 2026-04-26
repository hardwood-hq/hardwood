# Design: Large-File Memory-Mapped I/O and Zero-Copy Decoding

**Status: Implemented**

## 1. Overview

This design addresses the critical limitation of memory-mapped Parquet files larger than 2 GB (`Integer.MAX_VALUE`), which previously failed due to 32-bit integer limits in Java's `MappedByteBuffer` API. 

Furthermore, this design leverages the Java 22 Foreign Function & Memory (FFM) API to not only resolve the mapping limit natively but also introduce a complete zero-copy decoding pipeline. This pipeline directly consumes compressed bytes from native memory into typed arrays without intermediate JVM heap copies, drastically improving CPU efficiency and throughput.

## 2. Target Java Version & Architecture

- **Base Implementation:** Java 21 (multi-segment fallback)
- **Primary Implementation:** Java 22+ multi-release (`java.lang.foreign` FFM API)

### Multi-Release Project Structure

```
core/src/main/java/dev/hardwood/internal/
├── reader/MappedInputFile.java         # Java 21 fallback (multi-segment)
├── reader/PageDecoder.java             # Java 21 fallback (byte[] heap copy)
└── encoding/PlainDecoder.java          # Java 21 base decoder

core/src/main/java22/dev/hardwood/internal/
├── reader/MappedInputFile.java         # Java 22 native MemorySegment
├── reader/PageDecoder.java             # Java 22 zero-copy routing
├── encoding/NativePlainDecoder.java    # Java 22 zero-copy decoder (PLAIN)
└── encoding/NativeBssDecoder.java      # Java 22 zero-copy decoder (BSS)
```

### Key Design Principles

1. **Zero-Copy Where Possible**: Keep bytes off the Java heap until they reach their final primitive arrays.
2. **Safe Fallbacks**: Always provide an uncompromised, correctness-first Java 21 implementation.
3. **Transparent Routing**: Users do not configure FFM manually; the parser detects capability and column types to route implicitly.
4. **Lifecycle Safety**: Use `Arena.ofAuto()` for mappings to ensure safety and allow the JVM garbage collector to manage native memory lifecycles identically to `MappedByteBuffer`.

---

## 3. Large File Support (> 2GB)

### 3.1 The Problem
`MappedByteBuffer.slice(int position, int length)` takes `int` parameters. File offsets in Parquet chunks are `long`. For files > 2 GB, the `(int) offset` cast silently overflows, leading to catastrophic data corruption or unexpected exceptions.

### 3.2 Java 22+ Solution (Primary Path)
`java.lang.foreign.FileChannel.map(MapMode, long, long, Arena)` natively accepts `long` parameters and returns a `MemorySegment`. `MemorySegment.asSlice(long, long)` also uses `long`, ensuring the entire I/O path is free of `int` bounds.

- **Zero Allocation:** Slicing a `MemorySegment` does not allocate memory on the heap.
- **Bonus Accessor:** Exposed `readRangeAsSegment(long, long)` returning a read-only `MemorySegment`.

### 3.3 Java 21 Solution (Fallback Path)
`FileChannel.map` uses `long` for size, but returns an `int`-bounded `MappedByteBuffer`. The file is logically split into segments of at most `Integer.MAX_VALUE` bytes, cached in a `volatile ByteBuffer[]` array.

`readRange(long, int)` locates the correct segment using integer division on the absolute offset. When a read spans a segment boundary (exceedingly rare, as chunks are bounded by `DEFAULT_CHUNK_SIZE` of ~128MB), the data is safely assembled into an intermediate heap buffer.

---

## 4. Tier 3: Zero-Copy Native Decoding Pipeline

### 4.1 The Bottleneck
Even when reading from a memory-mapped file natively, decompression using `LibdeflateDecompressor` writes to an off-heap native buffer. Historically, this data was then immediately copied into a Java `byte[]` to be fed into value decoders (like `PlainDecoder`). For numeric columns, this native-to-heap copy generated massive JVM memory bandwidth pressure.

### 4.2 The FFM Fast Path
On Java 22+, if a column is compressed via GZIP and encoded as `PLAIN` or `BYTE_STREAM_SPLIT` for a numeric type, the copy is completely eliminated.

**Pipeline Flow:**
1. `LibdeflateDecompressor.decompressToSegment(MemorySegment, long, int)` returns a read-only slice of the thread-local native output buffer.
2. `PageDecoder` (java22 override) routes `DATA_PAGE` and `DATA_PAGE_V2` pages directly to native decoders.
3. `NativePlainDecoder` and `NativeBssDecoder` access the native memory directly via `MemorySegment.get(ValueLayout, offset)` or bulk `ByteBuffer.asLongBuffer().get()`.

**Architectural Change:**
```
Old path (every GZIP numeric page):
  libdeflate native output
    → MemorySegment.copy(native, JAVA_BYTE, 0, byte[], 0, size)  [ELIMINATED]
    → PlainDecoder(byte[])
    → long[]/double[]/int[]/float[]

New path (Java 22 + GZIP + PLAIN/BSS + numeric):
  libdeflate native output
    → MemorySegment.asSlice(0, size).asReadOnly()
    → NativePlainDecoder(MemorySegment)
    → long[]/double[]/int[]/float[]
```

---

## 5. Implementation Details & Refinements

### Enhancements & Safety Hardening

| Component | Improvement |
|-----------|-------------|
| **GC Pressure** | Mappings use `Arena.ofAuto()`. The file channel is eagerly closed via try-with-resources inside `open()`, and the mapping reference is nulled in `close()` to allow prompt collection. |
| **Concurrency** | `open()` is properly `synchronized` in both implementations to prevent mapping thrash. Safe publication of the buffer array via `volatile ByteBuffer[]` in Java 21. |
| **Immutability** | All returned buffers and segments strictly enforce read-only access. Java 21 buffers are wrapped via `.asReadOnlyBuffer()` and Java 22 segments use `.asReadOnly()`. |
| **Visibility** | Emits standard JFR `FileMappingEvent`. Added `nativeFastPath` boolean to `PageDecodedEvent` to measure the hit-rate of the zero-copy pipeline. |
| **Diagnostics** | Emits robust `IOException` messages with byte counts and file names on empty files, out-of-bounds access, or fallback index arithmetic overflow (`Math.toIntExact`). |

---

## 6. Verification Commands

### Build & Test

```bash
# Full clean verification across all versions
./mvnw clean verify

# Test specific module
./mvnw test -pl core
```

### JFR Observability

When evaluating the performance characteristics of the zero-copy pipeline, record a JFR trace and query the `PageDecodedEvent`:

```bash
jfr print --events dev.hardwood.PageDecoded recording.jfr
```

You can track the `nativeFastPath` metric to ensure `true` is reported for compatible INT32/INT64/DOUBLE/FLOAT columns, verifying the elimination of the native-to-heap copy penalty.

---

## 7. Summary

The complete large-file integration and zero-copy pipeline provides a massive performance leap for modern Java infrastructure:

1. **Breaks the 2GB Barrier**: Safely maps terabyte-scale analytics files.
2. **Eliminates Bandwidth Bottlenecks**: FFM avoids the `~uncompressedSize` intermediate heap allocation entirely for supported numerics.
3. **No Breaking API Changes**: `InputFile` interfaces remain unchanged.
4. **Seamless Degradation**: Users on Java 21 receive robust multi-segment mapping without any manual configuration or pipeline failures.
