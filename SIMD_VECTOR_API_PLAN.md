# Java Vector API (SIMD) Optimization Plan for Hardwood Parquet Parser

## Overview

Add Java Vector API optimizations to accelerate Parquet decoding operations. The implementation uses a multi-release JAR approach (similar to existing FFM/libdeflate integration) with runtime capability detection and scalar fallbacks.

## Target Java Version

- Base: Java 21 (scalar fallback)
- SIMD: Java 22+ multi-release (Vector API via `jdk.incubator.vector`)

> **Note:** The original plan targeted Java 25, but since the Vector API has been available as an incubator module since JDK 16, and the project already uses Java 22 for FFM support, we use Java 22 for consistency.

---

## 1. Architecture

### Strategy Pattern with Runtime Detection

```
core/src/main/java/dev/hardwood/hardwood/internal/encoding/simd/
├── VectorSupport.java        # Runtime detection (isVectorAvailable())
├── SimdOperations.java       # Interface for vectorized operations
└── ScalarOperations.java     # Scalar fallback implementation

core/src/main/java25/dev/hardwood/hardwood/internal/encoding/simd/
└── VectorOperations.java     # Vector API implementation (multi-release)
```

### Key Design Principles

1. **Modularity**: SIMD operations in separate interface, not inline in decoders
2. **Fallback**: Scalar path always available; auto-selected when batch size < threshold
3. **No Breaking Changes**: Public API unchanged; optimization is internal
4. **Testability**: Same tests validate both scalar and SIMD paths

---

## 2. Target Methods & Expected Speedup

| File | Method | Lines | SIMD Operation | Est. Speedup |
|------|--------|-------|----------------|--------------|
| `RleBitPackingHybridDecoder.java` | `countNonNulls()` | 215-232 | Vector compare + trueCount | 4-8x |
| `RleBitPackingHybridDecoder.java` | `decodeBitPacked()` width=1 | 280-294 | Bit expansion via broadcast+mask | 4-8x |
| `RleBitPackingHybridDecoder.java` | `decodeBitPacked()` width=2-8 | 296-329 | Vectorized shift+mask | 3-6x |
| `RleBitPackingHybridDecoder.java` | `applyDictionary()` | 135-213 | Vector gather | 3-6x |
| `ByteStreamSplitDecoder.java` | `gatherBytes()` + readXxx() | 68-167, 195-204 | Strided gather + reassemble | 4-8x |
| `ColumnValueIterator.java` | `markNulls()` | 124-133 | Vector compare + mask extraction | 2-4x |
| `DeltaBinaryPackedDecoder.java` | `unpackValue()` | 206-229 | Batched bit unpack + prefix sum | 2-4x |

---

## 3. Implementation Details

### 3.1 SimdOperations Interface

```java
public interface SimdOperations {
    // RleBitPackingHybridDecoder
    int countNonNulls(int[] defLevels, int maxDef);
    int unpackBitWidth1(byte[] data, int dataPos, int[] output, int outPos, int count);
    int unpackBitWidthN(byte[] data, int dataPos, int[] output, int outPos, int count, int bitWidth);
    void applyDictionaryLongs(long[] output, long[] dict, int[] indices, int count);
    void applyDictionaryDoubles(double[] output, double[] dict, int[] indices, int count);
    void applyDictionaryInts(int[] output, int[] dict, int[] indices, int count);
    void applyDictionaryFloats(float[] output, float[] dict, int[] indices, int count);

    // ByteStreamSplitDecoder
    void gatherDoubles(byte[] data, int numValues, double[] output, int startIdx, int count);
    void gatherLongs(byte[] data, int numValues, long[] output, int startIdx, int count);
    void gatherInts(byte[] data, int numValues, int[] output, int startIdx, int count);
    void gatherFloats(byte[] data, int numValues, float[] output, int startIdx, int count);

    // ColumnValueIterator
    void markNulls(BitSet nulls, int[] defLevels, int srcPos, int destPos, int count, int maxDefLevel);

    // DeltaBinaryPackedDecoder
    void prefixSum(long[] values, long[] deltas, int offset, int count, long initialValue);
}
```

### 3.2 Example: countNonNulls() Vectorized

```java
// VectorOperations.java (Java 25)
@Override
public int countNonNulls(int[] defLevels, int maxDef) {
    VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
    int step = species.length();
    int count = 0;
    int i = 0;

    IntVector maxDefVec = IntVector.broadcast(species, maxDef);

    for (; i + step <= defLevels.length; i += step) {
        IntVector vec = IntVector.fromArray(species, defLevels, i);
        count += vec.eq(maxDefVec).trueCount();
    }

    // Scalar tail
    for (; i < defLevels.length; i++) {
        if (defLevels[i] == maxDef) count++;
    }
    return count;
}
```

### 3.3 Example: applyDictionary() with Gather

```java
// VectorOperations.java (Java 25)
@Override
public void applyDictionaryInts(int[] output, int[] dict, int[] indices, int count) {
    VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
    int step = species.length();
    int i = 0;

    for (; i + step <= count; i += step) {
        IntVector idxVec = IntVector.fromArray(species, indices, i);
        // Gather values from dictionary using indices
        IntVector values = IntVector.fromArray(species, dict, 0, idxVec, 0);
        values.intoArray(output, i);
    }

    // Scalar tail
    for (; i < count; i++) {
        output[i] = dict[indices[i]];
    }
}
```

### 3.4 Example: markNulls() Vectorized

```java
// VectorOperations.java (Java 25)
@Override
public void markNulls(BitSet nulls, int[] defLevels, int srcPos, int destPos,
                      int count, int maxDefLevel) {
    VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
    int step = species.length();
    IntVector maxDefVec = IntVector.broadcast(species, maxDefLevel);
    int i = 0;

    for (; i + step <= count; i += step) {
        IntVector vec = IntVector.fromArray(species, defLevels, srcPos + i);
        VectorMask<Integer> isNull = vec.lt(maxDefVec);

        // Convert mask to bit positions
        long maskBits = isNull.toLong();
        while (maskBits != 0) {
            int bit = Long.numberOfTrailingZeros(maskBits);
            nulls.set(destPos + i + bit);
            maskBits &= maskBits - 1;
        }
    }

    // Scalar tail
    for (; i < count; i++) {
        if (defLevels[srcPos + i] < maxDefLevel) {
            nulls.set(destPos + i);
        }
    }
}
```

### 3.5 Example: ByteStreamSplit Gather

```java
// VectorOperations.java (Java 25)
@Override
public void gatherDoubles(byte[] data, int numValues, double[] output,
                          int startIdx, int count) {
    // For doubles: 8 bytes per value from 8 streams
    // Stream k starts at offset k * numValues

    for (int i = 0; i < count; i++) {
        int idx = startIdx + i;
        long assembled = 0;
        for (int k = 0; k < 8; k++) {
            assembled |= ((long)(data[k * numValues + idx] & 0xFF)) << (k * 8);
        }
        output[i] = Double.longBitsToDouble(assembled);
    }

    // Note: Full SIMD version would use vector shuffles to transpose
    // the byte layout, processing multiple values at once
}
```

### 3.6 Batch Size Thresholds

| Operation | Min Batch | Rationale |
|-----------|-----------|-----------|
| countNonNulls | 32 | Vector width (8-16) x 2-4 for overhead |
| unpackBitWidth1 | 64 | 8 bytes = 64 values |
| applyDictionary | 16 | Single gather = 8-16 values |
| gatherBytes | 8 | 1 full vector |
| markNulls | 32 | Vector width x 2-4 |

---

## 4. Files to Create

| Path | Purpose |
|------|---------|
| `core/src/main/java/dev/hardwood/hardwood/internal/encoding/simd/VectorSupport.java` | Runtime detection |
| `core/src/main/java/dev/hardwood/hardwood/internal/encoding/simd/SimdOperations.java` | Operations interface |
| `core/src/main/java/dev/hardwood/hardwood/internal/encoding/simd/ScalarOperations.java` | Scalar fallback |
| `core/src/main/java25/dev/hardwood/hardwood/internal/encoding/simd/VectorOperations.java` | SIMD implementation |
| `core/src/main/java25/dev/hardwood/hardwood/internal/encoding/simd/VectorSupport.java` | SIMD-enabled override |
| `core/src/test/java/dev/hardwood/hardwood/internal/encoding/simd/SimdOperationsTest.java` | Unit tests |
| `performance-testing/micro-benchmarks/src/main/java/dev/hardwood/hardwood/benchmarks/SimdBenchmark.java` | JMH benchmarks |

## 5. Files to Modify

| Path | Changes |
|------|---------|
| `core/pom.xml` | Add java25 multi-release compilation |
| `core/src/main/java/.../RleBitPackingHybridDecoder.java` | Delegate to SimdOperations |
| `core/src/main/java/.../ByteStreamSplitDecoder.java` | Add batch methods, delegate to SimdOperations |
| `core/src/main/java/.../ColumnValueIterator.java` | Delegate markNulls to SimdOperations |
| `core/src/main/java/.../DeltaBinaryPackedDecoder.java` | Add batch methods (optional, Phase 5) |

---

## 6. Maven Configuration

### core/pom.xml additions

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-compiler-plugin</artifactId>
  <executions>
    <!-- Existing Java 22 compilation for FFM -->
    <execution>
      <id>compile-java22</id>
      <!-- ... existing config ... -->
    </execution>

    <!-- NEW: Compile Java 25 specific code for Vector API -->
    <execution>
      <id>compile-java25</id>
      <phase>compile</phase>
      <goals>
        <goal>compile</goal>
      </goals>
      <configuration>
        <release>25</release>
        <compileSourceRoots>
          <compileSourceRoot>${project.basedir}/src/main/java25</compileSourceRoot>
        </compileSourceRoots>
        <multiReleaseOutput>true</multiReleaseOutput>
        <compilerArgs>
          <arg>--add-modules</arg>
          <arg>jdk.incubator.vector</arg>
        </compilerArgs>
      </configuration>
    </execution>
  </executions>
</plugin>
```

### Test/Surefire configuration

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <argLine>
      --add-modules jdk.incubator.vector
      --enable-native-access=ALL-UNNAMED
    </argLine>
  </configuration>
</plugin>
```

### Benchmark JAR configuration

```xml
<!-- performance-testing/micro-benchmarks/pom.xml -->
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <configuration>
    <transformers>
      <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
        <mainClass>org.openjdk.jmh.Main</mainClass>
        <manifestEntries>
          <Add-Opens>jdk.incubator.vector/jdk.incubator.vector=ALL-UNNAMED</Add-Opens>
        </manifestEntries>
      </transformer>
    </transformers>
  </configuration>
</plugin>
```

---

## 7. Implementation Phases

### Phase 1: Infrastructure ✅ COMPLETE
- [x] Create `core/src/main/java/dev/hardwood/hardwood/internal/encoding/simd/` package
- [x] Implement `VectorSupport.java` (base version returns scalar, Java 22+ has runtime detection)
- [x] Implement `SimdOperations.java` interface
- [x] Implement `ScalarOperations.java` (loop-unrolled scalar fallback)
- [x] Create `core/src/main/java22/` directory structure (using Java 22, not 25)
- [x] Update Maven configuration for multi-release JAR with Vector API module
- [x] Add INFO-level logging indicating SIMD availability

### Phase 2: Core SIMD Operations ✅ COMPLETE
- [x] Implement `VectorOperations.java` for Java 22+:
  - [x] `countNonNulls()` - vectorized comparison with trueCount
  - [x] `markNulls()` - comparison + mask extraction
  - [x] `unpackBitWidth1()` - bit expansion
  - [x] `unpackBitWidthN()` - for widths 2-8
  - [x] Dictionary application methods (longs, doubles, ints, floats)
- [x] Add 111 unit tests validating scalar/SIMD equivalence
- [x] Run correctness validation against scalar

### Phase 3: RleBitPackingHybridDecoder Integration ✅ COMPLETE
- [x] Integrate SIMD `countNonNulls()` in decoder
- [x] Integrate SIMD `applyDictionary*()` methods for all types
- [x] Integrate SIMD `markNulls()` in ColumnValueIterator
- [x] Update performance test configuration for Vector API
- [x] Create `SimdBenchmark.java` for JMH benchmarking
- [x] Verify multi-release JAR structure in benchmark JAR

### Phase 4: ByteStreamSplitDecoder (2 days)
- Refactor `gatherBytes()` to batch operation
- Implement SIMD gather for doubles/longs/floats/ints
- Add tests

### Phase 5: DeltaBinaryPackedDecoder (1-2 days) - Optional
- Implement SIMD prefix sum
- Refactor decoder to use batched approach
- Add tests

### Phase 6: Benchmarking & Tuning (2-3 days)
- Create `SimdBenchmark.java`
- Run benchmarks on various hardware (AVX2, AVX-512, ARM NEON)
- Tune SIMD thresholds
- Document performance characteristics

---

## 8. Testing Strategy

### Unit Tests

```java
// SimdOperationsTest.java
class SimdOperationsTest {

    private static final SimdOperations SCALAR = new ScalarOperations();
    private static final SimdOperations SIMD = VectorSupport.operations();

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 63, 64, 127, 128, 1000})
    void countNonNullsMatchesScalar(int size) {
        int[] defLevels = generateDefLevels(size);
        int maxDef = 3;

        int scalarResult = SCALAR.countNonNulls(defLevels, maxDef);
        int simdResult = SIMD.countNonNulls(defLevels, maxDef);

        assertThat(simdResult).isEqualTo(scalarResult);
    }

    @Test
    void applyDictionaryIntsMatchesScalar() {
        int[] dict = {100, 200, 300, 400, 500};
        int[] indices = {0, 2, 4, 1, 3, 0, 2, 4, 1, 3, 0, 2, 4, 1, 3, 0};

        int[] scalarOutput = new int[indices.length];
        int[] simdOutput = new int[indices.length];

        SCALAR.applyDictionaryInts(scalarOutput, dict, indices, indices.length);
        SIMD.applyDictionaryInts(simdOutput, dict, indices, indices.length);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }
}
```

### Integration Tests

- Run existing Parquet file tests with SIMD enabled
- Verify file reading produces identical results to scalar path

### Benchmark Tests

```java
// SimdBenchmark.java
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = {"--add-modules", "jdk.incubator.vector"})
public class SimdBenchmark {

    @Param({"128", "1024", "8192", "65536"})
    private int size;

    @Param({"scalar", "simd"})
    private String implementation;

    private int[] defLevels;
    private SimdOperations ops;

    @Setup
    public void setup() {
        defLevels = generateDefLevels(size);
        ops = "simd".equals(implementation)
            ? VectorSupport.operations()
            : new ScalarOperations();
    }

    @Benchmark
    public int countNonNulls() {
        return ops.countNonNulls(defLevels, 3);
    }
}
```

---

## 9. Verification Commands

### Build

```bash
./mvnw verify
```

### Run specific SIMD tests

```bash
./mvnw test -Dtest=SimdOperationsTest \
  -DargLine="--add-modules jdk.incubator.vector"
```

### Run benchmarks

```bash
./mvnw verify -Pperformance-test

java --add-modules jdk.incubator.vector \
     -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
     SimdBenchmark
```

### Compare scalar vs SIMD

```bash
java --add-modules jdk.incubator.vector \
     -jar benchmarks.jar SimdBenchmark \
     -p size=128,1024,8192,65536 \
     -p implementation=scalar,simd \
     -rf json -rff simd-comparison.json
```

### Validate end-to-end

```bash
./mvnw test -Pperformance-test -Dperf.contenders=HARDWOOD_INDEXED
```

---

## 10. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| **Incubator API changes** | Multi-release JAR isolates version-specific code; can add java26/27 versions as needed |
| **Platform differences** (x86 vs ARM) | Use `SPECIES_PREFERRED` for auto-selection; benchmark on multiple platforms |
| **Gather not optimized on all CPUs** | Detect via micro-benchmark at startup; fallback to scalar when slow |
| **Build complexity** | Clear directory naming (java22, java25); documented build process |
| **Debugging difficulty** | Comprehensive logging in VectorSupport; system property to force scalar path |

---

## 11. Reference: Existing Multi-Release Pattern

The project already uses multi-release JAR for FFM/libdeflate:

```
core/src/main/java22/dev/hardwood/hardwood/internal/compression/libdeflate/
├── LibdeflateLoader.java      # FFM implementation
└── LibdeflateDecompressor.java
```

The SIMD implementation follows this exact pattern with `java25/` directory.

---

## 12. Expected Results

Based on similar optimizations in other projects and the nature of the operations:

| Operation | Current (scalar) | Expected (SIMD) | Speedup |
|-----------|-----------------|-----------------|---------|
| countNonNulls (64K ints) | ~40 µs | ~5-10 µs | 4-8x |
| decodeBitPacked width=1 (64K values) | ~80 µs | ~10-20 µs | 4-8x |
| applyDictionary (64K values) | ~60 µs | ~10-20 µs | 3-6x |
| markNulls (64K values) | ~50 µs | ~15-25 µs | 2-4x |

**Overall end-to-end improvement**: 2-4x on compute-bound workloads (after decompression).

---

## Summary

This plan adds SIMD acceleration to key Parquet decoding hot paths using Java Vector API:

1. **Uses proven multi-release JAR pattern** (matches existing FFM integration)
2. **Maintains full backward compatibility** with scalar fallback
3. **Targets 3-8x speedup** on vectorizable operations
4. **Testable with existing infrastructure**
5. **Implementable incrementally** in 6 phases

The implementation prioritizes:
- `RleBitPackingHybridDecoder` (used by all columns with levels)
- `ByteStreamSplitDecoder` (excellent strided access pattern)
- `ColumnValueIterator.markNulls()` (hot path for nullable columns)
