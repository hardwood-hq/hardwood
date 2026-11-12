/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.alp;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.column.values.ValuesWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.internal.encoding.AlpEncoder;

/// ALP encode, Hardwood against parquet-java, in nanoseconds per value on one thread.
///
/// The two libraries choose each vector's (exponent, factor) pair from a preset they build at
/// different times: Hardwood searches it once over a column chunk's buffered values, parquet-java
/// searches every pair exhaustively on the chunk's first vectors and builds its preset from them.
/// The benchmarks measure the two costs apart:
///
/// | Benchmark | What it runs |
/// |---|---|
/// | `chunkDoubleHardwood`, `chunkDoubleParquetJava` | a column chunk of [#CHUNK_VALUES] doubles, cut into pages of [AlpBenchmarkData#PAGE_VALUES] values, through a writer created for the chunk: preset search, encode and page copy |
/// | `presetDoubleHardwood` | Hardwood's preset search over that chunk alone |
/// | `pageDoubleHardwood`, `pageFloatHardwood` | one page, preset already built |
/// | `pageDoubleParquetJava`, `pageFloatParquetJava` | one page from a writer whose preset is already built, i.e. past its exhaustive search |
///
/// The chunk benchmarks include parquet-java's exhaustive search, whose share falls as a chunk
/// grows.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar AlpEncodeBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class AlpEncodeBenchmark {

    private static final int PAGE_VALUES = AlpBenchmarkData.PAGE_VALUES;
    private static final int CHUNK_VALUES = 1 << 20;

    @State(Scope.Benchmark)
    public static class Chunk {

        @Param({ "decimal", "price", "random" })
        private String data;

        private double[] doubles;
        private final AlpEncoder encoder = new AlpEncoder(Double.BYTES);

        @Setup(Level.Trial)
        public void setUp() {
            doubles = AlpBenchmarkData.doubles(data, CHUNK_VALUES);
        }
    }

    @State(Scope.Benchmark)
    public static class Page {

        /// Vectors written to parquet-java's writers before measuring, past the eight it samples
        /// 15 vectors apart for its preset.
        private static final int PRIMING_VALUES = 128 * 1024;

        @Param({ "decimal", "price", "random" })
        private String data;

        private double[] doubles;
        private float[] floats;
        private int[] doublePreset;
        private int[] floatPreset;
        private final AlpEncoder doubleEncoder = new AlpEncoder(Double.BYTES);
        private final AlpEncoder floatEncoder = new AlpEncoder(Float.BYTES);
        private ValuesWriter doubleWriter;
        private ValuesWriter floatWriter;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            doubles = AlpBenchmarkData.doubles(data, PAGE_VALUES);
            floats = AlpBenchmarkData.floats(data, PAGE_VALUES);
            doublePreset = AlpEncoder.presetDoubles(doubles, 0, PAGE_VALUES);
            floatPreset = AlpEncoder.presetFloats(floats, 0, PAGE_VALUES);
            doubleWriter = AlpBenchmarkData.parquetJavaDoubleWriter();
            floatWriter = AlpBenchmarkData.parquetJavaFloatWriter();
            for (int i = 0; i < PRIMING_VALUES; i++) {
                doubleWriter.writeDouble(doubles[i % PAGE_VALUES]);
                floatWriter.writeFloat(floats[i % PAGE_VALUES]);
            }
            doubleWriter.getBytes();
            doubleWriter.reset();
            floatWriter.getBytes();
            floatWriter.reset();
        }
    }

    @Benchmark
    @OperationsPerInvocation(CHUNK_VALUES)
    public long chunkDoubleHardwood(Chunk chunk) {
        return AlpBenchmarkData.hardwoodChunk(chunk.encoder, chunk.doubles);
    }

    @Benchmark
    @OperationsPerInvocation(CHUNK_VALUES)
    public long chunkDoubleParquetJava(Chunk chunk) throws IOException {
        return AlpBenchmarkData.parquetJavaChunk(chunk.doubles);
    }

    @Benchmark
    @OperationsPerInvocation(CHUNK_VALUES)
    public int[] presetDoubleHardwood(Chunk chunk) {
        return AlpEncoder.presetDoubles(chunk.doubles, 0, CHUNK_VALUES);
    }

    @Benchmark
    @OperationsPerInvocation(PAGE_VALUES)
    public byte[] pageDoubleHardwood(Page page) {
        int length = page.doubleEncoder.encodeDoubles(page.doubles, 0, PAGE_VALUES, page.doublePreset);
        return Arrays.copyOf(page.doubleEncoder.buffer(), length);
    }

    @Benchmark
    @OperationsPerInvocation(PAGE_VALUES)
    public byte[] pageDoubleParquetJava(Page page) throws IOException {
        for (double value : page.doubles) {
            page.doubleWriter.writeDouble(value);
        }
        byte[] bytes = page.doubleWriter.getBytes().toByteArray();
        page.doubleWriter.reset();
        return bytes;
    }

    @Benchmark
    @OperationsPerInvocation(PAGE_VALUES)
    public byte[] pageFloatHardwood(Page page) {
        int length = page.floatEncoder.encodeFloats(page.floats, 0, PAGE_VALUES, page.floatPreset);
        return Arrays.copyOf(page.floatEncoder.buffer(), length);
    }

    @Benchmark
    @OperationsPerInvocation(PAGE_VALUES)
    public byte[] pageFloatParquetJava(Page page) throws IOException {
        for (float value : page.floats) {
            page.floatWriter.writeFloat(value);
        }
        byte[] bytes = page.floatWriter.getBytes().toByteArray();
        page.floatWriter.reset();
        return bytes;
    }
}
