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

import dev.hardwood.internal.encoding.AlpDecoder;
import dev.hardwood.metadata.PhysicalType;

/// ALP page decode, Hardwood against parquet-java, in nanoseconds per value on one thread.
///
/// Each library decodes one page of [AlpBenchmarkData#PAGE_VALUES] values, written by its own
/// encoder, the way its reader drives the decoder:
///
/// | Benchmark | What it runs |
/// |---|---|
/// | `doubleHardwood`, `floatHardwood` | `AlpDecoder` validates the page, decodes each vector into a vector buffer and copies it out, as `FlatPageValues` does |
/// | `doubleParquetJava`, `floatParquetJava` | `initFromPage`, then one `readDouble()` or `readFloat()` per value, as parquet-java's column reader does |
/// | `doubleHardwoodOnParquetJavaPage` | Hardwood's decoder on parquet-java's page |
/// | `doubleHardwoodVectors` | `decodeVector` into the vector buffer alone: the decode loop without validation or copy |
///
/// Setup decodes every page with both libraries and fails unless each returns the input bit for
/// bit.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar AlpDecodeBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class AlpDecodeBenchmark {

    private static final int VALUES = AlpBenchmarkData.PAGE_VALUES;

    @Param({ "decimal", "price", "random" })
    private String data;

    private byte[] hardwoodDoublePage;
    private byte[] parquetJavaDoublePage;
    private byte[] hardwoodFloatPage;
    private byte[] parquetJavaFloatPage;

    private final double[] doubleOutput = new double[VALUES];
    private final float[] floatOutput = new float[VALUES];
    private final double[] doubleVector = new double[1024];
    private final float[] floatVector = new float[1024];

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        double[] doubles = AlpBenchmarkData.doubles(data, VALUES);
        float[] floats = AlpBenchmarkData.floats(data, VALUES);
        hardwoodDoublePage = AlpBenchmarkData.hardwoodPage(doubles);
        parquetJavaDoublePage = AlpBenchmarkData.parquetJavaPage(doubles);
        hardwoodFloatPage = AlpBenchmarkData.hardwoodPage(floats);
        parquetJavaFloatPage = AlpBenchmarkData.parquetJavaPage(floats);
        verify(doubles, floats);
    }

    private void verify(double[] doubles, float[] floats) throws IOException {
        for (byte[] page : new byte[][]{ hardwoodDoublePage, parquetJavaDoublePage }) {
            AlpBenchmarkData.hardwoodDecode(page, doubleOutput, doubleVector);
            check(Arrays.equals(doubles, doubleOutput), "Hardwood", page);
            AlpBenchmarkData.parquetJavaDecode(page, doubleOutput);
            check(Arrays.equals(doubles, doubleOutput), "parquet-java", page);
        }
        for (byte[] page : new byte[][]{ hardwoodFloatPage, parquetJavaFloatPage }) {
            AlpBenchmarkData.hardwoodDecode(page, floatOutput, floatVector);
            check(Arrays.equals(floats, floatOutput), "Hardwood", page);
            AlpBenchmarkData.parquetJavaDecode(page, floatOutput);
            check(Arrays.equals(floats, floatOutput), "parquet-java", page);
        }
    }

    private void check(boolean equal, String decoder, byte[] page) {
        if (!equal) {
            String writer = page == hardwoodDoublePage || page == hardwoodFloatPage ? "Hardwood" : "parquet-java";
            throw new IllegalStateException(decoder + " did not decode the " + writer + " page of '" + data
                    + "' to its input");
        }
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public double[] doubleHardwood() {
        AlpBenchmarkData.hardwoodDecode(hardwoodDoublePage, doubleOutput, doubleVector);
        return doubleOutput;
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public double[] doubleParquetJava() throws IOException {
        AlpBenchmarkData.parquetJavaDecode(parquetJavaDoublePage, doubleOutput);
        return doubleOutput;
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public double[] doubleHardwoodOnParquetJavaPage() {
        AlpBenchmarkData.hardwoodDecode(parquetJavaDoublePage, doubleOutput, doubleVector);
        return doubleOutput;
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public double[] doubleHardwoodVectors() {
        AlpDecoder decoder = new AlpDecoder(hardwoodDoublePage, 0, hardwoodDoublePage.length, VALUES,
                PhysicalType.DOUBLE);
        int vectors = VALUES / decoder.vectorSize();
        for (int vector = 0; vector < vectors; vector++) {
            decoder.decodeVector(vector, null, doubleVector);
        }
        return doubleVector;
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public float[] floatHardwood() {
        AlpBenchmarkData.hardwoodDecode(hardwoodFloatPage, floatOutput, floatVector);
        return floatOutput;
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public float[] floatParquetJava() throws IOException {
        AlpBenchmarkData.parquetJavaDecode(parquetJavaFloatPage, floatOutput);
        return floatOutput;
    }
}
