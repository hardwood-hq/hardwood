/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;

/// Benchmark for the RLE/bit-packing hybrid decoder path used by dictionary indices.
///
/// Run with:
/// ```shell
/// java --add-modules jdk.incubator.vector -jar benchmarks.jar RleBitPackingHybridDecoderBenchmark
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class RleBitPackingHybridDecoderBenchmark {

    @Param({"128", "1024", "8192", "65536"})
    private int size;

    @Param({"1", "2", "4", "8"})
    private int bitWidth;

    private byte[] encoded;
    private int[] output;

    @Setup
    public void setup() {
        Random random = new Random(0x680L + bitWidth + size);
        int[] values = new int[size];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(1 << bitWidth);
        }
        encoded = encodeBitPackedRun(values, bitWidth);
        output = new int[size];
    }

    @Benchmark
    public void decodeBitPackedRun(Blackhole bh) {
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, bitWidth);
        decoder.readInts(output, 0, output.length);
        bh.consume(output);
    }

    private static byte[] encodeBitPackedRun(int[] values, int bitWidth) {
        int groups = (values.length + 7) / 8;
        int padded = groups * 8;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnsignedVarInt(out, ((long) groups << 1) | 1L);

        long bits = 0;
        int bitsInBuffer = 0;
        for (int i = 0; i < padded; i++) {
            int value = i < values.length ? values[i] : 0;
            bits |= ((long) value) << bitsInBuffer;
            bitsInBuffer += bitWidth;
            while (bitsInBuffer >= 8) {
                out.write((int) (bits & 0xFF));
                bits >>>= 8;
                bitsInBuffer -= 8;
            }
        }
        if (bitsInBuffer > 0) {
            out.write((int) (bits & 0xFF));
        }
        return out.toByteArray();
    }

    private static void writeUnsignedVarInt(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }
}
