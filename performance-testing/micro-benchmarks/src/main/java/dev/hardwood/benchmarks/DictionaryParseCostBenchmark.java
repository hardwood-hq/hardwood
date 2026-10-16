/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.metadata.PhysicalType;

/// SPIKE: what parsing one decompressed `BYTE_ARRAY` dictionary page costs, as main does it
/// (`Dictionary.parse`: one `byte[]` per entry, then flattened), against two single-pass
/// prototypes: copying entries into one flat array while recording offsets, and recording
/// each entry's start and end inside the page buffer without copying.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionaryParseCostBenchmark {

    @Param({"20000", "200000"})
    private int entries;

    private byte[] page;

    @Setup(Level.Trial)
    public void setup() {
        SplittableRandom random = new SplittableRandom(42);
        byte[][] values = new byte[entries][];
        int size = 0;
        for (int i = 0; i < entries; i++) {
            values[i] = String.format("https://example.com/%09d/item-%07d", random.nextInt(1_000_000_000), i)
                    .getBytes(StandardCharsets.UTF_8);
            size += Integer.BYTES + values[i].length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] value : values) {
            buffer.putInt(value.length);
            buffer.put(value);
        }
        page = buffer.array();
        if (((Dictionary.ByteArrayDictionary) parseMain()).size() != entries
                || flatCopy().offsets()[entries] != size - Integer.BYTES * entries
                || inPlace().ends()[entries - 1] != size) {
            throw new IllegalStateException("Prototype parse disagrees with the page");
        }
    }

    @Benchmark
    public Dictionary parseMain() {
        return Dictionary.parse(page, page.length, entries, PhysicalType.BYTE_ARRAY, null);
    }

    /// One pass: copy every entry into one array, recording its start.
    @Benchmark
    public Flat flatCopy() {
        byte[] bytes = new byte[page.length - Integer.BYTES * entries];
        int[] offsets = new int[entries + 1];
        int pos = 0;
        int out = 0;
        for (int i = 0; i < entries; i++) {
            int length = readLength(page, pos);
            pos += Integer.BYTES;
            System.arraycopy(page, pos, bytes, out, length);
            pos += length;
            out += length;
            offsets[i + 1] = out;
        }
        return new Flat(bytes, offsets);
    }

    /// One pass: keep the page as the entries' bytes, recording each entry's start and end.
    @Benchmark
    public InPlace inPlace() {
        int[] starts = new int[entries];
        int[] ends = new int[entries];
        int pos = 0;
        for (int i = 0; i < entries; i++) {
            int length = readLength(page, pos);
            pos += Integer.BYTES;
            starts[i] = pos;
            pos += length;
            ends[i] = pos;
        }
        return new InPlace(page, starts, ends);
    }

    private static int readLength(byte[] data, int pos) {
        return (data[pos] & 0xFF) | (data[pos + 1] & 0xFF) << 8 | (data[pos + 2] & 0xFF) << 16
                | (data[pos + 3] & 0xFF) << 24;
    }

    public record Flat(byte[] bytes, int[] offsets) {}

    public record InPlace(byte[] bytes, int[] starts, int[] ends) {}
}
