/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.alp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.alp.AlpValuesReaderForDouble;
import org.apache.parquet.column.values.alp.AlpValuesReaderForFloat;
import org.apache.parquet.column.values.alp.AlpValuesWriter;

import dev.hardwood.internal.encoding.AlpDecoder;
import dev.hardwood.internal.encoding.AlpEncoder;
import dev.hardwood.metadata.PhysicalType;

/// Data sets for the ALP benchmarks, and the page encode and decode entry points of Hardwood and
/// parquet-java, each driven the way its column writer and reader drive it.
///
/// | Data set | Values |
/// |---|---|
/// | `decimal` | 0 to 10,000 with two decimals |
/// | `price` | 2.50 to 80.00 with two decimals |
/// | `random` | uniform in [0, 1), nearly every value an exception |
///
/// Every generator draws from a `Random` seeded with 42.
final class AlpBenchmarkData {

    /// Values per page, Hardwood's and parquet-java's default for a page of doubles.
    static final int PAGE_VALUES = 1 << 17;

    private static final int SLAB_SIZE = 64 * 1024;
    private static final int PAGE_SIZE = 64 * 1024 * 1024;

    private AlpBenchmarkData() {
    }

    static double[] doubles(String data, int count) {
        Random random = new Random(42);
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            values[i] = switch (data) {
                case "decimal" -> Math.round(random.nextDouble() * 1e6) / 100.0;
                case "price" -> Math.round((2.5 + random.nextDouble() * 77.5) * 100) / 100.0;
                case "random" -> random.nextDouble();
                default -> throw new IllegalArgumentException("Unknown data set: " + data);
            };
        }
        return values;
    }

    static float[] floats(String data, int count) {
        Random random = new Random(42);
        float[] values = new float[count];
        for (int i = 0; i < count; i++) {
            values[i] = switch (data) {
                case "decimal" -> Math.round(random.nextFloat() * 1e6f) / 100.0f;
                case "price" -> Math.round((2.5f + random.nextFloat() * 77.5f) * 100) / 100.0f;
                case "random" -> random.nextFloat();
                default -> throw new IllegalArgumentException("Unknown data set: " + data);
            };
        }
        return values;
    }

    // Hardwood

    /// One page, its preset searched over the page's own values.
    static byte[] hardwoodPage(double[] values) {
        AlpEncoder encoder = new AlpEncoder(Double.BYTES);
        int[] preset = AlpEncoder.presetDoubles(values, 0, values.length);
        int length = encoder.encodeDoubles(values, 0, values.length, preset);
        return Arrays.copyOf(encoder.buffer(), length);
    }

    static byte[] hardwoodPage(float[] values) {
        AlpEncoder encoder = new AlpEncoder(Float.BYTES);
        int[] preset = AlpEncoder.presetFloats(values, 0, values.length);
        int length = encoder.encodeFloats(values, 0, values.length, preset);
        return Arrays.copyOf(encoder.buffer(), length);
    }

    /// A column chunk as `DoubleValueEncoder` writes it: one preset searched over the whole chunk,
    /// then a page every [#PAGE_VALUES] values, each copied out of the encoder's buffer.
    ///
    /// @return the bytes of all pages
    static long hardwoodChunk(AlpEncoder encoder, double[] values) {
        int[] preset = AlpEncoder.presetDoubles(values, 0, values.length);
        long total = 0;
        for (int from = 0; from < values.length; from += PAGE_VALUES) {
            int length = encoder.encodeDoubles(values, from, Math.min(PAGE_VALUES, values.length - from), preset);
            total += Arrays.copyOf(encoder.buffer(), length).length;
        }
        return total;
    }

    /// Validates the page, then decodes it vector by vector into a vector buffer copied out, as
    /// `FlatPageValues` does for a column without nulls.
    static void hardwoodDecode(byte[] page, double[] out, double[] vectorBuffer) {
        AlpDecoder decoder = new AlpDecoder(page, 0, page.length, out.length, PhysicalType.DOUBLE);
        decoder.validate();
        int vectorSize = decoder.vectorSize();
        for (int vector = 0, pos = 0; pos < out.length; vector++, pos += vectorSize) {
            int count = decoder.decodeVector(vector, null, vectorBuffer);
            System.arraycopy(vectorBuffer, 0, out, pos, count);
        }
    }

    static void hardwoodDecode(byte[] page, float[] out, float[] vectorBuffer) {
        AlpDecoder decoder = new AlpDecoder(page, 0, page.length, out.length, PhysicalType.FLOAT);
        decoder.validate();
        int vectorSize = decoder.vectorSize();
        for (int vector = 0, pos = 0; pos < out.length; vector++, pos += vectorSize) {
            int count = decoder.decodeVector(vector, vectorBuffer, null);
            System.arraycopy(vectorBuffer, 0, out, pos, count);
        }
    }

    // parquet-java

    static ValuesWriter parquetJavaDoubleWriter() {
        return new AlpValuesWriter.DoubleAlpValuesWriter(SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    }

    static ValuesWriter parquetJavaFloatWriter() {
        return new AlpValuesWriter.FloatAlpValuesWriter(SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    }

    /// One page from a fresh writer, so its preset comes from the page's first values.
    static byte[] parquetJavaPage(double[] values) throws IOException {
        ValuesWriter writer = parquetJavaDoubleWriter();
        for (double value : values) {
            writer.writeDouble(value);
        }
        byte[] page = writer.getBytes().toByteArray();
        writer.close();
        return page;
    }

    static byte[] parquetJavaPage(float[] values) throws IOException {
        ValuesWriter writer = parquetJavaFloatWriter();
        for (float value : values) {
            writer.writeFloat(value);
        }
        byte[] page = writer.getBytes().toByteArray();
        writer.close();
        return page;
    }

    /// A column chunk as parquet-java's column writer writes it: one ALP writer per chunk, a page
    /// every [#PAGE_VALUES] values, `reset()` after each page (which keeps the sampling state).
    ///
    /// @return the bytes of all pages
    static long parquetJavaChunk(double[] values) throws IOException {
        ValuesWriter writer = parquetJavaDoubleWriter();
        long total = 0;
        for (int from = 0; from < values.length; from += PAGE_VALUES) {
            int to = Math.min(values.length, from + PAGE_VALUES);
            for (int i = from; i < to; i++) {
                writer.writeDouble(values[i]);
            }
            total += writer.getBytes().toByteArray().length;
            writer.reset();
        }
        writer.close();
        return total;
    }

    /// One `readDouble()` per value after `initFromPage`, as parquet-java's column reader reads.
    static void parquetJavaDecode(byte[] page, double[] out) throws IOException {
        ValuesReader reader = new AlpValuesReaderForDouble();
        reader.initFromPage(out.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
        for (int i = 0; i < out.length; i++) {
            out[i] = reader.readDouble();
        }
    }

    static void parquetJavaDecode(byte[] page, float[] out) throws IOException {
        ValuesReader reader = new AlpValuesReaderForFloat();
        reader.initFromPage(out.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
        for (int i = 0; i < out.length; i++) {
            out[i] = reader.readFloat();
        }
    }
}
