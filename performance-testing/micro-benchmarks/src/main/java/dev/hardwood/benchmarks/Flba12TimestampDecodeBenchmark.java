/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.internal.conversion.Flba12Timestamps;
import dev.hardwood.metadata.LogicalType;

/// Decode cost of the extended-precision `TIMESTAMP` form —
/// `FIXED_LEN_BYTE_ARRAY(12)`, a signed two's complement little-endian count since
/// the epoch — against the existing `INT64` form, over one million values (#921).
///
/// Both carriers start from the byte buffer a `PLAIN`-encoded page hands over and
/// read it with a native-width little-endian load, so the comparison is of the
/// formats and not of some upstream difference. Two levels are measured:
///
/// - **`…Words` / `…Long`** — bytes to the integer representation only: a 96-bit
///   word pair for the extended form, a `long` for `INT64`. The byte-order arms work
///   at this level: `flba12WordsBigEndianSwap` loads a big-endian layout at native
///   width and swaps the bytes, and the `…ByteByByte` arms assemble each word from
///   single bytes at both orders. The assembly cost is an order of magnitude larger
///   than the byte-order difference, so the layouts compare at native loads.
/// - **`…Instant`** — the same values all the way to an `Instant[]`, which is what a
///   row-reader consumer actually pays. The 96-bit floor division into seconds plus
///   nanos-of-second shows up only here.
///
/// The extended-precision values span years 0001–9999, the range the format exists
/// for; the `INT64` values necessarily stay inside its own ±292-year window. That
/// difference is inherent to the comparison, not a bias in the harness — the wider
/// values are the ones that need the extra limb.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar Flba12TimestampDecodeBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@OperationsPerInvocation(Flba12TimestampDecodeBenchmark.VALUES)
public class Flba12TimestampDecodeBenchmark {

    static final int VALUES = 1_000_000;

    @Param({ "NANOS", "MILLIS" })
    private String timeUnit;

    private LogicalType.TimeUnit unit;

    /// A `PLAIN` page body for a `FIXED_LEN_BYTE_ARRAY(12)` column: values back to
    /// back, no length prefixes.
    private byte[] flbaLittleEndian;
    /// The same values with each 12-byte group reversed, for the byte-order arms.
    private byte[] flbaBigEndian;
    /// A `PLAIN` page body for an `INT64` column.
    private byte[] int64Bytes;

    /// Reused across invocations so the comparison is of decode cost, not of how fast
    /// the allocator can hand back a 1M-element array.
    private Instant[] sink;

    @Setup
    public void setup() {
        unit = LogicalType.TimeUnit.valueOf(timeUnit);
        sink = new Instant[VALUES];

        flbaLittleEndian = new byte[VALUES * Flba12Timestamps.WIDTH];
        flbaBigEndian = new byte[VALUES * Flba12Timestamps.WIDTH];
        int64Bytes = new byte[VALUES * Long.BYTES];

        Random random = new Random(20260813L);
        // Spread over the SQL range the extended form exists to carry. Instant's own
        // seconds bound scaled to the unit is the widest count that still decodes.
        long secondsSpan = Instant.parse("9999-12-31T23:59:59Z").getEpochSecond()
                - Instant.parse("0001-01-01T00:00:00Z").getEpochSecond();
        long firstSecond = Instant.parse("0001-01-01T00:00:00Z").getEpochSecond();

        for (int i = 0; i < VALUES; i++) {
            long seconds = firstSecond + Math.floorMod(random.nextLong(), secondsSpan);
            int subSecond = random.nextInt(unitsPerSecond());
            byte[] encoded = Flba12Timestamps.encode(seconds, subSecond, unit);
            System.arraycopy(encoded, 0, flbaLittleEndian, i * Flba12Timestamps.WIDTH,
                    Flba12Timestamps.WIDTH);
            for (int b = 0; b < Flba12Timestamps.WIDTH; b++) {
                flbaBigEndian[i * Flba12Timestamps.WIDTH + b] =
                        encoded[Flba12Timestamps.WIDTH - 1 - b];
            }

            // The INT64 form can only hold counts inside its own range, so its values
            // come from a window around the epoch rather than the full SQL range.
            long count = random.nextLong(-(1L << 61), 1L << 61);
            for (int b = 0; b < Long.BYTES; b++) {
                int64Bytes[i * Long.BYTES + b] = (byte) (count >>> (8 * b));
            }
        }
    }

    // ==================== Level 1: bytes to the integer representation ====================

    /// The 12-byte little-endian load, folded into a checksum so nothing is dead.
    @Benchmark
    public long flba12Words() {
        long acc = 0;
        for (int i = 0; i < VALUES; i++) {
            int offset = i * Flba12Timestamps.WIDTH;
            acc += Flba12Timestamps.lowWord(flbaLittleEndian, offset)
                    ^ Flba12Timestamps.highWord(flbaLittleEndian, offset);
        }
        return acc;
    }

    /// Assembling the little-endian words a byte at a time. Same bytes as
    /// `flba12Words`, but the shift-and-or reduction is a serial dependency chain.
    @Benchmark
    public long flba12WordsByteByByte() {
        long acc = 0;
        for (int i = 0; i < VALUES; i++) {
            int offset = i * Flba12Timestamps.WIDTH;
            acc += lowWordByteByByte(flbaLittleEndian, offset)
                    ^ highWordByteByByte(flbaLittleEndian, offset);
        }
        return acc;
    }

    /// Big-endian done properly: a native-width load plus a byte reverse, which is
    /// one `BSWAP` per word on x86. This is the arm the endianness question turns on
    /// — big-endian would let a reader reuse the `DECIMAL` comparators for
    /// statistics, and what it costs is this reversal, not the byte-at-a-time
    /// assembly of `flba12WordsBigEndianByteByByte`.
    @Benchmark
    public long flba12WordsBigEndianSwap() {
        long acc = 0;
        for (int i = 0; i < VALUES; i++) {
            int offset = i * Flba12Timestamps.WIDTH;
            // Big-endian layout: bytes 0..3 are the high word, 4..11 the low word.
            long lo = Long.reverseBytes((long) LONG_LE.get(flbaBigEndian, offset + 4));
            int hi = Integer.reverseBytes((int) INT_LE.get(flbaBigEndian, offset));
            acc += lo ^ hi;
        }
        return acc;
    }

    /// Big-endian assembled a byte at a time, kept only as the counterpart to
    /// `flba12WordsByteByByte` so the two layouts are compared on equal footing at
    /// both implementation qualities.
    @Benchmark
    public long flba12WordsBigEndianByteByByte() {
        long acc = 0;
        for (int i = 0; i < VALUES; i++) {
            int offset = i * Flba12Timestamps.WIDTH;
            acc += lowWordBigEndian(flbaBigEndian, offset) ^ highWordBigEndian(flbaBigEndian, offset);
        }
        return acc;
    }

    @Benchmark
    public long int64Long() {
        long acc = 0;
        for (int i = 0; i < VALUES; i++) {
            acc += longAt(int64Bytes, i * Long.BYTES);
        }
        return acc;
    }

    // ==================== Level 2: bytes to Instant ====================

    @Benchmark
    public Instant[] flba12Instant() {
        for (int i = 0; i < VALUES; i++) {
            sink[i] = Flba12Timestamps.toInstant(flbaLittleEndian, i * Flba12Timestamps.WIDTH,
                    Flba12Timestamps.WIDTH, unit);
        }
        return sink;
    }

    @Benchmark
    public Instant[] int64Instant() {
        int divisor = unitsPerSecond();
        int nanosPerUnit = nanosPerUnit();
        for (int i = 0; i < VALUES; i++) {
            long count = longAt(int64Bytes, i * Long.BYTES);
            sink[i] = Instant.ofEpochSecond(Math.floorDiv(count, divisor),
                    Math.floorMod(count, divisor) * nanosPerUnit);
        }
        return sink;
    }

    // ==================== Helpers ====================

    /// The `INT64` arm's load. A native-width little-endian read, matching both the
    /// extended-precision arm and what `PlainDecoder#readLongs` actually does (a bulk
    /// `asLongBuffer` copy) — assembling it byte by byte here would hand the
    /// comparison to FLBA(12) on an implementation difference rather than a format
    /// one.
    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_LE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static long longAt(byte[] bytes, int offset) {
        return (long) LONG_LE.get(bytes, offset);
    }

    private static long lowWordByteByByte(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFFL)
                | (bytes[offset + 1] & 0xFFL) << 8
                | (bytes[offset + 2] & 0xFFL) << 16
                | (bytes[offset + 3] & 0xFFL) << 24
                | (bytes[offset + 4] & 0xFFL) << 32
                | (bytes[offset + 5] & 0xFFL) << 40
                | (bytes[offset + 6] & 0xFFL) << 48
                | (bytes[offset + 7] & 0xFFL) << 56;
    }

    private static int highWordByteByByte(byte[] bytes, int offset) {
        return (bytes[offset + 8] & 0xFF)
                | (bytes[offset + 9] & 0xFF) << 8
                | (bytes[offset + 10] & 0xFF) << 16
                | bytes[offset + 11] << 24;
    }

    private static long lowWordBigEndian(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 4; i < 12; i++) {
            value = (value << 8) | (bytes[offset + i] & 0xFFL);
        }
        return value;
    }

    private static int highWordBigEndian(byte[] bytes, int offset) {
        return (bytes[offset] << 24)
                | (bytes[offset + 1] & 0xFF) << 16
                | (bytes[offset + 2] & 0xFF) << 8
                | (bytes[offset + 3] & 0xFF);
    }

    private int unitsPerSecond() {
        return switch (unit) {
            case MILLIS -> 1_000;
            case MICROS -> 1_000_000;
            case NANOS -> 1_000_000_000;
        };
    }

    private int nanosPerUnit() {
        return switch (unit) {
            case MILLIS -> 1_000_000;
            case MICROS -> 1_000;
            case NANOS -> 1;
        };
    }
}
