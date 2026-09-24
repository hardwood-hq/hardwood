/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/// The audit corpus: every column the fixtures hold, with the value each row stores.
///
/// The values live here, in memory, and are what the oracle compares against. Nothing the audit
/// expects is read back through Hardwood's accessors.
final class Columns {

    static final int ROWS = 600;
    static final long BASE_MS = 1_700_000_000_000L;
    static final long JULIAN_EPOCH_DAY = 2_440_588L;
    static final long NANOS_PER_DAY = 86_400_000_000_000L;

    /// The row whose value the literals of each column are built around.
    static final int PROBE_ROW = 310;

    private Columns() {
    }

    /// What a column is, as far as the rule is concerned: its physical type and annotation.
    enum Sem {
        BOOL, I32, I64, INT8S, UINT8, UINT32, UINT64, F32, F64, F16, DATE, TIME_MS, TIME_US, TIME_NS,
        TS_MS_UTC, TS_US_UTC, TS_NS_UTC, TS_MS_LOCAL, TS_US_LOCAL, TS12_MS_UTC, TS12_NS_UTC, TS12_US_LOCAL, INT96,
        DEC_I32, DEC_I64, DEC_FLBA, DEC_BA,
        STRING, ENUM, JSON, BSON, BA, FLBA, UUID, INTERVAL, NULL_I32, GEOM, ZZ
    }

    /// A column: its name, what it is, its decimal scale or fixed width where it has one, and the
    /// value row `r` stores, or `null` for a null row.
    record Col(String name, Sem sem, int scale, int width, IntFunction<Object> value, boolean nullable) {

        Object at(int row) {
            return nullable && isNullRow(row) ? null : value.apply(row);
        }
    }

    static boolean isNullRow(int row) {
        return row % 29 == 7;
    }

    private static int offset(int row) {
        return row - 300;
    }

    // ==================== Values ====================

    static float floatAt(int row) {
        return switch (row) {
            case 10 -> Float.intBitsToFloat(0x7fc00000);
            case 20 -> Float.intBitsToFloat(0xffc00000);
            case 30 -> Float.intBitsToFloat(0x7f800001);
            case 300 -> -0.0f;
            case 302 -> 0.0f;
            case 598 -> Float.POSITIVE_INFINITY;
            case 2 -> Float.NEGATIVE_INFINITY;
            default -> offset(row) * 0.5f;
        };
    }

    static double doubleAt(int row) {
        return switch (row) {
            case 10 -> Double.longBitsToDouble(0x7ff8000000000000L);
            case 20 -> Double.longBitsToDouble(0xfff8000000000000L);
            case 30 -> Double.longBitsToDouble(0x7ff0000000000001L);
            case 300 -> -0.0;
            case 302 -> 0.0;
            case 598 -> Double.POSITIVE_INFINITY;
            case 2 -> Double.NEGATIVE_INFINITY;
            default -> offset(row) * 0.5;
        };
    }

    /// Two little-endian bytes of a half: `NaN` under three encodings, both zeros, both infinities,
    /// and at row 303 a value whose bytes sort out of numeric order.
    static byte[] float16At(int row) {
        int bits = switch (row) {
            case 10 -> 0x7e00;
            case 20 -> 0xfe00;
            case 30 -> 0x7c01;
            case 300 -> 0x8000;
            case 302 -> 0x0000;
            case 598 -> 0x7c00;
            case 2 -> 0xfc00;
            case 303 -> 0x3c01;
            default -> Float.floatToFloat16(offset(row) * 0.25f) & 0xFFFF;
        };
        return new byte[] { (byte) bits, (byte) (bits >>> 8) };
    }

    /// The instant row `row` of the `INT96` column stores, in nanoseconds since the epoch. Row 1
    /// lies before 1900, and row 5 holds row 6's instant under a non-canonical encoding.
    static long int96Nanos(int row) {
        if (row == 1) {
            Instant early = Instant.parse("1800-01-01T00:00:00.000000001Z");
            return early.getEpochSecond() * 1_000_000_000L + early.getNano();
        }
        if (row == 5) {
            return int96Nanos(6);
        }
        return BASE_MS * 1_000_000L + offset(row) * 3_600_000_000_000L + row * 1000L;
    }

    /// The twelve bytes of an `INT96`: nanoseconds of the day, then the Julian day. A non-canonical
    /// encoding takes the day before and a nanoseconds-of-day field one day longer.
    static byte[] int96(long epochNanos, boolean canonical) {
        long day = Math.floorDiv(epochNanos, NANOS_PER_DAY) + JULIAN_EPOCH_DAY;
        long nanosOfDay = Math.floorMod(epochNanos, NANOS_PER_DAY);
        if (!canonical) {
            day -= 1;
            nanosOfDay += NANOS_PER_DAY;
        }
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(nanosOfDay).putInt(Math.toIntExact(day)).array();
    }

    /// Fifty years of 365.25 days, in nanoseconds: the step between the rows of the `ts12` columns.
    private static final BigInteger TS12_STEP_NANOS = BigInteger.valueOf(50L * 31_557_600L * 1_000_000_000L);

    /// The instant row `row` of the `ts12` columns stands for, in nanoseconds since the epoch: fifty
    /// years a row either side of row 300, which passes the `INT64` nanosecond range in both
    /// directions, except the nanosecond before the epoch at row 3 and the last nanosecond of year
    /// 9999 at row 6.
    static BigInteger ts12Nanos(int row) {
        return switch (row) {
            case 3 -> BigInteger.valueOf(-1);
            case 6 -> BigInteger.valueOf(253_402_300_799L).multiply(BigInteger.valueOf(1_000_000_000L))
                    .add(BigInteger.valueOf(999_999_999L));
            default -> BigInteger.valueOf(offset(row)).multiply(TS12_STEP_NANOS).add(BigInteger.valueOf(row * 1_000_001L));
        };
    }

    /// The twelve bytes a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` stores for `nanos`, floored to a
    /// unit of `unitNanos`: the count's two's complement, least significant byte first.
    static byte[] ts12(BigInteger nanos, long unitNanos) {
        BigInteger count = nanos.subtract(nanos.mod(BigInteger.valueOf(unitNanos))).divide(BigInteger.valueOf(unitNanos));
        byte[] bigEndian = fixedDecimal(count, 12);
        byte[] littleEndian = new byte[12];
        for (int i = 0; i < 12; i++) {
            littleEndian[i] = bigEndian[11 - i];
        }
        return littleEndian;
    }

    static byte[] fixedDecimal(BigInteger unscaled, int width) {
        byte[] minimal = unscaled.toByteArray();
        byte[] out = new byte[width];
        byte fill = (byte) (unscaled.signum() < 0 ? 0xFF : 0);
        int start = width - minimal.length;
        for (int i = 0; i < start; i++) {
            out[i] = fill;
        }
        System.arraycopy(minimal, 0, out, start, minimal.length);
        return out;
    }

    /// A `BYTE_ARRAY` decimal in the fewest bytes, except row 300 (zero as no bytes) and rows 301
    /// and 298, which carry a redundant sign-extension byte.
    static byte[] binaryDecimalAt(int row) {
        return switch (row) {
            case 300 -> new byte[0];
            case 301 -> new byte[] { 0x00, 0x02 };
            case 298 -> new byte[] { (byte) 0xFF, (byte) 0xFC };
            default -> BigInteger.valueOf(offset(row) * 2L).toByteArray();
        };
    }

    static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static String stringAt(int row) {
        return switch (row) {
            case 599 -> "～";
            case 598 -> "é";
            default -> String.format("k%04d", row);
        };
    }

    static byte[] binaryAt(int row) {
        if (row == 0) {
            return new byte[0];
        }
        int value = (row * 100) & 0xFFFF;
        return new byte[] { (byte) (value >>> 8), (byte) value };
    }

    static byte[] uuidAt(int row) {
        return ByteBuffer.allocate(16).putLong(((long) offset(row) << 40) | row).putLong(row).array();
    }

    static byte[] intervalAt(int row) {
        int months = row == 599 ? 0xFFFFFFFF : row;
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(months).putInt(row % 31).putInt(row * 1000).array();
    }

    static byte[] wkbPoint(double x, double y) {
        return ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 1).putInt(1).putDouble(x).putDouble(y).array();
    }

    // ==================== Column sets ====================

    /// One column per row of the design's per-column table that parquet-java writes directly.
    static List<Col> flat() {
        List<Col> c = new ArrayList<>();
        c.add(new Col("__row__", Sem.I64, 0, 0, row -> (long) row, false));
        c.add(new Col("zz", Sem.ZZ, 0, 0, row -> utf8("z"), false));
        c.add(new Col("bool", Sem.BOOL, 0, 0, row -> row >= 350, true));
        c.add(new Col("i32", Sem.I32, 0, 0, row -> offset(row) * 2, true));
        // No nulls, so a row group whose every value matches is decided in full from its statistics.
        c.add(new Col("i32_req", Sem.I32, 0, 0, row -> offset(row) * 2, false));
        c.add(new Col("i64", Sem.I64, 0, 0, row -> offset(row) * 20_000_000_000L, true));
        c.add(new Col("i8", Sem.INT8S, 0, 0, row -> row == 598 ? 1000 : row == 1 ? -1000 : offset(row) / 3, true));
        c.add(new Col("u8", Sem.UINT8, 0, 0, row -> row * 255 / 599, true));
        c.add(new Col("u32", Sem.UINT32, 0, 0, row -> (int) (2_147_483_648L - 300_000_000L + row * 1_000_000L), true));
        c.add(new Col("u64", Sem.UINT64, 0, 0, row -> Long.MIN_VALUE + offset(row) * 1_000_000_000_000_000L, true));
        c.add(new Col("f32", Sem.F32, 0, 0, Columns::floatAt, true));
        c.add(new Col("f64", Sem.F64, 0, 0, Columns::doubleAt, true));
        c.add(new Col("f16", Sem.F16, 0, 2, Columns::float16At, true));
        c.add(new Col("date", Sem.DATE, 0, 0, row -> 19000 + offset(row) * 2, true));
        c.add(new Col("time_ms", Sem.TIME_MS, 0, 0, row -> row == 597 ? 90_000_000 : 3_600_000 + row * 60_000, true));
        c.add(new Col("time_us", Sem.TIME_US, 0, 0, row -> row * 60_000_000L + row, true));
        c.add(new Col("time_ns", Sem.TIME_NS, 0, 0, row -> row * 60_000_000_000L + row, true));
        c.add(new Col("ts_ms_utc", Sem.TS_MS_UTC, 0, 0, row -> BASE_MS + offset(row) * 3_600_000L, true));
        c.add(new Col("ts_us_utc", Sem.TS_US_UTC, 0, 0, row -> BASE_MS * 1000 + offset(row) * 3_600_000_000L + row, true));
        c.add(new Col("ts_ns_utc", Sem.TS_NS_UTC, 0, 0,
                row -> BASE_MS * 1_000_000 + offset(row) * 3_600_000_000_000L + row, true));
        c.add(new Col("ts_ms_local", Sem.TS_MS_LOCAL, 0, 0, row -> BASE_MS + offset(row) * 3_600_000L, true));
        c.add(new Col("ts_us_local", Sem.TS_US_LOCAL, 0, 0,
                row -> BASE_MS * 1000 + offset(row) * 3_600_000_000L + row, true));
        c.add(new Col("ts96", Sem.INT96, 0, 12, row -> int96(int96Nanos(row), row != 5), true));
        c.add(new Col("dec_i32", Sem.DEC_I32, 2, 0, row -> offset(row) * 2 + 1, true));
        c.add(new Col("dec_i64", Sem.DEC_I64, 4, 0, row -> offset(row) * 20_000L + 1, true));
        c.add(new Col("dec_flba", Sem.DEC_FLBA, 2, 9, row -> fixedDecimal(BigInteger.valueOf(offset(row) * 2L), 9), true));
        c.add(new Col("dec_ba", Sem.DEC_BA, 3, 0, Columns::binaryDecimalAt, true));
        c.add(new Col("str", Sem.STRING, 0, 0, row -> utf8(stringAt(row)), true));
        c.add(new Col("enm", Sem.ENUM, 0, 0, row -> utf8(String.format("E%02d", row / 10)), true));
        c.add(new Col("json", Sem.JSON, 0, 0, row -> utf8(String.format("{\"a\":%d}", row)), true));
        c.add(new Col("ba", Sem.BA, 0, 0, Columns::binaryAt, true));
        c.add(new Col("flba4", Sem.FLBA, 0, 4, row -> ByteBuffer.allocate(4).putInt(offset(row) * 1_000_000).array(), true));
        c.add(new Col("uuid", Sem.UUID, 0, 16, Columns::uuidAt, true));
        c.add(new Col("itv", Sem.INTERVAL, 0, 12, Columns::intervalAt, true));
        return c;
    }

    /// `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, which no parquet-java release up to 1.18.1 writes: the
    /// pinned one writes plain fixed-width bytes, and `derive_fixtures.py` adds the annotation and the
    /// bounds afterwards.
    static List<Col> ts12() {
        List<Col> c = new ArrayList<>();
        c.add(new Col("__row__", Sem.I64, 0, 0, row -> (long) row, false));
        c.add(new Col("zz", Sem.ZZ, 0, 0, row -> utf8("z"), false));
        c.add(new Col("ts12_ns", Sem.TS12_NS_UTC, 0, 12, row -> ts12(ts12Nanos(row), 1L), true));
        c.add(new Col("ts12_us_local", Sem.TS12_US_LOCAL, 0, 12, row -> ts12(ts12Nanos(row), 1_000L), true));
        c.add(new Col("ts12_ms", Sem.TS12_MS_UTC, 0, 12, row -> ts12(ts12Nanos(row), 1_000_000L), true));
        return c;
    }

    /// Columns DuckDB cannot read in the same file as the flat ones.
    static List<Col> exotic() {
        List<Col> c = new ArrayList<>();
        c.add(new Col("__row__", Sem.I64, 0, 0, row -> (long) row, false));
        c.add(new Col("zz", Sem.ZZ, 0, 0, row -> utf8("z"), false));
        c.add(new Col("bson", Sem.BSON, 0, 0, row -> ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(8).put((byte) 0x10).put((byte) (row >>> 8)).put((byte) row).put((byte) 0).array(), true));
        c.add(new Col("nul", Sem.NULL_I32, 0, 0, row -> null, true));
        c.add(new Col("geom", Sem.GEOM, 0, 0, row -> wkbPoint(offset(row), row), true));
        return c;
    }

    /// The flat columns over 40 distinct values, `NaN` payloads included, so that parquet-java keeps
    /// every chunk dictionary-encoded.
    static List<Col> lowCardinality() {
        List<Col> out = new ArrayList<>();
        for (Col col : flat()) {
            if (col.name().equals("__row__") || col.name().equals("zz")) {
                out.add(col);
                continue;
            }
            out.add(new Col(col.name(), col.sem(), col.scale(), col.width(),
                    row -> col.value().apply(lowCardinalityRow(row)), col.nullable()));
        }
        return out;
    }

    private static int lowCardinalityRow(int row) {
        int k = row % 40;
        return switch (k) {
            case 0 -> 10;
            case 1 -> 20;
            case 2 -> 30;
            default -> 290 + k;
        };
    }

    /// `columns` with the column semantics a footer rewrite gives them.
    static List<Col> remap(List<Col> columns, Map<String, Sem> semantics) {
        List<Col> out = new ArrayList<>();
        for (Col col : columns) {
            out.add(new Col(col.name(), semantics.getOrDefault(col.name(), col.sem()), col.scale(), col.width(),
                    col.value(), col.nullable()));
        }
        return out;
    }

    /// The flat columns as read from the converted-type-only variant: a legacy `TIMESTAMP_*` is UTC.
    static List<Col> legacy() {
        return remap(flat(), Map.of("ts_ms_local", Sem.TS_MS_UTC, "ts_us_local", Sem.TS_US_UTC));
    }

    /// The flat columns as read from the variant whose annotations the physical type cannot carry.
    static List<Col> dropped() {
        return remap(flat(), Map.of("uuid", Sem.FLBA, "str", Sem.BA));
    }

    // ==================== Nested ====================

    static boolean structNull(int row) {
        return row % 31 == 3;
    }

    static boolean innerStructNull(int row) {
        return structNull(row) || row % 43 == 17;
    }

    static Integer nestedX(int row) {
        return structNull(row) || row % 37 == 11 ? null : offset(row) * 2;
    }

    static String nestedName(int row) {
        return structNull(row) || row % 41 == 13 ? null : String.format("n%04d", row);
    }

    static Long nestedY(int row) {
        return innerStructNull(row) || row % 47 == 19 ? null : (long) row;
    }

    static long topLevelKey(int row) {
        return ROWS - 1 - row;
    }

    static long requiredKey(int row) {
        return row;
    }

    static String requiredName(int row) {
        return String.format("r%04d", row);
    }

    static boolean listNull(int row) {
        return row % 5 == 0;
    }

    /// The leaves of the nested fixture, keyed by path. Their value functions return `null` for a row
    /// where the leaf or a struct above it is null, so they carry no null mask of their own.
    static Map<String, Col> nestedLeaves() {
        return Map.of(
                "s.x", new Col("s.x", Sem.I32, 0, 0, Columns::nestedX, false),
                "s.name", new Col("s.name", Sem.STRING, 0, 0,
                        row -> nestedName(row) == null ? null : utf8(nestedName(row)), false),
                "s.t.y", new Col("s.t.y", Sem.I64, 0, 0, Columns::nestedY, false),
                "key", new Col("key", Sem.I64, 0, 0, Columns::topLevelKey, false),
                "r.key", new Col("r.key", Sem.I64, 0, 0, Columns::requiredKey, false),
                "r.name", new Col("r.name", Sem.STRING, 0, 0, row -> utf8(requiredName(row)), false));
    }
}
