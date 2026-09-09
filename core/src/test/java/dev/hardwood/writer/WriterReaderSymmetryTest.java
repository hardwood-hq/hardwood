/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Every value the writer accepts is readable back as the identical value.
///
/// The round-trip tests elsewhere write ordinary values; these write the extremes of what each
/// annotation admits, which is where the writer's accepted range and the reader's materializable
/// range would part company. A value the writer takes but the reader cannot decode is a file
/// this project can produce and not read — the defect class the annotation range checks exist to
/// close, and these assertions are what pins that the two ranges are the same one.
class WriterReaderSymmetryTest {

    @Test
    void narrowIntegerExtremesMaterializeIdentically() throws Exception {
        assertIntExtremes(LogicalType.intType(8, true), -128, 127);
        assertIntExtremes(LogicalType.intType(8, false), 0, 255);
        assertIntExtremes(LogicalType.intType(16, true), -32_768, 32_767);
        assertIntExtremes(LogicalType.intType(16, false), 0, 65_535);
    }

    /// The full unsigned widths keep the raw two's-complement bits, which is the reader's
    /// contract for them: the caller widens, and the widened value is the one that was written.
    @Test
    void fullWidthUnsignedValuesKeepTheirBitPattern() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("i", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.intType(32, false))
                .addColumn("l", PhysicalType.INT64, RepetitionType.REQUIRED, LogicalType.intType(64, false))
                .build();

        ByteBufferOutputFile out = write(schema, batch -> batch
                .ints("i", new int[] { (int) 4_000_000_000L })
                .longs("l", new long[] { -1L }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(Integer.toUnsignedLong(rows.getInt("i"))).isEqualTo(4_000_000_000L);
            assertThat(Long.toUnsignedString(rows.getLong("l"))).isEqualTo("18446744073709551615");
        }
    }

    /// Every `int32` is a `DATE` and every `int64` is a `TIMESTAMP` of any unit, which is why
    /// neither is range-checked — asserted here at the extremes rather than assumed.
    @Test
    void temporalExtremesMaterializeIdentically() throws Exception {
        assertDate(Integer.MIN_VALUE);
        assertDate(Integer.MAX_VALUE);
        for (TimeUnit unit : TimeUnit.values()) {
            assertTimestamp(unit, Long.MIN_VALUE);
            assertTimestamp(unit, Long.MAX_VALUE);
            assertLocalTimestamp(unit, Long.MIN_VALUE);
            assertLocalTimestamp(unit, Long.MAX_VALUE);
        }
    }

    /// A `TIME` is bounded to its day, and both ends of that day materialize: the last value in
    /// range is the last instant `LocalTime` holds, so the writer's bound and the reader's are
    /// the same bound.
    @Test
    void timeExtremesMaterializeIdentically() throws Exception {
        assertTime(TimeUnit.MILLIS, PhysicalType.INT32, 0, LocalTime.MIDNIGHT);
        assertTime(TimeUnit.MILLIS, PhysicalType.INT32, 86_399_999, LocalTime.of(23, 59, 59, 999_000_000));
        assertTime(TimeUnit.MICROS, PhysicalType.INT64, 86_399_999_999L, LocalTime.of(23, 59, 59, 999_999_000));
        assertTime(TimeUnit.NANOS, PhysicalType.INT64, 86_399_999_999_999L, LocalTime.of(23, 59, 59, 999_999_999));
    }

    /// A `DECIMAL` reads back as the unscaled value that was written, at the widest precision
    /// each storage admits.
    @Test
    void decimalExtremesMaterializeIdentically() throws Exception {
        assertDecimal(single(PhysicalType.INT32, LogicalType.decimal(9, 2)),
                batch -> batch.ints(0, new int[] { -999_999_999, 999_999_999 }),
                new BigDecimal("-9999999.99"), new BigDecimal("9999999.99"));
        assertDecimal(single(PhysicalType.INT64, LogicalType.decimal(18, 4)),
                batch -> batch.longs(0, new long[] { -999_999_999_999_999_999L, 999_999_999_999_999_999L }),
                new BigDecimal("-99999999999999.9999"), new BigDecimal("99999999999999.9999"));
        assertDecimal(single(PhysicalType.BYTE_ARRAY, LogicalType.decimal(20, 2)),
                batch -> batch.bytes(0, new byte[][] {
                        new BigDecimal("-999999999999999999.99").unscaledValue().toByteArray(),
                        new BigDecimal("999999999999999999.99").unscaledValue().toByteArray() }),
                new BigDecimal("-999999999999999999.99"), new BigDecimal("999999999999999999.99"));
    }

    /// The fixed-width annotations carry their whole byte range: a `UUID` of all ones, an
    /// `INTERVAL` whose three unsigned components are at their maximum, and a `FLOAT16` `NaN`.
    @Test
    void fixedWidthAnnotationExtremesMaterializeIdentically() throws Exception {
        byte[] allOnes16 = new byte[16];
        Arrays.fill(allOnes16, (byte) 0xff);
        FileSchema uuid = fixed(16, LogicalType.uuid());

        try (ParquetFileReader reader = open(write(uuid, batch -> batch.fixed(0, new byte[][] { allOnes16 })));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getUuid("v")).isEqualTo(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));
            assertThat(rows.getBinary("v")).isEqualTo(allOnes16);
        }

        byte[] allOnes12 = new byte[12];
        Arrays.fill(allOnes12, (byte) 0xff);
        FileSchema interval = fixed(12, LogicalType.interval());

        try (ParquetFileReader reader = open(write(interval, batch -> batch.fixed(0, new byte[][] { allOnes12 })));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getInterval("v")).isEqualTo(new PqInterval(4_294_967_295L, 4_294_967_295L, 4_294_967_295L));
        }

        FileSchema float16 = fixed(2, LogicalType.float16());
        try (ParquetFileReader reader = open(write(float16,
                batch -> batch.fixed(0, new byte[][] { { 0x00, 0x3c }, { (byte) 0xff, (byte) 0xff } })));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getFloat("v")).isEqualTo(1.0f);
            rows.next();
            assertThat(rows.getFloat("v")).isNaN();
        }
    }

    /// A `STRING` written from a `String` reads back as that `String`, including the values that
    /// stress UTF-8: the empty string, and a code point outside the basic plane.
    @Test
    void stringValuesMaterializeIdentically() throws Exception {
        FileSchema schema = single(PhysicalType.BYTE_ARRAY, LogicalType.string());
        String astral = "🪵";

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setString("v", ""));
            rows.writeRow(row -> row.setString("v", astral));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getString("v")).isEmpty();
            rows.next();
            assertThat(rows.getString("v")).isEqualTo(astral);
        }
    }

    // ==================== Helpers ====================

    private static void assertIntExtremes(LogicalType.IntType annotation, int min, int max) throws Exception {
        FileSchema schema = single(PhysicalType.INT32, annotation);

        ByteBufferOutputFile out = write(schema, batch -> batch.ints(0, new int[] { min, max }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getInt("v")).as("%s min", annotation).isEqualTo(min);
            assertThat(((Number) rows.getValue("v")).intValue()).as("%s min value", annotation).isEqualTo(min);
            rows.next();
            assertThat(rows.getInt("v")).as("%s max", annotation).isEqualTo(max);
            assertThat(((Number) rows.getValue("v")).intValue()).as("%s max value", annotation).isEqualTo(max);
        }
    }

    private static void assertDate(int epochDay) throws Exception {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.date());

        ByteBufferOutputFile out = write(schema, batch -> batch.ints(0, new int[] { epochDay }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getDate("v")).as("day %d", epochDay).isEqualTo(LocalDate.ofEpochDay(epochDay));
            assertThat(rows.getInt("v")).isEqualTo(epochDay);
        }
    }

    private static void assertTimestamp(TimeUnit unit, long stored) throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(true, unit));

        ByteBufferOutputFile out = write(schema, batch -> batch.longs(0, new long[] { stored }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            Instant instant = rows.getTimestamp("v");
            assertThat(instant).as("TIMESTAMP(%s) %d", unit, stored).isNotNull();
            assertThat(rows.getLong("v")).isEqualTo(stored);
        }
    }

    private static void assertLocalTimestamp(TimeUnit unit, long stored) throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(false, unit));

        ByteBufferOutputFile out = write(schema, batch -> batch.longs(0, new long[] { stored }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            LocalDateTime wall = rows.getLocalTimestamp("v");
            assertThat(wall).as("local TIMESTAMP(%s) %d", unit, stored).isNotNull();
            assertThat(rows.getLong("v")).isEqualTo(stored);
        }
    }

    private static void assertTime(TimeUnit unit, PhysicalType type, long stored, LocalTime expected)
            throws Exception {
        FileSchema schema = single(type, LogicalType.time(true, unit));

        ByteBufferOutputFile out = write(schema, batch -> {
            if (type == PhysicalType.INT32) {
                batch.ints(0, new int[] { Math.toIntExact(stored) });
            }
            else {
                batch.longs(0, new long[] { stored });
            }
        });

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getTime("v")).as("TIME(%s) %d", unit, stored).isEqualTo(expected);
        }
    }

    private static void assertDecimal(FileSchema schema, Consumer<ColumnBatch> filler, BigDecimal min,
            BigDecimal max) throws Exception {
        ByteBufferOutputFile out = write(schema, filler);

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getDecimal("v")).isEqualByComparingTo(min);
            rows.next();
            assertThat(rows.getDecimal("v")).isEqualByComparingTo(max);
        }
    }

    private static FileSchema single(PhysicalType type, LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("v", type, RepetitionType.REQUIRED, logicalType)
                .build();
    }

    private static FileSchema fixed(int typeLength, LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, typeLength, logicalType)
                .build();
    }

    private static ByteBufferOutputFile write(FileSchema schema, Consumer<ColumnBatch> filler) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(filler);
        }
        return out;
    }

    private static ParquetFileReader open(ByteBufferOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
    }
}
