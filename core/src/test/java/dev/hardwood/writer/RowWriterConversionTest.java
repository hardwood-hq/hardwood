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
import java.time.ZoneOffset;
import java.util.List;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Logical-type value conversion in the row-oriented layer: what it accepts, and what it
/// rejects rather than rounding, truncating or wrapping.
class RowWriterConversionTest {

    // ==================== Rejections ====================

    @Test
    void instantFinerThanTheColumnUnitIsRejected() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.MILLIS));

        assertThatThrownBy(() -> write(schema,
                row -> row.setTimestamp("v", Instant.ofEpochSecond(1, 500_000))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: value has finer precision than the column's TIMESTAMP(MILLIS) unit. "
                         + "Truncate it at the call site (for example "
                         + "Instant.truncatedTo(ChronoUnit.MILLIS)), declare the column at a finer unit, "
                         + "or configure WriterConfig.precisionLossPolicy(TRUNCATE)")
                ;
    }

    @Test
    void timeFinerThanTheColumnUnitIsRejected() throws Exception {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.time(true, TimeUnit.MILLIS));

        assertThatThrownBy(() -> write(schema, row -> row.setTime("v", LocalTime.of(1, 2, 3, 4_000))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: value has finer precision than the column's TIME(MILLIS) unit. "
                         + "Truncate it at the call site (for example "
                         + "Instant.truncatedTo(ChronoUnit.MILLIS)), declare the column at a finer unit, "
                         + "or configure WriterConfig.precisionLossPolicy(TRUNCATE)");
    }

    @Test
    void timestampSetterMustMatchTheColumnsUtcAdjustment() throws Exception {
        FileSchema utc = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.MILLIS));
        FileSchema local = single(PhysicalType.INT64, LogicalType.timestamp(false, TimeUnit.MILLIS));

        assertThatThrownBy(() -> write(utc, row -> row.setLocalTimestamp("v", LocalDateTime.of(2026, 1, 1, 0, 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v is INT64 annotated TIMESTAMP(MILLIS, UTC); setLocalTimestamp requires "
                         + "an INT64 column annotated TIMESTAMP with isAdjustedToUTC=false");
        assertThatThrownBy(() -> write(local, row -> row.setTimestamp("v", Instant.EPOCH)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v is INT64 annotated TIMESTAMP(MILLIS, local); setTimestamp requires an "
                         + "INT64 column annotated TIMESTAMP with isAdjustedToUTC=true");
    }

    @Test
    void decimalNeedingARoundingRescaleIsRejected() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.decimal(18, 2));

        assertThatThrownBy(() -> write(schema, row -> row.setDecimal("v", new BigDecimal("1.005"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: 1.005 cannot be rescaled to the column's DECIMAL(18, 2) without "
                         + "dropping digits. Rescale it at the call site (for example "
                         + "BigDecimal.setScale(2, RoundingMode.HALF_UP)), declare a finer scale, or "
                         + "configure WriterConfig.precisionLossPolicy(TRUNCATE)");
    }

    @Test
    void decimalExceedingTheDeclaredPrecisionIsRejected() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.decimal(5, 2));

        assertThatThrownBy(() -> write(schema, row -> row.setDecimal("v", new BigDecimal("12345.67"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: 12345.67 has precision 7, exceeding the column's DECIMAL(5, 2)");
    }

    @Test
    void intValueOutsideANarrowAnnotationIsRejected() throws Exception {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.intType(8, true));

        assertThatThrownBy(() -> write(schema, row -> row.setInt("v", 200)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: 200 is out of range for a INT_8 column");
    }

    @Test
    void binaryOfTheWrongLengthForAFixedColumnIsRejected() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4)
                .build();

        assertThatThrownBy(() -> write(schema, row -> row.setBinary("v", new byte[] { 1, 2, 3 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: value is 3 bytes but the column is FIXED_LEN_BYTE_ARRAY(4)");
    }

    @Test
    void intervalComponentOutsideTheUnsignedRangeIsRejected() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 12,
                        LogicalType.interval())
                .build();

        assertThatThrownBy(() -> write(schema, row -> row.setInterval("v", new PqInterval(-1, 0, 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: INTERVAL months is -1, outside the unsigned 32-bit range the format "
                         + "stores");
    }

    // ==================== TRUNCATE ====================

    /// Under [PrecisionLossPolicy#TRUNCATE] the sub-unit digits are dropped rather than
    /// rejected. The instant floors, which is what [Instant#toEpochMilli()] does with it.
    @Test
    void truncatePolicyDropsSubUnitTimePrecision() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.MILLIS));
        Instant moment = Instant.ofEpochSecond(1_755_600_000L, 123_456_789);

        ByteBufferOutputFile out = write(schema, truncating(), row -> row.setTimestamp("v", moment));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getTimestamp("v")).isEqualTo(Instant.ofEpochMilli(moment.toEpochMilli()));
        }
    }

    /// The nanosecond-of-second the value carries is never negative, so a pre-epoch instant
    /// floors toward the past rather than toward the epoch — the same value the JDK produces.
    @Test
    void truncatePolicyFloorsAPreEpochInstant() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.MILLIS));
        Instant moment = Instant.ofEpochSecond(-1, 499_500_000);

        ByteBufferOutputFile out = write(schema, truncating(), row -> row.setTimestamp("v", moment));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getTimestamp("v").toEpochMilli()).isEqualTo(moment.toEpochMilli()).isEqualTo(-501L);
        }
    }

    @Test
    void truncatePolicyDropsSubUnitTimeOfDayPrecision() throws Exception {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.time(true, TimeUnit.MILLIS));

        ByteBufferOutputFile out = write(schema, truncating(),
                row -> row.setTime("v", LocalTime.of(1, 2, 3, 4_999_999)));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getTime("v")).isEqualTo(LocalTime.of(1, 2, 3, 4_000_000));
        }
    }

    /// A decimal drops the digits beyond the declared scale toward zero, so the written value
    /// is never larger in magnitude than the one handed over — in either direction.
    @Test
    void truncatePolicyDropsDecimalDigitsTowardZero() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.decimal(18, 2));

        assertThat(readDecimal(write(schema, truncating(), row -> row.setDecimal("v", new BigDecimal("1.009")))))
                .isEqualTo(new BigDecimal("1.00"));
        assertThat(readDecimal(write(schema, truncating(), row -> row.setDecimal("v", new BigDecimal("-1.009")))))
                .isEqualTo(new BigDecimal("-1.00"));
    }

    /// The policy governs precision, not magnitude: a value the column cannot represent at
    /// all is rejected under `TRUNCATE` too, because no narrowing would preserve it.
    @Test
    void truncatePolicyStillRejectsValuesTheColumnCannotRepresent() throws Exception {
        FileSchema decimal = single(PhysicalType.INT64, LogicalType.decimal(5, 2));
        FileSchema narrowInt = single(PhysicalType.INT32, LogicalType.intType(8, true));
        FileSchema fixed = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4)
                .build();

        assertThatThrownBy(() -> write(decimal, truncating(), row -> row.setDecimal("v", new BigDecimal("12345.67"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: 12345.67 has precision 7, exceeding the column's DECIMAL(5, 2)");
        assertThatThrownBy(() -> write(narrowInt, truncating(), row -> row.setInt("v", 200)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: 200 is out of range for a INT_8 column");
        assertThatThrownBy(() -> write(fixed, truncating(), row -> row.setBinary("v", new byte[] { 1, 2, 3 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: value is 3 bytes but the column is FIXED_LEN_BYTE_ARRAY(4)");
    }

    /// A `TIMESTAMP(NANOS)` column spans only about 1677 to 2262: an instant outside that is
    /// unrepresentable, not merely too precise. It is rejected the same way under either
    /// policy, and named as a range error rather than leaking the `long` overflow underneath.
    @Test
    void valueOutsideTheUnitsRangeIsRejectedUnderEitherPolicy() {
        FileSchema nanos = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.NANOS));
        FileSchema localNanos = single(PhysicalType.INT64, LogicalType.timestamp(false, TimeUnit.NANOS));
        Instant beyondNanos = LocalDateTime.of(2263, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);

        for (WriterConfig config : List.of(WriterConfig.defaults(), truncating())) {
            assertThatThrownBy(() -> write(nanos, config, row -> row.setTimestamp("v", beyondNanos)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field v: 2263-01-01T00:00:00Z is outside the range a TIMESTAMP(NANOS) "
                             + "column can represent")
                    ;
            assertThatThrownBy(() -> write(localNanos, config,
                    row -> row.setLocalTimestamp("v", LocalDateTime.of(2263, 1, 1, 0, 0))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field v: 2263-01-01T00:00 is outside the range a TIMESTAMP(NANOS) column "
                             + "can represent");
            // Instant.MAX carries sub-millisecond digits too; the magnitude is reported first,
            // so the same value fails the same way whatever the policy says about precision.
            assertThatThrownBy(() -> write(single(PhysicalType.INT64,
                    LogicalType.timestamp(true, TimeUnit.MILLIS)), config,
                    row -> row.setTimestamp("v", Instant.MAX)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field v: +1000000000-12-31T23:59:59.999999999Z is outside the range a "
                             + "TIMESTAMP(MILLIS) column can represent");
        }
    }

    /// The edges of a `TIMESTAMP(NANOS)` column are writable; only beyond them is rejected.
    @Test
    void theExtremesOfTheNanosRangeAreWritable() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.NANOS));
        Instant max = Instant.ofEpochSecond(Long.MAX_VALUE / 1_000_000_000L,
                Long.MAX_VALUE % 1_000_000_000L);

        ByteBufferOutputFile out = write(schema, row -> row.setTimestamp("v", max));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getTimestamp("v")).isEqualTo(max);
        }
    }

    @Test
    void rejectIsTheDefaultPolicy() {
        assertThat(WriterConfig.defaults().precisionLossPolicy()).isEqualTo(PrecisionLossPolicy.REJECT);
    }

    /// The rejection names both ways out, so the first caller to hand an `Instant.now()` to a
    /// `TIMESTAMP(MILLIS)` column learns what to do about it.
    @Test
    void rejectionNamesTheWaysOut() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.MILLIS));

        assertThatThrownBy(() -> write(schema, row -> row.setTimestamp("v", Instant.ofEpochSecond(1, 500_000))))
                .hasMessage("Field v: value has finer precision than the column's TIMESTAMP(MILLIS) unit. "
                         + "Truncate it at the call site (for example "
                         + "Instant.truncatedTo(ChronoUnit.MILLIS)), declare the column at a finer unit, "
                         + "or configure WriterConfig.precisionLossPolicy(TRUNCATE)")
                ;
    }

    // ==================== Accepted conversions ====================

    /// A rescale that loses nothing is performed rather than rejected, so a value carrying
    /// fewer decimals than the column declares is written at the column's scale.
    @Test
    void decimalIsRescaledWhenLossless() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.decimal(18, 2));

        ByteBufferOutputFile out = write(schema, row -> row.setDecimal("v", new BigDecimal("1234.5")));

        assertThat(readDecimal(out)).isEqualTo(new BigDecimal("1234.50"));
    }

    /// A byte-array-backed `DECIMAL` is big-endian two's complement, sign-extended to the
    /// column's width, so a negative value survives the round trip.
    @Test
    void negativeDecimalOnAFixedColumnSurvives() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 8,
                        LogicalType.decimal(18, 3))
                .build();

        ByteBufferOutputFile out = write(schema, row -> row.setDecimal("v", new BigDecimal("-42.125")));

        assertThat(readDecimal(out)).isEqualTo(new BigDecimal("-42.125"));
    }

    /// A narrow unsigned annotation is range-checked to the values it can actually hold. All
    /// 256 valid `UINT_8` values fit in `[0, 256)`, so nothing is unreachable — while writing
    /// 300 would produce a file whose values fall outside the range its own annotation
    /// declares, and which a `uint8` consumer reads as 44.
    @Test
    void narrowUnsignedAnnotationIsRangeChecked() throws Exception {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.intType(8, false));

        for (int accepted : new int[] { 0, 255 }) {
            try (ParquetFileReader reader = open(write(schema, row -> row.setInt("v", accepted)));
                 RowReader rows = reader.rowReader()) {
                rows.next();
                assertThat(rows.getInt("v")).isEqualTo(accepted);
            }
        }
        for (int rejected : new int[] { 256, 300, -1 }) {
            assertThatThrownBy(() -> write(schema, row -> row.setInt("v", rejected)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field v: " + rejected + " is out of range for a UINT_8 column");
        }
    }

    /// `UINT_32` keeps the raw two's-complement bits, which is what the reader returns for it:
    /// every bit pattern is a valid value of the column, and spelling one above
    /// `Integer.MAX_VALUE` as a negative `int` is the only way to reach it.
    @Test
    void unsignedIntTakesTheRawBits() throws Exception {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.intType(32, false));

        ByteBufferOutputFile out = write(schema, row -> row.setInt("v", (int) 4_000_000_000L));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(Integer.toUnsignedLong(rows.getInt("v"))).isEqualTo(4_000_000_000L);
        }
    }

    /// A physical setter works on an annotated column too, writing the stored value as it
    /// stands — the escape hatch alongside the logical setters, mirroring the reader.
    @Test
    void physicalSetterWritesTheStoredValueOfAnAnnotatedColumn() throws Exception {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.date());

        ByteBufferOutputFile out = write(schema, row -> row.setInt("v", 20_684));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getInt("v")).isEqualTo(20_684);
            assertThat(rows.getDate("v")).isEqualTo(LocalDate.ofEpochDay(20_684));
        }
    }

    @Test
    void nanosecondUnitsKeepTheirFullPrecision() throws Exception {
        FileSchema schema = single(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.NANOS));
        Instant moment = Instant.ofEpochSecond(1_755_600_000L, 123_456_789);

        ByteBufferOutputFile out = write(schema, row -> row.setTimestamp("v", moment));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getTimestamp("v")).isEqualTo(moment);
        }
    }

    @Test
    void uuidRoundTripsThroughItsSixteenBytes() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                        LogicalType.uuid())
                .build();
        UUID id = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

        ByteBufferOutputFile out = write(schema, row -> row.setUuid("v", id));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getUuid("v")).isEqualTo(id);
            assertThat(rows.getBinary("v")).startsWith((byte) 0x00, (byte) 0x11, (byte) 0x22, (byte) 0x33);
        }
    }

    private static FileSchema single(PhysicalType type, LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("v", type, RepetitionType.REQUIRED, logicalType)
                .build();
    }

    private static WriterConfig truncating() {
        return WriterConfig.builder().precisionLossPolicy(PrecisionLossPolicy.TRUNCATE).build();
    }

    private static ByteBufferOutputFile write(FileSchema schema, Consumer<StructBuilder> filler)
            throws Exception {
        return write(schema, WriterConfig.defaults(), filler);
    }

    private static ByteBufferOutputFile write(FileSchema schema, WriterConfig config,
            Consumer<StructBuilder> filler) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.rowWriter().writeRow(filler);
        }
        return out;
    }

    private static BigDecimal readDecimal(ByteBufferOutputFile out) throws Exception {
        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            return rows.getDecimal("v");
        }
    }

    private static ParquetFileReader open(ByteBufferOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
    }
}
