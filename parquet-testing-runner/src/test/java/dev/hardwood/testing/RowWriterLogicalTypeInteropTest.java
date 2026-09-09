/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.parquet.example.data.Group;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.StructBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/// The interop gate over the row-oriented layer's **logical-type value conversion**: a Java
/// value goes in through a `RowWriter` setter, and parquet-java reads back the physical value
/// an independent implementation says it should be.
///
/// This is the only place `PhysicalValueConverter` faces an oracle it did not write itself.
/// Hardwood's own round trips decode with `LogicalTypeConverter`, whose inverse it is, so any
/// error the two share — a byte order, a unit scale, a sign extension — survives them all.
/// The expectations here come from Avro's conversions where they exist (a separate
/// implementation of the same spec, on the classpath through `parquet-avro`) and from the
/// value's own definition spelled out in full where they do not.
class RowWriterLogicalTypeInteropTest {

    private static final String COLUMN = "v";

    private static final LocalDate DATE = LocalDate.of(2026, 8, 19);
    private static final LocalTime TIME = LocalTime.of(13, 45, 30, 250_000_000);
    private static final Instant INSTANT = Instant.parse("2026-08-19T13:45:30.250Z");
    private static final LocalDateTime LOCAL = LocalDateTime.of(2026, 8, 19, 13, 45, 30, 250_000_000);
    private static final UUID UUID_VALUE = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

    /// A pre-epoch instant and a negative decimal: the cases where a sign error survives every
    /// same-implementation round trip.
    private static final Instant PRE_EPOCH = Instant.parse("1962-03-04T05:06:07.008Z");
    private static final BigDecimal NEGATIVE = new BigDecimal("-42.125");

    /// One conversion under test: the column Hardwood declares, the value written through the
    /// row API, and the physical value an independent implementation expects on disk.
    record Case(String name, PhysicalType physicalType, Integer typeLength, LogicalType annotation,
            Consumer<StructBuilder> write, Object expected) {

        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Case> conversions() {
        Conversions.DecimalConversion decimals = new Conversions.DecimalConversion();
        Schema fixed8 = Schema.createFixed("d", null, null, 8);
        Schema bytes = Schema.create(Schema.Type.BYTES);
        LogicalTypes.Decimal decimal18_3 = LogicalTypes.decimal(18, 3);

        return Stream.of(
                new Case("STRING", PhysicalType.BYTE_ARRAY, null, LogicalType.string(),
                        row -> row.setString(COLUMN, "héllo"),
                        "héllo".getBytes(StandardCharsets.UTF_8)),

                new Case("DATE", PhysicalType.INT32, null, LogicalType.date(),
                        row -> row.setDate(COLUMN, DATE),
                        new TimeConversions.DateConversion().toInt(DATE, null, null)),
                new Case("DATE pre-epoch", PhysicalType.INT32, null, LogicalType.date(),
                        row -> row.setDate(COLUMN, LocalDate.of(1962, 3, 4)),
                        new TimeConversions.DateConversion().toInt(LocalDate.of(1962, 3, 4), null, null)),

                new Case("TIME(MILLIS)", PhysicalType.INT32, null,
                        LogicalType.time(true, TimeUnit.MILLIS),
                        row -> row.setTime(COLUMN, TIME),
                        new TimeConversions.TimeMillisConversion().toInt(TIME, null, null)),
                new Case("TIME(MICROS)", PhysicalType.INT64, null,
                        LogicalType.time(true, TimeUnit.MICROS),
                        row -> row.setTime(COLUMN, TIME),
                        new TimeConversions.TimeMicrosConversion().toLong(TIME, null, null)),
                // Avro carries no NANOS time conversion, so the definition is spelled out.
                new Case("TIME(NANOS)", PhysicalType.INT64, null,
                        LogicalType.time(true, TimeUnit.NANOS),
                        row -> row.setTime(COLUMN, TIME),
                        TIME.toNanoOfDay()),

                new Case("TIMESTAMP(MILLIS, UTC)", PhysicalType.INT64, null,
                        LogicalType.timestamp(true, TimeUnit.MILLIS),
                        row -> row.setTimestamp(COLUMN, INSTANT),
                        new TimeConversions.TimestampMillisConversion().toLong(INSTANT, null, null)),
                new Case("TIMESTAMP(MICROS, UTC)", PhysicalType.INT64, null,
                        LogicalType.timestamp(true, TimeUnit.MICROS),
                        row -> row.setTimestamp(COLUMN, INSTANT),
                        new TimeConversions.TimestampMicrosConversion().toLong(INSTANT, null, null)),
                new Case("TIMESTAMP(MILLIS, UTC) pre-epoch", PhysicalType.INT64, null,
                        LogicalType.timestamp(true, TimeUnit.MILLIS),
                        row -> row.setTimestamp(COLUMN, PRE_EPOCH),
                        new TimeConversions.TimestampMillisConversion().toLong(PRE_EPOCH, null, null)),
                new Case("TIMESTAMP(MICROS, UTC) pre-epoch", PhysicalType.INT64, null,
                        LogicalType.timestamp(true, TimeUnit.MICROS),
                        row -> row.setTimestamp(COLUMN, PRE_EPOCH),
                        new TimeConversions.TimestampMicrosConversion().toLong(PRE_EPOCH, null, null)),
                new Case("TIMESTAMP(NANOS, UTC)", PhysicalType.INT64, null,
                        LogicalType.timestamp(true, TimeUnit.NANOS),
                        row -> row.setTimestamp(COLUMN, INSTANT),
                        INSTANT.getEpochSecond() * 1_000_000_000L + INSTANT.getNano()),

                new Case("TIMESTAMP(MILLIS, local)", PhysicalType.INT64, null,
                        LogicalType.timestamp(false, TimeUnit.MILLIS),
                        row -> row.setLocalTimestamp(COLUMN, LOCAL),
                        new TimeConversions.LocalTimestampMillisConversion().toLong(LOCAL, null, null)),
                new Case("TIMESTAMP(MICROS, local)", PhysicalType.INT64, null,
                        LogicalType.timestamp(false, TimeUnit.MICROS),
                        row -> row.setLocalTimestamp(COLUMN, LOCAL),
                        new TimeConversions.LocalTimestampMicrosConversion().toLong(LOCAL, null, null)),

                new Case("DECIMAL(9,2) on INT32", PhysicalType.INT32, null,
                        LogicalType.decimal(9, 2),
                        row -> row.setDecimal(COLUMN, new BigDecimal("-1234.56")),
                        new BigDecimal("-1234.56").unscaledValue().intValueExact()),
                new Case("DECIMAL(18,2) on INT64", PhysicalType.INT64, null,
                        LogicalType.decimal(18, 2),
                        row -> row.setDecimal(COLUMN, new BigDecimal("-1234.56")),
                        new BigDecimal("-1234.56").unscaledValue().longValueExact()),
                new Case("DECIMAL(18,3) on FIXED_LEN_BYTE_ARRAY(8)", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                        LogicalType.decimal(18, 3),
                        row -> row.setDecimal(COLUMN, NEGATIVE),
                        decimals.toFixed(NEGATIVE, fixed8, decimal18_3).bytes()),
                new Case("DECIMAL(18,3) on BYTE_ARRAY", PhysicalType.BYTE_ARRAY, null,
                        LogicalType.decimal(18, 3),
                        row -> row.setDecimal(COLUMN, NEGATIVE),
                        remaining(decimals.toBytes(NEGATIVE, bytes, decimal18_3))),

                // Avro spells a UUID as a string, so the 16 big-endian bytes are laid out here.
                new Case("UUID", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, LogicalType.uuid(),
                        row -> row.setUuid(COLUMN, UUID_VALUE),
                        ByteBuffer.allocate(16)
                                .putLong(UUID_VALUE.getMostSignificantBits())
                                .putLong(UUID_VALUE.getLeastSignificantBits())
                                .array()),
                // INTERVAL has no Avro equivalent: three little-endian unsigned 4-byte fields.
                new Case("INTERVAL", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, LogicalType.interval(),
                        row -> row.setInterval(COLUMN, new PqInterval(14, 3, 7_200_000)),
                        ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                                .putInt(14).putInt(3).putInt(7_200_000)
                                .array()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conversions")
    void rowWrittenValueCarriesTheBytesAnIndependentImplementationExpects(Case testCase, @TempDir Path dir)
            throws IOException {
        FileSchema schema = declare(testCase);

        Path file = dir.resolve("row-logical.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(testCase.write());
            // A second, identical record so the column is dictionary-encoded with one entry —
            // the shape that has bitten this writer before.
            rows.writeRow(testCase.write());
        }

        List<Group> rows = ParquetJavaReader.readGroups(file);
        assertThat(rows).hasSize(2);
        for (Group row : rows) {
            assertThat(physical(row, testCase)).isEqualTo(testCase.expected());
        }
    }

    private static FileSchema declare(Case testCase) {
        FileSchema.Builder builder = FileSchema.builder("row-logical");
        if (testCase.typeLength() == null) {
            return builder.addColumn(COLUMN, testCase.physicalType(), RepetitionType.REQUIRED,
                    testCase.annotation()).build();
        }
        return builder.addColumn(COLUMN, testCase.physicalType(), RepetitionType.REQUIRED,
                testCase.typeLength(), testCase.annotation()).build();
    }

    /// The stored value as parquet-java hands it back, without any logical-type decoding.
    private static Object physical(Group row, Case testCase) {
        return switch (testCase.physicalType()) {
            case INT32 -> row.getInteger(COLUMN, 0);
            case INT64 -> row.getLong(COLUMN, 0);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> row.getBinary(COLUMN, 0).getBytes();
            default -> throw new IllegalStateException("No accessor for " + testCase.physicalType());
        };
    }

    private static byte[] remaining(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }
}
