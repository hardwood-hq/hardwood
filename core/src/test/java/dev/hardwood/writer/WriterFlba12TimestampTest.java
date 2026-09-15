/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Writing `TIMESTAMP` columns over `FIXED_LEN_BYTE_ARRAY(12)`: the stored count, the footer
/// annotations, the statistics order, and the values an `INT64` of the same unit cannot hold.
class WriterFlba12TimestampTest {

    private static final HexFormat HEX = HexFormat.of();

    private static final Instant YEAR_1 = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant BEFORE_EPOCH = Instant.parse("1969-12-31T23:59:59.999999999Z");
    private static final Instant YEAR_9999 = Instant.parse("9999-12-31T23:59:59.999999999Z");

    /// Every [Instant] round-trips at nanoseconds, including both ends of the `Instant` range.
    @Test
    void everyInstantRoundTripsAtNanoseconds() throws Exception {
        List<Instant> values = List.of(Instant.MIN, YEAR_1, BEFORE_EPOCH, Instant.EPOCH, YEAR_9999, Instant.MAX);
        FileSchema schema = fixed(LogicalType.timestamp(true, TimeUnit.NANOS));

        ByteBufferOutputFile out = writeRows(schema, values.stream()
                .<Consumer<StructBuilder>>map(value -> row -> row.setTimestamp("v", value)).toList());

        List<Instant> read = new ArrayList<>();
        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                read.add(rows.getTimestamp("v"));
            }
        }
        assertThat(read).isEqualTo(values);
    }

    /// The stored bytes are the count of the unit, two's complement, least significant byte first.
    @ParameterizedTest
    @EnumSource(TimeUnit.class)
    void storesTheLittleEndianCountOfTheUnit(TimeUnit unit) throws Exception {
        FileSchema schema = fixed(LogicalType.timestamp(false, unit));
        LocalDateTime oneUnitBeforeEpoch = LocalDateTime.parse("1970-01-01T00:00").minusNanos(switch (unit) {
            case MILLIS -> 1_000_000;
            case MICROS -> 1_000;
            case NANOS -> 1;
        });

        ByteBufferOutputFile out = writeRows(schema, List.of(
                row -> row.setLocalTimestamp("v", oneUnitBeforeEpoch),
                row -> row.setLocalTimestamp("v", LocalDateTime.parse("1970-01-01T00:00:01"))));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getBinary("v")).isEqualTo(HEX.parseHex("ffffffffffffffffffffffff"));
            assertThat(rows.getLocalTimestamp("v")).isEqualTo(oneUnitBeforeEpoch);
            rows.next();
            long oneSecond = switch (unit) {
                case MILLIS -> 1_000L;
                case MICROS -> 1_000_000L;
                case NANOS -> 1_000_000_000L;
            };
            assertThat(ByteBuffer.wrap(rows.getBinary("v")).order(ByteOrder.LITTLE_ENDIAN).getLong())
                    .isEqualTo(oneSecond);
        }
    }

    /// The footer carries the `TIMESTAMP` union member and no legacy converted type, which
    /// annotates an `INT64` only.
    @Test
    void theFooterCarriesNoConvertedType() throws Exception {
        FileSchema schema = fixed(LogicalType.timestamp(true, TimeUnit.MILLIS));

        ByteBufferOutputFile out = writeRows(schema, List.of(row -> row.setTimestamp("v", Instant.EPOCH)));

        try (ParquetFileReader reader = open(out)) {
            SchemaElement element = reader.getFileMetaData().schema().get(1);
            assertThat(element.logicalType()).isEqualTo(LogicalType.timestamp(true, TimeUnit.MILLIS));
            assertThat(element.convertedType()).isNull();
            assertThat(element.typeLength()).isEqualTo(12);
            assertThat(reader.getFileSchema().getColumn("v").logicalType())
                    .isEqualTo(LogicalType.timestamp(true, TimeUnit.MILLIS));
        }
    }

    /// Byte-wise the pre-epoch value sorts last and the year-1 value first by accident; in the
    /// order of the values year 1 is the minimum and year 9999 the maximum.
    @Test
    void boundsFollowTheSignedValue() throws Exception {
        FileSchema schema = fixed(LogicalType.timestamp(true, TimeUnit.NANOS));

        ByteBufferOutputFile out = writeRows(schema, List.of(
                row -> row.setTimestamp("v", BEFORE_EPOCH),
                row -> row.setTimestamp("v", YEAR_9999),
                row -> row.setTimestamp("v", YEAR_1),
                row -> row.setTimestamp("v", Instant.EPOCH)));

        try (ParquetFileReader reader = open(out)) {
            Statistics statistics = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData().statistics();
            assertThat(statistics.minValue()).isEqualTo(HEX.parseHex("00001a3deb03b2a1fcffffff"));
            assertThat(statistics.maxValue()).isEqualTo(HEX.parseHex("ffffaea2fed1a9bc0d000000"));

            // A predicate over those bounds keeps the row group for a value inside them.
            try (RowReader rows = reader.buildRowReader().filter(FilterPredicate.lt("v", Instant.EPOCH)).build()) {
                List<Instant> matched = new ArrayList<>();
                while (rows.hasNext()) {
                    rows.next();
                    matched.add(rows.getTimestamp("v"));
                }
                assertThat(matched).containsExactly(BEFORE_EPOCH, YEAR_1);
            }
        }
    }

    @Test
    void aValueFinerThanTheUnitFollowsThePrecisionLossPolicy() throws Exception {
        FileSchema schema = fixed(LogicalType.timestamp(true, TimeUnit.MILLIS));
        Instant finer = Instant.parse("1969-12-31T23:59:59.999999Z");

        assertThatThrownBy(() -> writeRows(schema, List.of(row -> row.setTimestamp("v", finer))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: value has finer precision than the column's TIMESTAMP(MILLIS) unit. "
                        + "Truncate it at the call site (for example Instant.truncatedTo(ChronoUnit.MILLIS)), "
                        + "declare the column at a finer unit, or configure "
                        + "WriterConfig.precisionLossPolicy(TRUNCATE)");

        WriterConfig truncating = WriterConfig.builder().precisionLossPolicy(PrecisionLossPolicy.TRUNCATE).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, truncating)) {
            writer.rowWriter().writeRow(row -> row.setTimestamp("v", finer));
        }
        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getTimestamp("v")).isEqualTo(Instant.parse("1969-12-31T23:59:59.999Z"));
        }
    }

    @Test
    void theSetterMustMatchTheUtcFlag() {
        FileSchema schema = fixed(LogicalType.timestamp(false, TimeUnit.MICROS));

        assertThatThrownBy(() -> writeRows(schema, List.of(row -> row.setTimestamp("v", Instant.EPOCH))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v is FIXED_LEN_BYTE_ARRAY annotated TIMESTAMP(MICROS, local); setTimestamp "
                        + "requires a column annotated TIMESTAMP with isAdjustedToUTC=true");
    }

    @Test
    void aListOfTimestampsRoundTrips() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, element -> element.primitive(PhysicalType.FIXED_LEN_BYTE_ARRAY,
                        RepetitionType.OPTIONAL, 12, LogicalType.timestamp(true, TimeUnit.NANOS)))
                .build();

        ByteBufferOutputFile out = writeRows(schema, List.of(
                row -> row.setList("v", list -> list.addTimestamp(YEAR_1).addTimestamp(null).addTimestamp(YEAR_9999))));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            PqList list = rows.getList("v");
            assertThat(list.timestamps()).containsExactly(YEAR_1, null, YEAR_9999);
        }
    }

    /// The column writer takes the stored bytes, and a count past `Instant` is written as given;
    /// only reading it as an `Instant` fails.
    @Test
    void theColumnWriterStoresTheBytesAsGiven() throws Exception {
        FileSchema schema = fixed(LogicalType.timestamp(true, TimeUnit.NANOS));
        byte[] pastInstant = HEX.parseHex("ffffffffffffffffffffff7f");

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, new byte[][] { pastInstant }));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getBinary("v")).isEqualTo(pastInstant);
            assertThatThrownBy(() -> rows.getTimestamp("v"))
                    .isInstanceOf(DateTimeException.class)
                    .hasMessage("[<memory>] The FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP 0xffffffffffffffffffffff7f "
                            + "(little-endian) lies outside the range of Instant");
        }
    }

    @Test
    void aTimestampIsDeclaredOnTwelveBytesOnly() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                        LogicalType.timestamp(true, TimeUnit.NANOS)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TIMESTAMP(NANOS, UTC) annotates a FIXED_LEN_BYTE_ARRAY of length 12, not 16 (column v)");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.timestamp(true, TimeUnit.NANOS)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TIMESTAMP(NANOS, UTC) annotates an INT64 or a FIXED_LEN_BYTE_ARRAY(12) column, "
                        + "not INT32 (column v)");
    }

    private static FileSchema fixed(LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 12, logicalType)
                .build();
    }

    private static ByteBufferOutputFile writeRows(FileSchema schema, List<Consumer<StructBuilder>> rows)
            throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rowWriter = writer.rowWriter();
            for (Consumer<StructBuilder> row : rows) {
                rowWriter.writeRow(row);
            }
        }
        return out;
    }

    private static ParquetFileReader open(ByteBufferOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
    }
}
