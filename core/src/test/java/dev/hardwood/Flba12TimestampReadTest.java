/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.conversion.Flba12Timestamps;
import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reading `TIMESTAMP` columns over `FIXED_LEN_BYTE_ARRAY(12)`, a signed two's complement
/// little-endian count of the unit since the epoch, written by PyArrow with the annotation added to
/// the footer.
///
/// The six rows are year 1, one nanosecond before the epoch, the epoch, a 2026 instant, the last
/// nanosecond of year 9999, and a null. Year 1 and year 9999 lie outside the `INT64` nanosecond
/// range.
class Flba12TimestampReadTest {

    private static final Path FILE = Paths.get("src/test/resources/flba12_timestamp_test.parquet");
    private static final Path STRUCT_FILE = Paths.get("src/test/resources/predicate/predicate_ts12_single.parquet");
    private static final Path PAGES_FILE = Paths.get("src/test/resources/predicate/predicate_ts12_pages.parquet");

    private static final Instant YEAR_1 = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant BEFORE_EPOCH = Instant.parse("1969-12-31T23:59:59.999999999Z");
    private static final Instant IN_2026 = Instant.parse("2026-08-13T12:34:56.123456789Z");
    private static final Instant YEAR_9999 = Instant.parse("9999-12-31T23:59:59.999999999Z");

    @Test
    void theSchemaKeepsTheAnnotation() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            ColumnSchema column = reader.getFileSchema().getColumn("utc_ns");
            assertThat(column.type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
            assertThat(column.typeLength()).isEqualTo(12);
            assertThat(column.logicalType()).isEqualTo(LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS));
        }
    }

    @Test
    void getTimestampReadsEveryUnit() throws IOException {
        assertThat(read(rows -> rows.getTimestamp("utc_ns")))
                .containsExactly(YEAR_1, BEFORE_EPOCH, Instant.EPOCH, IN_2026, YEAR_9999, null);
        assertThat(read(rows -> rows.getTimestamp("utc_ms"))).containsExactly(YEAR_1,
                Instant.parse("1969-12-31T23:59:59.999Z"), Instant.EPOCH, Instant.parse("2026-08-13T12:34:56.123Z"),
                Instant.parse("9999-12-31T23:59:59.999Z"), null);
    }

    @Test
    void getLocalTimestampReadsTheWallClock() throws IOException {
        assertThat(read(rows -> rows.getLocalTimestamp("local_us"))).containsExactly(
                LocalDateTime.parse("0001-01-01T00:00"),
                LocalDateTime.parse("1969-12-31T23:59:59.999999"),
                LocalDateTime.parse("1970-01-01T00:00"),
                LocalDateTime.parse("2026-08-13T12:34:56.123456"),
                LocalDateTime.parse("9999-12-31T23:59:59.999999"),
                null);
    }

    /// Where a value fits an `INT64` of nanoseconds, the two carriers of the annotation decode alike.
    @Test
    void bothCarriersAgree() throws IOException {
        List<Instant> fixed = read(rows -> rows.getTimestamp("utc_ns"));
        List<Instant> int64 = read(rows -> rows.getTimestamp("int64_ns"));
        assertThat(int64).containsExactly(null, fixed.get(1), fixed.get(2), fixed.get(3), null, null);
    }

    @Test
    void aDictionaryEncodedColumnDecodesItsEntries() throws IOException {
        assertThat(read(rows -> rows.getTimestamp("dict_us"))).containsExactly(Instant.EPOCH,
                Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH,
                Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH, null);
    }

    @Test
    void getValueDecodesAndGetRawValueKeepsTheBytes() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getValue("utc_ns")).isEqualTo(YEAR_1);
            assertThat(rows.getValue("local_us")).isEqualTo(LocalDateTime.parse("0001-01-01T00:00"));
            rows.next();
            byte[] minusOne = new byte[12];
            Arrays.fill(minusOne, (byte) 0xFF);
            assertThat(rows.getRawValue("utc_ns")).isEqualTo(minusOne);
            assertThat(rows.getBinary("utc_ns")).isEqualTo(minusOne);
        }
    }

    @Test
    void aListOfTimestampsDecodesEachElement() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqList first = rows.getList("list_ns");
            assertThat(first.timestamps()).containsExactly(YEAR_1, BEFORE_EPOCH, Instant.EPOCH);
            assertThat(first.values()).containsExactly(YEAR_1, BEFORE_EPOCH, Instant.EPOCH);
            rows.next();
            assertThat(rows.getList("list_ns").timestamps()).isEmpty();
            rows.next();
            assertThat(rows.getList("list_ns")).isNull();
            rows.next();
            assertThat(rows.getList("list_ns").timestamps()).containsExactly(IN_2026, YEAR_9999, null);
        }
    }

    /// A struct field reads through the nested path, as a top-level column reads through the flat one.
    @Test
    void aStructFieldDecodesThroughTheNestedPath() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT_FILE));
             RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("__row__", 3L)).build()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            PqStruct struct = rows.getStruct("s");
            assertThat(struct.getTimestamp("ts12")).isEqualTo(BEFORE_EPOCH);
            assertThat(struct.getValue("ts12")).isEqualTo(BEFORE_EPOCH);
            assertThat(rows.getTimestamp("ts12_ns")).isEqualTo(BEFORE_EPOCH);
        }
    }

    @Test
    void theColumnReaderReturnsTheStoredBytes() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader column = reader.buildColumnReader("utc_ms").build()) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getBinaries()[2]).isEqualTo(new byte[12]);
        }
    }

    /// The accessor pair splits on `isAdjustedToUTC` here as it does over `INT64`.
    @Test
    void theAccessorOfTheOtherKindIsRefused() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThatThrownBy(() -> rows.getLocalTimestamp("utc_ns"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Column 'utc_ns' is a UTC-adjusted TIMESTAMP "
                            + "(isAdjustedToUTC=true)");
            assertThatThrownBy(() -> rows.getTimestamp("local_us"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Column 'local_us' is a local-wall-clock TIMESTAMP "
                            + "(isAdjustedToUTC=false)");
        }
    }

    /// `predicate_ts12_pages` holds one value per page, so its page index is exact in any order
    /// and `PredicatePathAgreementTest` exercises page-level pruning on it. This pins that layout:
    /// each of the 389 present values has a page, bounded by that value; a null shares its page.
    @Test
    void thePagesFixtureBoundsEachPageByItsOneValue() throws IOException {
        byte[] file = Files.readAllBytes(PAGES_FILE);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PAGES_FILE))) {
            ColumnChunk chunk = reader.getFileMetaData().rowGroups().getFirst().columns()
                    .get(reader.getFileSchema().getColumn("ts12_ns").columnIndex());
            ColumnIndex index = ColumnIndexReader.read(new ThriftCompactReader(ByteBuffer.wrap(file,
                    Math.toIntExact(chunk.columnIndexOffset()), chunk.columnIndexLength())));

            assertThat(index.getPageCount()).isEqualTo(389);
            List<Instant> bounds = new ArrayList<>();
            for (int page = 0; page < index.getPageCount(); page++) {
                assertThat(index.nullPages()[page]).isFalse();
                assertThat(index.minValues().get(page)).isEqualTo(index.maxValues().get(page));
                bounds.add(Flba12Timestamps.toInstant(index.minValues().get(page), LogicalType.TimeUnit.NANOS));
            }
            assertThat(bounds).doesNotHaveDuplicates().contains(YEAR_9999, BEFORE_EPOCH);
        }
    }

    private static <T> List<T> read(Function<RowReader, T> accessor) throws IOException {
        List<T> values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                values.add(accessor.apply(rows));
            }
        }
        return values;
    }
}
