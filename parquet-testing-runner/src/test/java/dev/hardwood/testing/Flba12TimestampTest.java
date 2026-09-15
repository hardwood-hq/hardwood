/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `flba12_timestamp.parquet`: three `TIMESTAMP` columns over `FIXED_LEN_BYTE_ARRAY(12)`, one per
/// unit, written by a parquet-java development build (1.18.0-SNAPSHOT). Each holds the six instants its
/// `flba12_timestamp.md` documents; at nanoseconds the last two lie outside the `INT64` range.
///
/// No parquet-java release up to 1.18.1 reads the file, so this holds Hardwood to the documented values
/// rather than to a reference reader, and holds Hardwood's writer to the bytes and bounds the file
/// stores for the same values.
class Flba12TimestampTest {

    private static final String FIXTURE = "data/flba12_timestamp.parquet";

    private static final List<String> COLUMNS = List.of("timestamp_millis", "timestamp_micros", "timestamp_nanos");

    private static final List<Instant> VALUES = List.of(
            Instant.parse("1970-01-01T00:00:00Z"),
            Instant.parse("1970-01-01T00:00:01Z"),
            Instant.parse("1969-12-31T23:59:59Z"),
            Instant.parse("2262-04-11T23:47:16Z"),
            Instant.parse("9999-12-31T23:59:59Z"),
            Instant.parse("0001-01-01T00:00:00Z"));

    @Test
    void everyColumnHoldsTheDocumentedInstants() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture()));
             RowReader rows = reader.rowReader()) {
            for (String column : COLUMNS) {
                assertThat(reader.getFileSchema().getColumn(column).logicalType()).as(column)
                        .isInstanceOf(LogicalType.TimestampType.class);
            }
            List<List<Instant>> read = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
            while (rows.hasNext()) {
                rows.next();
                for (int i = 0; i < COLUMNS.size(); i++) {
                    read.get(i).add(rows.getTimestamp(COLUMNS.get(i)));
                    assertThat(rows.getValue(COLUMNS.get(i))).isEqualTo(read.get(i).getLast());
                }
            }
            for (int i = 0; i < COLUMNS.size(); i++) {
                assertThat(read.get(i)).as(COLUMNS.get(i)).isEqualTo(VALUES);
            }
        }
    }

    /// The bounds span year 1 to year 9999 in the order of the values.
    @Test
    void theBoundsPruneInTheOrderOfTheValues() throws IOException {
        for (String column : COLUMNS) {
            assertThat(countMatching(FilterPredicate.gt(column, Instant.parse("9999-12-31T23:59:59Z")))).isZero();
            assertThat(countMatching(FilterPredicate.lt(column, Instant.parse("0001-01-01T00:00:00Z")))).isZero();
            assertThat(countMatching(FilterPredicate.lt(column, Instant.EPOCH))).as(column).isEqualTo(2);
            assertThat(countMatching(FilterPredicate.in(column, Instant.parse("2262-04-11T23:47:16Z"),
                    Instant.parse("1969-12-31T23:59:59Z")))).as(column).isEqualTo(2);
        }
    }

    /// Hardwood writes the same values as the bytes parquet-java stored, with the same bounds and
    /// the same footer annotation.
    @Test
    void hardwoodWritesWhatParquetJavaWrote(@TempDir Path dir) throws IOException {
        Path written = dir.resolve("flba12_timestamp.parquet");
        FileSchema.Builder schema = FileSchema.builder("flba12_timestamp");
        for (String column : COLUMNS) {
            schema.addColumn(column, PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 12,
                    fixtureLogicalType(column));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(written), schema.build(),
                WriterConfig.defaults())) {
            RowWriter rows = writer.rowWriter();
            for (Instant value : VALUES) {
                rows.writeRow(row -> {
                    for (String column : COLUMNS) {
                        row.setTimestamp(column, value);
                    }
                });
            }
        }

        try (ParquetFileReader expected = ParquetFileReader.open(InputFile.of(fixture()));
             ParquetFileReader actual = ParquetFileReader.open(InputFile.of(written));
             RowReader expectedRows = expected.rowReader();
             RowReader actualRows = actual.rowReader()) {
            for (int i = 0; i < COLUMNS.size(); i++) {
                assertThat(actual.getFileMetaData().schema().get(i + 1))
                        .as(COLUMNS.get(i))
                        .isEqualTo(expected.getFileMetaData().schema().get(i + 1));
                Statistics expectedStatistics = chunk(expected, i).metaData().statistics();
                Statistics actualStatistics = chunk(actual, i).metaData().statistics();
                assertThat(actualStatistics.minValue()).as(COLUMNS.get(i)).isEqualTo(expectedStatistics.minValue());
                assertThat(actualStatistics.maxValue()).as(COLUMNS.get(i)).isEqualTo(expectedStatistics.maxValue());
            }
            while (expectedRows.hasNext()) {
                assertThat(actualRows.hasNext()).isTrue();
                expectedRows.next();
                actualRows.next();
                for (String column : COLUMNS) {
                    assertThat(actualRows.getBinary(column)).as(column).isEqualTo(expectedRows.getBinary(column));
                }
            }
            assertThat(actualRows.hasNext()).isFalse();
        }
    }

    /// Why this module reads the fixture without parquet-java, and why the writer's coverage cells
    /// for the carrier are waived: parquet-java refuses the annotation while parsing the footer. A
    /// parquet-java that reads the carrier fails here, and the waivers in [CoverageWaivers] and the
    /// skip in [Utils] come off.
    @Test
    void parquetJavaRejectsTheAnnotation() {
        assertThatThrownBy(() -> ParquetJavaReader.readFooter(fixture()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("TIMESTAMP(MILLIS,true) can only annotate INT64");
    }

    private static LogicalType fixtureLogicalType(String column) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture()))) {
            return reader.getFileSchema().getColumn(column).logicalType();
        }
    }

    private static ColumnChunk chunk(ParquetFileReader reader, int column) {
        return reader.getFileMetaData().rowGroups().getFirst().columns().get(column);
    }

    private static long countMatching(FilterPredicate filter) throws IOException {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture()));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private static Path fixture() throws IOException {
        return ParquetTestingRepoCloner.getTestFile(FIXTURE);
    }
}
