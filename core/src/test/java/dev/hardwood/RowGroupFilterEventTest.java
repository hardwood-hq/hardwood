/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts the two row-group filtering stages each emit their own JFR event exactly once
/// for a single-file read: `dev.hardwood.RowGroupByteRangeFilter` for byte-range split
/// selection (emitted by [ParquetFileReader]) and `dev.hardwood.RowGroupFilter` for
/// statistics predicate push-down (emitted by the row-group iterator).
///
/// Regression guard for #718: the column-readers path used to evaluate the first file's
/// statistics twice — once eagerly in [ParquetFileReader] and again in the iterator —
/// emitting two `RowGroupFilter` events whose second `totalRowGroups` reported the
/// already-pruned count rather than the file's real total.
class RowGroupFilterEventTest extends AbstractJfrRecorderTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final String BYTE_RANGE_EVENT = "dev.hardwood.RowGroupByteRangeFilter";
    private static final String PUSH_DOWN_EVENT = "dev.hardwood.RowGroupFilter";
    private static final int NULL_ROWS = 10;

    private static long fileLen;

    @BeforeAll
    static void readFixture() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(3);
        }
        fileLen = FIXTURE.toFile().length();
    }

    @Test
    void predicatePushDownEmitsSinglePushDownEventAndNoByteRangeEvent() throws Exception {
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(FilterPredicate.gt("id", 150L))
                .build());

        List<RecordedEvent> pushDown = events(PUSH_DOWN_EVENT).toList();
        assertThat(pushDown)
                .as("exactly one predicate-push-down event for the single file")
                .hasSize(1);
        assertThat(pushDown.get(0).getInt("totalRowGroups"))
                .as("total must be the file's real row-group count")
                .isEqualTo(3);
        assertThat(pushDown.getFirst().getInt("rowGroupsKept")).isEqualTo(2);
        assertThat(pushDown.getFirst().getInt("rowGroupsSkipped")).isEqualTo(1);

        assertThat(events(BYTE_RANGE_EVENT).count())
                .as("no byte-range event without a RowGroupPredicate")
                .isZero();
    }

    @Test
    void predicatePushDownReportsFullyMatchingRowGroups() throws Exception {
        // gt(150) skips RG1 (1-100), partially matches RG2 (101-200), and fully
        // matches RG3 (201-300): exactly one group must be reported as fully
        // matching. Pins that the ALWAYS_MATCHES decision actually fires — the
        // row-set correctness tests would still pass if it silently degraded to
        // MIGHT_MATCH.
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(FilterPredicate.gt("id", 150L))
                .build());

        List<RecordedEvent> pushDown = events(PUSH_DOWN_EVENT).toList();
        assertThat(pushDown).hasSize(1);
        assertThat(pushDown.getFirst().getInt("rowGroupsKept")).isEqualTo(2);
        assertThat(pushDown.getFirst().getInt("rowGroupsFullyMatching"))
                .as("only RG3 is proven fully matching; partial RG2 must not count")
                .isEqualTo(1);
    }

    @Test
    void boundaryAlignedCutoffLeavesNoPartiallyMatchingGroup() throws Exception {
        // gtEq(201) lands exactly on RG3's first id: RG1/RG2 drop outright, and
        // RG3's min satisfies the bound, so every *surviving* group is fully
        // matching and the read collapses onto the wholesale path. The complement
        // of predicatePushDownReportsFullyMatchingRowGroups, where an interior
        // cutoff leaves RG2 partial and forces the per-batch path.
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(FilterPredicate.gtEq("id", 201L))
                .build());

        RecordedEvent pushDown = events(PUSH_DOWN_EVENT).findFirst().orElseThrow();
        assertThat(pushDown.getInt("rowGroupsKept")).isEqualTo(1);
        assertThat(pushDown.getInt("rowGroupsFullyMatching"))
                .as("a boundary-aligned cutoff leaves no partially matching group")
                .isEqualTo(1);
    }

    @Test
    void byteRangeEmitsByteRangeEventNotPushDownEvent() throws Exception {
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(RowGroupPredicate.byteRange(0, fileLen))
                .build());

        assertThat(events(BYTE_RANGE_EVENT).count())
                .as("byte-range selection emits exactly one byte-range event")
                .isEqualTo(1);
        assertThat(events(PUSH_DOWN_EVENT).count())
                .as("no push-down event without a FilterPredicate")
                .isZero();
    }

    @Test
    void byteRangeAndPredicateEmitOneEventEach() throws Exception {
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(FilterPredicate.gt("id", 150L))
                .filter(RowGroupPredicate.byteRange(0, fileLen))
                .build());

        assertThat(events(BYTE_RANGE_EVENT).count())
                .as("one byte-range event")
                .isEqualTo(1);
        assertThat(events(PUSH_DOWN_EVENT).count())
                .as("statistics evaluated once: one push-down event")
                .isEqualTo(1);
    }

    @Test
    void isNullOnALeafNullOnEveryRowFullyMatches() throws Exception {
        // Every row's city is null: the address is absent on even rows and present without a city
        // on odd ones. The leaf's null count is the row count either way, which proves IS NULL on
        // every row.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, address -> address
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
                .build();
        byte[] file = writeRows(schema, rows -> {
            for (int i = 0; i < NULL_ROWS; i++) {
                int id = i;
                rows.writeRow(row -> {
                    row.setInt("id", id);
                    if (id % 2 == 1) {
                        row.setStruct("address", address -> address.setString("city", null));
                    }
                });
            }
        });

        // The column readers' record count is not asserted: they drop the rows where the address
        // is present (#1189).
        readFullyMatching(file, FilterPredicate.isNull("address.city"));
    }

    @Test
    void isNullOnAStructOfRequiredFieldsAbsentOnEveryRowFullyMatches() throws Exception {
        // No definition level separates `c` from `a`, so `a` is null exactly where `c` is absent
        // and its null count answers IS NULL on `c`.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("c", RepetitionType.OPTIONAL, c -> c
                        .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        byte[] file = writeRows(schema, rows -> {
            for (int i = 0; i < NULL_ROWS; i++) {
                int id = i;
                rows.writeRow(row -> row.setInt("id", id));
            }
        });

        assertThat(readFullyMatching(file, FilterPredicate.isNull("c")))
                .as("column readers return every row")
                .isEqualTo(NULL_ROWS);
    }

    /// Reads `file` under `filter` through a row reader and through column readers, asserts that
    /// the row reader returns every row and that each read reports its one row group fully
    /// matching, and returns the number of records the column readers returned.
    private int readFullyMatching(byte[] file, FilterPredicate filter) throws Exception {
        List<Integer> rowIds = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                rowIds.add(rows.getInt("id"));
            }
        }
        int columnRecords = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns("id"))
                     .filter(filter)
                     .build()) {
            while (cols.nextBatch()) {
                columnRecords += cols.getRecordCount();
            }
        }
        awaitEvents();

        assertThat(rowIds).containsExactlyElementsOf(IntStream.range(0, NULL_ROWS).boxed().toList());
        assertThat(events(PUSH_DOWN_EVENT).map(event -> event.getInt("rowGroupsFullyMatching")).toList())
                .as("one push-down event per read, each proving its one row group fully matching")
                .containsExactly(1, 1);
        return columnRecords;
    }

    private static byte[] writeRows(FileSchema schema, RowWrite filler) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            filler.accept(writer.rowWriter());
        }
        return out.toByteArray();
    }

    @FunctionalInterface
    private interface RowWrite {
        void accept(RowWriter rows) throws Exception;
    }

    /// Builds a [ColumnReaders] over the fixture via `build`, then stops the recording.
    /// Single-file row-group filtering (both the byte-range pre-filter and the iterator's
    /// statistics push-down) runs eagerly during `build`, so no read is needed to emit the
    /// events.
    private void readIdColumn(ColumnReadersFactory build) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReaders cols = build.create(reader)) {
            assertThat(cols.getColumnReader(0)).isNotNull();
        }
        awaitEvents();
    }

    @FunctionalInterface
    private interface ColumnReadersFactory {
        ColumnReaders create(ParquetFileReader reader) throws Exception;
    }
}
