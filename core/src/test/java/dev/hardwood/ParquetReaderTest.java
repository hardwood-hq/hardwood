/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetReaderTest {

    @Test
    void testReadPlainParquet() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Verify file metadata
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.version()).isEqualTo(2);
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            FileSchema schema = reader.getFileSchema();
            assertThat(schema).isNotNull();
            assertThat(schema.getColumnCount()).isEqualTo(2);

            // Verify column names and types
            ColumnSchema idColumn = schema.getColumn(0);
            assertThat(idColumn.name()).isEqualTo("id");
            assertThat(idColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(idColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            ColumnSchema valueColumn = schema.getColumn(1);
            assertThat(valueColumn.name()).isEqualTo("value");
            assertThat(valueColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(valueColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            // Read and verify 'id' column using batch API
            try (ColumnReader idReader = reader.columnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);
                assertThat(idReader.getValueCount()).isEqualTo(3);

                long[] idValues = idReader.getLongs();
                assertThat(idValues[0]).isEqualTo(1L);
                assertThat(idValues[1]).isEqualTo(2L);
                assertThat(idValues[2]).isEqualTo(3L);

                // No nulls for required column
                assertThat(idReader.getElementNulls()).isNull();

                // Flat column
                assertThat(idReader.getNestingDepth()).isEqualTo(0);

                assertThat(idReader.nextBatch()).isFalse();
            }

            // Read and verify 'value' column using batch API
            try (ColumnReader valueReader = reader.columnReader("value")) {
                assertThat(valueReader.nextBatch()).isTrue();
                assertThat(valueReader.getRecordCount()).isEqualTo(3);

                long[] valueValues = valueReader.getLongs();
                assertThat(valueValues[0]).isEqualTo(100L);
                assertThat(valueValues[1]).isEqualTo(200L);
                assertThat(valueValues[2]).isEqualTo(300L);

                assertThat(valueReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testReadPlainParquetWithNulls() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Verify file metadata
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.version()).isEqualTo(2);
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            FileSchema schema = reader.getFileSchema();
            assertThat(schema).isNotNull();
            assertThat(schema.getColumnCount()).isEqualTo(2);

            // Verify column names and types
            ColumnSchema idColumn = schema.getColumn(0);
            assertThat(idColumn.name()).isEqualTo("id");
            assertThat(idColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(idColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            ColumnSchema nameColumn = schema.getColumn(1);
            assertThat(nameColumn.name()).isEqualTo("name");
            assertThat(nameColumn.type()).isEqualTo(PhysicalType.BYTE_ARRAY);
            assertThat(nameColumn.repetitionType()).isEqualTo(RepetitionType.OPTIONAL);

            // Read and verify 'id' column (all non-null)
            try (ColumnReader idReader = reader.columnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);

                long[] idValues = idReader.getLongs();
                assertThat(idValues[0]).isEqualTo(1L);
                assertThat(idValues[1]).isEqualTo(2L);
                assertThat(idValues[2]).isEqualTo(3L);

                assertThat(idReader.nextBatch()).isFalse();
            }

            // Read and verify 'name' column (with one null)
            try (ColumnReader nameReader = reader.columnReader("name")) {
                assertThat(nameReader.nextBatch()).isTrue();
                assertThat(nameReader.getRecordCount()).isEqualTo(3);

                byte[][] nameValues = nameReader.getBinaries();
                BitSet nulls = nameReader.getElementNulls();

                // Verify: 'alice', null, 'charlie'
                assertThat(nulls).isNotNull();
                assertThat(nulls.get(0)).isFalse();
                assertThat(new String(nameValues[0])).isEqualTo("alice");
                assertThat(nulls.get(1)).isTrue(); // null
                assertThat(nulls.get(2)).isFalse();
                assertThat(new String(nameValues[2])).isEqualTo("charlie");

                assertThat(nameReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testReadSnappyCompressedParquet() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_snappy.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Verify file metadata
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.version()).isEqualTo(2);
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            FileSchema schema = reader.getFileSchema();
            assertThat(schema).isNotNull();
            assertThat(schema.getColumnCount()).isEqualTo(2);

            // Read and verify 'id' column - should be SNAPPY compressed
            assertThat(metadata.rowGroups().get(0).columns().get(0).metaData().codec())
                    .isEqualTo(CompressionCodec.SNAPPY);

            try (ColumnReader idReader = reader.columnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);

                long[] idValues = idReader.getLongs();
                assertThat(idValues[0]).isEqualTo(1L);
                assertThat(idValues[1]).isEqualTo(2L);
                assertThat(idValues[2]).isEqualTo(3L);

                assertThat(idReader.nextBatch()).isFalse();
            }

            // Read and verify 'value' column - should be SNAPPY compressed
            assertThat(metadata.rowGroups().get(0).columns().get(1).metaData().codec())
                    .isEqualTo(CompressionCodec.SNAPPY);

            try (ColumnReader valueReader = reader.columnReader("value")) {
                assertThat(valueReader.nextBatch()).isTrue();
                assertThat(valueReader.getRecordCount()).isEqualTo(3);

                long[] valueValues = valueReader.getLongs();
                assertThat(valueValues[0]).isEqualTo(100L);
                assertThat(valueValues[1]).isEqualTo(200L);
                assertThat(valueValues[2]).isEqualTo(300L);

                assertThat(valueReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testColumnReaderByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Read column by index
            try (ColumnReader idReader = reader.columnReader(0)) {
                assertThat(idReader.getColumnSchema().name()).isEqualTo("id");
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);

                long[] values = idReader.getLongs();
                assertThat(values[0]).isEqualTo(1L);
                assertThat(values[1]).isEqualTo(2L);
                assertThat(values[2]).isEqualTo(3L);

                assertThat(idReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testNestedColumnLookupByFieldPath() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileSchema schema = reader.getFileSchema();

            // Lookup nested columns by dot-separated field path
            ColumnSchema streetCol = schema.getColumn("address.street");
            assertThat(streetCol.name()).isEqualTo("street");
            assertThat(streetCol.fieldPath()).isEqualTo(FieldPath.of("address", "street"));

            ColumnSchema zipCol = schema.getColumn("address.zip");
            assertThat(zipCol.name()).isEqualTo("zip");
            assertThat(zipCol.type()).isEqualTo(PhysicalType.INT32);

            // Lookup by FieldPath object
            ColumnSchema cityCol = schema.getColumn(FieldPath.of("address", "city"));
            assertThat(cityCol.name()).isEqualTo("city");

            // Top-level column still works with plain name
            ColumnSchema idCol = schema.getColumn("id");
            assertThat(idCol.name()).isEqualTo("id");
            assertThat(idCol.fieldPath()).isEqualTo(FieldPath.of("id"));
        }
    }

    @Test
    void testNestedColumnReaderByFieldPath() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Read nested column by dot-path name
            // Data: address=[{street: "123 Main St", zip: 10001}, {street: "456 Oak Ave", zip: 90001}, null]
            try (ColumnReader zipReader = reader.columnReader("address.zip")) {
                assertThat(zipReader.getColumnSchema().name()).isEqualTo("zip");
                assertThat(zipReader.nextBatch()).isTrue();
                assertThat(zipReader.getRecordCount()).isEqualTo(3);

                int[] values = zipReader.getInts();
                assertThat(values[0]).isEqualTo(10001);
                assertThat(values[1]).isEqualTo(90001);

                // Row 3 has null address, so zip should be null
                assertThat(zipReader.getElementNulls().get(2)).isTrue();

                assertThat(zipReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testDuplicateLeafNamesResolveToDistinctColumns() throws Exception {
        // list_basic_test.parquet has tags (list<string>) and scores (list<int32>),
        // both with leaf name "element". Verifies that field-path-based lookup
        // disambiguates them correctly, and bare leaf name lookup fails.
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileSchema schema = reader.getFileSchema();

            // Both leaf columns are named "element" but have different field paths
            ColumnSchema tagsElement = schema.getColumn("tags.list.element");
            ColumnSchema scoresElement = schema.getColumn("scores.list.element");

            assertThat(tagsElement.name()).isEqualTo("element");
            assertThat(scoresElement.name()).isEqualTo("element");
            assertThat(tagsElement.type()).isEqualTo(PhysicalType.BYTE_ARRAY);
            assertThat(scoresElement.type()).isEqualTo(PhysicalType.INT32);
            assertThat(tagsElement.columnIndex()).isNotEqualTo(scoresElement.columnIndex());

            // Bare leaf name "element" must not resolve — it's ambiguous
            assertThatThrownBy(() -> schema.getColumn("element"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void negativeMaxRowsReturnsTailAndSkipsEarlierRowGroups() throws Exception {
        // filter_pushdown_int.parquet has three row groups of 100 rows each:
        // RG0: id 1-100, RG1: id 101-200, RG2: id 201-300.
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Tail of 10 fits inside RG2 so the reader must skip RG0 and RG1
            // entirely and yield exactly ids 291..300.
            try (RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).tail(10L).build()) {
                long firstId = Long.MAX_VALUE;
                long lastId = Long.MIN_VALUE;
                long count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    long id = rows.getLong(0);
                    firstId = Math.min(firstId, id);
                    lastId = Math.max(lastId, id);
                    count++;
                }
                assertThat(count).isEqualTo(10);
                assertThat(firstId).isEqualTo(291L);
                assertThat(lastId).isEqualTo(300L);
            }
        }
    }

    @Test
    void negativeMaxRowsSpansMultipleRowGroupsWhenNeeded() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Tail of 150 crosses the RG1/RG2 boundary: reader must include RG1
            // and RG2 but skip RG0, and trim the 50 leading rows of RG1.
            try (RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).tail(150L).build()) {
                long firstId = Long.MAX_VALUE;
                long lastId = Long.MIN_VALUE;
                long count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    long id = rows.getLong(0);
                    firstId = Math.min(firstId, id);
                    lastId = Math.max(lastId, id);
                    count++;
                }
                assertThat(count).isEqualTo(150);
                assertThat(firstId).isEqualTo(151L);
                assertThat(lastId).isEqualTo(300L);
            }
        }
    }

    @Test
    void negativeMaxRowsLargerThanFileReadsAllRows() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).tail(10_000L).build()) {
                long count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(300);
            }
        }
    }

    @Test
    void zeroMaxRowsIsRejected() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThatThrownBy(() -> reader.buildRowReader().projection(ColumnProjection.all()).head(0L).build())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void negativeMaxRowsWithFilterIsRejected() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThatThrownBy(() -> reader.buildRowReader().projection(ColumnProjection.all()).filter(dev.hardwood.reader.FilterPredicate.gt("id", 0L)).tail(10L).build())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void firstRowSkipsEarlierRowGroups() throws Exception {
        // filter_pushdown_int.parquet: 3 row groups of 100 rows each — id 1..100, 101..200, 201..300.
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().firstRow(150).build()) {
            long firstId = -1;
            long lastId = -1;
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                long id = rows.getLong("id");
                if (firstId < 0) {
                    firstId = id;
                }
                lastId = id;
                count++;
            }
            // firstRow=150 lands inside RG 1 (rows 100..199, ids 101..200)
            // at within-RG offset 50 → first id yielded is 151.
            assertThat(count).as("rows from firstRow=150").isEqualTo(150);
            assertThat(firstId).as("first id at firstRow=150").isEqualTo(151L);
            assertThat(lastId).as("last id at file end").isEqualTo(300L);
        }
    }

    @Test
    void firstRowAtRowGroupBoundary() throws Exception {
        // firstRow=100 lands at the exact start of RG 1.
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().firstRow(100).build()) {
            int count = 0;
            long firstId = -1;
            while (rows.hasNext()) {
                rows.next();
                if (firstId < 0) {
                    firstId = rows.getLong("id");
                }
                count++;
            }
            assertThat(count).isEqualTo(200);
            assertThat(firstId).isEqualTo(101L);
        }
    }

    @Test
    void firstRowComposesWithHead() throws Exception {
        // firstRow=150 + head(20) → rows 150..169 → ids 151..170.
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().firstRow(150).head(20).build()) {
            long firstId = -1;
            long lastId = -1;
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                long id = rows.getLong("id");
                if (firstId < 0) {
                    firstId = id;
                }
                lastId = id;
                count++;
            }
            assertThat(count).isEqualTo(20);
            assertThat(firstId).isEqualTo(151L);
            assertThat(lastId).isEqualTo(170L);
        }
    }

    @Test
    void firstRowAtTotalRowsYieldsEmpty() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            long total = reader.getFileMetaData().numRows();
            try (RowReader rows = reader.buildRowReader().firstRow(total).build()) {
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void firstRowRejectsNegative() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThatThrownBy(() -> reader.buildRowReader().firstRow(-1).build())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void firstRowAndTailAreMutuallyExclusive() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThatThrownBy(() -> reader.buildRowReader().firstRow(100).tail(10L).build())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

}
