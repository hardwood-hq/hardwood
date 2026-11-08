/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SortingColumn;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.metadata.SortingColumn.ascending;
import static dev.hardwood.metadata.SortingColumn.descending;
import static dev.hardwood.metadata.SortingColumn.NullOrder.NULLS_FIRST;
import static dev.hardwood.metadata.SortingColumn.NullOrder.NULLS_LAST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `sorting_columns`: the declaration reaches every row group's footer entry, rows in the
/// declared order are written, and the first row out of it fails the write.
///
/// The footer is read through parquet-format's own Thrift classes rather than Hardwood's reader,
/// which does not surface `sorting_columns`.
class WriterSortingColumnsTest {

    private static final FileSchema TWO_LONGS = FileSchema.builder("schema")
            .addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("b", PhysicalType.INT64, RepetitionType.OPTIONAL)
            .build();

    // ==================== Footer ====================

    @Test
    void everyRowGroupRecordsTheDeclaredOrder() throws Exception {
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(descending("b", NULLS_LAST), ascending("a", NULLS_FIRST))
                .rowGroupTargetRows(2)
                .build();

        FileMetaData footer = footerOf(write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[] { 1, 2, 3, 4, 5 })
                .longs(1, new long[] { 9, 9, 7, 7, 0 }, new boolean[] { false, false, false, false, true })));

        assertThat(footer.getRow_groups()).hasSize(3);
        for (RowGroup rowGroup : footer.getRow_groups()) {
            assertThat(rowGroup.getSorting_columns()).containsExactly(
                    new SortingColumn(1, true, false),
                    new SortingColumn(0, false, true));
        }
    }

    @Test
    void noDeclarationWritesNoSortingColumns() throws Exception {
        FileMetaData footer = footerOf(write(TWO_LONGS, WriterConfig.defaults(), batch -> batch
                .longs(0, new long[] { 2, 1 })
                .longs(1, new long[] { 1, 2 })));

        assertThat(footer.getRow_groups().get(0).isSetSorting_columns()).isFalse();
    }

    @Test
    void sortingColumnIndexIsTheLeafIndexOfANestedColumn() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, group -> group
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("y", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("s.y", NULLS_FIRST)).build();

        // A null struct makes its leaves null, and nulls sort first here.
        FileMetaData footer = footerOf(write(schema, config, batch -> batch
                .ints(0, new int[] { 1, 2, 3, 4 })
                .struct("s", Validity.ofNulls(new boolean[] { true, false, false, false }))
                .ints("s.x", new int[] { 0, 0, 0, 0 })
                .ints("s.y", new int[] { 0, 0, 5, 6 }, new boolean[] { false, true, false, false })));

        assertThat(footer.getRow_groups().get(0).getSorting_columns())
                .containsExactly(new SortingColumn(2, false, true));
    }

    // ==================== Accepted orders ====================

    @Test
    void acceptsRowsInLexicographicOrderWithTiesAndNulls() throws Exception {
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST), descending("b", NULLS_LAST))
                .build();

        // b descends within each run of equal a and restarts with every new a.
        write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[] { 1, 1, 1, 2, 2, 3 })
                .longs(1, new long[] { 9, 5, 0, 7, 7, 1 }, new boolean[] { false, false, true, false, false, false }));
    }

    @Test
    void eachRowGroupIsCheckedOnItsOwn() throws Exception {
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST))
                .rowGroupTargetRows(3)
                .build();

        // Sorted within each row group of three; the second group starts below where the first
        // ended, a cut in the middle of the batch.
        write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[] { 7, 8, 9, 1, 2, 3 })
                .longs(1, new long[] { 0, 0, 0, 0, 0, 0 }));
    }

    @Test
    void anExplicitRowGroupEndStartsTheOrderAgain() throws Exception {
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("a", NULLS_FIRST)).build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, TWO_LONGS, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs(0, new long[] { 5, 6 }).longs(1, new long[2]));
            writer.endRowGroup();
            writer.columnWriter().writeBatch(batch -> batch.longs(0, new long[] { 1, 2 }).longs(1, new long[2]));
        }
        assertThat(footerOf(out).getRow_groups()).hasSize(2);
    }

    // ==================== Rejected orders ====================

    @Test
    void rejectsARowBelowItsPredecessor() {
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("a", NULLS_FIRST)).build();

        assertThatThrownBy(() -> write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[] { 1, 3, 2 })
                .longs(1, new long[3])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 2 of row group 0 sorts before row 1 on column 'a', against the declared sorting "
                        + "columns [a ASC NULLS FIRST]");
    }

    @Test
    void rejectsASecondColumnOutOfOrderWithinATieOfTheFirst() {
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST), descending("b", NULLS_FIRST))
                .build();

        assertThatThrownBy(() -> write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[] { 1, 2, 2 })
                .longs(1, new long[] { 0, 4, 5 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 2 of row group 0 sorts before row 1 on column 'b', against the declared sorting "
                        + "columns [a ASC NULLS FIRST, b DESC NULLS FIRST]");
    }

    @Test
    void reportsTheFirstRowOutOfOrderWhicheverColumnItDiffersOn() {
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST), ascending("b", NULLS_FIRST))
                .build();

        // Row 2 breaks the order on b within a tie on a; row 3 breaks it on a.
        assertThatThrownBy(() -> write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[] { 1, 2, 2, 1 })
                .longs(1, new long[] { 0, 5, 4, 0 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 2 of row group 0 sorts before row 1 on column 'b', against the declared sorting "
                        + "columns [a ASC NULLS FIRST, b ASC NULLS FIRST]");
    }

    @Test
    void checksTiesAcrossMoreRowsThanOneReadOfTheSource() {
        // 5,000 rows read 1,024 at a time: a runs of 7 equal values straddle the reads, b ascends
        // within each run, and the one row out of order sits past the fourth read.
        int n = 5_000;
        long[] a = new long[n];
        long[] b = new long[n];
        for (int i = 0; i < n; i++) {
            a[i] = i / 7;
            b[i] = i % 7;
        }
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST), ascending("b", NULLS_FIRST))
                .build();
        write(TWO_LONGS, config, batch -> batch.longs(0, a).longs(1, b));

        // Row 4604 is the sixth of a's run starting at row 4599; 0 there falls below row 4603's 4.
        b[4_604] = 0;
        assertThatThrownBy(() -> write(TWO_LONGS, config, batch -> batch.longs(0, a).longs(1, b)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 4604 of row group 0 sorts before row 4603 on column 'b', against the declared "
                        + "sorting columns [a ASC NULLS FIRST, b ASC NULLS FIRST]");
    }

    @Test
    void rejectsANullOnTheWrongSide() {
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("b", NULLS_LAST)).build();

        assertThatThrownBy(() -> write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[3])
                .longs(1, new long[] { 0, 1, 2 }, new boolean[] { true, false, false })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'b', against the declared sorting "
                        + "columns [b ASC NULLS LAST]");
    }

    @Test
    void rejectsARowBelowTheLastRowOfThePreviousBatch() throws Exception {
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("a", NULLS_FIRST)).build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, TWO_LONGS, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs(0, new long[] { 5, 6 }).longs(1, new long[2]));
            assertThatThrownBy(() -> writer.columnWriter()
                    .writeBatch(batch -> batch.longs(0, new long[] { 4 }).longs(1, new long[1])))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Row 2 of row group 0 sorts before row 1 on column 'a', against the declared "
                            + "sorting columns [a ASC NULLS FIRST]");
        }
    }

    @Test
    void numbersTheRowGroupTheViolationIsIn() {
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST))
                .rowGroupTargetRows(2)
                .build();

        assertThatThrownBy(() -> write(TWO_LONGS, config, batch -> batch
                .longs(0, new long[] { 1, 2, 3, 4, 6, 5 })
                .longs(1, new long[6])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 2 sorts before row 0 on column 'a', against the declared sorting "
                        + "columns [a ASC NULLS FIRST]");
    }

    @Test
    void aViolationThroughTheRowWriterFailsTheWriterAndDiscardsTheFile(@TempDir Path dir) throws Exception {
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("a", NULLS_FIRST)).build();
        Path file = dir.resolve("sorted.parquet");

        // The row writer stages rows and submits them as a batch, here on close.
        assertThatThrownBy(() -> {
            try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), TWO_LONGS, config)) {
                RowWriter rows = writer.rowWriter();
                rows.writeRow(row -> row.setLong("a", 2).setNull("b"));
                rows.writeRow(row -> row.setLong("a", 1).setNull("b"));
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'a', against the declared sorting "
                        + "columns [a ASC NULLS FIRST]");
        assertThat(Files.exists(file)).isFalse();
    }

    // ==================== Column orders ====================

    @Test
    void comparesAnUnsignedIntegerAsUnsigned() {
        FileSchema schema = oneColumn(PhysicalType.INT32, null, LogicalType.intType(32, false));
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        // -1 is UINT32's maximum.
        write(schema, config, batch -> batch.ints(0, new int[] { 1, 2, -1 }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.ints(0, new int[] { -1, 1 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void comparesAnUnsignedLongAsUnsigned() {
        FileSchema schema = oneColumn(PhysicalType.INT64, null, LogicalType.intType(64, false));
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        write(schema, config, batch -> batch.longs(0, new long[] { 1, Long.MAX_VALUE, Long.MIN_VALUE, -1 }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.longs(0, new long[] { -1, 1 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void comparesAFixedLengthDecimalByItsNumber() {
        FileSchema schema = oneColumn(PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, LogicalType.decimal(9, 0));
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        write(schema, config, batch -> batch.fixed(0, new byte[][] { fixedDecimal(-2), fixedDecimal(-1),
                fixedDecimal(0), fixedDecimal(70_000) }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.fixed(0,
                new byte[][] { fixedDecimal(0), fixedDecimal(-1) })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void comparesATwelveByteTimestampByItsInstant() throws Exception {
        FileSchema schema = oneColumn(PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS));
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();
        Instant early = Instant.parse("1969-12-31T23:59:59Z");
        Instant late = Instant.parse("2026-11-15T00:00:00Z");

        // A pre-epoch instant is a negative count, which unsigned bytes would put last.
        InMemoryOutputFile ok = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(ok, schema, config)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setTimestamp("v", early));
            rows.writeRow(row -> row.setTimestamp("v", late));
        }
        assertThatThrownBy(() -> {
            try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema, config)) {
                RowWriter rows = writer.rowWriter();
                rows.writeRow(row -> row.setTimestamp("v", late));
                rows.writeRow(row -> row.setTimestamp("v", early));
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void comparesADecimalByItsNumber() {
        FileSchema schema = oneColumn(PhysicalType.BYTE_ARRAY, null, LogicalType.decimal(10, 0));
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        // Unsigned byte order would put -1 (0xFF) after 1.
        write(schema, config, batch -> batch.bytes(0, new byte[][] { decimal(-300), decimal(-1), decimal(1),
                decimal(300) }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.bytes(0,
                new byte[][] { decimal(1), decimal(-1) })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void comparesAStringByteByByte() {
        FileSchema schema = oneColumn(PhysicalType.BYTE_ARRAY, null, LogicalType.string());
        WriterConfig config = WriterConfig.builder().sortingColumns(descending("v", NULLS_FIRST)).build();

        // "é" is 0xC3 0xA9, above every ASCII byte; a prefix sorts before what extends it.
        write(schema, config, batch -> batch.bytes(0, utf8("é", "z", "ab", "a", "")));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.bytes(0, utf8("a", "ab"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v DESC NULLS FIRST]");
    }

    @Test
    void sortsNaNAfterEveryNumberAndNegativeZeroWithZero() {
        FileSchema schema = oneColumn(PhysicalType.DOUBLE, null, null);
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        write(schema, config, batch -> batch.doubles(0, new double[] { Double.NEGATIVE_INFINITY, 0.0, -0.0, 0.0,
                Double.POSITIVE_INFINITY, Double.NaN, Double.NaN }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.doubles(0,
                new double[] { Double.NaN, Double.POSITIVE_INFINITY })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void comparesAFloatAsAFloat() {
        FileSchema schema = oneColumn(PhysicalType.FLOAT, null, null);
        WriterConfig config = WriterConfig.builder().sortingColumns(descending("v", NULLS_FIRST)).build();

        write(schema, config, batch -> batch.floats(0, new float[] { Float.NaN, 2.5f, 0.0f, -0.0f, -2.5f }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.floats(0, new float[] { -2.5f, 2.5f })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v DESC NULLS FIRST]");
    }

    @Test
    void comparesAFloat16AsAFloat() {
        FileSchema schema = oneColumn(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, LogicalType.float16());
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        write(schema, config, batch -> batch.fixed(0, new byte[][] { half(-2.0f), half(-0.0f), half(0.0f),
                half(1.5f), half(Float.NaN) }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.fixed(0,
                new byte[][] { half(1.5f), half(-2.0f) })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void comparesABooleanFalseFirst() {
        FileSchema schema = oneColumn(PhysicalType.BOOLEAN, null, null);
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        write(schema, config, batch -> batch.booleans(0, new boolean[] { false, false, true }));
        assertThatThrownBy(() -> write(schema, config, batch -> batch.booleans(0, new boolean[] { true, false })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared sorting "
                        + "columns [v ASC NULLS FIRST]");
    }

    @Test
    void keepsAStringPastTheBatchThatHeldIt() throws Exception {
        // The caller reuses its array for the next batch; the order is still checked against
        // the value the first batch held.
        FileSchema schema = oneColumn(PhysicalType.BYTE_ARRAY, null, LogicalType.string());
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();
        byte[] reused = "m".getBytes(StandardCharsets.UTF_8);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, new byte[][] { reused }));
            reused[0] = 'a';
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.bytes(0, utf8("b"))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Row 1 of row group 0 sorts before row 0 on column 'v', against the declared "
                            + "sorting columns [v ASC NULLS FIRST]");
        }
    }

    // ==================== Configuration ====================

    @Test
    void rejectsAColumnTheSchemaDoesNotHave() {
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("c", NULLS_FIRST)).build();

        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.inMemory(), TWO_LONGS, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sorting column configured for column 'c', which the schema does not have. Its leaf "
                        + "columns are: [a, b]");
    }

    @Test
    void rejectsAColumnInsideAList() {
        FileSchema schema = FileSchema.builder("schema")
                .list("l", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("l.list.element", NULLS_FIRST))
                .build();

        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.inMemory(), schema, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sorting column 'l.list.element' is repeated or inside a list or map; a sorting column "
                        + "holds one value per row.");
    }

    @Test
    void rejectsAColumnWhoseTypeHasNoOrder() {
        FileSchema schema = oneColumn(PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, LogicalType.interval());
        WriterConfig config = WriterConfig.builder().sortingColumns(ascending("v", NULLS_FIRST)).build();

        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.inMemory(), schema, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sorting column 'v' is " + LogicalType.interval() + ", whose values have no defined "
                        + "order.");
    }

    @Test
    void rejectsAColumnDeclaredTwice() {
        assertThatThrownBy(() -> WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST), descending("a", NULLS_LAST)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'a' is declared more than once among the sorting columns "
                        + "[a ASC NULLS FIRST, a DESC NULLS LAST]");
    }

    @Test
    void rejectsANullSortingColumn() {
        assertThatThrownBy(() -> WriterConfig.builder().sortingColumns(ascending("a", NULLS_FIRST), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("sortingColumns must not contain null: [a ASC NULLS FIRST, null]");
        assertThatThrownBy(() -> ascending("a", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nullOrder must not be null for sorting column a");
        assertThatThrownBy(() -> descending(null, NULLS_FIRST))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columnPath must not be null");
    }

    @Test
    void aLaterDeclarationReplacesAnEarlierOne() {
        WriterConfig config = WriterConfig.builder()
                .sortingColumns(ascending("a", NULLS_FIRST))
                .sortingColumns(List.of())
                .build();

        assertThat(config.sortingColumns()).isEmpty();
        assertThat(WriterConfig.defaults().sortingColumns()).isEmpty();
    }

    // ==================== Helpers ====================

    private interface BatchFiller {
        void fill(ColumnBatch batch);
    }

    private static InMemoryOutputFile write(FileSchema schema, WriterConfig config, BatchFiller filler) {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(filler::fill);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    private static FileSchema oneColumn(PhysicalType type, Integer typeLength, LogicalType logicalType) {
        FileSchema.Builder builder = FileSchema.builder("schema");
        if (typeLength == null) {
            builder.addColumn("v", type, RepetitionType.REQUIRED, col -> col.logicalType(logicalType));
        }
        else {
            builder.addColumn("v", type, RepetitionType.REQUIRED,
                    col -> col.typeLength(typeLength).logicalType(logicalType));
        }
        return builder.build();
    }

    private static FileMetaData footerOf(InMemoryOutputFile out) throws Exception {
        ByteBuffer file = out.buffer().order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = file.getInt(file.limit() - 8);
        byte[] footer = new byte[footerLength];
        file.get(file.limit() - 8 - footerLength, footer);
        return Util.readFileMetaData(new ByteArrayInputStream(footer));
    }

    private static byte[] fixedDecimal(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static byte[] decimal(long value) {
        return BigInteger.valueOf(value).toByteArray();
    }

    private static byte[][] utf8(String... values) {
        byte[][] bytes = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = values[i].getBytes(StandardCharsets.UTF_8);
        }
        return bytes;
    }

    private static byte[] half(float value) {
        short bits = Float.floatToFloat16(value);
        return new byte[] { (byte) bits, (byte) (bits >>> 8) };
    }
}
