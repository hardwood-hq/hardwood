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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.DoubleDictionary;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The dictionary ids [ColumnReader] exposes for flat numeric columns: each id resolves through
/// the typed dictionary to the value the value accessor returns, the values of batches read
/// through their ids alone are there when read later, and a batch holding a value stored outside
/// one dictionary exposes no ids.
class ColumnReaderNumericDictionaryIdsTest {

    /// One row group of 4,000 rows. `fallback`: dictionary pages of 1.5/2.5/3.5 below row 2,000,
    /// distinct values `10_000 + row` after, which the writer stored plain once the dictionary was
    /// full. `temperature`: optional, [#POOL] cycling, null where `row % 7 == 3` or
    /// `row % 500` is `0` or `499`. `plain`: [#POOL] cycling, without a dictionary.
    private static final Path FILE = Paths.get("src/test/resources/dict_numeric_ids.parquet");

    private static final double[] POOL = { -12.5, 0.0, 3.25, 17.75, 41.0 };

    private static Double temperature(int row) {
        return row % 7 == 3 || row % 500 == 0 || row % 500 == 499 ? null : POOL[row % 5];
    }

    private static double fallback(int row) {
        return row < 2_000 ? new double[]{ 1.5, 2.5, 3.5 }[row % 3] : 10_000.0 + row;
    }

    @Test
    void eachIdResolvesToItsValueAndANullHasIdMinusOne() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("temperature").batchSize(64).build()) {
            int row = 0;
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIds();
                assertThat(ids).hasSize(col.getValueCount());
                DoubleDictionary dictionary = col.getDoubleDictionary();
                double[] values = col.getDoubles();
                for (int i = 0; i < col.getValueCount(); i++, row++) {
                    Double expected = temperature(row);
                    if (expected == null) {
                        assertThat(ids[i]).as("row %d", row).isEqualTo(-1);
                        assertThat(col.getLeafValidity().isNull(i)).isTrue();
                    }
                    else {
                        assertThat(dictionary.getDouble(ids[i])).as("row %d", row).isEqualTo(expected);
                        assertThat(values[i]).as("row %d", row).isEqualTo(expected);
                    }
                }
            }
            assertThat(row).isEqualTo(4_000);
        }
    }

    /// Batches read through their ids alone leave their values unwritten; values read after the
    /// reader switched to them are each row's.
    @Test
    void valuesReadAfterIdsOnlyBatchesAreEachRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("temperature").batchSize(64).build()) {
            int row = 0;
            int batch = 0;
            while (col.nextBatch()) {
                if (batch++ < 20) {
                    int[] ids = col.getDictionaryIds();
                    for (int i = 0; i < col.getValueCount(); i++) {
                        Double expected = temperature(row + i);
                        assertThat(ids[i] < 0 ? null : col.getDoubleDictionary().getDouble(ids[i])).isEqualTo(expected);
                    }
                }
                double[] values = col.getDoubles();
                assertThat(values).hasSize(col.getValueCount());
                for (int i = 0; i < col.getValueCount(); i++, row++) {
                    Double expected = temperature(row);
                    if (expected != null) {
                        assertThat(values[i]).as("row %d", row).isEqualTo(expected);
                    }
                }
            }
            assertThat(row).isEqualTo(4_000);
        }
    }

    /// A reader that reads values before it asks for ids gets the ids of every batch it is handed
    /// ids for right, and every batch's values.
    @Test
    void idsAskedForAfterValuesResolveWhereABatchHasThem() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("temperature").batchSize(64).build()) {
            int row = 0;
            while (col.nextBatch()) {
                double[] values = col.getDoubles();
                int[] ids = col.getDictionaryIds();
                for (int i = 0; i < col.getValueCount(); i++, row++) {
                    Double expected = temperature(row);
                    if (expected != null) {
                        assertThat(values[i]).as("row %d", row).isEqualTo(expected);
                    }
                    if (ids != null) {
                        assertThat(ids[i] < 0 ? null : col.getDoubleDictionary().getDouble(ids[i])).isEqualTo(expected);
                    }
                }
            }
            assertThat(row).isEqualTo(4_000);
        }
    }

    @Test
    void aBatchHoldingPlainValuesExposesNoIds() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("fallback").batchSize(500).build()) {
            List<Boolean> hasIds = new ArrayList<>();
            int row = 0;
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIds();
                hasIds.add(ids != null);
                if (ids == null) {
                    assertThat(col.getDoubleDictionary()).isNull();
                }
                for (int i = 0; i < col.getValueCount(); i++, row++) {
                    double actual = ids != null ? col.getDoubleDictionary().getDouble(ids[i]) : col.getDoubles()[i];
                    assertThat(actual).as("row %d", row).isEqualTo(fallback(row));
                }
            }
            assertThat(hasIds).containsExactly(true, true, true, true, true, false, false, false);
        }
    }

    /// Batches of 300 rows run across the pages of 500, so the batch where the dictionary ends
    /// holds both dictionary and plain pages. A reader of ids alone meets that batch with the
    /// values of both written.
    @Test
    void aBatchMixingDictionaryAndPlainPagesHasEveryValue() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("fallback").batchSize(300).build()) {
            int row = 0;
            int withoutIds = 0;
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIds();
                if (ids != null) {
                    for (int i = 0; i < col.getValueCount(); i++) {
                        assertThat(col.getDoubleDictionary().getDouble(ids[i])).isEqualTo(fallback(row + i));
                    }
                    row += col.getValueCount();
                    continue;
                }
                withoutIds++;
                double[] values = col.getDoubles();
                for (int i = 0; i < col.getValueCount(); i++, row++) {
                    assertThat(values[i]).as("row %d", row).isEqualTo(fallback(row));
                }
            }
            assertThat(row).isEqualTo(4_000);
            assertThat(withoutIds).isEqualTo(6);
        }
    }

    @Test
    void batchesOfOneDictionaryShareItsObjectAndHaveIdsOfTheirOwn() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("temperature").batchSize(64).build()) {
            assertThat(col.nextBatch()).isTrue();
            int[] firstIds = col.getDictionaryIds();
            DoubleDictionary first = col.getDoubleDictionary();
            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getDictionaryIds()).isNotSameAs(firstIds);
            assertThat(col.getDoubleDictionary()).isSameAs(first);
            assertThat(first.size()).isEqualTo(5);
            assertThatThrownBy(() -> first.getDouble(5))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Dictionary entry 5 out of bounds for 5 entries");
        }
    }

    @Test
    void aColumnWithoutADictionaryExposesNoIds() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.columnReader("plain")) {
            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getDictionaryIds()).isNull();
            assertThat(col.getDoubleDictionary()).isNull();
            assertThat(col.getDoubles()[1]).isEqualTo(0.0);
        }
    }

    @Test
    void aFilteredReadExposesNoNumericIds() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("temperature")
                     .filter(FilterPredicate.eq("temperature", 41.0)).build()) {
            int kept = 0;
            while (col.nextBatch()) {
                assertThat(col.getDictionaryIds()).isNull();
                for (double value : col.getDoubles()) {
                    assertThat(value).isEqualTo(41.0);
                    kept++;
                }
            }
            assertThat(kept).isEqualTo(679);
        }
    }

    @Test
    void eachNumericTypeExposesItsDictionaryAndRefusesAnother() throws Exception {
        byte[] file = writeNumbers(600, 600);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("i", "l", "f", "d"))
                     .batchSize(128).build()) {
            ColumnReader i = columns.getColumnReader("i");
            ColumnReader l = columns.getColumnReader("l");
            ColumnReader f = columns.getColumnReader("f");
            ColumnReader d = columns.getColumnReader("d");
            int rows = 0;
            while (columns.nextBatch()) {
                int[] ii = i.getDictionaryIds();
                int[] li = l.getDictionaryIds();
                int[] fi = f.getDictionaryIds();
                int[] di = d.getDictionaryIds();
                for (int r = 0; r < columns.getRecordCount(); r++) {
                    assertThat(i.getIntDictionary().getInt(ii[r])).isEqualTo(i.getInts()[r]);
                    assertThat(l.getLongDictionary().getLong(li[r])).isEqualTo(l.getLongs()[r]);
                    assertThat(f.getFloatDictionary().getFloat(fi[r])).isEqualTo(f.getFloats()[r]);
                    assertThat(d.getDoubleDictionary().getDouble(di[r])).isEqualTo(d.getDoubles()[r]);
                }
                assertThat(i.getIntDictionary().size()).isEqualTo(4);
                assertThatThrownBy(i::getDoubleDictionary)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("[<memory>] Column 'i' is INT32, not double");
                assertThatThrownBy(d::getBinaryDictionary)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("[<memory>] Column 'd' is DOUBLE, not byte[]");
                rows += columns.getRecordCount();
            }
            assertThat(rows).isEqualTo(600);
        }
    }

    /// A read of numeric columns alone runs its batches across row groups, and a batch that does
    /// draws on two dictionaries: it exposes no ids, and its values are each row's.
    @Test
    void aNumericBatchAcrossRowGroupsExposesNoIds() throws Exception {
        byte[] file = writeNumbers(1_000, 300);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader col = reader.buildColumnReader("l").batchSize(128).build()) {
            List<Boolean> hasIds = new ArrayList<>();
            int row = 0;
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIds();
                hasIds.add(ids != null);
                long[] values = col.getLongs();
                for (int r = 0; r < col.getRecordCount(); r++, row++) {
                    assertThat(values[r]).isEqualTo(row % 4 * 1_000L);
                    if (ids != null) {
                        assertThat(col.getLongDictionary().getLong(ids[r])).isEqualTo(values[r]);
                    }
                }
            }
            assertThat(row).isEqualTo(1_000);
            // rows [256, 384), [512, 640) and [896, 1000) hold the boundaries at 300, 600 and 900
            assertThat(hasIds).containsExactly(true, true, false, true, false, true, true, false);
        }
    }

    @Test
    void aRepeatedNumericColumnExposesNoIds() throws Exception {
        FileSchema schema = FileSchema.builder("readings")
                .list("values", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.DOUBLE, RepetitionType.REQUIRED))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int r = 0; r < 300; r++) {
                double value = POOL[r % 5];
                writer.rowWriter().writeRow(row -> row.setList("values", list -> list.addDouble(value).addDouble(value)));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(InMemoryFiles.toByteArray(out))));
             ColumnReader col = reader.columnReader("values.list.element")) {
            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getDictionaryIds()).isNull();
            assertThat(col.getDoubleDictionary()).isNull();
            assertThat(col.getDoubles()[2]).isEqualTo(0.0);
        }
    }

    /// `rows` rows in row groups of `rowGroupRows` of four values cycling per column: `i`
    /// (`INT32`), `l` (`INT64`, `row % 4 * 1000`), `f` (`FLOAT`), `d` (`DOUBLE`), and `label`.
    private static byte[] writeNumbers(int rows, long rowGroupRows) throws IOException {
        FileSchema schema = FileSchema.builder("numbers")
                .addColumn("i", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("l", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .addColumn("d", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("label", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema,
                WriterConfig.builder().rowGroupTargetRows(rowGroupRows).build())) {
            for (int r = 0; r < rows; r++) {
                int k = r % 4;
                writer.rowWriter().writeRow(row -> row.setInt("i", k * 7).setLong("l", k * 1_000L)
                        .setFloat("f", k + 0.5f).setDouble("d", k - 0.25).setString("label", "s" + k));
            }
        }
        return InMemoryFiles.toByteArray(out);
    }
}
