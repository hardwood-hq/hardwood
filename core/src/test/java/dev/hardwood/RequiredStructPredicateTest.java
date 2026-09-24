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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// A column reader filtering on a leaf under a struct that is required all the way down. The
/// leaf contributes no layer, so the column reader decodes it as a flat column, while the
/// predicate names it by its nested path (#1279).
class RequiredStructPredicateTest {

    private static final int ROWS = 100;

    private static byte[] file;

    /// `key` shares its name with `r.key` and holds different values, so a predicate reading one
    /// in place of the other selects different rows. `o` is an optional struct, which decodes as
    /// nested.
    @BeforeAll
    static void writeFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("key", PhysicalType.INT64, RepetitionType.REQUIRED)
                .struct("r", RepetitionType.REQUIRED, r -> r
                        .addColumn("key", PhysicalType.INT64, RepetitionType.REQUIRED)
                        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                new LogicalType.StringType()))
                .struct("o", RepetitionType.OPTIONAL, o -> o
                        .addColumn("v", PhysicalType.INT64, RepetitionType.OPTIONAL))
                .build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < ROWS; i++) {
                final long key = i;
                writer.rowWriter().writeRow(row -> row.setDouble("amount", key * 0.5)
                        .setLong("key", 1000 + key)
                        .setStruct("r", r -> r.setLong("key", key).setString("name", "n" + key))
                        .setStruct("o", o -> o.setLong("v", key)));
            }
        }
        file = out.toByteArray();
    }

    @Test
    void filtersOnAComparisonOverALeafUnderARequiredStruct() throws Exception {
        List<Double> expected = new ArrayList<>();
        for (int key = 0; key < 30; key++) {
            expected.add(key * 0.5);
        }
        assertThat(amounts(FilterPredicate.lt("r.key", 30L))).isEqualTo(expected);
    }

    @Test
    void filtersOnABinaryPredicateOverALeafUnderARequiredStruct() throws Exception {
        assertThat(amounts(FilterPredicate.eq("r.name", "n5"))).containsExactly(2.5);
    }

    @Test
    void filtersOnAnInPredicateOverALeafUnderARequiredStruct() throws Exception {
        assertThat(amounts(FilterPredicate.in("r.key", 5L, 7L))).containsExactly(2.5, 3.5);
        assertThat(amounts(FilterPredicate.in("r.name", "n5", "n7"))).containsExactly(2.5, 3.5);
    }

    @Test
    void distinguishesATopLevelColumnFromALeafOfTheSameName() throws Exception {
        assertThat(amounts(FilterPredicate.and(
                FilterPredicate.in("key", 1005L, 1006L), FilterPredicate.lt("r.key", 30L))))
                .containsExactly(2.5, 3.0);
    }

    @Test
    void filtersOnALeafUnderARequiredStructBesideANestedLeaf() throws Exception {
        assertThat(amounts(FilterPredicate.and(
                FilterPredicate.lt("o.v", 50L), FilterPredicate.eq("r.name", "n5"))))
                .containsExactly(2.5);
    }

    @Test
    void filtersOnTheNullityOfARequiredStruct() throws Exception {
        assertThat(amounts(FilterPredicate.isNull("r"))).isEmpty();
        assertThat(amounts(FilterPredicate.isNotNull("r"))).hasSize(ROWS);
    }

    private static List<Double> amounts(FilterPredicate filter) throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader column = reader.buildColumnReader("amount").filter(filter).build()) {
            while (column.nextBatch()) {
                double[] values = column.getDoubles();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    amounts.add(values[i]);
                }
            }
        }
        return amounts;
    }
}
