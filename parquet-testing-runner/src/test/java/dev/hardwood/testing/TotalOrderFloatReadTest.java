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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// parquet-java declares `IEEE_754_TOTAL_ORDER` for its floating-point columns and records their
/// `nan_count`. Such a file surfaces the order it declares, and its bounds prune without losing
/// a row a predicate matches: the opposite zero and `NaN`.
class TotalOrderFloatReadTest {

    @TempDir
    static Path tmpDir;

    private static Path file;

    @BeforeAll
    static void writeFile() throws IOException {
        file = tmpDir.resolve("total_order_floats.parquet");
        MessageType schema = MessageTypeParser.parseMessageType(
                "message schema { required float f; required double d; }");
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(new Configuration())
                .withType(schema)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            writer.write(factory.newGroup().append("f", -0.0f).append("d", -0.0));
            writer.write(factory.newGroup().append("f", 1.0f).append("d", 1.0));
            writer.write(factory.newGroup().append("f", Float.NaN).append("d", Double.NaN));
        }
    }

    @Test
    void surfacesTheDeclaredOrderAndTheNaNCount() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileMetaData metaData = reader.getFileMetaData();
            assertThat(metaData.columnOrders())
                    .containsExactly(ColumnOrder.IEEE754_TOTAL_ORDER, ColumnOrder.IEEE754_TOTAL_ORDER);
            assertThat(metaData.rowGroups().get(0).columns())
                    .allSatisfy(chunk -> assertThat(chunk.metaData().statistics().nanCount()).isEqualTo(1L));
        }
    }

    @Test
    void zeroPredicatesSelectTheirOwnZero() throws IOException {
        assertThat(floats(FilterPredicate.eq("f", -0.0f))).containsExactly(-0.0f);
        assertThat(floats(FilterPredicate.eq("f", 0.0f))).isEmpty();
        assertThat(floats(FilterPredicate.gtEq("f", 0.0f))).containsExactly(1.0f, Float.NaN);
        assertThat(floats(FilterPredicate.eq("d", -0.0))).containsExactly(-0.0f);
    }

    @Test
    void naNRowsMatchThePredicatesTheySatisfy() throws IOException {
        assertThat(floats(FilterPredicate.gt("f", 10.0f))).containsExactly(Float.NaN);
        assertThat(floats(FilterPredicate.eq("d", Double.NaN))).containsExactly(Float.NaN);
    }

    private static List<Float> floats(FilterPredicate filter) throws IOException {
        List<Float> values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                values.add(rows.getFloat("f"));
            }
        }
        return values;
    }
}
