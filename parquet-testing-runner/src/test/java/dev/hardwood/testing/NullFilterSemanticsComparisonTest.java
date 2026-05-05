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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Documents how Hardwood and parquet-java differ in null handling for comparison
/// predicates (see GitHub issue #251).
///
/// Test data is three rows with `age` = {25, 35, null}.
///
/// **Case A** — `not(gt(age, 30))`: both engines drop the null row.
/// parquet-java's `LogicalInverseRewriter` rewrites `not(gt)` to `ltEq`, and
/// `LtEq.updateNull()` returns false. Hardwood's `ResolvedPredicate.negate()`
/// applies the same operator inversion, and the compiled record-level matcher
/// short-circuits null comparisons to false.
///
/// **Case B** — `not(eq(age, 30))`: parquet-java keeps the null row, Hardwood
/// drops it. The rewriter produces `notEq`; parquet-java's `NotEq.updateNull()`
/// returns true ("null is not 30"), whereas Hardwood applies uniform
/// SQL three-valued-logic semantics (any null comparison → drop).
class NullFilterSemanticsComparisonTest {

    @TempDir
    static Path tmpDir;

    private static Path parquetFile;

    @BeforeAll
    static void writeFile() throws IOException {
        parquetFile = tmpDir.resolve("null_filter_semantics.parquet");
        MessageType schema = MessageTypeParser.parseMessageType(
                "message schema { optional int32 age; }");
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.toUri());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf)
                .withType(schema)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            writer.write(factory.newGroup().append("age", 25));
            writer.write(factory.newGroup().append("age", 35));
            // Third row: age left unset → null.
            writer.write(factory.newGroup());
        }
    }

    @Test
    void bothEnginesDropNullFromNotGt() throws IOException {
        FilterPredicate hwPred = FilterPredicate.not(FilterPredicate.gt("age", 30));
        assertThat(readWithHardwood(hwPred)).containsExactly(25);

        org.apache.parquet.filter2.predicate.FilterPredicate pjPred =
                FilterApi.not(FilterApi.gt(FilterApi.intColumn("age"), 30));
        assertThat(readWithParquetJava(pjPred)).containsExactly(25);
    }

    @Test
    void parquetJavaKeepsNullsFromNotEqWhileHardwoodDrops() throws IOException {
        FilterPredicate hwPred = FilterPredicate.not(FilterPredicate.eq("age", 30));

        // Hardwood: any null-vs-value comparison yields false at the record
        // level, so the null row is dropped.
        assertThat(readWithHardwood(hwPred)).containsExactly(25, 35);

        org.apache.parquet.filter2.predicate.FilterPredicate pjPred =
                FilterApi.not(FilterApi.eq(FilterApi.intColumn("age"), 30));

        // parquet-java: not(eq) → notEq; NotEq.updateNull() returns true,
        // so the null row passes the filter.
        assertThat(readWithParquetJava(pjPred)).containsExactly(25, 35, null);
    }

    private List<Integer> readWithHardwood(FilterPredicate filter) throws IOException {
        List<Integer> result = new ArrayList<>();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rowReader = fileReader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                result.add(rowReader.isNull("age") ? null : rowReader.getInt("age"));
            }
        }
        return result;
    }

    private List<Integer> readWithParquetJava(org.apache.parquet.filter2.predicate.FilterPredicate filter)
            throws IOException {
        List<Integer> result = new ArrayList<>();
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.toUri());
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(hadoopPath, conf))
                .withConf(conf)
                .withFilter(FilterCompat.get(filter))
                .build()) {
            GenericRecord rec;
            while ((rec = reader.read()) != null) {
                Object v = rec.get("age");
                result.add(v == null ? null : ((Number) v).intValue());
            }
        }
        return result;
    }
}
