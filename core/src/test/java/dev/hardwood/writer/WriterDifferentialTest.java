/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential test for the writer, the inverse direction of [dev.hardwood.DifferentialReadTest]:
/// hardwood writes a Parquet file and DuckDB reads it back through `read_parquet`.
/// DuckDB is an independent engine, so agreement proves the produced bytes are
/// spec-correct, not merely consistent with hardwood's own reader.
///
/// Each row carries a synthetic `r` index column so the comparison is robust to
/// scan order via `ORDER BY r`.
class WriterDifferentialTest {

    @Test
    void duckDbReadsWrittenInts(@TempDir Path dir) throws Exception {
        // Boundary values exercise the signed little-endian PLAIN INT32 encoding.
        int[] v = { 0, 1, -1, 42, -100_000, 123_456, Integer.MAX_VALUE, Integer.MIN_VALUE };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();

        Path file = dir.resolve("written.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.writeBatch(batch -> batch.ints(0, r).ints(1, v));
        }

        List<Integer> actual = new ArrayList<>();
        long rowCount;
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            String from = "read_parquet('" + file.toAbsolutePath() + "')";
            try (ResultSet rs = stmt.executeQuery("SELECT v FROM " + from + " ORDER BY r")) {
                while (rs.next()) {
                    actual.add(rs.getInt("v"));
                }
            }
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) AS n FROM " + from)) {
                rs.next();
                rowCount = rs.getLong("n");
            }
        }

        List<Integer> expected = new ArrayList<>(v.length);
        for (int value : v) {
            expected.add(value);
        }
        assertThat(rowCount).isEqualTo(v.length);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsMultiPageColumn(@TempDir Path dir) throws Exception {
        // More than one target page, so the column is written across several pages.
        int n = 600_000;
        int[] v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("multipage.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.writeBatch(batch -> batch.ints(0, v));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, sum(v) AS s, max(v) AS mx FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            assertThat(rs.getLong("n")).isEqualTo(n);
            assertThat(rs.getLong("s")).isEqualTo((long) n * (n - 1) / 2);
            assertThat(rs.getInt("mx")).isEqualTo(n - 1);
        }
    }

    @Test
    void duckDbReadsNullableInts(@TempDir Path dir) throws Exception {
        // Interior, leading and trailing nulls, with signed extremes at present rows.
        int[] v = { 0, 0, -1, 0, Integer.MAX_VALUE, Integer.MIN_VALUE, 0 };
        boolean[] nulls = { true, false, false, true, false, false, true };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();

        Path file = dir.resolve("nullable.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.writeBatch(batch -> batch.ints(0, r).ints(1, v, nulls));
        }

        List<Integer> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                int value = rs.getInt("v");
                actual.add(rs.wasNull() ? null : value);
            }
        }

        List<Integer> expected = new ArrayList<>(v.length);
        for (int i = 0; i < v.length; i++) {
            expected.add(nulls[i] ? null : v[i]);
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsAllNullColumn(@TempDir Path dir) throws Exception {
        int n = 1_000;
        int[] r = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            r[i] = i;
            nulls[i] = true;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();

        Path file = dir.resolve("allnull.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.writeBatch(batch -> batch.ints(0, r).ints(1, new int[n], nulls));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, count(v) AS present FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            assertThat(rs.getLong("n")).isEqualTo(n);
            assertThat(rs.getLong("present")).isZero();
        }
    }

    @Test
    void duckDbReadsStruct(@TempDir Path dir) throws Exception {
        // required int32 r; optional group address { required int32 street; optional int32 zip }.
        // DuckDB resolves address.street / address.zip and must see the field absent wherever
        // the struct is null, and zip absent where zip itself is null.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, s -> s
                        .addColumn("street", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] r = { 0, 1, 2, 3 };
        Validity addressNulls = Validity.ofNulls(new boolean[] { true, false, false, false });
        int[] street = { 0, 10, 20, 30 };
        int[] zip = { 0, 0, 200, 300 };
        boolean[] zipNulls = { false, true, false, false };

        Path file = dir.resolve("struct.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.writeBatch(batch -> batch
                    .ints("r", r)
                    .struct("address", addressNulls)
                    .ints("address.street", street)
                    .ints("address.zip", zip, zipNulls));
        }

        List<Integer> streets = new ArrayList<>();
        List<Integer> zips = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT address.street AS street, address.zip AS zip FROM read_parquet('"
                                + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                int st = rs.getInt("street");
                streets.add(rs.wasNull() ? null : st);
                int zp = rs.getInt("zip");
                zips.add(rs.wasNull() ? null : zp);
            }
        }

        assertThat(streets).containsExactly(null, 10, 20, 30);
        assertThat(zips).containsExactly(null, null, 200, 300);
    }

    @Test
    void duckDbReadsListOfInts(@TempDir Path dir) throws Exception {
        // required int32 r; optional list<optional int32> phones. Absent list, empty list,
        // and a null element must all survive to DuckDB distinctly.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("phones", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] r = { 0, 1, 2, 3 };
        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 1, 2, 3, 0, 5 };
        boolean[] elementNulls = { false, false, false, true, false };

        Path file = dir.resolve("listofints.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.writeBatch(batch -> batch
                    .ints("r", r)
                    .list("phones", offsets, listNulls)
                    .ints("phones.list.element", elements, elementNulls));
        }

        List<List<Integer>> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT phones FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                java.sql.Array array = rs.getArray("phones");
                if (rs.wasNull()) {
                    actual.add(null);
                    continue;
                }
                List<Integer> list = new ArrayList<>();
                for (Object element : (Object[]) array.getArray()) {
                    list.add(element == null ? null : ((Number) element).intValue());
                }
                actual.add(list);
            }
        }

        assertThat(actual).containsExactly(List.of(1, 2), List.of(), null, java.util.Arrays.asList(3, null, 5));
    }

    @Test
    void duckDbReadsMultipleRowGroups(@TempDir Path dir) throws Exception {
        int n = 5_000;
        int[] v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        // A tiny row-group target forces the single batch to be split across many row
        // groups; DuckDB must read across all of them transparently.
        WriterConfig config = WriterConfig.builder().rowGroupTargetBytes(4096).build();
        Path file = dir.resolve("multirowgroup.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.writeBatch(batch -> batch.ints(0, v));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, sum(v) AS s, max(v) AS mx FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            assertThat(rs.getLong("n")).isEqualTo(n);
            assertThat(rs.getLong("s")).isEqualTo((long) n * (n - 1) / 2);
            assertThat(rs.getInt("mx")).isEqualTo(n - 1);
        }
    }
}
