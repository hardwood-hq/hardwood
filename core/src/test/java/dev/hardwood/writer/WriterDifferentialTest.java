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
