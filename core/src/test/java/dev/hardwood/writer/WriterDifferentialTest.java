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
            writer.writeInts(0, r);
            writer.writeInts(1, v);
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
}
