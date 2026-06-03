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
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.OffsetDateTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential **value** comparison (P2): reads every row of the typed corpus through hardwood
/// and through DuckDB and asserts the decoded values agree, column by column, across the
/// physical and logical types (`INT32`/`INT64`/`FLOAT`/`DOUBLE`/`BOOLEAN`/`STRING`, plus the
/// `DATE`/`TIMESTAMP`/`DECIMAL` logical types and raw `BYTE_ARRAY`).
///
/// Where P1 ([DifferentialReadTest]) checks that the *right rows* are selected, this checks that
/// each row *decodes* to the right value — the conformance layer that catches decode bugs of the
/// #537 class. Float/double are compared exactly (a reader must reproduce stored bits); decimals
/// by numeric value; dates/timestamps as `LocalDate`/`Instant`.
@Tag("differential")
class DifferentialValueTest {

    private static final Path FILE = Paths.get("src/test/resources/differential/diff_types.parquet");
    private static final long EXPECTED_ROWS = 200;

    @Test
    void decodedValuesMatchOracleAcrossTypes() throws Exception {
        String sql = "SELECT * FROM read_parquet('" + FILE.toAbsolutePath() + "') ORDER BY __row__";
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.buildRowReader().build();
             Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            long seen = 0;
            while (rs.next()) {
                assertThat(rows.hasNext()).as("hardwood has a row for oracle row %d", seen).isTrue();
                rows.next();
                long row = rows.getLong("__row__");
                assertThat(row).as("row order").isEqualTo(rs.getLong("__row__"));

                assertThat(rows.getInt("i32")).as("i32 @%d", row).isEqualTo(rs.getInt("i32"));
                assertThat(rows.getLong("i64")).as("i64 @%d", row).isEqualTo(rs.getLong("i64"));
                assertThat(rows.getFloat("f32")).as("f32 @%d", row).isEqualTo(rs.getFloat("f32"));
                assertThat(rows.getDouble("f64")).as("f64 @%d", row).isEqualTo(rs.getDouble("f64"));
                assertThat(rows.getBoolean("flag")).as("flag @%d", row).isEqualTo(rs.getBoolean("flag"));
                assertThat(rows.getString("s")).as("s @%d", row).isEqualTo(rs.getString("s"));
                assertThat(rows.getDate("d")).as("d @%d", row).isEqualTo(rs.getObject("d", LocalDate.class));
                assertThat(rows.getTimestamp("ts")).as("ts @%d", row)
                        .isEqualTo(rs.getObject("ts", OffsetDateTime.class).toInstant());
                assertThat(rows.getDecimal("dec")).as("dec @%d", row)
                        .isEqualByComparingTo(rs.getBigDecimal("dec"));
                Blob bin = rs.getBlob("bin");
                assertThat(rows.getBinary("bin")).as("bin @%d", row)
                        .isEqualTo(bin.getBytes(1L, Math.toIntExact(bin.length())));

                seen++;
            }
            assertThat(rows.hasNext()).as("hardwood exhausted when oracle is").isFalse();
            assertThat(seen).isEqualTo(EXPECTED_ROWS);
        }
    }
}
