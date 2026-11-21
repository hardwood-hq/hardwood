/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.arrow.ArrowTestSupport.coreFixture;
import static dev.hardwood.arrow.ArrowTestSupport.fixture;
import static org.assertj.core.api.Assertions.assertThat;

/// Exports [ArrowBatchReader] over the C Data Interface (`ArrowArrayStream`) and compares DuckDB
/// aggregates against DuckDB's own `read_parquet`.
class ArrowCDataDuckDbTest {

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void allocatorIsEmpty() {
        assertThat(allocator.getAllocatedMemory()).isZero();
        allocator.close();
    }

    @Test
    void duckDbQueriesTheExportedStream() throws IOException, SQLException {
        Path file = coreFixture("dict_plain_fallback");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(700).build();
            Data.exportArrayStream(allocator, reader, stream);
            try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                 Statement statement = connection.createStatement()) {
                ((DuckDBConnection) connection).registerArrowStream("exported", stream);
                String aggregates = "count(*), count(label), count(DISTINCT label), min(label), max(label)";
                List<Object> overStream = row(statement, "SELECT " + aggregates + " FROM exported");
                List<Object> overFile = row(statement, "SELECT " + aggregates + " FROM read_parquet('" + file + "')");
                assertThat(overStream).isEqualTo(overFile);
                assertThat((Long) overStream.get(0)).isEqualTo(4000L);
            }
        }
    }

    @Test
    void duckDbQueriesNestedStream() throws IOException, SQLException {
        Path file = fixture("nested");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(2).build();
            Data.exportArrayStream(allocator, reader, stream);
            try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                 Statement statement = connection.createStatement()) {
                ((DuckDBConnection) connection).registerArrowStream("exported", stream);
                String sql = "SELECT count(*), count(address), count(address.street), sum(len(phones)), "
                        + "sum(len(tags)), count(props), sum(cardinality(props)), count(owner.place.city) FROM ";
                List<Object> overStream = row(statement, sql + "exported");
                List<Object> overFile = row(statement, sql + "read_parquet('" + file + "')");
                assertThat(overStream).isEqualTo(overFile);
                assertThat((Long) overStream.get(0)).isEqualTo(5L);
            }
        }
    }

    private static List<Object> row(Statement statement, String sql) throws SQLException {
        try (ResultSet result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue();
            List<Object> values = new ArrayList<>();
            for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                values.add(result.getObject(i));
            }
            return values;
        }
    }
}
