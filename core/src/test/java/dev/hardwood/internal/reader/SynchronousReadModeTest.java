/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Verifies the synchronous (thread-free, pull-based) read path produces byte-identical output
/// to the default parallel path.
class SynchronousReadModeTest {

    private static final Path RESOURCES = Path.of("src", "test", "resources");

    /// Flat fixtures: compare every row's rendered values between the two execution modes.
    @ParameterizedTest
    @ValueSource(strings = {
            "plain_uncompressed.parquet",
            "plain_snappy.parquet",
            "yellow_tripdata_sample.parquet",
            "filter_all_pages_match.parquet",
    })
    void synchronousMatchesParallelForFlatFixtures(String fixture) throws IOException {
        byte[] bytes = Files.readAllBytes(RESOURCES.resolve(fixture));
        List<String> parallel = readRows(bytes, HardwoodContextImpl.create());
        List<String> synchronous = readRows(bytes, HardwoodContextImpl.synchronous(Map.of()));
        assertThat(synchronous).isNotEmpty();
        assertThat(synchronous).containsExactlyElementsOf(parallel);
    }

    /// Nested fixture: the row count must match (exercises the nested worker's pump).
    @Test
    void synchronousMatchesParallelRowCountForNested() throws IOException {
        byte[] bytes = Files.readAllBytes(RESOURCES.resolve("nested_dict_batch_boundary.parquet"));
        long parallel = countRows(bytes, HardwoodContextImpl.create());
        long synchronous = countRows(bytes, HardwoodContextImpl.synchronous(Map.of()));
        assertThat(synchronous).isEqualTo(parallel).isPositive();
    }

    private static List<String> readRows(byte[] bytes, HardwoodContext context) throws IOException {
        InputFile inputFile = new ByteBufferInputFile(ByteBuffer.wrap(bytes));
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, context);
                RowReader rows = reader.rowReader()) {
            List<ColumnSchema> columns = reader.getFileSchema().getColumns();
            List<String> out = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                out.add(renderRow(rows, columns));
            }
            return out;
        }
        finally {
            close(context);
        }
    }

    private static long countRows(byte[] bytes, HardwoodContext context) throws IOException {
        InputFile inputFile = new ByteBufferInputFile(ByteBuffer.wrap(bytes));
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, context);
                RowReader rows = reader.rowReader()) {
            long n = 0;
            while (rows.hasNext()) {
                rows.next();
                n++;
            }
            return n;
        }
        finally {
            close(context);
        }
    }

    private static String renderRow(RowReader row, List<ColumnSchema> columns) {
        StringBuilder sb = new StringBuilder();
        for (ColumnSchema column : columns) {
            String name = column.name();
            sb.append(name).append('=');
            if (row.isNull(name)) {
                sb.append("null");
            }
            else {
                sb.append(switch (column.type()) {
                    case INT32 -> Integer.toString(row.getInt(name));
                    case INT64 -> Long.toString(row.getLong(name));
                    case FLOAT -> Float.toString(row.getFloat(name));
                    case DOUBLE -> Double.toString(row.getDouble(name));
                    case BOOLEAN -> Boolean.toString(row.getBoolean(name));
                    case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> HexFormat.of().formatHex(row.getBinary(name));
                });
            }
            sb.append(';');
        }
        return sb.toString();
    }

    private static void close(HardwoodContext context) {
        if (context instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            }
            catch (Exception ignored) {
                // best-effort
            }
        }
    }
}
