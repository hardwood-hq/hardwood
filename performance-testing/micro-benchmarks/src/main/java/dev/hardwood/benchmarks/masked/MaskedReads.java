/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.masked;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

/// The reads shared by the masked-read benchmarks. Every read projects all columns, so each
/// nested column of a narrowed row group carries a mask on the pages it keeps.
final class MaskedReads {

    private MaskedReads() {
    }

    /// One read of a file, returning the rows it read.
    @FunctionalInterface
    interface Read {
        long rows() throws IOException;
    }

    /// Resolves `fileName` in `dataDir`, failing when the fixture is missing.
    static Path fixture(String dataDir, String fileName) {
        Path path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw new IllegalStateException("Parquet file not found: " + path
                    + ". Run 'python performance-testing/generate_nested_masking_data.py' first.");
        }
        return path;
    }

    /// Reads every row of `path`, with no tail or filter.
    static long full(Path path) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path));
             RowReader rows = reader.rowReader()) {
            return count(rows);
        }
    }

    /// Reads the last `tailRows` rows of `path`.
    static long tail(Path path, long tailRows) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path));
             RowReader rows = reader.buildRowReader().tail(tailRows).build()) {
            return count(rows);
        }
    }

    /// Reads the rows of `path` that match `filter`.
    static long filtered(Path path, FilterPredicate filter) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            return count(rows);
        }
    }

    /// Fails unless a read returned the rows it should have, so a benchmark never times a read
    /// that went wrong.
    static long checked(long rows, long expectedRows) {
        if (rows != expectedRows) {
            throw new IllegalStateException("Read " + rows + " rows, expected " + expectedRows);
        }
        return rows;
    }

    private static long count(RowReader rows) throws IOException {
        long count = 0;
        while (rows.hasNext()) {
            rows.next();
            count++;
        }
        return count;
    }
}
