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
import java.util.stream.Stream;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.arrow.ArrowTestSupport.assertSameTable;
import static dev.hardwood.arrow.ArrowTestSupport.coreFixture;
import static dev.hardwood.arrow.ArrowTestSupport.fixture;
import static dev.hardwood.arrow.ArrowTestSupport.readAll;
import static org.assertj.core.api.Assertions.assertThat;

/// Arrow C++'s Parquet reader, reached through `arrow-dataset`, reads the same file and hands
/// its batches over in memory through the C Data Interface. Its table is the specification
/// the module implements, schema included, so the comparison is the strict one.
class ArrowDatasetOracleTest {

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void allocatorIsEmpty() {
        assertThat(allocator.getAllocatedMemory()).isZero();
        allocator.close();
    }

    static Stream<Path> files() throws IOException {
        return Stream.of(
                fixture("scalars"),
                fixture("scalars_dict"),
                fixture("empty"),
                fixture("field_ids"),
                coreFixture("int96_timestamp_test"),
                coreFixture("dict_int96"),
                coreFixture("dict_plain_fallback"),
                coreFixture("dict_cross_chunk"));
    }

    @ParameterizedTest
    @MethodSource("files")
    void matchesArrowCpp(Path file) throws Exception {
        try (DatasetFactory factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, file.toUri().toString());
             Dataset dataset = factory.finish();
             Scanner scanner = dataset.newScan(new ScanOptions(1024));
             ArrowReader arrowCpp = scanner.scanBatches();
             VectorSchemaRoot expected = readAll(arrowCpp, allocator);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator);
             VectorSchemaRoot actual = readAll(reader, allocator)) {
            assertSameTable(expected, actual);
        }
    }
}
