/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.arrow.ArrowTestSupport.assertSameTable;
import static dev.hardwood.arrow.ArrowTestSupport.expected;
import static dev.hardwood.arrow.ArrowTestSupport.fixture;
import static dev.hardwood.arrow.ArrowTestSupport.readAll;
import static org.assertj.core.api.Assertions.assertThat;

/// The reader feeds Arrow's own IPC writers: what they write and read back is the expected table.
class ArrowIpcRoundTripTest {

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void allocatorIsEmpty() {
        assertThat(allocator.getAllocatedMemory()).isZero();
        allocator.close();
    }

    @ParameterizedTest
    @FieldSource("dev.hardwood.arrow.ArrowTestSupport#FIXTURES")
    void streamRoundTrip(String name) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int batches = 0;
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture(name)));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(3).build();
             ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), reader, bytes)) {
            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
                batches++;
            }
            writer.end();
        }
        try (VectorSchemaRoot expected = expected(name, allocator);
             ArrowStreamReader back = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
             VectorSchemaRoot actual = readAll(back, allocator)) {
            // Batches end at row-group boundaries too, so there are at least as many as the size implies.
            assertThat(batches).isGreaterThanOrEqualTo((expected.getRowCount() + 2) / 3);
            assertSameTable(expected, actual);
        }
    }

    /// The file format, over the nested fixture: the writer's blocks carry the container
    /// nodes and buffers, and what comes back is what a plain read of the file gives.
    @Test
    void fileRoundTripCarriesNestedBatches() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("nested")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(2).build();
             ArrowFileWriter writer = new ArrowFileWriter(reader.getVectorSchemaRoot(), reader,
                     Channels.newChannel(bytes))) {
            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
        }
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("nested")));
             ArrowBatchReader direct = ArrowReaders.batchReader(fileReader, allocator);
             VectorSchemaRoot expected = readAll(direct, allocator);
             ArrowFileReader back = new ArrowFileReader(
                     new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(bytes.toByteArray())), allocator);
             VectorSchemaRoot actual = readAll(back, allocator)) {
            assertSameTable(expected, actual);
        }
    }
}
