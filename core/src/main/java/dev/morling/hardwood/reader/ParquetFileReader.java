/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import dev.morling.hardwood.internal.thrift.FileMetaDataReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Main reader for Parquet files.
 * Handles file structure validation and metadata parsing.
 */
public class ParquetFileReader implements AutoCloseable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    private final Path path;
    private final FileChannel channel;
    private final FileMetaData fileMetaData;
    private final ExecutorService executor;

    private ParquetFileReader(Path path, FileChannel channel, FileMetaData fileMetaData) {
        this.path = path;
        this.channel = channel;
        this.fileMetaData = fileMetaData;

        // Use virtual threads to handle nested parallelism (column-level + page-level)
        // without deadlocking on a fixed-size thread pool
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public static ParquetFileReader open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            // Validate file size
            long fileSize = channel.size();
            if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
                throw new IOException("File too small to be a valid Parquet file");
            }

            // Read and validate magic number at start
            ByteBuffer startMagicBuf = ByteBuffer.allocate(MAGIC_SIZE);
            readFully(channel, startMagicBuf, 0);
            if (!Arrays.equals(startMagicBuf.array(), MAGIC)) {
                throw new IOException("Not a Parquet file (invalid magic number at start)");
            }

            // Read footer size and magic number at end
            long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
            ByteBuffer footerInfoBuf = ByteBuffer.allocate(FOOTER_LENGTH_SIZE + MAGIC_SIZE);
            readFully(channel, footerInfoBuf, footerInfoPos);
            footerInfoBuf.order(ByteOrder.LITTLE_ENDIAN);
            int footerLength = footerInfoBuf.getInt();
            byte[] endMagic = new byte[MAGIC_SIZE];
            footerInfoBuf.get(endMagic);
            if (!Arrays.equals(endMagic, MAGIC)) {
                throw new IOException("Not a Parquet file (invalid magic number at end)");
            }

            // Validate footer length
            long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
            if (footerStart < MAGIC_SIZE) {
                throw new IOException("Invalid footer length: " + footerLength);
            }

            // Read footer
            ByteBuffer footerBuf = ByteBuffer.allocate(footerLength);
            readFully(channel, footerBuf, footerStart);

            // Parse file metadata
            ThriftCompactReader reader = new ThriftCompactReader(new ByteArrayInputStream(footerBuf.array()));
            FileMetaData fileMetaData = FileMetaDataReader.read(reader);

            return new ParquetFileReader(path, channel, fileMetaData);
        }
        catch (Exception e) {
            // Close channel if there was an error during initialization
            try {
                channel.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    /**
     * Read from channel at position until buffer is full.
     */
    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + buffer.position());
            if (read < 0) {
                throw new IOException("Unexpected end of file");
            }
        }
        buffer.flip();
    }

    public FileMetaData getFileMetaData() {
        return fileMetaData;
    }

    public FileSchema getFileSchema() {
        return FileSchema.fromSchemaElements(fileMetaData.schema());
    }

    public ColumnReader getColumnReader(ColumnSchema idColumn, ColumnChunk idColumnChunk) throws IOException {
        return new ColumnReader(path, idColumn, idColumnChunk);
    }

    /**
     * Create a RowReader that iterates over all rows in all row groups.
     */
    public RowReader createRowReader() {
        FileSchema schema = getFileSchema();
        String fileName = path.getFileName().toString();
        if (schema.isFlatSchema()) {
            return new FlatRowReader(schema, channel, fileMetaData.rowGroups(), executor, fileName);
        }
        else {
            return new NestedRowReader(schema, channel, fileMetaData.rowGroups(), executor, fileName);
        }
    }

    @Override
    public void close() throws IOException {
        // Shutdown executor
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Close channel
        channel.close();
    }
}
