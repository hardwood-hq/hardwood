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

import dev.morling.hardwood.internal.thrift.FileMetaDataReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * Reader for individual Parquet files.
 *
 * <p>For single-file usage:</p>
 * <pre>{@code
 * try (ParquetFileReader reader = ParquetFileReader.open(path)) {
 *     RowReader rows = reader.createRowReader();
 *     // ...
 * }
 * }</pre>
 *
 * <p>For multi-file usage with shared thread pool, use {@link Hardwood}.</p>
 */
public class ParquetFileReader implements AutoCloseable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    private final Path path;
    private final FileChannel channel;
    private final FileMetaData fileMetaData;
    private final HardwoodContext context;
    private final boolean ownsContext;

    private ParquetFileReader(Path path, FileChannel channel, FileMetaData fileMetaData,
                              HardwoodContext context, boolean ownsContext) {
        this.path = path;
        this.channel = channel;
        this.fileMetaData = fileMetaData;
        this.context = context;
        this.ownsContext = ownsContext;
    }

    /**
     * Open a Parquet file with a dedicated context.
     * The context is closed when this reader is closed.
     */
    public static ParquetFileReader open(Path path) throws IOException {
        HardwoodContext context = HardwoodContext.create();
        return open(path, context, true);
    }

    /**
     * Open a Parquet file with a shared context.
     * The context is NOT closed when this reader is closed.
     */
    static ParquetFileReader open(Path path, HardwoodContext context) throws IOException {
        return open(path, context, false);
    }

    private static ParquetFileReader open(Path path, HardwoodContext context,
                                          boolean ownsContext) throws IOException {
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

            return new ParquetFileReader(path, channel, fileMetaData, context, ownsContext);
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
        return new ColumnReader(path, idColumn, idColumnChunk, context);
    }

    /**
     * Create a RowReader that iterates over all rows in all row groups.
     */
    public RowReader createRowReader() {
        return createRowReader(ColumnProjection.all());
    }

    /**
     * Create a RowReader that iterates over selected columns in all row groups.
     *
     * @param projection specifies which columns to read
     * @return a RowReader for the selected columns
     */
    public RowReader createRowReader(ColumnProjection projection) {
        FileSchema schema = getFileSchema();
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection);
        String fileName = path.getFileName().toString();
        return new SingleFileRowReader(schema, projectedSchema, channel, fileMetaData.rowGroups(), context, fileName);
    }

    @Override
    public void close() throws IOException {
        // Only close context if we created it
        // When opened via Hardwood, the context is closed when Hardwood is closed
        if (ownsContext) {
            context.close();
        }

        // Close channel
        channel.close();
    }
}
