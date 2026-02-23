/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import dev.hardwood.Hardwood;
import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.FileMappingEvent;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

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

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer fileMapping;
    private final FileMetaData fileMetaData;
    private final HardwoodContextImpl context;
    private final boolean ownsContext;

    private ParquetFileReader(Path path, FileChannel channel, MappedByteBuffer fileMapping,
                              FileMetaData fileMetaData, HardwoodContextImpl context, boolean ownsContext) {
        this.path = path;
        this.channel = channel;
        this.fileMapping = fileMapping;
        this.fileMetaData = fileMetaData;
        this.context = context;
        this.ownsContext = ownsContext;
    }

    /**
     * Open a Parquet file with a dedicated context.
     * The context is closed when this reader is closed.
     */
    public static ParquetFileReader open(Path path) throws IOException {
        HardwoodContextImpl context = HardwoodContextImpl.create();
        return open(path, context, true);
    }

    /**
     * Open a Parquet file with a shared context.
     * The context is NOT closed when this reader is closed.
     */
    public static ParquetFileReader open(Path path, HardwoodContext context) throws IOException {
        return open(path, (HardwoodContextImpl) context, false);
    }

    private static ParquetFileReader open(Path path, HardwoodContextImpl context,
                                          boolean ownsContext) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            // Map the entire file once - used for both metadata and data reading
            long fileSize = channel.size();

            FileMappingEvent event = new FileMappingEvent();
            event.begin();

            MappedByteBuffer fileMapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

            event.path = path.toString();
            event.offset = 0;
            event.size = fileSize;
            event.column = "(entire file)";
            event.commit();

            // Read metadata from the mapping
            FileMetaData fileMetaData = ParquetMetadataReader.readMetadata(fileMapping, path);

            return new ParquetFileReader(path, channel, fileMapping, fileMetaData, context, ownsContext);
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

    public FileMetaData getFileMetaData() {
        return fileMetaData;
    }

    public FileSchema getFileSchema() {
        return FileSchema.fromSchemaElements(fileMetaData.schema());
    }

    /**
     * Create a ColumnReader for a named column, spanning all row groups.
     */
    public ColumnReader createColumnReader(String columnName) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnName, schema, fileMapping, fileMetaData.rowGroups(), context);
    }

    /**
     * Create a ColumnReader for a column by index, spanning all row groups.
     */
    public ColumnReader createColumnReader(int columnIndex) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnIndex, schema, fileMapping, fileMetaData.rowGroups(), context);
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
        return new SingleFileRowReader(schema, projectedSchema, fileMapping, fileMetaData.rowGroups(), context, fileName);
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
