/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

/**
 * Parquet reader with parquet-java compatible API.
 * <p>
 * This class provides a drop-in replacement for parquet-java's ParquetReader.
 * It wraps Hardwood's ParquetFileReader and RowReader to provide the familiar
 * builder pattern and read() API.
 * </p>
 *
 * <pre>{@code
 * ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build();
 * Group record;
 * while ((record = reader.read()) != null) {
 *     String name = record.getString("name", 0);
 *     int age = record.getInteger("age", 0);
 * }
 * reader.close();
 * }</pre>
 *
 * @param <T> the record type (currently only Group is supported)
 */
public class ParquetReader<T> implements AutoCloseable {

    private final ParquetFileReader hardwoodReader;
    private final RowReader rowReader;
    private final MessageType messageType;

    private ParquetReader(Path path) throws IOException {
        this.hardwoodReader = ParquetFileReader.open(path.toNioPath());
        this.rowReader = hardwoodReader.createRowReader();
        this.messageType = SchemaConverter.toMessageType(hardwoodReader.getFileSchema());
    }

    /**
     * Read the next record.
     *
     * @return the next record, or null if no more records
     * @throws IOException if reading fails
     */
    @SuppressWarnings("unchecked")
    public T read() throws IOException {
        if (rowReader.hasNext()) {
            rowReader.next();
            return (T) new SimpleGroup(rowReader, messageType);
        }
        return null;
    }

    /**
     * Get the schema of the file.
     *
     * @return the message type schema
     */
    public MessageType getSchema() {
        return messageType;
    }

    @Override
    public void close() throws IOException {
        try {
            rowReader.close();
        }
        finally {
            hardwoodReader.close();
        }
    }

    /**
     * Create a builder for ParquetReader.
     *
     * @param readSupport the read support (must be GroupReadSupport)
     * @param path the path to the Parquet file
     * @return the builder
     */
    public static Builder<Group> builder(GroupReadSupport readSupport, Path path) {
        return new Builder<>(path);
    }

    /**
     * Create a builder for ParquetReader using a string path.
     *
     * @param readSupport the read support (must be GroupReadSupport)
     * @param path the path to the Parquet file
     * @return the builder
     */
    public static Builder<Group> builder(GroupReadSupport readSupport, String path) {
        return new Builder<>(new Path(path));
    }

    /**
     * Builder for ParquetReader.
     *
     * @param <T> the record type
     */
    public static class Builder<T> {

        private final Path path;
        private Configuration conf;

        Builder(Path path) {
            this.path = path;
        }

        /**
         * Set the Hadoop configuration (ignored - provided for API compatibility).
         *
         * @param conf the configuration
         * @return this builder
         */
        public Builder<T> withConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        /**
         * Build the ParquetReader.
         *
         * @return the reader
         * @throws IOException if opening the file fails
         */
        public ParquetReader<T> build() throws IOException {
            return new ParquetReader<>(path);
        }
    }
}
