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
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.InputFiles;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

/// Parquet reader with parquet-java compatible API.
///
/// This class provides a drop-in replacement for parquet-java's ParquetReader.
/// It wraps Hardwood's ParquetFileReader and RowReader to provide the familiar
/// builder pattern and read() API.
///
/// ```java
/// ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build();
/// Group record;
/// while ((record = reader.read()) != null) {
///     String name = record.getString("name", 0);
///     int age = record.getInteger("age", 0);
/// }
/// reader.close();
/// ```
///
/// @param <T> the record type (currently only Group is supported)
public class ParquetReader<T> implements AutoCloseable {

    private final ParquetFileReader hardwoodReader;
    private final RowReader rowReader;
    private final MessageType messageType;

    private ParquetReader(dev.hardwood.InputFile inputFile,
            dev.hardwood.reader.FilterPredicate filter) throws IOException {
        this.hardwoodReader = ParquetFileReader.open(inputFile);
        this.rowReader = filter != null
                ? hardwoodReader.createRowReader(filter)
                : hardwoodReader.createRowReader();
        this.messageType = SchemaConverter.toMessageType(hardwoodReader.getFileSchema());
    }

    /// Read the next record.
    ///
    /// @return the next record, or null if no more records
    /// @throws IOException if reading fails
    @SuppressWarnings("unchecked")
    public T read() throws IOException {
        if (rowReader.hasNext()) {
            rowReader.next();
            return (T) new SimpleGroup(rowReader, messageType);
        }
        return null;
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

    /// Create a builder for ParquetReader.
    ///
    /// @param readSupport the read support (must be GroupReadSupport)
    /// @param path the path to the Parquet file
    /// @return the builder
    public static Builder<Group> builder(GroupReadSupport readSupport, Path path) {
        return new Builder<>(path, null);
    }

    /// Create a builder for ParquetReader using an InputFile.
    ///
    /// @param readSupport the read support (must be GroupReadSupport)
    /// @param inputFile the input file (e.g. from [HadoopInputFile#fromPath])
    /// @return the builder
    public static Builder<Group> builder(GroupReadSupport readSupport, InputFile inputFile) {
        return new Builder<>(null, inputFile);
    }

    /// Builder for ParquetReader.
    ///
    /// @param <T> the record type
    public static class Builder<T> {

        private final Path path;
        private final InputFile inputFile;
        private Configuration conf;
        private FilterCompat.Filter filter;

        Builder(Path path, InputFile inputFile) {
            this.path = path;
            this.inputFile = inputFile;
        }

        /// Set the Hadoop configuration.
        ///
        /// For S3 paths, the configuration supplies credentials and endpoint settings.
        /// For local paths, the configuration is ignored.
        ///
        /// @param conf the configuration
        /// @return this builder
        public Builder<T> withConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        /// Set a filter for predicate pushdown.
        ///
        /// Row groups whose statistics prove that no rows can match the
        /// predicate will be skipped entirely.
        ///
        /// @param filter the filter (from [FilterCompat#get(org.apache.parquet.filter2.predicate.FilterPredicate)])
        /// @return this builder
        public Builder<T> withFilter(FilterCompat.Filter filter) {
            this.filter = filter;
            return this;
        }

        /// Build the ParquetReader.
        ///
        /// @return the reader
        /// @throws IOException if opening the file fails
        public ParquetReader<T> build() throws IOException {
            return new ParquetReader<>(resolveHardwoodInputFile(), resolveFilter());
        }

        private dev.hardwood.InputFile resolveHardwoodInputFile() {
            if (inputFile != null) {
                return InputFiles.unwrap(inputFile);
            }
            Configuration c = conf != null ? conf : new Configuration();
            return InputFiles.unwrap(HadoopInputFile.fromPath(path, c));
        }

        private dev.hardwood.reader.FilterPredicate resolveFilter() {
            if (filter instanceof FilterCompat.FilterPredicateCompat fpc) {
                return InputFiles.convertFilter(fpc.getFilterPredicate());
            }
            return null;
        }
    }
}
