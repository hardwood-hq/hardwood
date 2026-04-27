/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro;

import org.apache.avro.Schema;

import dev.hardwood.avro.internal.AvroSchemaConverter;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

/// Factory for creating [AvroRowReader] instances from a
/// [ParquetFileReader].
///
/// Mirrors the noun / buildNoun shape of [ParquetFileReader]: the
/// no-arg [#rowReader(ParquetFileReader)] returns the default reader;
/// [#buildRowReader(ParquetFileReader)] returns a builder for
/// projection / filter / head / tail configuration.
///
/// ```java
/// try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
///      AvroRowReader reader = AvroReaders.rowReader(fileReader)) {
///     while (reader.hasNext()) {
///         GenericRecord record = reader.next();
///         long id = (Long) record.get("id");
///     }
/// }
/// ```
public final class AvroReaders {

    private AvroReaders() {
    }

    /// Create an `AvroRowReader` that reads all rows and columns.
    public static AvroRowReader rowReader(ParquetFileReader reader) {
        return buildRowReader(reader).build();
    }

    /// Begin configuring an [AvroRowReader] with optional projection,
    /// filter, and head/tail row limit.
    public static RowReaderBuilder buildRowReader(ParquetFileReader reader) {
        return new RowReaderBuilder(reader);
    }

    /// Builds an [AvroRowReader] with optional projection, filter, and
    /// head/tail row limit. Mirrors [ParquetFileReader.RowReaderBuilder]
    /// — settings are forwarded to the underlying parquet row reader,
    /// then wrapped in an [AvroRowReader] with the converted schema.
    public static final class RowReaderBuilder {

        private final ParquetFileReader fileReader;
        private ColumnProjection projection = ColumnProjection.all();
        private FilterPredicate filter;
        private long headRows;
        private long tailRows;

        private RowReaderBuilder(ParquetFileReader fileReader) {
            this.fileReader = fileReader;
        }

        /// Restrict reading to the given columns. Default: all columns.
        public RowReaderBuilder projection(ColumnProjection projection) {
            if (projection == null) {
                throw new IllegalArgumentException("projection must not be null");
            }
            this.projection = projection;
            return this;
        }

        /// Apply a row-group / record-level filter predicate. Default: no filter.
        public RowReaderBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        /// Limit to the first `maxRows` rows. Mutually exclusive with [#tail].
        public RowReaderBuilder head(long maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("head row count must be positive: " + maxRows);
            }
            this.headRows = maxRows;
            return this;
        }

        /// Limit to the last `tailRows` rows. Mutually exclusive with
        /// [#head] and [#filter]. Single-file only.
        public RowReaderBuilder tail(long tailRows) {
            if (tailRows <= 0) {
                throw new IllegalArgumentException("tail row count must be positive: " + tailRows);
            }
            this.tailRows = tailRows;
            return this;
        }

        public AvroRowReader build() {
            ParquetFileReader.RowReaderBuilder underlying = fileReader.buildRowReader()
                    .projection(projection);
            if (filter != null) {
                underlying.filter(filter);
            }
            if (headRows > 0) {
                underlying.head(headRows);
            }
            if (tailRows > 0) {
                underlying.tail(tailRows);
            }
            RowReader rowReader = underlying.build();
            Schema avroSchema = AvroSchemaConverter.convert(fileReader.getFileSchema());
            return new AvroRowReader(rowReader, avroSchema);
        }
    }
}
