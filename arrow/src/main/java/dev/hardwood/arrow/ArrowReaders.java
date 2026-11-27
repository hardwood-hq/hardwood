/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.hardwood.Experimental;
import dev.hardwood.arrow.internal.ArrowPlan;
import dev.hardwood.arrow.internal.ArrowSchemaConverter;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.schema.ColumnProjection;

/// Factory for converting Parquet schemas to Arrow and creating [ArrowBatchReader] instances from
/// a [ParquetFileReader].
///
/// [#schema(ParquetFileReader)] and [#schema(ParquetFileReader, ColumnProjection)] convert the
/// schema without starting column workers or requiring a [BufferAllocator];
/// [#batchReader(ParquetFileReader, BufferAllocator)] reads every column;
/// [#buildBatchReader(ParquetFileReader, BufferAllocator)] returns a builder for projection,
/// filtering, batch size, and dictionary output.
///
/// **This API is [Experimental]:** it may change in future releases without prior deprecation.
@Experimental
public final class ArrowReaders {

    private ArrowReaders() {
    }

    /// Convert every column of `reader`'s schema to an Arrow [Schema] without starting column
    /// workers or allocating buffers.
    public static Schema schema(ParquetFileReader reader) {
        return schema(reader, ColumnProjection.all());
    }

    /// Convert the projected columns of `reader`'s schema to an Arrow [Schema] without starting
    /// column workers or allocating buffers.
    public static Schema schema(ParquetFileReader reader, ColumnProjection projection) {
        if (reader == null) {
            throw new IllegalArgumentException("reader must not be null");
        }
        if (projection == null) {
            throw new IllegalArgumentException("projection must not be null");
        }
        return ArrowSchemaConverter.plan(reader.getFileSchema(), reader.getFileMetaData().keyValueMetadata(),
                projection).schema();
    }

    /// Create an [ArrowBatchReader] over every column of the file.
    public static ArrowBatchReader batchReader(ParquetFileReader reader, BufferAllocator allocator)
            throws IOException {
        return buildBatchReader(reader, allocator).build();
    }

    /// Begin configuring an [ArrowBatchReader] with optional projection, filters, batch size, and
    /// dictionary encoding.
    public static BatchReaderBuilder buildBatchReader(ParquetFileReader reader, BufferAllocator allocator) {
        return new BatchReaderBuilder(reader, allocator);
    }

    /// Builds an [ArrowBatchReader], mirroring [ParquetFileReader.ColumnReadersBuilder].
    public static final class BatchReaderBuilder {

        private final ParquetFileReader fileReader;
        private final BufferAllocator allocator;
        private ColumnProjection projection = ColumnProjection.all();
        private FilterPredicate filter;
        private RowGroupPredicate rowGroupFilter;
        private int batchSize;
        private boolean dictionaryEncoding;
        private final Set<String> dictionaryColumns = new LinkedHashSet<>();

        private BatchReaderBuilder(ParquetFileReader fileReader, BufferAllocator allocator) {
            if (fileReader == null) {
                throw new IllegalArgumentException("reader must not be null");
            }
            if (allocator == null) {
                throw new IllegalArgumentException("allocator must not be null");
            }
            this.fileReader = fileReader;
            this.allocator = allocator;
        }

        /// Restrict reading to the given columns, in the order they are first named.
        public BatchReaderBuilder projection(ColumnProjection projection) {
            if (projection == null) {
                throw new IllegalArgumentException("projection must not be null");
            }
            this.projection = projection;
            return this;
        }

        /// Apply a row-group / record-level filter predicate.
        public BatchReaderBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        /// Apply a row-group selection predicate (e.g. byte-range, for split-aware reading).
        /// Single-file only.
        public BatchReaderBuilder filter(RowGroupPredicate rowGroupFilter) {
            this.rowGroupFilter = rowGroupFilter;
            return this;
        }

        /// Set the maximum number of rows per batch.
        public BatchReaderBuilder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
            }
            this.batchSize = batchSize;
            return this;
        }

        /// When `true`, emit Arrow dictionary-encoded (`Int32`-indexed) vectors for every
        /// projected `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` column (except `UNKNOWN`)
        /// whose footer metadata uses Parquet dictionary encoding (on a multi-file reader, the
        /// first file's footer is consulted; use [#dictionaryColumns(String...)] for heterogeneous
        /// files). Default: `false`.
        public BatchReaderBuilder dictionaryEncoding(boolean enabled) {
            this.dictionaryEncoding = enabled;
            return this;
        }

        /// Explicitly emit the named leaf columns (dot-separated Parquet field paths) as Arrow
        /// dictionary-encoded (`Int32`-indexed) vectors. Combines with
        /// [#dictionaryEncoding(boolean)].
        public BatchReaderBuilder dictionaryColumns(String... columns) {
            if (columns == null) {
                throw new IllegalArgumentException("columns must not be null");
            }
            for (String column : columns) {
                if (column == null) {
                    throw new IllegalArgumentException("dictionary column name must not be null");
                }
                this.dictionaryColumns.add(column);
            }
            return this;
        }

        /// Build the configured [ArrowBatchReader], converting and validating the schema before
        /// starting column workers.
        public ArrowBatchReader build() throws IOException {
            ArrowPlan plan = ArrowSchemaConverter.plan(fileReader.getFileSchema(),
                    fileReader.getFileMetaData().keyValueMetadata(), projection, dictionaryEncoding,
                    dictionaryColumns, fileReader.getFileMetaData().rowGroups());
            ParquetFileReader.ColumnReadersBuilder underlying = fileReader.buildColumnReaders(projection);
            if (filter != null) {
                underlying.filter(filter);
            }
            if (rowGroupFilter != null) {
                underlying.filter(rowGroupFilter);
            }
            if (batchSize > 0) {
                underlying.batchSize(batchSize);
            }
            return new ArrowBatchReader(allocator, underlying.build(), plan);
        }
    }
}
