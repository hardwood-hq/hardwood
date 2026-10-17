/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;

import dev.hardwood.Experimental;
import dev.hardwood.arrow.internal.ArrowPlan;
import dev.hardwood.arrow.internal.ArrowSchemaConverter;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;

/// Factory for creating [ArrowBatchReader] instances from a [ParquetFileReader].
///
/// Mirrors the noun / buildNoun shape of [ParquetFileReader]: the two-argument
/// [#batchReader(ParquetFileReader, BufferAllocator)] returns the default reader over every
/// column; [#buildBatchReader(ParquetFileReader, BufferAllocator)] returns a builder for
/// projection, filter and batch-size configuration.
///
/// The reader serves primitive columns and the structs, lists and maps around them; a
/// `repeated` field outside a `LIST` or `MAP` group reads as a list. A projection that selects
/// a Variant group, or a map's key without its value or value without its key, fails at
/// `build()`; a projection that leaves them out reads the rest of the file.
///
/// ```java
/// try (BufferAllocator allocator = new RootAllocator();
///      ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
///      ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator)) {
///     VectorSchemaRoot root = reader.getVectorSchemaRoot();
///     while (reader.loadNextBatch()) {
///         // read root's vectors
///     }
/// }
/// ```
///
/// **This API is [Experimental]:** the columns the reader serves and the shape of its
/// builder may change in future releases without prior deprecation, as may the
/// [dev.hardwood.reader.ColumnReader] it is built on.
@Experimental
public final class ArrowReaders {

    private ArrowReaders() {
    }

    /// Create an `ArrowBatchReader` over every column of the file.
    ///
    /// @param reader the file to read
    /// @param allocator the allocator every Arrow buffer is taken from
    /// @throws UnsupportedOperationException if the file holds a Variant group or a map with no
    ///         value; see [ArrowReaders]
    public static ArrowBatchReader batchReader(ParquetFileReader reader, BufferAllocator allocator)
            throws IOException {
        return buildBatchReader(reader, allocator).build();
    }

    /// Begin configuring an [ArrowBatchReader] with optional projection, filter and batch size.
    public static BatchReaderBuilder buildBatchReader(ParquetFileReader reader, BufferAllocator allocator) {
        return new BatchReaderBuilder(reader, allocator);
    }

    /// Builds an [ArrowBatchReader] with optional projection, filter and batch size. Mirrors
    /// [ParquetFileReader.ColumnReadersBuilder]: settings are forwarded to the underlying
    /// column readers, then wrapped in an [ArrowBatchReader] with the converted schema.
    public static final class BatchReaderBuilder {

        private final ParquetFileReader fileReader;
        private final BufferAllocator allocator;
        private ColumnProjection projection = ColumnProjection.all();
        private FilterPredicate filter;
        private int batchSize;

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

        /// Restrict reading to the given columns, in the order they are first named. Default:
        /// all columns. Names are Parquet column or group names, dotted into groups; a name
        /// that selects a Variant group fails at [#build()].
        public BatchReaderBuilder projection(ColumnProjection projection) {
            if (projection == null) {
                throw new IllegalArgumentException("projection must not be null");
            }
            this.projection = projection;
            return this;
        }

        /// Apply a row-group / record-level filter predicate. Every batch holds only the rows
        /// matching `filter`. The predicate may reference a column the projection does not
        /// select, nested columns included. Default: no filter.
        public BatchReaderBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        /// Set the maximum number of rows per batch. Default: chosen adaptively from the
        /// projected columns' widths, as [ParquetFileReader.ColumnReadersBuilder#batchSize(int)]
        /// describes.
        public BatchReaderBuilder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
            }
            this.batchSize = batchSize;
            return this;
        }

        /// @throws IllegalArgumentException if a projected name is not in the schema
        /// @throws UnsupportedOperationException if a selected node is a Variant group, a map
        ///         with only one of its key and value selected or with no value, a column of a
        ///         type the reader does not map, or a `DECIMAL` wider than Arrow's decimals
        /// @throws dev.hardwood.reader.SchemaIncompatibleException if a `FIXED_LEN_BYTE_ARRAY`
        ///         column declares no positive width
        public ArrowBatchReader build() throws IOException {
            // Convert before building the underlying readers: conversion rejects schemas Arrow
            // cannot represent here, and building the underlying readers already starts a column
            // worker per projected column, which a rejection afterwards would orphan with no
            // ArrowBatchReader to close them.
            ArrowPlan plan = ArrowSchemaConverter.plan(fileReader.getFileSchema(),
                    fileReader.getFileMetaData().schema(), fileReader.getFileMetaData().keyValueMetadata(), projection);
            ParquetFileReader.ColumnReadersBuilder underlying = fileReader.buildColumnReaders(projection);
            if (filter != null) {
                underlying.filter(filter);
            }
            if (batchSize > 0) {
                underlying.batchSize(batchSize);
            }
            return new ArrowBatchReader(allocator, underlying.build(), plan);
        }
    }
}
