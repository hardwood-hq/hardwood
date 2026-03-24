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
/// ```java
/// try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
///      AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {
///     while (reader.hasNext()) {
///         GenericRecord record = reader.next();
///         long id = (Long) record.get("id");
///     }
/// }
/// ```
public final class AvroReaders {

    private AvroReaders() {
    }

    /// Create an AvroRowReader that reads all rows and columns.
    ///
    /// @param reader the opened ParquetFileReader
    /// @return a new AvroRowReader
    public static AvroRowReader createRowReader(ParquetFileReader reader) {
        Schema avroSchema = AvroSchemaConverter.convert(reader.getFileSchema());
        RowReader rowReader = reader.createRowReader();
        return new AvroRowReader(rowReader, avroSchema);
    }

    /// Create an AvroRowReader with predicate pushdown.
    ///
    /// @param reader the opened ParquetFileReader
    /// @param filter the filter predicate for row group pruning
    /// @return a new AvroRowReader
    public static AvroRowReader createRowReader(ParquetFileReader reader, FilterPredicate filter) {
        Schema avroSchema = AvroSchemaConverter.convert(reader.getFileSchema());
        RowReader rowReader = reader.createRowReader(filter);
        return new AvroRowReader(rowReader, avroSchema);
    }

    /// Create an AvroRowReader with column projection.
    ///
    /// @param reader the opened ParquetFileReader
    /// @param projection the columns to read
    /// @return a new AvroRowReader
    public static AvroRowReader createRowReader(ParquetFileReader reader, ColumnProjection projection) {
        Schema avroSchema = AvroSchemaConverter.convert(reader.getFileSchema());
        RowReader rowReader = reader.createRowReader(projection);
        return new AvroRowReader(rowReader, avroSchema);
    }

    /// Create an AvroRowReader with column projection and predicate pushdown.
    ///
    /// @param reader the opened ParquetFileReader
    /// @param projection the columns to read
    /// @param filter the filter predicate for row group pruning
    /// @return a new AvroRowReader
    public static AvroRowReader createRowReader(ParquetFileReader reader,
            ColumnProjection projection, FilterPredicate filter) {
        Schema avroSchema = AvroSchemaConverter.convert(reader.getFileSchema());
        RowReader rowReader = reader.createRowReader(projection, filter);
        return new AvroRowReader(rowReader, avroSchema);
    }
}
