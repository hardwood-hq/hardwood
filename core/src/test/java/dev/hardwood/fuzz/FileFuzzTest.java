/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.fuzz;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.util.function.Supplier;

import org.junit.jupiter.params.provider.MethodSource;

import com.code_intelligence.jazzer.junit.FuzzTest;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Reads arbitrary bytes as a Parquet file through the public API: every column through
/// [ColumnReaders], every row through a [RowReader] with each value converted, and the rows a
/// predicate on each of the first columns selects, which consults statistics, the page index,
/// dictionaries and bloom filters. A read completes, or fails with [ParquetReadException] or, for
/// a file Hardwood does not read, [UnsupportedOperationException]; a value conversion may also
/// fail with [DateTimeException], for a stored value outside the range of its Java type. Any
/// other throwable is a finding.
class FileFuzzTest {

    /// Columns a filtered read is made on, from the first
    private static final int FILTERED_COLUMNS = 3;

    /// A context of zero threads runs each read on the fuzzer's thread, so that a failure
    /// surfaces where the input is known, and runs it the same way each time.
    private static final HardwoodContext CONTEXT = HardwoodContext.create(0);

    @MethodSource("dev.hardwood.fuzz.Seeds#files")
    @FuzzTest
    void read(byte[] file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), CONTEXT)) {
            readColumns(reader);
            readRows(reader.rowReader());
            FileSchema schema = reader.getFileSchema();
            for (int column = 0; column < Math.min(FILTERED_COLUMNS, schema.getColumnCount()); column++) {
                FilterPredicate predicate = predicateOn(schema, schema.getColumn(column));
                if (predicate != null) {
                    readRows(reader.buildRowReader().filter(predicate).build());
                }
            }
        }
        catch (ParquetReadException | UnsupportedOperationException e) {
            // malformed, or a file Hardwood does not read
        }
    }

    private static void readColumns(ParquetFileReader reader) throws IOException {
        try (ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            while (columns.nextBatch()) {
                for (int i = 0; i < columns.getColumnCount(); i++) {
                    readBatch(columns.getColumnReader(i));
                }
            }
        }
    }

    private static void readBatch(ColumnReader column) {
        for (int layer = 0; layer < column.getLayerCount(); layer++) {
            column.getLayerValidity(layer);
            if (column.getLayerKind(layer) == LayerKind.REPEATED) {
                column.getLayerOffsets(layer);
            }
        }
        column.getLeafValidity();
        switch (column.getColumnSchema().type()) {
            case INT32 -> column.getInts();
            case INT64 -> column.getLongs();
            case FLOAT -> column.getFloats();
            case DOUBLE -> column.getDoubles();
            case BOOLEAN -> column.getBooleans();
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                column.getBinaries();
                if (column.getDictionaryIds() != null) {
                    column.getBinaryDictionary();
                }
            }
        }
    }

    private static void readRows(RowReader rows) throws IOException {
        try (rows) {
            while (rows.hasNext()) {
                rows.next();
                visitFields(rows);
            }
        }
    }

    private static void visitFields(StructAccessor fields) {
        for (int i = 0; i < fields.getFieldCount(); i++) {
            int field = i;
            visit(() -> fields.getValue(field));
        }
    }

    /// Visits the value `conversion` returns, if the stored value lies within its Java type.
    private static void visit(Supplier<Object> conversion) {
        Object value;
        try {
            value = conversion.get();
        }
        catch (DateTimeException e) {
            return;
        }
        switch (value) {
            case StructAccessor fields -> visitFields(fields);
            case PqList list -> {
                for (int i = 0; i < list.size(); i++) {
                    int element = i;
                    visit(() -> list.get(element));
                }
            }
            case PqMap map -> map.getEntries().forEach(entry -> {
                visit(entry::getKey);
                visit(entry::getValue);
            });
            case PqVariant variant -> visitVariant(variant);
            case null, default -> {
            }
        }
    }

    private static void visitVariant(PqVariant variant) {
        if (variant == null) {
            // a null field of a Variant object
            return;
        }
        switch (variant.type()) {
            case OBJECT -> {
                PqVariantObject object = variant.asObject();
                for (int i = 0; i < object.getFieldCount(); i++) {
                    visitVariant(object.getVariant(object.getFieldName(i)));
                }
            }
            case ARRAY -> {
                PqVariantArray array = variant.asArray();
                for (PqVariant element : array) {
                    visitVariant(element);
                }
            }
            default -> variant.toString();
        }
    }

    /// An equality predicate on `column` that the schema accepts, or `null` where it accepts none
    /// of the physical type's value.
    private static FilterPredicate predicateOn(FileSchema schema, ColumnSchema column) {
        String name = column.fieldPath().toString();
        FilterPredicate predicate = switch (column.type()) {
            case INT32 -> FilterPredicate.eq(name, 0);
            case INT64 -> FilterPredicate.eq(name, 0L);
            case FLOAT -> FilterPredicate.eq(name, 0f);
            case DOUBLE -> FilterPredicate.eq(name, 0d);
            case BOOLEAN -> FilterPredicate.eq(name, true);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> FilterPredicate.eq(name, new byte[column.typeLength() != null ? column.typeLength() : 1]);
            case INT96 -> FilterPredicate.isNull(name);
        };
        try {
            // A predicate the schema rejects is the caller's error, not the file's
            FilterPredicateResolver.resolve(predicate, schema);
            return predicate;
        }
        catch (IllegalArgumentException e) {
            return null;
        }
    }
}
