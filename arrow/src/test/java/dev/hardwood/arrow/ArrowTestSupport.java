/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.VectorAppender;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Fixture paths, recursive Arrow vector validation, and table comparison helpers for tests.
final class ArrowTestSupport {

    static final Path CORE_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    static final List<String> FIXTURES = List.of("scalars", "scalars_dict", "empty");

    private ArrowTestSupport() {
    }

    static Path fixture(String name) throws IOException {
        ArrowFixtures.ensureWritten();
        return ArrowFixtures.file(name);
    }

    static Path coreFixture(String name) {
        return CORE_RESOURCES.resolve(name + ".parquet");
    }

    static VectorSchemaRoot readFile(Path file, BufferAllocator allocator) throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator)) {
            return readAll(reader, allocator);
        }
    }

    /// Reads every batch of `reader` into one plain `VectorSchemaRoot` (decoding any top-level
    /// dictionary-encoded vectors against `reader`'s current dictionaries), validating every
    /// loaded batch and dictionary.
    static VectorSchemaRoot readAll(ArrowReader reader, BufferAllocator allocator) throws IOException {
        VectorSchemaRoot source = reader.getVectorSchemaRoot();
        Schema plainSchema = decodedSchema(source.getSchema(), reader);
        VectorSchemaRoot target = VectorSchemaRoot.create(plainSchema, allocator);
        try {
            target.allocateNew();
            int rows = 0;
            while (reader.loadNextBatch()) {
                validateVectors(source, reader);
                for (Dictionary dict : reader.getDictionaryVectors().values()) {
                    validateVector(dict.getVector(), null);
                }
                for (int i = 0; i < source.getFieldVectors().size(); i++) {
                    FieldVector vector = source.getVector(i);
                    DictionaryEncoding encoding = vector.getField().getDictionary();
                    if (encoding != null) {
                        Dictionary dict = reader.lookup(encoding.getId());
                        try (FieldVector decoded = (FieldVector) DictionaryEncoder.decode(vector, dict)) {
                            decoded.accept(new VectorAppender(target.getVector(i)), null);
                        }
                    }
                    else {
                        vector.accept(new VectorAppender(target.getVector(i)), null);
                    }
                }
                rows += source.getRowCount();
            }
            target.setRowCount(rows);
            return target;
        }
        catch (IOException | RuntimeException e) {
            target.close();
            throw e;
        }
    }

    private static Schema decodedSchema(Schema schema, DictionaryProvider provider) {
        List<Field> fields = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            DictionaryEncoding encoding = field.getDictionary();
            if (encoding != null) {
                Dictionary dict = provider.lookup(encoding.getId());
                Field valueField = dict.getVector().getField();
                fields.add(new Field(field.getName(),
                        new FieldType(field.isNullable(), valueField.getType(), null, field.getMetadata()),
                        valueField.getChildren()));
            }
            else {
                fields.add(field);
            }
        }
        return new Schema(fields, schema.getCustomMetadata());
    }

    /// Recursively validates Arrow structural invariants on every vector in `root`.
    static void validateVectors(VectorSchemaRoot root) {
        validateVectors(root, null);
    }

    static void validateVectors(VectorSchemaRoot root, DictionaryProvider dictionaries) {
        for (FieldVector vector : root.getFieldVectors()) {
            validateVector(vector, dictionaries);
        }
    }

    static void validateVector(FieldVector vector) {
        validateVector(vector, null);
    }

    static void validateVector(FieldVector vector, DictionaryProvider dictionaries) {
        if (!vector.getField().isNullable()) {
            assertThat(vector.getNullCount())
                    .as("non-nullable vector %s must have nullCount == 0", vector.getField())
                    .isZero();
        }
        int count = vector.getValueCount();
        if (vector instanceof BaseVariableWidthVector varWidth && count > 0) {
            validateOffsets(varWidth.getOffsetBuffer(), count, varWidth.getDataBuffer().capacity(),
                    vector.getField().getName());
        }
        else if (vector instanceof ListVector list && count > 0) {
            validateOffsets(list.getOffsetBuffer(), count, list.getDataVector().getValueCount(),
                    vector.getField().getName());
        }
        DictionaryEncoding encoding = vector.getField().getDictionary();
        if (encoding != null && dictionaries != null && vector instanceof IntVector indices) {
            Dictionary dict = dictionaries.lookup(encoding.getId());
            assertThat(dict).as("dictionary %s for %s", encoding.getId(), vector.getName()).isNotNull();
            int dictSize = dict.getVector().getValueCount();
            for (int i = 0; i < count; i++) {
                if (!indices.isNull(i)) {
                    int idx = indices.get(i);
                    assertThat(idx)
                            .as("dictionary index at %s[%s]", vector.getName(), i)
                            .isGreaterThanOrEqualTo(0)
                            .isLessThan(dictSize);
                }
            }
        }
        for (FieldVector child : vector.getChildrenFromFields()) {
            validateVector(child, dictionaries);
        }
    }

    private static void validateOffsets(ArrowBuf offsets, int count, long maxOffset, String name) {
        int prev = offsets.getInt(0);
        assertThat(prev).as("first offset of %s", name).isGreaterThanOrEqualTo(0);
        for (int i = 1; i <= count; i++) {
            int curr = offsets.getInt((long) i * Integer.BYTES);
            assertThat(curr).as("offset[%s] of %s", i, name).isGreaterThanOrEqualTo(prev);
            prev = curr;
        }
        assertThat((long) prev).as("final offset of %s", name).isLessThanOrEqualTo(maxOffset);
    }

    static List<List<Object>> rows(VectorSchemaRoot root) {
        List<List<Object>> rows = new ArrayList<>(root.getRowCount());
        for (int r = 0; r < root.getRowCount(); r++) {
            List<Object> row = new ArrayList<>(root.getFieldVectors().size());
            for (FieldVector vector : root.getFieldVectors()) {
                row.add(normalize(vector.getObject(r)));
            }
            rows.add(row);
        }
        return rows;
    }

    static Object normalize(Object value) {
        if (value instanceof byte[] bytes) {
            return HexFormat.of().formatHex(bytes);
        }
        if (value instanceof Text text) {
            return text.toString();
        }
        return value;
    }

    static void assertSameTable(VectorSchemaRoot expected, VectorSchemaRoot actual) {
        validateVectors(actual);
        assertThat(actual.getSchema()).isEqualTo(expected.getSchema());
        assertThat(actual.getRowCount()).isEqualTo(expected.getRowCount());
        for (int i = 0; i < expected.getFieldVectors().size(); i++) {
            FieldVector expectedVector = expected.getVector(i);
            FieldVector actualVector = actual.getVector(i);
            assertThat(actualVector.getNullCount())
                    .as("null count of %s", expectedVector.getName())
                    .isEqualTo(expectedVector.getNullCount());
            assertThat(VectorEqualsVisitor.vectorEquals(expectedVector, actualVector))
                    .as("column %s: expected %s but was %s", expectedVector.getName(),
                            column(expectedVector), column(actualVector))
                    .isTrue();
        }
    }

    /// Compares type, null count, and values while ignoring field names and metadata (for files
    /// carrying `ARROW:schema`, where Arrow C++ restores its stored names).
    static void assertSameTableIgnoringNames(VectorSchemaRoot expected, VectorSchemaRoot actual) {
        validateVectors(actual);
        assertThat(actual.getFieldVectors()).hasSameSizeAs(expected.getFieldVectors());
        assertThat(actual.getRowCount()).isEqualTo(expected.getRowCount());
        for (int i = 0; i < expected.getFieldVectors().size(); i++) {
            FieldVector expectedVector = expected.getVector(i);
            FieldVector actualVector = actual.getVector(i);
            assertThat(new TypeEqualsVisitor(expectedVector, false, false).equals(actualVector))
                    .as("type of %s: expected %s but was %s", actualVector.getName(), expectedVector.getField(),
                            actualVector.getField())
                    .isTrue();
            assertThat(actualVector.getNullCount())
                    .as("null count of %s", actualVector.getName())
                    .isEqualTo(expectedVector.getNullCount());
            RangeEqualsVisitor values = new RangeEqualsVisitor(expectedVector, actualVector, (left, right) -> true);
            assertThat(values.rangeEquals(new Range(0, 0, expectedVector.getValueCount())))
                    .as("column %s: expected %s but was %s", actualVector.getName(),
                            column(expectedVector), column(actualVector))
                    .isTrue();
        }
    }

    private static List<Object> column(FieldVector vector) {
        List<Object> values = new ArrayList<>(vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            values.add(normalize(vector.getObject(i)));
        }
        return values;
    }
}
