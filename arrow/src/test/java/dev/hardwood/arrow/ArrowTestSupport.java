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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.VectorAppender;

import static org.assertj.core.api.Assertions.assertThat;

/// Fixture paths, expected tables and table comparison for the module's tests.
final class ArrowTestSupport {

    /// Core's checked-in fixtures: the shapes this module rejects, and the ones Hardwood's
    /// writer cannot produce for the Arrow C++ comparison.
    static final Path CORE_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    /// The fixtures the tests write for themselves.
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

    /// The table the reader must produce from `name`, as one root. The caller closes it.
    static VectorSchemaRoot expected(String name, BufferAllocator allocator) {
        return switch (name) {
            case "scalars", "scalars_dict", "scalars_stored_schema" -> ArrowFixtures.expectedScalars(allocator);
            case "empty" -> ArrowFixtures.expectedEmpty(allocator);
            default -> throw new IllegalArgumentException("No expected table for " + name);
        };
    }

    /// Every batch of `reader`, appended into one root of its schema. The caller closes it.
    static VectorSchemaRoot readAll(ArrowReader reader, BufferAllocator allocator) throws IOException {
        VectorSchemaRoot source = reader.getVectorSchemaRoot();
        VectorSchemaRoot target = VectorSchemaRoot.create(source.getSchema(), allocator);
        try {
            // The appender grows an allocated vector; it does not allocate an empty one.
            target.allocateNew();
            int rows = 0;
            while (reader.loadNextBatch()) {
                for (int i = 0; i < source.getFieldVectors().size(); i++) {
                    source.getVector(i).accept(new VectorAppender(target.getVector(i)), null);
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

    /// The rows of `root`, each value in a form `equals` compares by content: `byte[]`
    /// payloads as hex, `Text` as `String`, everything else as the vector's own object.
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

    /// Asserts that `actual` has `expected`'s schema and, column by column, equal vectors.
    static void assertSameTable(VectorSchemaRoot expected, VectorSchemaRoot actual) {
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

    private static List<Object> column(FieldVector vector) {
        List<Object> values = new ArrayList<>(vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            values.add(normalize(vector.getObject(i)));
        }
        return values;
    }
}
