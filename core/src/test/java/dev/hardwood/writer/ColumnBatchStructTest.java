/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `struct(...)` takes every `OPTIONAL` group other than a `LIST` or `MAP`; a list and a map
/// have verbs of their own, and a leaf has no group nulls to set.
class ColumnBatchStructTest {

    private static final FileSchema SCHEMA = FileSchema.builder("schema")
            .primitive("id", PhysicalType.INT32, RepetitionType.OPTIONAL)
            .list("tags", RepetitionType.OPTIONAL,
                    element -> element.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
            .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32, key -> { },
                    value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
            .build();

    @ParameterizedTest
    @ValueSource(strings = { "id", "tags", "props" })
    void rejectsALeafAListAndAMap(String path) throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), SCHEMA)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                    batch -> batch.struct(path, Validity.ofNulls(new boolean[] { false }))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Path " + path + " does not name a struct group");
        }
    }
}
