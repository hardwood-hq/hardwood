/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.metadata.SchemaElements;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A `LIST` or `MAP` group whose children the row reader cannot find elements or entries in,
/// or a `LIST` group without a column to count its elements from, which [FileSchema] accepts,
/// fails the row reader's construction.
class TopLevelFieldMapShapeTest {

    @Test
    void listGroupWithoutRepeatedFieldFails() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElements.root("root", 1),
                SchemaElements.group("l", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElements.group("list", RepetitionType.OPTIONAL, 1),
                SchemaElements.primitive("element", PhysicalType.INT32, RepetitionType.OPTIONAL)));

        assertThatThrownBy(() -> build(schema))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("LIST group 'l' holds no repeated field, so its rows have no elements to read");
    }

    @Test
    void listGroupWithoutColumnFails() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElements.root("root", 1),
                SchemaElements.group("l", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElements.group("list", RepetitionType.REPEATED, 1),
                SchemaElements.group("element", RepetitionType.OPTIONAL, 0)));

        assertThatThrownBy(() -> build(schema))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("LIST group 'l' holds no column, so its rows have no elements to read");
    }

    @Test
    void mapGroupOfRepeatedPrimitiveFails() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElements.root("root", 1),
                SchemaElements.group("m", RepetitionType.OPTIONAL, 1, LogicalType.map()),
                SchemaElements.primitive("key", PhysicalType.INT32, RepetitionType.REPEATED)));

        assertThatThrownBy(() -> build(schema))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("MAP group 'm' holds no repeated group with a key, so its rows have no entries to read");
    }

    @Test
    void mapGroupWhoseKeyHoldsNoColumnFails() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElements.root("root", 1),
                SchemaElements.group("m", RepetitionType.OPTIONAL, 1, LogicalType.map()),
                SchemaElements.group("key_value", RepetitionType.REPEATED, 2),
                SchemaElements.group("key", RepetitionType.REQUIRED, 0),
                SchemaElements.primitive("value", PhysicalType.INT32, RepetitionType.OPTIONAL)));

        assertThatThrownBy(() -> build(schema))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("MAP group 'm' holds no column in its key, so its rows have no entries to read");
    }

    private static TopLevelFieldMap build(FileSchema schema) {
        return TopLevelFieldMap.build(schema, ProjectedSchema.create(schema, ColumnProjection.all()));
    }
}
