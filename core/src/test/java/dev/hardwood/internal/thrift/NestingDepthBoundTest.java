/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.ThriftCompactConstants.ElementType;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The two parsers that recurse once per level a file declares: skipping a Thrift value and
/// building the schema tree. Each accepts nesting up to its bound and fails one level past it
/// with a [ParquetReadException], where unbounded recursion would raise `StackOverflowError`.
class NestingDepthBoundTest {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private static ThriftCompactReader reader(byte[] bytes) {
        return new ThriftCompactReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    /// A struct holding a struct, `depth` structs in all, the innermost empty.
    private static byte[] nestedStructs(int depth) {
        // Each struct but the innermost opens with the header of its field 1, and every struct
        // ends with a stop
        byte[] structs = new byte[2 * depth - 1];
        Arrays.fill(structs, 0, depth - 1, ThriftStructBuilder.fieldHeader(1, FieldType.STRUCT));
        return structs;
    }

    /// A list holding one list, `depth` lists in all, the innermost an empty `list<i32>`.
    private static byte[] nestedLists(int depth) {
        byte[] lists = new byte[depth];
        for (int i = 0; i < depth - 1; i++) {
            lists[i] = ThriftStructBuilder.listHeader(1, ElementType.LIST);
        }
        lists[depth - 1] = ThriftStructBuilder.listHeader(0, ElementType.I32);
        return lists;
    }

    @Test
    void aStructNestedToTheBoundIsSkipped() {
        byte[] bytes = nestedStructs(ThriftCompactReader.MAX_SKIP_DEPTH);
        ThriftCompactReader reader = reader(bytes);

        reader.skipField(FieldType.STRUCT.code());

        assertThat(reader.getBytesRead()).isEqualTo(bytes.length);
    }

    @Test
    void aStructNestedPastTheBoundFails() {
        ThriftCompactReader reader = reader(nestedStructs(ThriftCompactReader.MAX_SKIP_DEPTH + 1));

        assertThatThrownBy(() -> reader.skipField(FieldType.STRUCT.code()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Skipped value nests deeper than the maximum depth of 64");
    }

    @Test
    void aListNestedToTheBoundIsSkipped() {
        byte[] bytes = nestedLists(ThriftCompactReader.MAX_SKIP_DEPTH);
        ThriftCompactReader reader = reader(bytes);

        reader.skipField(FieldType.LIST.code());

        assertThat(reader.getBytesRead()).isEqualTo(bytes.length);
    }

    @Test
    void aListNestedPastTheBoundFails() {
        ThriftCompactReader reader = reader(nestedLists(ThriftCompactReader.MAX_SKIP_DEPTH + 1));

        assertThatThrownBy(() -> reader.skipField(FieldType.LIST.code()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Skipped value nests deeper than the maximum depth of 64");
    }

    /// A footer field this reader does not know, nested ten thousand levels deep: about 10 KB.
    @Test
    void anUnknownFooterFieldNestedFarPastTheBoundFails() {
        // Field 10 is not one of FileMetaData's, so the reader skips it
        ByteArrayOutputStream footer = new ByteArrayOutputStream();
        footer.write(ThriftStructBuilder.fieldHeader(10, FieldType.STRUCT));
        footer.writeBytes(nestedStructs(10_000));
        footer.write(ThriftCompactConstants.STOP);

        assertThatThrownBy(() -> FileMetaDataReader.read(reader(footer.toByteArray())))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("FileMetaData field 10 — Skipped value nests deeper than the maximum depth of 64");
    }

    @Test
    void aSchemaNestedToTheBoundOpens() throws Exception {
        InputFile file = InputFile.of(ByteBuffer.wrap(fileOfSchema(FileMetaDataReader.MAX_SCHEMA_DEPTH)));

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            ColumnSchema leaf = reader.getFileSchema().getColumn(0);
            assertThat(leaf.fieldPath().elements()).hasSize(FileMetaDataReader.MAX_SCHEMA_DEPTH + 1);
            assertThat(leaf.maxDefinitionLevel()).isEqualTo(FileMetaDataReader.MAX_SCHEMA_DEPTH + 1);
        }
    }

    @Test
    void aSchemaNestedPastTheBoundFails() {
        byte[] footer = footerOfSchema(FileMetaDataReader.MAX_SCHEMA_DEPTH + 1);

        assertThatThrownBy(() -> FileMetaDataReader.read(reader(footer)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("FileMetaData.schema — groups must not nest deeper than 500 levels, "
                        + "but group 'g500' is at level 501");
    }

    /// Twenty thousand nested groups, past what building the schema tree recursively survives.
    @Test
    void aSchemaNestedFarPastTheBoundFailsToOpen() {
        InputFile file = InputFile.of(ByteBuffer.wrap(fileOfSchema(20_000)));

        assertThatThrownBy(() -> ParquetFileReader.open(file))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] FileMetaData.schema — groups must not nest deeper than 500 levels, "
                        + "but group 'g500' is at level 501");
    }

    /// A schema of `depth` optional groups, each holding the next, around one `INT32` leaf.
    private static byte[] footerOfSchema(int depth) {
        List<SchemaElement> schema = new ArrayList<>(depth + 2);
        schema.add(new SchemaElement("root", null, null, null, 1, null, null, null, null, null));
        for (int i = 0; i < depth; i++) {
            schema.add(new SchemaElement("g" + i, null, null, RepetitionType.OPTIONAL, 1, null, null, null, null,
                    null));
        }
        schema.add(new SchemaElement("leaf", PhysicalType.INT32, null, RepetitionType.OPTIONAL, null, null, null,
                null, null, null));
        ThriftCompactWriter writer = new ThriftCompactWriter();
        FileMetaDataWriter.write(writer, new FileMetaData(2, schema, 0, List.of(), Map.of(), null, List.of()));
        return writer.toByteArray();
    }

    /// A file of no row groups whose footer declares a schema [#footerOfSchema] builds.
    private static byte[] fileOfSchema(int depth) {
        byte[] footer = footerOfSchema(depth);
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes(MAGIC);
        file.writeBytes(footer);
        file.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(footer.length).array());
        file.writeBytes(MAGIC);
        return file.toByteArray();
    }
}
