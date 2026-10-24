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

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

/// Builds footers and files that nest as deep as a caller asks: a skipped Thrift value of nested
/// structs, and a schema of nested groups.
public final class DeepNestingFile {

    /// The deepest a skipped value nests, for callers outside this package
    public static final int MAX_SKIP_DEPTH = ThriftCompactReader.MAX_SKIP_DEPTH;
    /// The deepest a schema's groups nest, for callers outside this package
    public static final int MAX_SCHEMA_DEPTH = FileMetaDataReader.MAX_SCHEMA_DEPTH;

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private DeepNestingFile() {
    }

    /// A struct holding a struct, `depth` structs in all, the innermost empty.
    public static byte[] nestedStructs(int depth) {
        // Each struct but the innermost opens with the header of its field 1, and every struct
        // ends with a stop
        byte[] structs = new byte[2 * depth - 1];
        Arrays.fill(structs, 0, depth - 1, ThriftStructBuilder.fieldHeader(1, FieldType.STRUCT));
        return structs;
    }

    /// A footer of nothing but field 10, which is not one of `FileMetaData`'s, holding
    /// [#nestedStructs] of `depth`.
    public static byte[] footerSkipping(int depth) {
        ByteArrayOutputStream footer = new ByteArrayOutputStream();
        footer.write(ThriftStructBuilder.fieldHeader(10, FieldType.STRUCT));
        footer.writeBytes(nestedStructs(depth));
        footer.write(ThriftCompactConstants.STOP);
        return footer.toByteArray();
    }

    /// A footer of no row groups whose schema is `depth` optional groups, each holding the next,
    /// around one `INT32` leaf.
    public static byte[] footerOfSchema(int depth) {
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

    /// A file of nothing but `footer`, between the magic bytes and before its length.
    public static byte[] fileOf(byte[] footer) {
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes(MAGIC);
        file.writeBytes(footer);
        file.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(footer.length).array());
        file.writeBytes(MAGIC);
        return file.toByteArray();
    }
}
