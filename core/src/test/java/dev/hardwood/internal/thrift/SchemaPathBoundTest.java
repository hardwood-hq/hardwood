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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The schema's column and group paths are held to a multiple of the footer's length before any
/// of them is built: a long group name over many leaves, a few bytes of footer per leaf, would
/// otherwise make the open build its name once per leaf.
class SchemaPathBoundTest {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    @Test
    void aLongGroupNameOverManyLeavesFailsToOpen() {
        byte[] file = fileOfSchema("g".repeat(10_000), 10_000);

        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] FileMetaData.schema — the paths of the schema's columns and groups "
                        + "run to 100068890 characters, more than 256 times the footer's 128920 bytes "
                        + "(33003520)");
    }

    @Test
    void aWideSchemaUnderOneStructOpens() throws Exception {
        String group = "a_struct_whose_name_runs_to_sixty_four_characters_like_some_do__";
        byte[] file = fileOfSchema(group, 20_000);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            assertThat(reader.getFileSchema().getColumn(group + ".c19999").columnIndex()).isEqualTo(19_999);
        }
    }

    /// A file of no row groups whose schema holds one group named `group` around `leaves`
    /// required `INT32` leaves `c0`, `c1`, ….
    private static byte[] fileOfSchema(String group, int leaves) {
        List<SchemaElement> schema = new ArrayList<>(leaves + 2);
        schema.add(new SchemaElement("root", null, null, null, 1, null, null, null, null, null));
        schema.add(new SchemaElement(group, null, null, RepetitionType.REQUIRED, leaves, null, null, null, null,
                null));
        for (int i = 0; i < leaves; i++) {
            schema.add(new SchemaElement("c" + i, PhysicalType.INT32, null, RepetitionType.REQUIRED, null, null,
                    null, null, null, null));
        }
        ThriftCompactWriter writer = new ThriftCompactWriter();
        FileMetaDataWriter.write(writer, new FileMetaData(2, schema, 0, List.of(), Map.of(), null, List.of()));
        byte[] footer = writer.toByteArray();
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes(MAGIC);
        file.writeBytes(footer);
        file.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(footer.length).array());
        file.writeBytes(MAGIC);
        return file.toByteArray();
    }
}
