/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Names a field of a footer a real writer produced, rather than one built by
/// hand.
///
/// [ThriftFieldNamingTest] pins each form of the name on input assembled byte
/// by byte, which is exact but says nothing about whether the naming reaches a
/// failure in a file PyArrow wrote, through the API a user calls. This damages
/// one, opens it the way a caller would, and checks the whole message they see
/// — the file, the byte and the field.
class CorruptFooterNamingTest {

    /// Byte 195 of `plain_uncompressed.parquet` is the field header for
    /// `SchemaElement.type`; flipping bit 3 turns its wire type into 13, which
    /// is not a type.
    ///
    /// The offset is a property of the checked-in bytes, so regenerating the
    /// fixture will move it. That fails this test with the message it did
    /// produce, which is what a new offset is derived from.
    private static final int FIELD_HEADER_BYTE = 195;
    private static final int WIRE_TYPE_BIT = 3;

    @Test
    void aCorruptFieldHeaderNamesTheFieldItStoppedOn() throws Exception {
        byte[] damaged = Files.readAllBytes(
                Path.of("src/test/resources/plain_uncompressed.parquet"));
        damaged[FIELD_HEADER_BYTE] ^= (byte) (1 << WIRE_TYPE_BIT);

        // The byte the message names is the byte this test damaged: the region
        // says where the footer starts and the parse says how far into it it
        // got, and neither half knows the answer on its own.
        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(damaged))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] footer at byte " + FIELD_HEADER_BYTE
                        + String.format(" (0x%06x)", FIELD_HEADER_BYTE)
                        + " — SchemaElement.type — Unknown field type: 13");
    }
}
