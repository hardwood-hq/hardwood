/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.decode.PageFormatProbe.HeaderAt;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.internal.thrift.ThriftCompactConstants;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads a page header from the bytes a read holds, widening its peek ([PageFormatProbe#readHeader]).
class PageFormatProbeTest {

    /// A header longer than the initial peek, read while only that peek is held, needs the
    /// doubled peek; read again from the peek it needed, once that is held, it parses.
    @Test
    void aHeaderReadResumesFromTheWiderPeekItNeeded() throws Exception {
        byte[] header = dictionaryPageHeader(0, 1500);
        int chunkLength = 8 * PageFormatProbe.INITIAL_PEEK_SIZE;
        ByteBuffer file = ByteBuffer.allocate(chunkLength).put(header).clear();
        ByteSource initialPeek = (offset, length) -> offset + length <= PageFormatProbe.INITIAL_PEEK_SIZE
                ? file.slice(Math.toIntExact(offset), length)
                : null;

        Parsed<HeaderAt> needed = PageFormatProbe.readHeader(initialPeek, 0, chunkLength,
                PageFormatProbe.INITIAL_PEEK_SIZE, "");

        assertThat(needed.needsBytes()).isTrue();
        assertThat(needed.needOffset()).isZero();
        assertThat(needed.needLength()).isEqualTo(2 * PageFormatProbe.INITIAL_PEEK_SIZE);

        // A source that holds only the needed range shows the read starts from it, not from the
        // initial peek
        ByteSource exactly = (offset, length) -> length == needed.needLength() ? file.slice(0, length) : null;
        HeaderAt read = PageFormatProbe.readHeader(exactly, 0, chunkLength, needed.needLength(), "").get();

        assertThat(read.header().type()).isEqualTo(PageType.DICTIONARY_PAGE);
        assertThat(read.length()).isEqualTo(header.length);
    }

    /// A header that runs past the end of its column chunk marks the file corrupt once the peek
    /// takes in the whole of the chunk.
    @Test
    void aHeaderLongerThanItsChunkFails() {
        ByteBuffer file = ByteBuffer.wrap(dictionaryPageHeader(0, 1500));
        ByteSource whole = (offset, length) -> file.slice(Math.toIntExact(offset), length);

        assertThatThrownBy(() -> PageFormatProbe.readHeader(whole, 0, 1200, PageFormatProbe.INITIAL_PEEK_SIZE, "[f] "))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[f] Page header at offset 0 does not fit in the 1200 bytes left in its"
                        + " column chunk — the file is likely corrupt");
    }

    /// The header of a dictionary page whose body is `compressedSize` bytes.
    private static byte[] dictionaryPageHeader(int compressedSize) {
        return dictionaryPageHeader(compressedSize, 0);
    }

    /// As [#dictionaryPageHeader(int)], carrying an unknown binary field of `padding` bytes, which
    /// a reader skips but has to have in its buffer to skip.
    private static byte[] dictionaryPageHeader(int compressedSize, int padding) {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(2); // Thrift PageType.DICTIONARY_PAGE
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(compressedSize);
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(compressedSize);
        if (padding > 0) {
            writer.writeFieldBegin(99, ThriftCompactConstants.FieldType.BINARY);
            writer.writeBinary(new byte[padding]);
        }
        writer.writeFieldStop();
        return writer.toByteArray();
    }
}
