/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.decode.PageFormatProbe.Extent;
import dev.hardwood.internal.reader.decode.PageFormatProbe.HeaderAt;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.internal.thrift.PageLimit;
import dev.hardwood.internal.thrift.ThriftCompactConstants;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads a page header from the bytes a read holds, widening its peek ([PageFormatProbe#readHeader]).
class PageFormatProbeTest {

    /// A header longer than the initial peek, read while only that peek is held, needs a peek past
    /// its declared length; read again from the peek it needed, once that is held, it parses.
    @Test
    void aHeaderReadResumesFromTheWiderPeekItNeeded() throws Exception {
        byte[] header = dictionaryPageHeader(0, 1500);
        int chunkLength = 8 * PageFormatProbe.INITIAL_PEEK_SIZE;
        ByteBuffer file = ByteBuffer.allocate(chunkLength).put(header).clear();
        ByteSource initialPeek = (offset, length) -> offset + length <= PageFormatProbe.INITIAL_PEEK_SIZE
                ? file.slice(Math.toIntExact(offset), length)
                : null;

        Parsed<HeaderAt> needed = PageFormatProbe.readHeader(initialPeek, 0, chunkLength, Extent.CHUNK,
                PageFormatProbe.INITIAL_PEEK_SIZE, "");

        assertThat(needed.needsBytes()).isTrue();
        assertThat(needed.needOffset()).isZero();
        // past the padding's declared length, with a first peek's worth for what follows it
        assertThat(needed.needLength()).isBetween(header.length, header.length + PageFormatProbe.INITIAL_PEEK_SIZE);

        // A source that holds only the needed range shows the read starts from it, not from the
        // initial peek
        ByteSource exactly = (offset, length) -> length == needed.needLength() ? file.slice(0, length) : null;
        HeaderAt read = PageFormatProbe.readHeader(exactly, 0, chunkLength, Extent.CHUNK, needed.needLength(), "").get();

        assertThat(read.header().type()).isEqualTo(PageType.DICTIONARY_PAGE);
        assertThat(read.length()).isEqualTo(header.length);
    }

    /// A header that runs past the end of its column chunk marks the file corrupt once the peek
    /// takes in the whole of the chunk.
    @Test
    void aHeaderLongerThanItsChunkFails() {
        ByteBuffer file = ByteBuffer.wrap(dictionaryPageHeader(0, 1500));
        ByteSource whole = (offset, length) -> file.slice(Math.toIntExact(offset), length);

        assertThatThrownBy(() -> PageFormatProbe.readHeader(whole, 0, 1200, Extent.CHUNK,
                PageFormatProbe.INITIAL_PEEK_SIZE, "[f] "))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[f] Page header at offset 0 does not fit in the 1200 bytes left in its"
                        + " column chunk — the file is likely corrupt");
    }

    /// The same for a header longer than its page, where the page's size is known.
    @Test
    void aHeaderLongerThanItsPageFails() {
        ByteBuffer file = ByteBuffer.wrap(dictionaryPageHeader(0, 1500));
        ByteSource whole = (offset, length) -> file.slice(Math.toIntExact(offset), length);

        assertThatThrownBy(() -> PageFormatProbe.readHeader(whole, 0, 1200, Extent.PAGE,
                PageFormatProbe.INITIAL_PEEK_SIZE, "[f] "))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[f] Page header at offset 0 does not fit in the 1200 bytes left in its"
                        + " page — the file is likely corrupt");
    }

    /// No bound but the extent applies: a header of a few mebibytes, as long inline statistics
    /// make one, parses where its extent holds it.
    @Test
    void aHeaderOfAnySizeWithinItsExtentParses() throws Exception {
        byte[] header = dictionaryPageHeader(0, 3 * 1024 * 1024);
        ByteBuffer file = ByteBuffer.wrap(header);
        ByteSource whole = (offset, length) -> file.slice(Math.toIntExact(offset), length);

        HeaderAt read = PageFormatProbe.readHeader(whole, 0, header.length, Extent.PAGE,
                PageFormatProbe.INITIAL_PEEK_SIZE, "").get();

        assertThat(read.header().type()).isEqualTo(PageType.DICTIONARY_PAGE);
        assertThat(read.length()).isEqualTo(header.length);
    }

    /// A truncated read that knows what it needs, a binary value's declared length, widens the
    /// peek to it at once: a long header costs two windows, not a doubling per mebibyte.
    @Test
    void aLongHeaderIsReadInTwoWindows() throws Exception {
        byte[] header = dictionaryPageHeader(0, 3 * 1024 * 1024);
        ByteBuffer file = ByteBuffer.wrap(header);
        List<Integer> windows = new ArrayList<>();
        ByteSource recorded = (offset, length) -> {
            windows.add(length);
            return file.slice(Math.toIntExact(offset), length);
        };

        HeaderAt read = PageFormatProbe.readHeader(recorded, 0, header.length, Extent.CHUNK,
                PageFormatProbe.INITIAL_PEEK_SIZE, "").get();

        assertThat(read.length()).isEqualTo(header.length);
        assertThat(windows).hasSize(2).first().isEqualTo(PageFormatProbe.INITIAL_PEEK_SIZE);
    }

    /// A binary value declaring more bytes than the header's extent holds, as a corrupt length
    /// does, fails at the first window, without reading toward the extent's end.
    @Test
    void aDeclaredLengthPastTheExtentFailsWithoutReadingIt() {
        byte[] header = truncatedHeaderDeclaring(1L << 30);
        ByteBuffer file = ByteBuffer.allocate(PageFormatProbe.INITIAL_PEEK_SIZE).put(header).clear();
        List<Integer> windows = new ArrayList<>();
        ByteSource recorded = (offset, length) -> {
            windows.add(length);
            return file.slice(Math.toIntExact(offset), Math.min(length, file.capacity()));
        };
        long chunk = 64L * 1024 * 1024;

        assertThatThrownBy(() -> PageFormatProbe.readHeader(recorded, 0, chunk, Extent.CHUNK,
                PageFormatProbe.INITIAL_PEEK_SIZE, "[f] "))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[f] Page header at offset 0 does not fit in the " + chunk + " bytes left in its"
                        + " column chunk — the file is likely corrupt");
        assertThat(windows).containsExactly(PageFormatProbe.INITIAL_PEEK_SIZE);
    }

    /// In a chunk longer than the largest page, a header is bounded by that page, and its failure
    /// says so.
    @Test
    void aHeaderLongerThanTheLargestPageFails() {
        byte[] header = truncatedHeaderDeclaring(Integer.MAX_VALUE);
        ByteBuffer file = ByteBuffer.allocate(PageFormatProbe.INITIAL_PEEK_SIZE).put(header).clear();
        ByteSource whole = (offset, length) -> file.slice(Math.toIntExact(offset), Math.min(length, file.capacity()));

        assertThatThrownBy(() -> PageFormatProbe.readHeader(whole, 0, 3_000_000_000L, Extent.CHUNK,
                PageFormatProbe.INITIAL_PEEK_SIZE, "[f] "))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[f] Page header at offset 0 does not fit in " + PageLimit.MAX_PAGE_BYTES
                        + " bytes, the most a page holds — the file is likely corrupt");
    }

    /// The start of a dictionary page header whose unknown binary field declares `length` bytes,
    /// cut off after the declaration.
    private static byte[] truncatedHeaderDeclaring(long length) {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(2); // Thrift PageType.DICTIONARY_PAGE
        writer.writeFieldBegin(99, ThriftCompactConstants.FieldType.BINARY);
        byte[] fields = writer.toByteArray();
        byte[] declared = new byte[10];
        int n = 0;
        long v = length;
        while ((v & ~0x7FL) != 0) {
            declared[n++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        declared[n++] = (byte) v;
        byte[] bytes = new byte[fields.length + n];
        System.arraycopy(fields, 0, bytes, 0, fields.length);
        System.arraycopy(declared, 0, bytes, fields.length, n);
        return bytes;
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
        writeDictionaryPageHeader(writer);
        if (padding > 0) {
            writer.writeFieldBegin(99, ThriftCompactConstants.FieldType.BINARY);
            writer.writeBinary(new byte[padding]);
        }
        writer.writeFieldStop();
        return writer.toByteArray();
    }

    /// Writes a `dictionary_page_header` of no values, `PLAIN`-encoded, as field 7 of a page header.
    private static void writeDictionaryPageHeader(ThriftCompactWriter writer) {
        writer.writeFieldBegin(7, ThriftCompactConstants.FieldType.STRUCT);
        short saved = writer.pushFieldIdContext();
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(0);
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(0);
        writer.writeFieldStop();
        writer.popFieldIdContext(saved);
    }
}
