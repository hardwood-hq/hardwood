/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page header that outruns the buffer it was given says so as a truncation.
///
/// `SequentialFetchPlan` and `PageFormatProbe` read a page header by peeking a guessed number of
/// bytes in front of it and doubling the guess when the header does not fit. Both catch
/// [ThriftTruncatedException] and nothing else, so every way a header can outrun the peek has to
/// raise that type — otherwise a valid file whose header is merely longer than the guess fails
/// instead of being read on the next, larger, attempt.
///
/// `DataPageHeader.statistics` is what makes the header long: its `min_value`/`max_value` are
/// file-supplied binaries with no bound in the format, so the declared length is noticed by
/// `checkedBinaryLength` before the bytes are reached.
class PageHeaderPeekGrowthTest {

    private static ThriftCompactReader reader(byte[] bytes) {
        return new ThriftCompactReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    /// A `DATA_PAGE` header whose statistics carry a `max_value` of `boundLength` bytes.
    private static byte[] pageHeaderWithStatisticsBound(int boundLength) {
        byte[] statistics = new ThriftStructBuilder()
                .field(5, FieldType.BINARY).binary(new byte[boundLength])
                .stop().build();
        byte[] dataPageHeader = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(10)   // num_values
                .field(2, FieldType.I32).i32(0)    // encoding = PLAIN
                .field(3, FieldType.I32).i32(3)    // definition_level_encoding = RLE
                .field(4, FieldType.I32).i32(3)    // repetition_level_encoding = RLE
                .field(5, FieldType.STRUCT).nested(statistics)
                .stop().build();
        return new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(0)    // type = DATA_PAGE
                .field(2, FieldType.I32).i32(100)  // uncompressed_page_size
                .field(3, FieldType.I32).i32(100)  // compressed_page_size
                .field(5, FieldType.STRUCT).nested(dataPageHeader)
                .stop().build();
    }

    @Test
    void aStatisticsBoundPastThePeekIsATruncation() {
        byte[] full = pageHeaderWithStatisticsBound(200);
        // Stop short of the bound's payload, which is what a peek smaller than the header does.
        byte[] peek = Arrays.copyOf(full, full.length - 150);

        assertThatThrownBy(() -> PageHeaderReader.read(reader(peek)))
                .as("the peek can only grow if a header longer than it says so as a truncation")
                .isInstanceOf(ThriftTruncatedException.class);
    }

    @Test
    void theSameHeaderParsesOnceThePeekCoversIt() {
        byte[] full = pageHeaderWithStatisticsBound(200);

        assertThatCode(() -> PageHeaderReader.read(reader(full)))
                .as("a peek that covers the header must parse it, so the loop terminates")
                .doesNotThrowAnyException();
    }

    @Test
    void aDeclaredLengthPastTheBufferIsATruncation() {
        // The declared length is checked against what is left before the bytes are reached, so
        // this is the frame a short peek trips first.
        byte[] bytes = new byte[12];
        bytes[0] = (byte) 0xD0;   // varint 2000, over a buffer holding 10 more bytes
        bytes[1] = (byte) 0x0F;

        assertThatThrownBy(() -> reader(bytes).readBinary())
                .isInstanceOf(ThriftTruncatedException.class)
                .hasMessage("Malformed Parquet metadata: binary value declares 2000 bytes but only 10 "
                         + "remain");
    }
}
