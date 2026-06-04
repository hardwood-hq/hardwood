/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.metadata.PageHeader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/// Verifies that the Thrift metadata readers reject malformed/adversarial
/// sizes, counts and offsets with a controlled error instead of letting a
/// negative value reach a buffer allocation or slice downstream (see the
/// cross-reader compatibility matrix discussed on dev@parquet, May 2026).
///
/// Inputs are hand-crafted Thrift Compact Protocol bytes. Field header byte is
/// `(fieldIdDelta << 4) | type`; `i32`/`i64` values are zigzag varints
/// (`zigzag(-1) = 1`, `zigzag(10) = 20`, `zigzag(8) = 16`); `0x00` is STOP.
class MalformedMetadataValidationTest {

    private static ThriftCompactReader reader(int... bytes) {
        byte[] b = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            b[i] = (byte) bytes[i];
        }
        return new ThriftCompactReader(ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN));
    }

    @Test
    void negativeCompressedPageSizeRejected() {
        // PageHeader: field1 type=DATA_PAGE(0), field2 uncompressed=10, field3 compressed=-1
        assertThatThrownBy(() -> PageHeaderReader.read(
                reader(0x15, 0x00, 0x15, 0x14, 0x15, 0x01, 0x00)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("compressed_page_size");
    }

    @Test
    void negativeUncompressedPageSizeRejected() {
        // PageHeader: field1 type=DATA_PAGE(0), field2 uncompressed=-1
        assertThatThrownBy(() -> PageHeaderReader.read(
                reader(0x15, 0x00, 0x15, 0x01, 0x00)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("uncompressed_page_size");
    }

    @Test
    void negativeDataPageOffsetRejected() {
        // ColumnMetaData: field9 data_page_offset (i64) = -1
        assertThatThrownBy(() -> ColumnMetaDataReader.read(
                reader(0x96, 0x01, 0x00)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("data_page_offset");
    }

    @Test
    void validPageHeaderStillParses() {
        // PageHeader: field1 type=DATA_PAGE(0), field2 uncompressed=10, field3 compressed=8
        PageHeader header = assertDoesNotThrow(() -> PageHeaderReader.read(
                reader(0x15, 0x00, 0x15, 0x14, 0x15, 0x10, 0x00)));
        assertThat(header.type()).isEqualTo(PageHeader.PageType.DATA_PAGE);
        assertThat(header.uncompressedPageSize()).isEqualTo(10);
        assertThat(header.compressedPageSize()).isEqualTo(8);
    }
}
