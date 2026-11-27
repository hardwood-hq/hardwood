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

import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page's header and the bytes it was located with agree on where its body ends: a body the
/// header declares past the page's bytes is refused, whoever sized them, the header scan or a
/// chunk's offset index.
class DataPageTest {

    @Test
    void aBodyRunningPastThePageIsRefused() {
        assertThatThrownBy(() -> new DataPage(header(13), 25, ByteBuffer.allocate(35)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Page header declares a body of 13 bytes, but its page holds 10 bytes after its "
                        + "25-byte header");
    }

    @Test
    void aBodyEndingWithThePageIsTaken() {
        DataPage page = new DataPage(header(10), 25, ByteBuffer.allocate(35));

        assertThat(page.body().remaining()).isEqualTo(10);
    }

    private static PageHeader header(int compressedPageSize) {
        DataPageHeader dataPageHeader = new DataPageHeader(1, Encoding.PLAIN, 0, Encoding.RLE, 3, Encoding.RLE, 3,
                null);
        return new PageHeader(PageType.DATA_PAGE, compressedPageSize, compressedPageSize, dataPageHeader, null, null,
                null);
    }
}
