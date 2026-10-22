/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.internal.thrift.RleExpansionFile.Chunk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A data page's declared value count is held to the page limit before the decode sizes
/// anything from it, on files whose every layer agrees with the pages.
class DeclaredValueCountTest {

    private static final String MAX_PAGE_BYTES_PROPERTY = "hardwood.internal.maxPageBytes";

    @Test
    void pageOfMaxIntValuesFailsInsteadOfAllocating() {
        // The shape of DuckDB's 42.parquet: 2^31 - 1 values per page, from three bytes of RLE
        byte[] file = RleExpansionFile.build(Integer.MAX_VALUE, 2, 2L * Integer.MAX_VALUE);

        assertThatThrownBy(() -> countRows(file))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'b', page 0] DataPageHeader.num_values — "
                        + "must not exceed 67108864 (hardwood.internal.maxPageBytes / 4) but was 2147483647");
    }

    @Test
    void v2PageOfMaxIntValuesFailsInsteadOfAllocating() {
        byte[] file = RleExpansionFile.build(true, new Chunk(Integer.MAX_VALUE, 1, Integer.MAX_VALUE));

        assertThatThrownBy(() -> countRows(file))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'b', page 0] DataPageHeaderV2.num_values \u2014 "
                        + "must not exceed 67108864 (hardwood.internal.maxPageBytes / 4) but was 2147483647");
    }

    @Test
    void pageAtTheLimitReads() throws IOException {
        System.setProperty(MAX_PAGE_BYTES_PROPERTY, "4096");
        try {
            assertThat(countRows(RleExpansionFile.build(1024, 3, 3 * 1024))).isEqualTo(3 * 1024);
        }
        finally {
            System.clearProperty(MAX_PAGE_BYTES_PROPERTY);
        }
    }

    @Test
    void pageOneValuePastTheLimitFails() {
        System.setProperty(MAX_PAGE_BYTES_PROPERTY, "4096");
        try {
            assertThatThrownBy(() -> countRows(RleExpansionFile.build(1025, 1, 1025)))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("[<memory>: row group 0, column 'b', page 0] DataPageHeader.num_values — "
                            + "must not exceed 1024 (hardwood.internal.maxPageBytes / 4) but was 1025");
        }
        finally {
            System.clearProperty(MAX_PAGE_BYTES_PROPERTY);
        }
    }

    @Test
    void pageDeclaringMoreValuesThanItsChunkFails() {
        assertThatThrownBy(() -> countRows(RleExpansionFile.build(1000, 1, 999)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'b', page 0] Data page declares 1000 values, "
                        + "more than the 999 of its column chunk");
    }

    @Test
    void pagesAddingUpToTheirChunkRead() throws IOException {
        assertThat(countRows(RleExpansionFile.build(1000, 3, 3000))).isEqualTo(3000);
    }

    @Test
    void v2PageDeclaringMoreValuesThanItsChunkFails() {
        assertThatThrownBy(() -> countRows(RleExpansionFile.build(true, new Chunk(1000, 1, 999))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'b', page 0] Data page declares 1000 values, "
                        + "more than the 999 of its column chunk");
    }

    @Test
    void v2PagesAddingUpToTheirChunkRead() throws IOException {
        assertThat(countRows(RleExpansionFile.build(true, new Chunk(1000, 3, 3000)))).isEqualTo(3000);
    }

    @Test
    void eachPageIsCheckedAgainstItsOwnRowGroupsChunk() throws IOException {
        // The second row group's page holds more values than the first row group's whole chunk:
        // checked against the first chunk, as a decoder kept across chunks would, it is refused.
        byte[] file = RleExpansionFile.build(false, new Chunk(10, 1, 10), new Chunk(1000, 1, 1000));

        assertThat(countRows(file)).isEqualTo(1010);
    }

    /// The rows of the first 100 000 a row reader yields, each checked to hold the one dictionary
    /// value.
    private static long countRows(byte[] file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader().head(100_000).build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("b")).isEqualTo(Long.MAX_VALUE);
                count++;
            }
            return count;
        }
    }
}
