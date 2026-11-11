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
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.PlainInt64File;
import dev.hardwood.internal.thrift.PlainInt64File.Page;
import dev.hardwood.metadata.PageLocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An OffsetIndex's page locations are held to their column chunk and to each other: each page
/// has bytes, starts no earlier than the previous page ends and ends within the chunk, and the
/// pages' first rows run from row 0, never backwards, to at most the row group's row count.
class OffsetIndexPageLocationsTest {

    // three pages of 10 values, each of 105 bytes, from offset 4
    private static final List<Page> PAGES = List.of(new Page(0, 10), new Page(10, 10), new Page(20, 10));

    @Test
    void pagesAsWrittenRead() throws IOException {
        assertThat(sum(PlainInt64File.build(PAGES, UnaryOperator.identity()))).isEqualTo(435);
    }

    @Test
    void pageOfNoBytesFails() {
        assertFails(page(1, l -> new PageLocation(l.offset(), 0, l.firstRowIndex())),
                "OffsetIndex page 1 has a compressed_page_size of 0");
    }

    @Test
    void pageStartingBeforeThePreviousPageEndsFails() {
        assertFails(page(2, l -> new PageLocation(l.offset() - 1, l.compressedPageSize(), l.firstRowIndex())),
                "OffsetIndex page 2 starts at offset 213, before page 1 ends at offset 214");
    }

    @Test
    void pageRepeatingThePreviousPagesOffsetFails() {
        assertFails(page(1, l -> new PageLocation(4, l.compressedPageSize(), l.firstRowIndex())),
                "OffsetIndex page 1 starts at offset 4, before page 0 ends at offset 109");
    }

    @Test
    void pageStartingBeforeItsChunkFails() {
        assertFails(page(0, l -> new PageLocation(3, l.compressedPageSize(), l.firstRowIndex())),
                "OffsetIndex page 0 starts at offset 3, before its column chunk starts at offset 4");
    }

    @Test
    void pageEndingPastItsChunkFails() {
        assertFails(page(2, l -> new PageLocation(l.offset(), l.compressedPageSize() + 1, l.firstRowIndex())),
                "OffsetIndex page 2 of 106 bytes at offset 214 ends past the end of its column chunk at offset 319");
    }

    @Test
    void pageWhoseEndWrapsPastLongMaxFails() {
        assertFails(page(2, l -> new PageLocation(Long.MAX_VALUE - 5, l.compressedPageSize(), l.firstRowIndex())),
                "OffsetIndex page 2 of 105 bytes at offset 9223372036854775802 ends past the end of its column chunk"
                        + " at offset 319");
    }

    @Test
    void firstPageStartingPastRowZeroFails() {
        assertFails(page(0, l -> new PageLocation(l.offset(), l.compressedPageSize(), 1)),
                "OffsetIndex page 0 has a first_row_index of 1, not 0");
    }

    @Test
    void pageStartingBeforeThePreviousPagesRowFails() {
        assertFails(page(2, l -> new PageLocation(l.offset(), l.compressedPageSize(), 5)),
                "OffsetIndex page 2 has a first_row_index of 5, before page 1's 10");
    }

    @Test
    void lastPageStartingPastTheRowGroupsEndFails() {
        assertFails(page(2, l -> new PageLocation(l.offset(), l.compressedPageSize(), 31)),
                "OffsetIndex page 2 has a first_row_index of 31, past the row group's 30 rows");
    }

    /// Replaces page `index`'s location with what `change` makes of it.
    private static UnaryOperator<List<PageLocation>> page(int index, UnaryOperator<PageLocation> change) {
        return locations -> {
            List<PageLocation> changed = new ArrayList<>(locations);
            changed.set(index, change.apply(locations.get(index)));
            return changed;
        };
    }

    private static void assertFails(UnaryOperator<List<PageLocation>> index, String message) {
        byte[] file = PlainInt64File.build(PAGES, index);

        assertThatThrownBy(() -> sum(file))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'v'] Failed to compute the fetch plan: "
                        + "Malformed Parquet metadata: " + message);
    }

    private static long sum(byte[] file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader().build()) {
            long sum = 0;
            while (rows.hasNext()) {
                rows.next();
                sum += rows.getLong("v");
            }
            return sum;
        }
    }
}
