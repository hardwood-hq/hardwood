/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.PageFilterEvaluator.IndexPair;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.internal.thrift.ThriftStructBuilder;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [UnitPageIndex] resolves a unit's page index from the bytes its window holds, each structure of
/// each chunk once, and hands page filtering a pair only where the window holds both and they
/// agree.
class UnitPageIndexTest {

    /// three row groups of 1000 rows, `id` 0 to 2999 and `value` 1000 to 3999, a page index per chunk
    private static final Path FILE = Path.of("src/test/resources/multi_row_group_page_index.parquet");

    private static ByteBuffer bytes;
    private static RowGroup rowGroup;

    private final List<Class<?>> parses = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void readFile() throws Exception {
        bytes = ByteBuffer.wrap(Files.readAllBytes(FILE));
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(bytes.duplicate()))) {
            rowGroup = reader.getFileMetaData().rowGroups().get(1);
        }
    }

    @AfterEach
    void stopCounting() {
        PageFilterEvaluator.parseObserver = null;
    }

    @Test
    void eachStructureIsParsedOnceAcrossRequests() {
        UnitPageIndex indexes = new UnitPageIndex(rowGroup, UnitPageIndexTest::slice, columns(0, 1), columns(0));
        countParses();

        IndexPair pair = indexes.pageIndex(0);
        assertThat(indexes.pageIndex(0)).isSameAs(pair);
        assertThat(indexes.offsetIndex(0)).isSameAs(pair.offsetIndex());
        assertThat(indexes.neededPages(0, RowRanges.range(0, 1))).hasSize(1);

        assertThat(parses).containsExactly(ColumnIndex.class, OffsetIndex.class);
    }

    @Test
    void anOffsetIndexParsedForPlanningServesPageFiltering() {
        UnitPageIndex indexes = new UnitPageIndex(rowGroup, UnitPageIndexTest::slice, columns(0), columns(0));
        countParses();

        OffsetIndex offsetIndex = indexes.offsetIndex(0);
        assertThat(indexes.pageIndex(0).offsetIndex()).isSameAs(offsetIndex);
        assertThat(parses).containsExactly(OffsetIndex.class, ColumnIndex.class);
    }

    @Test
    void aColumnWithoutAFetchedOffsetIndexHasNoPair() {
        UnitPageIndex indexes = new UnitPageIndex(rowGroup, UnitPageIndexTest::slice, columns(), columns(0));
        countParses();

        assertThat(indexes.pageIndex(0)).isNull();
        assertThat(parses).isEmpty();
    }

    @Test
    void aColumnWithoutAFetchedColumnIndexHasNoPair() {
        UnitPageIndex indexes = new UnitPageIndex(rowGroup, UnitPageIndexTest::slice, columns(0), columns());

        assertThat(indexes.pageIndex(0)).isNull();
    }

    @Test
    void aColumnNotFetchedIsRefused() {
        UnitPageIndex indexes = new UnitPageIndex(rowGroup, UnitPageIndexTest::slice, columns(0), columns(0));

        assertThatThrownBy(() -> indexes.pageIndex(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Page index of column 1 was not fetched for this read");
    }

    @Test
    void aPairDescribingDifferentPagesIsRejected() {
        ColumnChunk id = rowGroup.columns().get(0);
        int pages = offsetIndexPages();
        byte[] onePageMore = new ThriftStructBuilder()
                .field(1, FieldType.LIST).boolList(new boolean[pages + 1])
                .field(2, FieldType.LIST).binaryList(bounds(pages + 1))
                .field(3, FieldType.LIST).binaryList(bounds(pages + 1))
                .field(4, FieldType.I32).i32(0)
                .stop().build();
        UnitPageIndex.Window window = (offset, length) -> offset == id.columnIndexOffset()
                ? ByteBuffer.wrap(onePageMore)
                : slice(offset, length);
        UnitPageIndex indexes = new UnitPageIndex(rowGroup, window, columns(0), columns(0));

        assertThatThrownBy(() -> indexes.pageIndex(0))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Failed to parse the page index: Malformed Parquet metadata: ColumnIndex describes " + (pages + 1)
                        + " pages but OffsetIndex locates " + pages);
    }

    @Test
    void bytesTheWindowDoesNotHoldAreRefused() {
        ColumnChunk id = rowGroup.columns().get(0);
        UnitPageIndex indexes = new UnitPageIndex(rowGroup, (offset, length) -> null, columns(0), columns(0));

        assertThatThrownBy(() -> indexes.offsetIndex(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Range [" + id.offsetIndexOffset() + ", " + (id.offsetIndexOffset() + id.offsetIndexLength())
                        + ") is not present");
    }

    private static int offsetIndexPages() {
        return new UnitPageIndex(rowGroup, UnitPageIndexTest::slice, columns(0), columns()).offsetIndex(0)
                .pageLocations().size();
    }

    private static byte[][] bounds(int pages) {
        byte[][] bounds = new byte[pages][];
        for (int i = 0; i < pages; i++) {
            bounds[i] = new byte[8];
        }
        return bounds;
    }

    private void countParses() {
        Thread counting = Thread.currentThread();
        PageFilterEvaluator.parseObserver = structure -> {
            if (Thread.currentThread() == counting) {
                parses.add(structure);
            }
        };
    }

    private static ByteBuffer slice(long offset, int length) {
        return bytes.slice(Math.toIntExact(offset), length);
    }

    private static BitSet columns(int... ordinals) {
        BitSet set = new BitSet();
        for (int ordinal : ordinals) {
            set.set(ordinal);
        }
        return set;
    }
}
