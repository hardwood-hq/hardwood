/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;

import static org.assertj.core.api.Assertions.assertThat;

/// The bloom-filter structure of an index window, on the row groups of
/// bloom_filter_multi_rg_test.parquet (five row groups; bloom filters on `id`, `name` and `code`,
/// stored together row group by row group; no page index).
class IndexWindowBloomFilterTest {

    private static final Path FIXTURE = Path.of("src/test/resources/bloom_filter_multi_rg_test.parquet");
    private static final int ROW_GROUPS = 5;
    private static final int CODE = 3;

    @Test
    void bloomFilterBytesCountTowardTheWindowBudget() throws Exception {
        List<RowGroup> rowGroups = rowGroups();
        long largest = rowGroups.stream().mapToLong(rowGroup -> metaData(rowGroup).bloomFilterLength())
                .max().orElseThrow();
        // `code`'s filters are strided by the other columns' filters, which the merge bridges:
        // one filter fits a budget of the largest, two such strided spans do not.
        List<IndexWindow> windows = plan(largest, rowGroups, repeat(column(CODE), ROW_GROUPS));

        assertThat(windows).hasSize(ROW_GROUPS).allSatisfy(window -> {
            assertThat(window.memberCount()).isOne();
            assertThat(window.fetchedBytes()).isLessThanOrEqualTo(largest);
        });
        assertThat(plan(Long.MAX_VALUE, rowGroups, repeat(column(CODE), ROW_GROUPS)))
                .extracting(IndexWindow::memberCount).containsExactly(ROW_GROUPS);
    }

    @Test
    void aMemberWithoutBloomFilterColumnsFetchesNothingAndOneWithFetchesTheWindowsFiltersOnce() throws Exception {
        CountingInputFile file = countingFile();
        BitSet[] columns = { new BitSet(), column(CODE), column(CODE) };
        IndexWindow window = IndexWindow.plan(Long.MAX_VALUE, file, 0, rowGroups().subList(0, 3).toArray(new RowGroup[0]),
                indexes(3), repeat(new BitSet(), 3), repeat(new BitSet(), 3), columns).getFirst();

        assertThat(window.bloomFiltersFor(0)).containsOnlyNulls();
        assertThat(file.reads()).isEmpty();

        ByteBuffer first = window.bloomFiltersFor(1)[CODE];
        ByteBuffer second = window.bloomFiltersFor(2)[CODE];

        assertThat(file.reads()).extracting(CountingInputFile.Read::reason).containsExactly("rg=0-2 pruning");
        ColumnMetaData code = metaData(rowGroups().get(1));
        assertThat(first).isEqualTo(countingFile().readRange(code.bloomFilterOffset(), code.bloomFilterLength()));
        assertThat(second.remaining()).isEqualTo(metaData(rowGroups().get(2)).bloomFilterLength());
    }

    @Test
    void aFilterWithoutALengthIsLeftOutOfTheWindowAndReadByTheSource() throws Exception {
        CountingInputFile file = countingFile();
        RowGroup withoutLength = withoutBloomFilterLength(rowGroups().get(0), CODE);
        IndexWindow window = IndexWindow.plan(Long.MAX_VALUE, file, 0, new RowGroup[] { withoutLength },
                indexes(1), repeat(new BitSet(), 1), repeat(new BitSet(), 1), repeat(column(CODE), 1)).getFirst();

        ByteBuffer[] fetched = window.bloomFiltersFor(0);

        assertThat(fetched).containsOnlyNulls();
        assertThat(file.reads()).isEmpty();
        BloomFilter filter = new RowGroupBloomFilterSource(file, withoutLength, fetched).forColumn(CODE);
        assertThat(filter).isNotNull();
        assertThat(file.reads()).isNotEmpty();
    }

    private static List<IndexWindow> plan(long budget, List<RowGroup> rowGroups, BitSet[] bloomFilterColumns)
            throws Exception {
        int count = rowGroups.size();
        return IndexWindow.plan(budget, countingFile(), 0, rowGroups.toArray(new RowGroup[0]), indexes(count),
                repeat(new BitSet(), count), repeat(new BitSet(), count), bloomFilterColumns);
    }

    private static RowGroup withoutBloomFilterLength(RowGroup rowGroup, int column) {
        List<ColumnChunk> chunks = new ArrayList<>(rowGroup.columns());
        ColumnChunk chunk = chunks.get(column);
        ColumnMetaData m = chunk.metaData();
        ColumnMetaData withoutLength = new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(), m.codec(),
                m.numValues(), m.totalUncompressedSize(), m.totalCompressedSize(), m.keyValueMetadata(),
                m.dataPageOffset(), m.dictionaryPageOffset(), m.statistics(), m.geospatialStatistics(),
                m.bloomFilterOffset(), null, m.encodingStats(), m.sizeStatistics());
        chunks.set(column, new ColumnChunk(withoutLength, chunk.offsetIndexOffset(), chunk.offsetIndexLength(),
                chunk.columnIndexOffset(), chunk.columnIndexLength(), chunk.filePath()));
        return new RowGroup(chunks, rowGroup.totalByteSize(), rowGroup.numRows());
    }

    private static ColumnMetaData metaData(RowGroup rowGroup) {
        return rowGroup.columns().get(CODE).metaData();
    }

    private static BitSet column(int column) {
        BitSet columns = new BitSet();
        columns.set(column);
        return columns;
    }

    private static int[] indexes(int count) {
        int[] indexes = new int[count];
        Arrays.setAll(indexes, i -> i);
        return indexes;
    }

    private static BitSet[] repeat(BitSet columns, int count) {
        BitSet[] repeated = new BitSet[count];
        Arrays.fill(repeated, columns);
        return repeated;
    }

    private static List<RowGroup> rowGroups() throws Exception {
        return ParquetMetadataReader.readMetadata(countingFile()).rowGroups();
    }

    private static CountingInputFile countingFile() throws IOException {
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        return file;
    }
}
