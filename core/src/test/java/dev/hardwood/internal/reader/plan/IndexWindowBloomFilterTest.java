/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.reader.step.ByteSource;
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
    void theWindowsFiltersAreFetchedOnceAndServeEveryMemberFromTheWindow() throws Exception {
        CountingInputFile file = countingFile();
        BitSet[] columns = { new BitSet(), column(CODE), column(CODE) };
        IndexWindow window = IndexWindow.plan(Long.MAX_VALUE, file, 0, members(rowGroups().subList(0, 3), columns))
                .getFirst();
        window.insertSections(FetchSequence.demandOnly(context));
        assertThat(file.reads()).isEmpty();

        IndexWindowTest.awaitMet(window.bloomFiltersNeed());
        ByteSource outsideTheWindow = (offset, length) -> {
            throw new AssertionError("Range at " + offset + " read outside the window");
        };
        ColumnMetaData firstCode = metaData(rowGroups().get(1));
        ByteBuffer first = window.bloomFilters(1, outsideTheWindow)
                .read(firstCode.bloomFilterOffset(), firstCode.bloomFilterLength());
        ColumnMetaData secondCode = metaData(rowGroups().get(2));
        ByteBuffer second = window.bloomFilters(2, outsideTheWindow)
                .read(secondCode.bloomFilterOffset(), secondCode.bloomFilterLength());

        assertThat(file.reads()).extracting(CountingInputFile.Read::reason).containsExactly("rg=0-2 pruning");
        assertThat(first).isEqualTo(countingFile().readRange(firstCode.bloomFilterOffset(),
                firstCode.bloomFilterLength()));
        assertThat(second.remaining()).isEqualTo(secondCode.bloomFilterLength());
    }

    @Test
    void aFilterWithoutALengthIsLeftOutOfTheWindowAndReadByTheSource() throws Exception {
        CountingInputFile file = countingFile();
        RowGroup withoutLength = withoutBloomFilterLength(rowGroups().get(0), CODE);
        IndexWindow window = IndexWindow.plan(Long.MAX_VALUE, file, 0,
                members(List.of(withoutLength), new BitSet[] { column(CODE) })).getFirst();
        window.insertSections(FetchSequence.demandOnly(context));
        assertThat(window.bloomFiltersNeed()).isNull();
        assertThat(file.reads()).isEmpty();

        BloomFilter filter = new RowGroupBloomFilterSource(file, window.bloomFilters(0, ByteSource.of(file)),
                withoutLength).forColumn(CODE);

        assertThat(filter).isNotNull();
        assertThat(file.reads()).isNotEmpty();
    }

    private final HardwoodContextImpl context = HardwoodContextImpl.create(0);

    @AfterEach
    void closeContext() {
        context.close();
    }

    private static List<IndexWindow> plan(long budget, List<RowGroup> rowGroups, BitSet[] bloomFilterColumns)
            throws Exception {
        return IndexWindow.plan(budget, countingFile(), 0, members(rowGroups, bloomFilterColumns));
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

    private static BitSet[] repeat(BitSet columns, int count) {
        BitSet[] repeated = new BitSet[count];
        Arrays.fill(repeated, columns);
        return repeated;
    }

    /// The row groups as window members, each the row group of its index, needing no page index
    /// and the bloom filters of its entry of `bloomFilterColumns`.
    private static List<IndexWindow.Member> members(List<RowGroup> rowGroups, BitSet[] bloomFilterColumns) {
        List<IndexWindow.Member> members = new ArrayList<>(rowGroups.size());
        for (int i = 0; i < rowGroups.size(); i++) {
            members.add(new IndexWindow.Member(rowGroups.get(i), i, new BitSet(), new BitSet(), bloomFilterColumns[i]));
        }
        return members;
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
