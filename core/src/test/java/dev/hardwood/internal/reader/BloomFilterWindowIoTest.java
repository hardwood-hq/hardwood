/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The bloom-filter requests of whole reads: a window's filters are fetched together, when the
/// read reaches the window, never while the file is planned.
///
/// Uses bloom_filter_multi_rg_test.parquet: five row groups of 200 rows, `id` counting 0..999,
/// `code` cycling 0, 3, 6, 9 within every row group, bloom filters on `id`, `name` and `code`, no
/// dictionary and no page index; bloom_filter_multi_rg_dict_index_test.parquet holds the same rows
/// dictionary-encoded and with a page index. At the default budget the five row groups are one
/// window.
class BloomFilterWindowIoTest {

    private static final Path FIXTURE = Path.of("src/test/resources/bloom_filter_multi_rg_test.parquet");
    private static final Path DICT_INDEX_FIXTURE =
            Path.of("src/test/resources/bloom_filter_multi_rg_dict_index_test.parquet");
    private static final int CODE = 3;
    private static final int ROW_GROUPS = 5;

    @Test
    void aWindowsBloomFiltersAreFetchedInOneRequest() throws Exception {
        FileMetaData metaData = metaData();
        CountingInputFile file = countingFile();

        // Between the statistics' 0 and 9, but in no row group: every row group is a candidate,
        // and its bloom filter drops it.
        long rows = count(file, FilterPredicate.eq("code", 5));

        assertThat(rows).isZero();
        long start = Long.MAX_VALUE;
        long end = 0;
        for (int rg = 0; rg < ROW_GROUPS; rg++) {
            ColumnMetaData code = metaData.rowGroups().get(rg).columns().get(CODE).metaData();
            start = Math.min(start, code.bloomFilterOffset());
            end = Math.max(end, code.bloomFilterOffset() + code.bloomFilterLength());
        }
        assertThat(pruningReads(file)).containsExactly(
                new CountingInputFile.Read(start, Math.toIntExact(end - start), "rg=0-4 pruning"));
    }

    @Test
    void aRowGroupItsStatisticsDropHasNoBloomFilterFetched() throws Exception {
        FileMetaData metaData = metaData();
        CountingInputFile file = countingFile();

        // `id` is ascending, so statistics keep row group 2 alone, and only its filter is read.
        long rows = count(file, FilterPredicate.eq("id", 450L));

        assertThat(rows).isEqualTo(1);
        ColumnMetaData id = metaData.rowGroups().get(2).columns().get(0).metaData();
        assertThat(pruningReads(file)).containsExactly(new CountingInputFile.Read(
                id.bloomFilterOffset(), id.bloomFilterLength(), "rg=2 pruning"));
    }

    @Test
    void aRowGroupStatisticsProveToMatchHasNoBloomFilterFetched() throws Exception {
        FileMetaData metaData = metaData();
        CountingInputFile file = countingFile();

        // `eq(code, 5)` is open in every row group and asks for `code`'s filter before
        // `lt(id, 200)` decides the OR: statistics prove row group 0 matches in full, so its
        // filter cannot change the decision, and the bloom filters drop row groups 1 to 4.
        long rows = count(file, FilterPredicate.or(FilterPredicate.eq("code", 5), FilterPredicate.lt("id", 200L)));

        assertThat(rows).isEqualTo(200);
        ColumnMetaData first = metaData.rowGroups().get(1).columns().get(CODE).metaData();
        ColumnMetaData last = metaData.rowGroups().get(ROW_GROUPS - 1).columns().get(CODE).metaData();
        long end = last.bloomFilterOffset() + last.bloomFilterLength();
        assertThat(pruningReads(file)).containsExactly(new CountingInputFile.Read(first.bloomFilterOffset(),
                Math.toIntExact(end - first.bloomFilterOffset()), "rg=0-4 pruning"));
    }

    @Test
    void rowGroupsTheirBloomFiltersDropReadNoDictionaryAndNoPageIndex() throws Exception {
        // The same rows, dictionary-encoded and with a page index: `code`'s dictionary could
        // drop every row group too, and the read would need the page index of a kept one.
        CountingInputFile file = countingFile(DICT_INDEX_FIXTURE);

        long rows = count(file, FilterPredicate.eq("code", 5));

        assertThat(rows).isZero();
        assertThat(file.reads()).extracting(CountingInputFile.Read::reason)
                .filteredOn(reason -> reason.startsWith("rg="))
                .containsExactly("rg=0-4 pruning");
    }

    @Test
    void planningAFileReadsNoBloomFilter() throws Exception {
        FileMetaData metaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            metaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        CountingInputFile file = countingFile();
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = new RowGroupIterator(List.of(file), context, 0);
            try {
                iterator.setFirstFile(schema, metaData.rowGroups());
                iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.columns("code")),
                        FilterPredicateResolver.resolve(FilterPredicate.eq("code", 5), schema));

                assertThat(iterator.getWorkItems()).hasSize(ROW_GROUPS);
                assertThat(pruningReads(file)).isEmpty();

                iterator.getSharedMetadata(iterator.workItemAt(0));

                assertThat(pruningReads(file)).extracting(CountingInputFile.Read::reason)
                        .containsExactly("rg=0-4 pruning");
            }
            finally {
                iterator.close();
            }
        }
    }

    private static long count(CountingInputFile file, FilterPredicate filter) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }

    private static List<CountingInputFile.Read> pruningReads(CountingInputFile file) {
        return file.reads().stream().filter(read -> read.reason().endsWith(" pruning")).toList();
    }

    private static FileMetaData metaData() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            return reader.getFileMetaData();
        }
    }

    private static CountingInputFile countingFile() throws Exception {
        return countingFile(FIXTURE);
    }

    private static CountingInputFile countingFile(Path fixture) throws Exception {
        CountingInputFile file = new CountingInputFile(InputFile.of(fixture));
        file.open();
        return file;
    }
}
