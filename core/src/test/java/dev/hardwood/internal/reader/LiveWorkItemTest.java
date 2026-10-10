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
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Whether a read has a row group left once its bloom filters have been consulted, which a column
/// read asks before it builds its pipeline.
///
/// Uses bloom_filter_multi_rg_test.parquet: five row groups of 200 rows, `id` counting 0..999,
/// `code` cycling 0, 3, 6, 9 within every row group, bloom filters on `id`, `name` and `code`.
class LiveWorkItemTest {

    private static final Path FIXTURE = Path.of("src/test/resources/bloom_filter_multi_rg_test.parquet");

    @Test
    void aReadWhoseBloomFiltersDropEveryRowGroupHasNoLiveWorkItem() throws Exception {
        // Between the statistics' 0 and 9, but in no row group: every row group is planned, and
        // its bloom filter drops it.
        CountingInputFile file = countingFile();

        assertThat(hasLiveWorkItem(file, FilterPredicate.eq("code", 5))).isFalse();
        assertThat(pruningReads(file)).extracting(CountingInputFile.Read::reason)
                .containsExactly("rg=0-4 pruning");
    }

    @Test
    void aRowGroupItsBloomFilterKeepsIsLive() throws Exception {
        assertThat(hasLiveWorkItem(countingFile(), FilterPredicate.eq("code", 3))).isTrue();
    }

    @Test
    void aRowGroupStatisticsProveToMatchIsLiveWithoutABloomFilterRead() throws Exception {
        // Statistics prove row group 0 matches in full, so no bloom filter could drop it.
        CountingInputFile file = countingFile();

        assertThat(hasLiveWorkItem(file,
                FilterPredicate.or(FilterPredicate.eq("code", 5), FilterPredicate.lt("id", 200L)))).isTrue();
        assertThat(pruningReads(file)).isEmpty();
    }

    @Test
    void anUnfilteredReadIsLiveWithoutABloomFilterRead() throws Exception {
        CountingInputFile file = countingFile();

        assertThat(hasLiveWorkItem(file, null)).isTrue();
        assertThat(pruningReads(file)).isEmpty();
    }

    private static boolean hasLiveWorkItem(CountingInputFile file, FilterPredicate filter) throws Exception {
        FileMetaData metaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            metaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = new RowGroupIterator(List.of(file), context, 0);
            try {
                iterator.setFirstFile(schema, metaData.rowGroups());
                iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.columns("code")),
                        filter == null ? null : FilterPredicateResolver.resolve(filter, schema));
                return iterator.hasLiveWorkItem();
            }
            finally {
                iterator.close();
            }
        }
    }

    private static List<CountingInputFile.Read> pruningReads(CountingInputFile file) {
        return file.reads().stream().filter(read -> read.reason().endsWith(" pruning")).toList();
    }

    private static CountingInputFile countingFile() throws Exception {
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        return file;
    }
}
