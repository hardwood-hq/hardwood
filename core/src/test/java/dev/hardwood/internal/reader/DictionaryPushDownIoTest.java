/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// The I/O cost model of dictionary predicate push-down, pinned as request counts rather than
/// wall-clock (see `_designs/STATISTICS_PRUNING.md`).
///
/// Push-down reads a row group's dictionary page before any data page. What that buys, and what it
/// costs, is a matter of counting requests:
///
/// - a value the dictionary proves absent drops the row group, so *only* the dictionary page is
///   read and no data page is touched, and
/// - a value it cannot prove absent leaves the read to proceed as it would have anyway, so the
///   dictionary read is exactly one extra request that pruned nothing.
class DictionaryPushDownIoTest {

    /// One row group, 10 000 rows; `category` cycles ten dictionary-encoded values `"cat_0"`…`"cat_9"`.
    private static final Path FIXTURE = Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    /// Inside the `["cat_0", "cat_9"]` statistics range but in no dictionary, so only the dictionary
    /// can prove it absent.
    private static final FilterPredicate ABSENT = FilterPredicate.eq("category", "cat_5x");

    private static final FilterPredicate PRESENT = FilterPredicate.eq("category", "cat_5");

    /// Four row groups of 100 rows with disjoint `i64` ranges, every column dictionary-encoded.
    private static final Path FOUR_ROW_GROUPS = Paths.get("src/test/resources/predicate/predicate_dict.parquet");

    /// One row group, 20 000 rows; `label` is Snappy-compressed and dictionary-encoded, so its
    /// page's compressed and uncompressed sizes differ by roughly 10x.
    private static final Path COMPRESSED_FIXTURE = Paths.get("src/test/resources/dict_compressed_page.parquet");

    /// Absent from the dictionary — only even suffixes are written — but between the column's
    /// `"pad_0_…"` minimum and `"pad_98_…"` maximum.
    /// In the dictionary, on 79 of the rows.
    private static final FilterPredicate COMPRESSED_PRESENT =
            FilterPredicate.eq("label", "pad_0_" + "z".repeat(40));

    private static final FilterPredicate COMPRESSED_ABSENT =
            FilterPredicate.eq("label", "pad_1_" + "z".repeat(40));

    @Test
    void aProvenAbsentValueReadsTheDictionaryAndNoDataPage() throws Exception {
        Reads unfiltered = read(null);
        Reads absent = read(ABSENT);

        assertThat(absent.rows()).isZero();
        assertThat(absent.requests())
                .as("only the dictionary page is fetched; the row group is dropped before any data")
                .isEqualTo(1);
        assertThat(absent.bytes())
                .as("dictionary page only (%d bytes) versus the full column chunk (%d bytes)",
                        absent.bytes(), unfiltered.bytes())
                .isLessThan(unfiltered.bytes() / 10);
    }

    @Test
    void aValueItCannotPruneCostsExactlyOneExtraRequest() throws Exception {
        Reads unfiltered = read(null);
        Reads present = read(PRESENT);

        assertThat(present.rows()).isEqualTo(1000);
        assertThat(present.requests())
                .as("the dictionary page is one request on top of the unfiltered read, and it "
                        + "pruned nothing")
                .isEqualTo(unfiltered.requests() + 1);
    }

    @Test
    void theDictionaryPageIsFetchedOnceWhenItCannotPrune() throws Exception {
        // Pruning reads the dictionary page to test the value; the read then decodes with that
        // dictionary rather than fetching the page again with the data pages.
        long dictionaryStart;
        long dictionaryEnd;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            ColumnMetaData metaData = reader.getFileMetaData().rowGroups().getFirst().columns()
                    .get(reader.getFileSchema().getColumn("category").columnIndex()).metaData();
            dictionaryStart = metaData.dictionaryPageOffset();
            dictionaryEnd = metaData.dataPageOffset();
        }

        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        int rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                ColumnReader values = reader.buildColumnReader("category").filter(PRESENT).build()) {
            while (values.nextBatch()) {
                rows += values.getRecordCount();
            }
        }

        assertThat(rows).isEqualTo(1000);
        assertThat(file.reads())
                .filteredOn(read -> read.offset() < dictionaryEnd && dictionaryStart < read.end())
                .as("reads of the dictionary page [%d, %d)", dictionaryStart, dictionaryEnd)
                .hasSize(1);
    }

    @Test
    void theDictionaryPageIsFetchedOnceWhenItCannotPruneAChunkWithoutAPageIndex() throws Exception {
        // Without a page index the read walks the chunk from its start; with the dictionary
        // pruning has read, it starts at the first data page instead.
        long dictionaryStart;
        long dictionaryEnd;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(COMPRESSED_FIXTURE))) {
            ColumnMetaData metaData = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            dictionaryStart = metaData.dictionaryPageOffset();
            dictionaryEnd = metaData.dataPageOffset();
        }

        CountingInputFile file = new CountingInputFile(InputFile.of(COMPRESSED_FIXTURE));
        file.open();
        int rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                ColumnReader values = reader.buildColumnReader("label").filter(COMPRESSED_PRESENT).build()) {
            while (values.nextBatch()) {
                rows += values.getRecordCount();
            }
        }

        assertThat(rows).isEqualTo(79);
        assertThat(file.reads())
                .filteredOn(read -> read.offset() < dictionaryEnd && dictionaryStart < read.end())
                .as("reads of the dictionary page [%d, %d)", dictionaryStart, dictionaryEnd)
                .hasSize(1);
    }

    @Test
    void eachRowGroupsDictionaryIsReadOnceWhenTheReadOutpacesItsPrefetch() throws Exception {
        // Every row group of the fixture is dropped by its dictionary, so the read passes each one
        // as soon as it is planned and releases it, and closes before the prefetch tasks planning
        // the row group after it have all run. None of those tasks may plan a released row group,
        // or one of a closed read, a second time.
        FilterPredicate absentFromEveryRowGroup = FilterPredicate.in("i64",
                -3_990_000_000_000L, -1_990_000_000_000L, 10_000_000_000L, 2_010_000_000_000L);
        for (int read = 0; read < 100; read++) {
            CountingInputFile file = new CountingInputFile(InputFile.of(FOUR_ROW_GROUPS));
            file.open();
            int rows = 0;
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    RowReader rowReader = reader.buildRowReader().filter(absentFromEveryRowGroup).build()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rows++;
                }
            }
            assertThat(ForkJoinPool.commonPool().awaitQuiescence(10, TimeUnit.SECONDS)).isTrue();

            assertThat(rows).isZero();
            assertThat(file.reads())
                    .filteredOn(fetch -> fetch.reason().endsWith(" pruning"))
                    .as("one dictionary read per row group, read %d", read)
                    .hasSize(4);
        }
    }

    @Test
    void theDictionaryReadIsSizedByTheCompressedPageLength() throws Exception {
        // `pageLength` sizes a read, so it must come from compressed_page_size — the page's extent
        // in the file. uncompressed_page_size is what the body expands to in memory once decoded;
        // it is never a file offset and must not size anything read off disk.
        //
        // Why that needs its own test: getting it wrong does not produce a wrong dictionary.
        // `DictionaryParser.parse` re-reads the header and slices the body by its own compressed
        // length, so an over-sized region still parses correctly, carrying trailing bytes it
        // ignores. The only symptom is bytes fetched — hence the assertion on the count — and only
        // on a compressed fixture, since with compression off the two sizes are equal and the
        // mistake leaves no trace at all.
        //
        // On this fixture the over-read is large enough (13 275 against a 2 805-byte chunk) to run
        // past the chunk and trip the malformed-metadata guard before the count is ever compared.
        // That is fixture-specific: a chunk with more data pages would absorb the over-read
        // silently, and then only the byte count catches it.
        long dictionaryRegionBytes;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(COMPRESSED_FIXTURE))) {
            ColumnMetaData metaData = reader.getFileMetaData().rowGroups().getFirst()
                    .columns().getFirst().metaData();
            dictionaryRegionBytes = metaData.dataPageOffset() - metaData.dictionaryPageOffset();
        }

        Reads absent = read(COMPRESSED_FIXTURE, "label", COMPRESSED_ABSENT);

        assertThat(absent.rows()).isZero();
        assertThat(absent.bytes())
                .as("exactly the dictionary page, not its decompressed size")
                .isEqualTo(dictionaryRegionBytes);
    }

    /// Requests, bytes and rows attributable to reading `category` — metadata reads performed while
    /// opening the file are excluded by taking the counters after `open`.
    private record Reads(int requests, long bytes, int rows) {}

    private static Reads read(FilterPredicate filter) throws Exception {
        return read(FIXTURE, "category", filter);
    }

    private static Reads read(Path fixture, String column, FilterPredicate filter) throws Exception {
        CountingInputFile file = new CountingInputFile(InputFile.of(fixture));
        file.open();
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            int requestsBefore = file.readCount();
            long bytesBefore = file.bytesRead();

            ParquetFileReader.ColumnReaderBuilder builder = reader.buildColumnReader(column);
            if (filter != null) {
                builder = builder.filter(filter);
            }
            int rows = 0;
            try (ColumnReader values = builder.build()) {
                while (values.nextBatch()) {
                    rows += values.getRecordCount();
                }
            }
            return new Reads(file.readCount() - requestsBefore, file.bytesRead() - bytesBefore, rows);
        }
    }
}
