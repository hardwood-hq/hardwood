/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A probe's reads whose extent the footer does not give (FETCH_PLANNING.md, Pruning reads): a
/// Bloom filter without `bloom_filter_length` and a dictionary page without
/// `dictionary_page_offset`, both of which older writers omit, are read header first and still
/// prune, so the read fetches no column data.
class PruningReadsTest {

    /// `code` cycles 0, 3, 6, 9 in each of five row groups, with a Bloom filter; no dictionary.
    private static final Path BLOOM_FILTERS = Path.of("src/test/resources/bloom_filter_multi_rg_test.parquet");
    /// Four row groups whose `i64` chunks are dictionary-encoded.
    private static final Path DICTIONARIES = Path.of("src/test/resources/predicate/predicate_dict.parquet");

    @Test
    void aBloomFilterWithoutItsLengthStillDropsTheRowGroup() throws Exception {
        byte[] bytes = FooterRewriter.rewrite(Files.readAllBytes(BLOOM_FILTERS), chunks(m -> withBloomFilterLength(m, null)));

        CountingInputFile file = read(bytes, FilterPredicate.eq("code", 5));

        assertThat(file.reads()).extracting(CountingInputFile.Read::reason).noneMatch(reason -> reason.contains(" col="));
    }

    @Test
    void aDictionaryPageWithoutItsOffsetStillDropsTheRowGroup() throws Exception {
        // as parquet-mr 1.12 writes it: no dictionary_page_offset, data_page_offset at the dictionary page
        byte[] bytes = FooterRewriter.rewrite(Files.readAllBytes(DICTIONARIES), chunks(m -> m.dictionaryPageOffset() == null ? m
                : withOffsets(m, m.dictionaryPageOffset())));

        CountingInputFile file = read(bytes, FilterPredicate.in("i64",
                -3_990_000_000_000L, -1_990_000_000_000L, 10_000_000_000L, 2_010_000_000_000L));

        assertThat(file.reads()).extracting(CountingInputFile.Read::reason).noneMatch(reason -> reason.contains(" col="));
    }

    /// A filter one leaf of which only a Bloom filter rules out and the other only a dictionary:
    /// the row groups are dropped only where the probe evaluates both, from what it read in one
    /// round.
    @Test
    void aBloomFilterAndADictionaryDecideOneFilterTogether() throws Exception {
        byte[] bytes = bloomAndDictionary();

        CountingInputFile either = read(bytes, FilterPredicate.or(FilterPredicate.eq("a", 1001L),
                FilterPredicate.eq("b", "v-0005")));
        CountingInputFile both = read(bytes, FilterPredicate.and(FilterPredicate.eq("a", 1000L),
                FilterPredicate.eq("b", "v-0005")));

        assertThat(either.reads()).extracting(CountingInputFile.Read::reason).noneMatch(reason -> reason.contains(" col="));
        assertThat(both.reads()).extracting(CountingInputFile.Read::reason).noneMatch(reason -> reason.contains(" col="));
    }

    /// Four row groups of 1,000 rows: `a` the even numbers below 8,000, shuffled so statistics
    /// decide no literal in their range, with a Bloom filter; `b` one of 50 strings `v-000` to
    /// `v-049`, dictionary-encoded.
    private static byte[] bloomAndDictionary() throws Exception {
        int rows = 4_000;
        long[] a = new long[rows];
        for (int i = 0; i < rows; i++) {
            a[i] = 2L * i;
        }
        Random random = new Random(7);
        for (int i = rows - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            long t = a[i];
            a[i] = a[j];
            a[j] = t;
        }
        FileSchema schema = FileSchema.builder("t")
                .addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .build();
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(1_000).bloomFilter("a").build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            RowWriter rowWriter = writer.rowWriter();
            for (int i = 0; i < rows; i++) {
                long value = a[i];
                String text = String.format("v-%03d", i % 50);
                rowWriter.writeRow(row -> {
                    row.setLong("a", value);
                    row.setString("b", text);
                });
            }
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// Reads `bytes` under `filter`, which keeps no row.
    private static CountingInputFile read(byte[] bytes, FilterPredicate filter) throws Exception {
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));
        file.open();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        assertThat(rows).isZero();
        return file;
    }

    /// A footer patch applying `patch` to every column chunk's metadata.
    private static UnaryOperator<FileMetaData> chunks(UnaryOperator<ColumnMetaData> patch) {
        return metaData -> new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(),
                metaData.rowGroups().stream().map(rowGroup -> {
                    List<ColumnChunk> columns = rowGroup.columns().stream().map(chunk -> new ColumnChunk(
                            patch.apply(chunk.metaData()), chunk.offsetIndexOffset(), chunk.offsetIndexLength(),
                            chunk.columnIndexOffset(), chunk.columnIndexLength(), chunk.filePath())).toList();
                    return new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());
                }).toList(),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
    }

    private static ColumnMetaData withBloomFilterLength(ColumnMetaData m, Integer length) {
        return new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(), m.codec(), m.numValues(),
                m.totalUncompressedSize(), m.totalCompressedSize(), m.keyValueMetadata(), m.dataPageOffset(),
                m.dictionaryPageOffset(), m.statistics(), m.geospatialStatistics(), m.bloomFilterOffset(), length,
                m.encodingStats(), m.sizeStatistics());
    }

    /// `m` without `dictionary_page_offset`, its `data_page_offset` at `firstPage`.
    private static ColumnMetaData withOffsets(ColumnMetaData m, long firstPage) {
        return new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(), m.codec(), m.numValues(),
                m.totalUncompressedSize(), m.totalCompressedSize(), m.keyValueMetadata(), firstPage, null,
                m.statistics(), m.geospatialStatistics(), m.bloomFilterOffset(), m.bloomFilterLength(),
                m.encodingStats(), m.sizeStatistics());
    }
}
