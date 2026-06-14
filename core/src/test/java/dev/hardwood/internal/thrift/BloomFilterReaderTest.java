/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that [BloomFilterHeaderReader] and [BloomFilterReader] parse the bloom filter
/// stored at `bloom_filter_offset`: the `BloomFilterHeader` thrift struct followed by its
/// raw bitset. The fixture `bloom_filter_test.parquet` writes a bloom filter on column `id`
/// using the only variants the format currently defines: split-block algorithm, xxHash, and
/// no compression.
class BloomFilterReaderTest {

    @Test
    void parsesBloomFilterHeaderFromColumnChunk() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/bloom_filter_test.parquet");
        InputFile inputFile = InputFile.of(parquetFile);

        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            RowGroup rowGroup = fileReader.getFileMetaData().rowGroups().getFirst();
            ColumnChunk idChunk = rowGroup.columns().getFirst();
            ColumnMetaData metaData = idChunk.metaData();

            long offset = metaData.bloomFilterOffset();
            int length = metaData.bloomFilterLength();

            ByteBuffer buffer = inputFile.readRange(offset, length);
            ThriftCompactReader thriftReader = new ThriftCompactReader(buffer);
            BloomFilterHeader header = BloomFilterHeaderReader.read(thriftReader);

            assertThat(header.numBytes()).isPositive();
            assertThat(header.algorithm()).isEqualTo(BloomFilterHeader.Algorithm.BLOCK);
            assertThat(header.hash()).isEqualTo(BloomFilterHeader.Hash.XXHASH);
            assertThat(header.compression()).isEqualTo(BloomFilterHeader.Compression.UNCOMPRESSED);

            // The bitset follows the header, so the chunk's total length must split exactly
            // into the header bytes we consumed plus the bitset's declared numBytes.
            assertThat(thriftReader.getBytesRead() + header.numBytes()).isEqualTo(length);
        }
    }

    @Test
    void parsesFullBloomFilterFromColumnChunk() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/bloom_filter_test.parquet");
        InputFile inputFile = InputFile.of(parquetFile);

        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            ColumnMetaData metaData = fileReader.getFileMetaData().rowGroups().getFirst()
                    .columns().getFirst().metaData();

            int length = metaData.bloomFilterLength();
            ByteBuffer buffer = inputFile.readRange(metaData.bloomFilterOffset(), length);
            ThriftCompactReader thriftReader = new ThriftCompactReader(buffer);
            BloomFilter bloomFilter = BloomFilterReader.read(thriftReader);

            // Reading the header plus its numBytes of bitset must consume the whole filter region,
            // proving the bitset was read to exactly the end of the chunk.
            assertThat(thriftReader.getBytesRead()).isEqualTo(length);
            assertThat(bloomFilter.bitset().capacity()).isEqualTo(bloomFilter.header().numBytes());
        }
    }
}
