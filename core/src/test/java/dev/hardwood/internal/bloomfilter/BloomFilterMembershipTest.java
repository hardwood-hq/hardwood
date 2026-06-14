/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.BloomFilterReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end membership tests exercising the whole stack — read the bloom filter
/// ([BloomFilterReader]), hash a value's plain encoding ([XxHash64]), probe the filter
/// ([BloomFilter] / [SplitBlockBloomFilter]) — across three column types in
/// `bloom_filter_test.parquet`:
///
/// - `id` (INT64, values `0..63`) — the 8-byte hash path,
/// - `code` (INT32, values `0,3,..,189`) — the 4-byte-tail hash path,
/// - `name` (STRING, values `""`..`"x"*63`) — the empty value hits the zero-length path, short
///   names the 1-byte tail, long ones the `>= 32`-byte accumulator-lane loop, so the
///   variable-length column covers every xxHash64 path through the real read→hash→probe chain.
///
/// A bloom filter has no false negatives, so every stored value must report present.
class BloomFilterMembershipTest {

    @Test
    void containsAllStoredInt64Ids() throws Exception {
        BloomFilter filter = readBloomFilter("id");

        for (long id = 0; id < 64; id++) {
            assertThat(filter.mightContain(XxHash64.hash(id)))
                    .as("stored id %d", id)
                    .isTrue();
        }

        // Discrimination: across 1000 never-stored values, at least one must be rejected, so the
        // filter is not simply returning true for everything.
        long rejected = 0;
        for (long id = 64; id < 1064; id++) {
            if (!filter.mightContain(XxHash64.hash(id))) {
                rejected++;
            }
        }
        assertThat(rejected).isPositive();
    }

    @Test
    void containsAllStoredInt32Codes() throws Exception {
        BloomFilter filter = readBloomFilter("code");

        for (int v = 0; v < 64; v++) {
            int code = v * 3;
            assertThat(filter.mightContain(XxHash64.hash(code)))
                    .as("stored code %d", code)
                    .isTrue();
        }
    }

    @Test
    void containsAllStoredStrings() throws Exception {
        BloomFilter filter = readBloomFilter("name");

        for (int i = 0; i < 64; i++) {
            String name = "x".repeat(i); // lengths 0..63 (incl. empty) exercise every xxHash64 code path
            assertThat(filter.mightContain(XxHash64.hash(name.getBytes(StandardCharsets.UTF_8))))
                    .as("stored name of length %d", name.length())
                    .isTrue();
        }
    }

    private static BloomFilter readBloomFilter(String columnPath) throws Exception {
        Path parquetFile = Paths.get("src/test/resources/bloom_filter_test.parquet");
        InputFile inputFile = InputFile.of(parquetFile);
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            for (ColumnChunk column : fileReader.getFileMetaData().rowGroups().getFirst().columns()) {
                ColumnMetaData metaData = column.metaData();
                if (metaData.pathInSchema().toString().equals(columnPath)) {
                    ByteBuffer buffer = inputFile.readRange(
                            metaData.bloomFilterOffset(), metaData.bloomFilterLength());
                    return BloomFilterReader.read(new ThriftCompactReader(buffer));
                }
            }
        }
        throw new IllegalStateException("No bloom filter for column '" + columnPath + "'");
    }
}
