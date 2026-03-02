/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.StatisticsDecoder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

class StatisticsTest {

    @Test
    void testStatisticsAreParsedFromFilterPushdownFile() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata.rowGroups()).hasSize(3);

            // Row group 0: id 1-100
            RowGroup rg0 = metadata.rowGroups().get(0);
            Statistics stats0 = rg0.columns().get(0).metaData().statistics();
            assertThat(stats0).isNotNull();
            assertThat(stats0.minValue()).isNotNull();
            assertThat(stats0.maxValue()).isNotNull();
            assertThat(StatisticsDecoder.decodeLong(stats0.minValue())).isEqualTo(1L);
            assertThat(StatisticsDecoder.decodeLong(stats0.maxValue())).isEqualTo(100L);

            // Row group 1: id 101-200
            RowGroup rg1 = metadata.rowGroups().get(1);
            Statistics stats1 = rg1.columns().get(0).metaData().statistics();
            assertThat(stats1).isNotNull();
            assertThat(StatisticsDecoder.decodeLong(stats1.minValue())).isEqualTo(101L);
            assertThat(StatisticsDecoder.decodeLong(stats1.maxValue())).isEqualTo(200L);

            // Row group 2: id 201-300
            RowGroup rg2 = metadata.rowGroups().get(2);
            Statistics stats2 = rg2.columns().get(0).metaData().statistics();
            assertThat(stats2).isNotNull();
            assertThat(StatisticsDecoder.decodeLong(stats2.minValue())).isEqualTo(201L);
            assertThat(StatisticsDecoder.decodeLong(stats2.maxValue())).isEqualTo(300L);
        }
    }

    @Test
    void testStatisticsAreParsedForMixedTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_mixed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata.rowGroups()).hasSize(3);

            // Row group 0: id 1-5, price 10.0-50.0
            RowGroup rg0 = metadata.rowGroups().get(0);

            // int32 column (id)
            Statistics idStats = rg0.columns().get(0).metaData().statistics();
            assertThat(idStats).isNotNull();
            assertThat(StatisticsDecoder.decodeInt(idStats.minValue())).isEqualTo(1);
            assertThat(StatisticsDecoder.decodeInt(idStats.maxValue())).isEqualTo(5);

            // float64 column (price)
            Statistics priceStats = rg0.columns().get(1).metaData().statistics();
            assertThat(priceStats).isNotNull();
            assertThat(StatisticsDecoder.decodeDouble(priceStats.minValue())).isEqualTo(10.0);
            assertThat(StatisticsDecoder.decodeDouble(priceStats.maxValue())).isEqualTo(50.0);

            // float32 column (rating)
            Statistics ratingStats = rg0.columns().get(2).metaData().statistics();
            assertThat(ratingStats).isNotNull();
            assertThat(StatisticsDecoder.decodeFloat(ratingStats.minValue())).isEqualTo(1.0f);
            assertThat(StatisticsDecoder.decodeFloat(ratingStats.maxValue())).isEqualTo(5.0f);
        }
    }

    @Test
    void testNullCountIsParsed() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileMetaData metadata = reader.getFileMetaData();
            RowGroup rg0 = metadata.rowGroups().get(0);
            Statistics stats = rg0.columns().get(0).metaData().statistics();
            assertThat(stats).isNotNull();
            // Required columns should have 0 null count
            assertThat(stats.nullCount()).isEqualTo(0L);
        }
    }

    @Test
    void testExistingFilesStillParse() throws Exception {
        // Ensure existing test files still parse correctly with statistics field addition
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);
        }
    }

    // ==================== StatisticsDecoder Tests ====================

    @Test
    void testDecodeInt() {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(42).array();
        assertThat(StatisticsDecoder.decodeInt(bytes)).isEqualTo(42);

        byte[] negBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(-100).array();
        assertThat(StatisticsDecoder.decodeInt(negBytes)).isEqualTo(-100);
    }

    @Test
    void testDecodeLong() {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(123456789L).array();
        assertThat(StatisticsDecoder.decodeLong(bytes)).isEqualTo(123456789L);
    }

    @Test
    void testDecodeFloat() {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(3.14f).array();
        assertThat(StatisticsDecoder.decodeFloat(bytes)).isEqualTo(3.14f);
    }

    @Test
    void testDecodeDouble() {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(2.718281828).array();
        assertThat(StatisticsDecoder.decodeDouble(bytes)).isEqualTo(2.718281828);
    }

    @Test
    void testDecodeBoolean() {
        assertThat(StatisticsDecoder.decodeBoolean(new byte[]{1})).isTrue();
        assertThat(StatisticsDecoder.decodeBoolean(new byte[]{0})).isFalse();
    }

    @Test
    void testCompareBinary() {
        byte[] a = {0x01, 0x02, 0x03};
        byte[] b = {0x01, 0x02, 0x04};
        byte[] c = {0x01, 0x02, 0x03};
        byte[] shorter = {0x01, 0x02};

        assertThat(StatisticsDecoder.compareBinary(a, b)).isNegative();
        assertThat(StatisticsDecoder.compareBinary(b, a)).isPositive();
        assertThat(StatisticsDecoder.compareBinary(a, c)).isZero();
        assertThat(StatisticsDecoder.compareBinary(shorter, a)).isNegative();
        assertThat(StatisticsDecoder.compareBinary(a, shorter)).isPositive();
    }
}
