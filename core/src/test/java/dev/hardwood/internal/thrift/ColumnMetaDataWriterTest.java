/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnMetaDataWriterTest {

    @Test
    void omitsEncodingStatsWhenListIsEmpty() {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        ColumnMetaDataWriter.write(writer, metadata(List.of()));

        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray()));
        boolean sawEncodingStats = false;
        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }
            sawEncodingStats |= ThriftCompactReader.fieldId(header) == 13;
            reader.skipField(ThriftCompactReader.fieldType(header));
        }

        assertThat(sawEncodingStats).isFalse();
    }

    @Test
    void rejectsUnknownPageTypeWhenWritingEncodingStats() {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        ColumnMetaData metadata = metadata(List.of(
                new PageEncodingStats(PageType.UNKNOWN, Encoding.PLAIN, 1)));

        assertThatThrownBy(() -> ColumnMetaDataWriter.write(writer, metadata))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No Thrift value for page type: UNKNOWN");
    }

    private static ColumnMetaData metadata(List<PageEncodingStats> encodingStats) {
        return new ColumnMetaData(
                PhysicalType.INT32,
                List.of(Encoding.PLAIN),
                FieldPath.of("column"),
                CompressionCodec.UNCOMPRESSED,
                1,
                1,
                1,
                Map.of(),
                0,
                null,
                null,
                null,
                null,
                null,
                encodingStats,
                null);
    }
}
