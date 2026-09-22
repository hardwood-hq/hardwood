/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.List;

import dev.hardwood.metadata.PageEncodingStats;

/// Writer for a Thrift `list<PageEncodingStats>`, the inverse of
/// [PageEncodingStatsReader].
class PageEncodingStatsWriter {

    /// Writes the list body. The caller has written the field header the list belongs to.
    static void write(ThriftCompactWriter writer, List<PageEncodingStats> stats) {
        writer.writeListBegin(stats.size(), ThriftCompactConstants.ElementType.STRUCT);
        for (PageEncodingStats entry : stats) {
            writeStats(writer, entry);
        }
    }

    private static void writeStats(ThriftCompactWriter writer, PageEncodingStats stats) {
        short saved = writer.pushFieldIdContext();
        try {
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(stats.pageType()));
            writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(stats.encoding()));
            writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(stats.count());
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
