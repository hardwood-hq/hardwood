/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import dev.hardwood.Validity;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

/// Helpers shared by the writer round-trip tests: the schemas they write, the readers that
/// take the values back out, and the page-level accessors that read what a value comparison
/// cannot see.
///
/// These sit here rather than in one of the test classes because more than one needs them,
/// and duplicating a reader between test classes is how two of them come to disagree about
/// what the file said.
final class WriterTestSupport {

    private WriterTestSupport() {
    }

    static FileSchema twoColumns() {
        return FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    static FileSchema oneOptionalColumn() {
        return FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();
    }

    static FileSchema oneColumn() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    static ColumnMetaData columnMeta(ParquetFileReader reader, int columnIndex) {
        return reader.getFileMetaData().rowGroups().get(0).columns().get(columnIndex).metaData();
    }

    /// Walks a column chunk's contiguous data pages from `startOffset`, returning how many pages
    /// cover `totalValues`.
    static int countDataPages(byte[] file, long startOffset, long totalValues) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(file);
        int offset = Math.toIntExact(startOffset);
        long seen = 0;
        int pages = 0;
        while (seen < totalValues) {
            ThriftCompactReader reader = new ThriftCompactReader(buf, offset);
            PageHeader header = PageHeaderReader.read(reader);
            pages++;
            seen += header.dataPageHeader().numValues();
            offset += reader.getBytesRead() + header.compressedPageSize();
        }
        return pages;
    }

    static int[] readInts(ParquetFileReader reader, int columnIndex) throws IOException {
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            int[] result = new int[reader.getFileMetaData().numRows() < 0 ? 0
                    : Math.toIntExact(reader.getFileMetaData().numRows())];
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getValueCount();
                int[] batch = column.getInts();
                System.arraycopy(batch, 0, result, pos, count);
                pos += count;
            }
            return result;
        }
    }

    /// Reads a flat column back into a boxed array, `null` at each null row, so both the
    /// values and their null positions can be asserted in one comparison.
    static Integer[] readNullable(ParquetFileReader reader, int columnIndex) throws IOException {
        int rows = Math.toIntExact(reader.getFileMetaData().numRows());
        Integer[] result = new Integer[rows];
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getRecordCount();
                int[] batch = column.getInts();
                Validity validity = column.getLeafValidity();
                for (int i = 0; i < count; i++) {
                    result[pos + i] = validity.isNull(i) ? null : batch[i];
                }
                pos += count;
            }
        }
        return result;
    }

    /// Reconstructs a `MAP` of `INT32` to `INT32` as one map per record — `null` for an
    /// absent map, empty for an empty one, a `null` entry value for a null value. The key
    /// and value columns share the map's REPEATED layer offsets, so entries align by index.
    static List<Map<Integer, Integer>> readMapOfInts(ColumnReader keys, ColumnReader values) {
        List<Map<Integer, Integer>> out = new ArrayList<>();
        int[] offsets = values.getLayerOffsets(0);
        Validity mapValidity = values.getLayerValidity(0);
        int[] ks = keys.getInts();
        int[] vs = values.getInts();
        Validity valueValidity = values.getLeafValidity();
        for (int r = 0; r < values.getRecordCount(); r++) {
            if (mapValidity.isNull(r)) {
                out.add(null);
                continue;
            }
            Map<Integer, Integer> map = new LinkedHashMap<>();
            for (int e = offsets[r]; e < offsets[r + 1]; e++) {
                map.put(ks[e], valueValidity.isNull(e) ? null : vs[e]);
            }
            out.add(map);
        }
        return out;
    }

    /// Builds a small ordered map of up to two `Integer` entries, allowing a `null` value
    /// (which `Map.of` forbids). A `null` key marks the second entry absent.
    static Map<Integer, Integer> mapOf(Integer k1, Integer v1, Integer k2, Integer v2) {
        Map<Integer, Integer> map = new LinkedHashMap<>();
        map.put(k1, v1);
        if (k2 != null) {
            map.put(k2, v2);
        }
        return map;
    }

    /// Reconstructs a `LIST` of `INT32` as one list per record — `null` for an absent list,
    /// empty for an empty one, a `null` entry for a null element.
    static List<List<Integer>> readListOfInts(ParquetFileReader reader, int columnIndex) throws IOException {
        List<List<Integer>> out = new ArrayList<>();
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            while (column.nextBatch()) {
                int records = column.getRecordCount();
                int[] offsets = column.getLayerOffsets(0);
                Validity listValidity = column.getLayerValidity(0);
                int[] values = column.getInts();
                Validity leafValidity = column.getLeafValidity();
                for (int r = 0; r < records; r++) {
                    if (listValidity.isNull(r)) {
                        out.add(null);
                        continue;
                    }
                    List<Integer> list = new ArrayList<>();
                    for (int e = offsets[r]; e < offsets[r + 1]; e++) {
                        list.add(leafValidity.isNull(e) ? null : values[e]);
                    }
                    out.add(list);
                }
            }
        }
        return out;
    }

    static Integer[] expectedNullable(int[] values, boolean[] nulls) {
        Integer[] expected = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            expected[i] = nulls[i] ? null : values[i];
        }
        return expected;
    }
}
