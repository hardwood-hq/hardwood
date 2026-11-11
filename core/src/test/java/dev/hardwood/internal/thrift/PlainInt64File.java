/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.zip.CRC32;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;

/// Builds a file of one row group holding one required `INT64` column `v`, stored uncompressed and
/// `PLAIN`-encoded in `V1` data pages, whose page headers and offset index a test may misstate.
public final class PlainInt64File {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private PlainInt64File() {
    }

    /// A data page of `values` consecutive values from `first`, whose header declares
    /// `compressedPageSize` as the size of its body, or the body's own size when `null`.
    public record Page(long first, int values, Integer compressedPageSize) {

        /// A page whose header states its body's size.
        public Page(long first, int values) {
            this(first, values, null);
        }
    }

    /// The file of `pages`, without an offset index.
    public static byte[] build(List<Page> pages) {
        return build(pages, null);
    }

    /// The file of `pages`, with an offset index of the page locations `index` makes of the pages'
    /// own, or none when `index` is `null`.
    public static byte[] build(List<Page> pages, UnaryOperator<List<PageLocation>> index) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(MAGIC);
        long chunkStart = out.size();
        List<PageLocation> locations = new ArrayList<>();
        long rows = 0;
        for (Page page : pages) {
            byte[] body = body(page);
            ThriftCompactWriter header = new ThriftCompactWriter();
            int declared = page.compressedPageSize() == null ? body.length : page.compressedPageSize();
            PageHeaderWriter.writeDataPageV1(header, page.values(), body.length, declared, crc(body),
                    Encoding.PLAIN);
            long offset = out.size();
            out.writeBytes(header.toByteArray());
            out.writeBytes(body);
            locations.add(new PageLocation(offset, Math.toIntExact(out.size() - offset), rows));
            rows += page.values();
        }
        long chunkSize = out.size() - chunkStart;
        Long indexOffset = null;
        Integer indexLength = null;
        if (index != null) {
            ThriftCompactWriter writer = new ThriftCompactWriter();
            OffsetIndexWriter.write(writer, new OffsetIndex(index.apply(locations), null));
            byte[] bytes = writer.toByteArray();
            indexOffset = (long) out.size();
            indexLength = bytes.length;
            out.writeBytes(bytes);
        }

        ColumnMetaData metaData = new ColumnMetaData(PhysicalType.INT64, List.of(Encoding.PLAIN), FieldPath.of("v"),
                CompressionCodec.UNCOMPRESSED, rows, chunkSize, chunkSize, Map.of(), chunkStart, null, null, null,
                null, null, List.of(), null);
        RowGroup rowGroup = new RowGroup(List.of(new ColumnChunk(metaData, indexOffset, indexLength, null, null,
                null)), chunkSize, rows);
        List<SchemaElement> schema = List.of(
                new SchemaElement("r", null, null, null, 1, null, null, null, null, null),
                new SchemaElement("v", PhysicalType.INT64, null, RepetitionType.REQUIRED, null, null, null, null,
                        null, null));
        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, new FileMetaData(1, schema, rows, List.of(rowGroup), Map.of(), null,
                List.of()));
        byte[] footerBytes = footer.toByteArray();
        out.writeBytes(footerBytes);
        out.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(footerBytes.length).array());
        out.writeBytes(MAGIC);
        return out.toByteArray();
    }

    private static byte[] body(Page page) {
        ByteBuffer body = ByteBuffer.allocate(page.values() * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < page.values(); i++) {
            body.putLong(page.first() + i);
        }
        return body.array();
    }

    private static int crc(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
