/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.GZIPOutputStream;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;

/// Builds a file of the shape of DuckDB's `42.parquet`: a required `INT64` column `b` whose
/// dictionary holds one value, `Long.MAX_VALUE`, and whose data pages each repeat entry 0
/// `pageValues` times in a single RLE run of a few bytes. Every layer of the file agrees with
/// the pages, so only a bound on the declared count itself refuses it.
public final class RleExpansionFile {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private static final int PAGE_TYPE_DATA_PAGE_V2 = 3;

    /// The encoding stats of a chunk whose one data page is dictionary-encoded, which lets pruning
    /// read its dictionary.
    private static final List<PageEncodingStats> DICTIONARY_ENCODED = List.of(
            new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
            new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 1));

    private RleExpansionFile() {
    }

    /// One row group of the file: `pageCount` data pages of `pageValues` values each, with
    /// `chunkValues` as the column chunk's and the row group's declared count.
    public record Chunk(int pageValues, int pageCount, long chunkValues) {
    }

    /// One row group of `V1` data pages; see [Chunk].
    public static byte[] build(int pageValues, int pageCount, long chunkValues) {
        return build(false, new Chunk(pageValues, pageCount, chunkValues));
    }

    /// One row group per chunk, each with its own dictionary, of `V1` data pages or, with `v2`,
    /// of `DATA_PAGE_V2` ones.
    public static byte[] build(boolean v2, Chunk... chunks) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(MAGIC);
        List<RowGroup> rowGroups = new ArrayList<>();
        long numRows = 0;
        for (Chunk chunk : chunks) {
            rowGroups.add(writeRowGroup(out, chunk, v2));
            numRows += chunk.chunkValues();
        }

        List<SchemaElement> schema = List.of(
                new SchemaElement("r", null, null, null, 1, null, null, null, null, null),
                new SchemaElement("b", PhysicalType.INT64, null, RepetitionType.REQUIRED, null, null, null, null,
                        null, null));
        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, new FileMetaData(1, schema, numRows, rowGroups, Map.of(), null, List.of()));
        byte[] footerBytes = footer.toByteArray();
        out.writeBytes(footerBytes);
        out.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(footerBytes.length).array());
        out.writeBytes(MAGIC);
        return out.toByteArray();
    }

    /// `rowGroups` row groups of `columns` required `INT64` columns `c0`, `c1`, …, compressed with
    /// GZIP: each chunk a dictionary page of `dictionaryBytes` zero bytes, `dictionaryBytes / 8`
    /// entries of `0`, and one data page repeating entry 0 `pageValues` times, which the encoding
    /// stats record, so that pruning can read the dictionary. The dictionary pages take a few KiB
    /// each on disk whatever they declare.
    public static byte[] buildWide(int columns, int rowGroups, int dictionaryBytes, int pageValues) {
        byte[] dictionary = gzipZeros(dictionaryBytes);
        byte[] body = dataPageBody(pageValues);
        byte[] compressedBody = gzip(body);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(MAGIC);
        List<RowGroup> groups = new ArrayList<>();
        for (int g = 0; g < rowGroups; g++) {
            List<ColumnChunk> chunks = new ArrayList<>();
            long groupSize = 0;
            for (int c = 0; c < columns; c++) {
                long dictionaryOffset = out.size();
                ThriftCompactWriter dictionaryHeader = new ThriftCompactWriter();
                PageHeaderWriter.writeDictionaryPageV1(dictionaryHeader, dictionaryBytes / Long.BYTES,
                        dictionaryBytes, dictionary.length, crc(dictionary), Encoding.PLAIN);
                out.writeBytes(dictionaryHeader.toByteArray());
                out.writeBytes(dictionary);

                long dataOffset = out.size();
                ThriftCompactWriter dataHeader = new ThriftCompactWriter();
                PageHeaderWriter.writeDataPageV1(dataHeader, pageValues, body.length, compressedBody.length,
                        crc(compressedBody), Encoding.RLE_DICTIONARY);
                out.writeBytes(dataHeader.toByteArray());
                out.writeBytes(compressedBody);
                long chunkSize = out.size() - dictionaryOffset;
                groupSize += chunkSize;

                ColumnMetaData metaData = new ColumnMetaData(PhysicalType.INT64,
                        List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY), FieldPath.of("c" + c),
                        CompressionCodec.GZIP, pageValues, chunkSize, chunkSize, Map.of(), dataOffset,
                        dictionaryOffset, null, null, null, null, DICTIONARY_ENCODED, null);
                chunks.add(new ColumnChunk(metaData, null, null, null, null, null));
            }
            groups.add(new RowGroup(chunks, groupSize, pageValues));
        }

        List<SchemaElement> schema = new ArrayList<>();
        schema.add(new SchemaElement("r", null, null, null, columns, null, null, null, null, null));
        for (int c = 0; c < columns; c++) {
            schema.add(new SchemaElement("c" + c, PhysicalType.INT64, null, RepetitionType.REQUIRED, null, null,
                    null, null, null, null));
        }
        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, new FileMetaData(1, schema, (long) pageValues * rowGroups, groups, Map.of(),
                null, List.of()));
        byte[] footerBytes = footer.toByteArray();
        out.writeBytes(footerBytes);
        out.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(footerBytes.length).array());
        out.writeBytes(MAGIC);
        return out.toByteArray();
    }

    private static byte[] gzipZeros(int size) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] zeros = new byte[1 << 20];
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            for (int written = 0; written < size; written += zeros.length) {
                gzip.write(zeros, 0, Math.min(zeros.length, size - written));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static byte[] gzip(byte[] bytes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(bytes);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static RowGroup writeRowGroup(ByteArrayOutputStream out, Chunk chunk, boolean v2) {
        long dictionaryOffset = out.size();
        byte[] dictionary = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(Long.MAX_VALUE).array();
        ThriftCompactWriter dictionaryHeader = new ThriftCompactWriter();
        PageHeaderWriter.writeDictionaryPageV1(dictionaryHeader, 1, dictionary.length, dictionary.length,
                crc(dictionary), Encoding.PLAIN);
        out.writeBytes(dictionaryHeader.toByteArray());
        out.writeBytes(dictionary);

        long dataOffset = out.size();
        byte[] body = dataPageBody(chunk.pageValues());
        ThriftCompactWriter dataHeader = new ThriftCompactWriter();
        if (v2) {
            writeDataPageV2(dataHeader, chunk.pageValues(), body);
        }
        else {
            PageHeaderWriter.writeDataPageV1(dataHeader, chunk.pageValues(), body.length, body.length, crc(body),
                    Encoding.RLE_DICTIONARY);
        }
        byte[] header = dataHeader.toByteArray();
        for (int i = 0; i < chunk.pageCount(); i++) {
            out.writeBytes(header);
            out.writeBytes(body);
        }
        long chunkSize = out.size() - dictionaryOffset;

        ColumnMetaData metaData = new ColumnMetaData(PhysicalType.INT64,
                List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY), FieldPath.of("b"), CompressionCodec.UNCOMPRESSED,
                chunk.chunkValues(), chunkSize, chunkSize, Map.of(), dataOffset, dictionaryOffset, null, null, null,
                null, List.of(), null);
        return new RowGroup(List.of(new ColumnChunk(metaData, null, null, null, null, null)), chunkSize,
                chunk.chunkValues());
    }

    /// A `DATA_PAGE_V2` header over `body`, a required column's values alone: no level sections,
    /// no nulls, one row per value, stored uncompressed.
    private static void writeDataPageV2(ThriftCompactWriter writer, int values, byte[] body) {
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32); // type
        writer.writeI32(PAGE_TYPE_DATA_PAGE_V2);
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32); // uncompressed_page_size
        writer.writeI32(body.length);
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32); // compressed_page_size
        writer.writeI32(body.length);
        writer.writeFieldBegin(8, ThriftCompactConstants.FieldType.STRUCT); // data_page_header_v2
        short saved = writer.pushFieldIdContext();
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32); // num_values
        writer.writeI32(values);
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32); // num_nulls
        writer.writeI32(0);
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32); // num_rows
        writer.writeI32(values);
        writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32); // encoding
        writer.writeI32(ThriftEnumLookup.thriftValue(Encoding.RLE_DICTIONARY));
        writer.writeFieldBegin(5, ThriftCompactConstants.FieldType.I32); // definition_levels_byte_length
        writer.writeI32(0);
        writer.writeFieldBegin(6, ThriftCompactConstants.FieldType.I32); // repetition_levels_byte_length
        writer.writeI32(0);
        writer.writeBool(7, false); // is_compressed
        writer.writeFieldStop();
        writer.popFieldIdContext(saved);
        writer.writeFieldStop();
    }

    /// Bit width 1, then one RLE run: its count shifted left past the run-kind bit as a ULEB128
    /// varint, and the repeated id 0 in one byte.
    private static byte[] dataPageBody(int values) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(1);
        long header = (long) values << 1;
        while ((header & ~0x7FL) != 0) {
            body.write((int) ((header & 0x7F) | 0x80));
            header >>>= 7;
        }
        body.write((int) header);
        body.write(0);
        return body.toByteArray();
    }

    private static int crc(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
