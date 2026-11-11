/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page header declaring an uncompressed size its codec cannot expand the page's stored bytes
/// to fails before the decompression output is allocated from it, while a page compressed as far
/// as its codec goes reads.
class DeclaredUncompressedSizeTest {

    private static final ColumnSchema COLUMN = DataPages.required(PhysicalType.INT64, null);

    /// Stored bytes of the crafted pages, whatever they hold: they are refused before they are read.
    private static final byte[] STORED = new byte[33];

    private static final int DECLARED = 2_000_000_000;

    /// The values of a page of zeros: 1 MiB, which each codec compresses as far as it goes.
    private static final int ZEROS = 131_072;

    @ParameterizedTest
    @CsvSource({ "SNAPPY, 704", "LZ4, 8415", "LZ4_RAW, 8415", "GZIP, 34056", "ZSTD, 1081344" })
    void refusesADataPageDeclaringMoreThanItsCodecExpandsTo(CompressionCodec codec, long bound) {
        ByteBuffer page = page(1, STORED, DECLARED);

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(COLUMN, codec), COLUMN, page, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Data page's uncompressed_page_size of 2000000000 bytes exceeds the " + bound
                        + " bytes that 33 bytes of " + codec + " decompress to at most");
    }

    @Test
    void refusesAnUncompressedDataPageDeclaringMoreThanItStores() {
        ByteBuffer page = page(1, STORED, DECLARED);

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(COLUMN, CompressionCodec.UNCOMPRESSED), COLUMN,
                page, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Uncompressed page size mismatch: expected 2000000000, got 33");
    }

    /// Only the value section of a `DATA_PAGE_V2` is compressed, so its bound is on the
    /// uncompressed size less the level sections.
    @Test
    void refusesAV2ValueSectionDeclaringMoreThanItsCodecExpandsTo() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT64, null);
        ByteBuffer page = DataPages.storedPageV2(Encoding.PLAIN, 1, 1, new byte[0], new byte[2], 2, STORED,
                DECLARED, true);

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(column, CompressionCodec.SNAPPY), column, page,
                null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DATA_PAGE_V2 value section's uncompressed size (uncompressed_page_size less its level "
                        + "sections) of 1999999998 bytes exceeds the 704 bytes that 33 bytes of SNAPPY "
                        + "decompress to at most");
    }

    @Test
    void refusesV2LevelSectionsLongerThanTheUncompressedPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT64, null);
        ByteBuffer page = DataPages.storedPageV2(Encoding.PLAIN, 1, 1, new byte[0], new byte[2], 2, STORED, 1, true);

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(column, CompressionCodec.SNAPPY), column, page,
                null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DATA_PAGE_V2 level sections of 0 and 2 bytes exceed its uncompressed_page_size of 1");
    }

    @Test
    void refusesADictionaryPageDeclaringMoreThanItsCodecExpandsTo() throws Exception {
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDictionaryPageV1(header, 1, DECLARED, STORED.length, crc(STORED), Encoding.PLAIN);
        ByteBuffer page = withBody(header.toByteArray(), STORED);

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            assertThatThrownBy(() -> DictionaryParser.parse(DataPage.parse(page), COLUMN,
                    metaData(CompressionCodec.GZIP), context))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Dictionary page's uncompressed_page_size of 2000000000 bytes exceeds the 34056 "
                            + "bytes that 33 bytes of GZIP decompress to at most");
        }
    }

    /// Zeros are the content each codec expands most, so these pages lie close to their bounds.
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = { "SNAPPY", "LZ4_RAW", "GZIP", "ZSTD" })
    void readsAPageOfZerosCompressedAsFarAsItsCodecGoes(CompressionCodec codec) {
        byte[] values = PlainEncoder.encodeLongs(new long[ZEROS], 0, ZEROS);
        byte[] stored = new CompressorFactory().getCompressor(codec).compress(values, 0, values.length);

        Object read = DataPages.read(DataPages.decoder(COLUMN, codec), COLUMN, page(ZEROS, stored, values.length),
                null);

        assertThat((long[]) read).isEqualTo(new long[ZEROS]);
    }

    /// `LZ4` pages are read as a raw `LZ4` block first.
    @Test
    void readsAnLz4PageOfZeros() {
        byte[] values = PlainEncoder.encodeLongs(new long[ZEROS], 0, ZEROS);
        byte[] stored = new CompressorFactory().getCompressor(CompressionCodec.LZ4_RAW).compress(values, 0,
                values.length);

        Object read = DataPages.read(DataPages.decoder(COLUMN, CompressionCodec.LZ4), COLUMN,
                page(ZEROS, stored, values.length), null);

        assertThat((long[]) read).isEqualTo(new long[ZEROS]);
    }

    /// A `DATA_PAGE` with `stored` as its body and `uncompressedSize` as its declared size.
    private static ByteBuffer page(int numValues, byte[] stored, int uncompressedSize) {
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, numValues, uncompressedSize, stored.length, crc(stored),
                Encoding.PLAIN);
        return withBody(header.toByteArray(), stored);
    }

    private static ColumnMetaData metaData(CompressionCodec codec) {
        return new ColumnMetaData(PhysicalType.INT64, List.of(Encoding.PLAIN), FieldPath.of("c"), codec,
                Long.MAX_VALUE, 0, 0, Map.of(), 0, null, null, null, null, null, List.of(), null);
    }

    private static int crc(byte[] stored) {
        CRC32 crc = new CRC32();
        crc.update(stored);
        return (int) crc.getValue();
    }

    private static ByteBuffer withBody(byte[] header, byte[] body) {
        ByteBuffer page = ByteBuffer.allocate(header.length + body.length);
        page.put(header).put(body).flip();
        return page;
    }
}
