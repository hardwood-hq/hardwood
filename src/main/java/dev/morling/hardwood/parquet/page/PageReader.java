/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.parquet.page;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import dev.morling.hardwood.parquet.Encoding;
import dev.morling.hardwood.parquet.compression.Decompressor;
import dev.morling.hardwood.parquet.compression.DecompressorFactory;
import dev.morling.hardwood.parquet.encoding.PlainDecoder;
import dev.morling.hardwood.parquet.encoding.RleBitPackingHybridDecoder;
import dev.morling.hardwood.parquet.metadata.ColumnMetaData;
import dev.morling.hardwood.parquet.metadata.DataPageHeader;
import dev.morling.hardwood.parquet.metadata.PageHeader;
import dev.morling.hardwood.parquet.schema.Column;
import dev.morling.hardwood.parquet.thrift.ThriftCompactReader;

/**
 * Reader for individual pages within a column chunk.
 */
public class PageReader {

    private final RandomAccessFile file;
    private final ColumnMetaData columnMetaData;
    private final Column column;
    private long currentOffset;
    private long valuesRead = 0;
    private Object[] dictionary = null;

    public PageReader(RandomAccessFile file, ColumnMetaData columnMetaData, Column column) {
        this.file = file;
        this.columnMetaData = columnMetaData;
        this.column = column;

        if (columnMetaData.dictionaryPageOffset() != null) {
            this.currentOffset = columnMetaData.dictionaryPageOffset();
        }
        else {
            this.currentOffset = columnMetaData.dataPageOffset();
        }
    }

    /**
     * Read the next page. Returns null if no more pages.
     */
    public Page readPage() throws IOException {
        if (valuesRead >= columnMetaData.numValues()) {
            return null;
        }

        // Seek to page position
        file.seek(currentOffset);

        // Read page header using a tracking input stream
        RandomAccessFileInputStream headerStream = new RandomAccessFileInputStream(file);
        ThriftCompactReader headerReader = new ThriftCompactReader(headerStream);
        PageHeader pageHeader = PageHeader.read(headerReader);

        // Read page data
        byte[] pageData = new byte[pageHeader.compressedPageSize()];
        file.readFully(pageData);

        // Update offset for next page
        currentOffset = file.getFilePointer();

        // Decompress page data
        Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
        byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());

        // Handle different page types
        return switch (pageHeader.type()) {
            case DICTIONARY_PAGE -> {
                // Read and store dictionary values
                parseDictionaryPage(pageHeader.dictionaryPageHeader(), uncompressedData);
                yield readPage(); // Read next page (the data page)
            }
            case DATA_PAGE -> {
                DataPageHeader dataHeader = pageHeader.dataPageHeader();
                valuesRead += dataHeader.numValues();
                yield parseDataPage(dataHeader, uncompressedData);
            }
            case DATA_PAGE_V2 -> throw new UnsupportedOperationException("DATA_PAGE_V2 not yet supported");
            default -> throw new IOException("Unexpected page type: " + pageHeader.type());
        };
    }

    private Page parseDataPage(DataPageHeader header, byte[] data) throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);

        // Read definition levels
        int[] definitionLevels = null;
        if (column.getMaxDefinitionLevel() > 0) {
            // Read definition level length (4 bytes)
            byte[] lengthBytes = new byte[4];
            dataStream.read(lengthBytes);
            int defLevelLength = java.nio.ByteBuffer.wrap(lengthBytes)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();

            // Read definition levels
            byte[] defLevelData = new byte[defLevelLength];
            dataStream.read(defLevelData);

            definitionLevels = new int[header.numValues()];
            RleBitPackingHybridDecoder defLevelDecoder = new RleBitPackingHybridDecoder(
                    new ByteArrayInputStream(defLevelData),
                    getBitWidth(column.getMaxDefinitionLevel()));
            defLevelDecoder.readInts(definitionLevels, 0, header.numValues());
        }

        // Read repetition levels (for Milestone 1, always 0 for flat schemas)
        // We skip this for now as max repetition level is 0

        // Count non-null values to read from encoded data
        int numNonNullValues = header.numValues();
        if (definitionLevels != null) {
            int maxDefLevel = column.getMaxDefinitionLevel();
            numNonNullValues = 0;
            for (int i = 0; i < header.numValues(); i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    numNonNullValues++;
                }
            }
        }

        // Read values using appropriate encoding
        Object[] values = new Object[header.numValues()];

        if (header.encoding() == Encoding.PLAIN) {
            // Read only the non-null values
            Object[] encodedValues = new Object[numNonNullValues];
            PlainDecoder decoder = new PlainDecoder(dataStream, column.type());
            decoder.readValues(encodedValues, 0, numNonNullValues);

            // Map encoded values to output array using definition levels
            if (definitionLevels != null) {
                int encodedIndex = 0;
                int maxDefLevel = column.getMaxDefinitionLevel();
                for (int i = 0; i < header.numValues(); i++) {
                    if (definitionLevels[i] == maxDefLevel) {
                        values[i] = encodedValues[encodedIndex++];
                    }
                    // else values[i] stays null
                }
            }
            else {
                System.arraycopy(encodedValues, 0, values, 0, numNonNullValues);
            }
        }
        else if (header.encoding() == Encoding.RLE_DICTIONARY || header.encoding() == Encoding.PLAIN_DICTIONARY) {
            // Decode dictionary indices
            if (dictionary == null) {
                throw new IOException("Dictionary page not found for RLE_DICTIONARY encoding");
            }

            int bitWidth = getBitWidth(dictionary.length - 1);

            // Read 1-byte length prefix (dictionary indices format for Data Page V1)
            dataStream.read();

            // Read the RLE/Bit-Packing Hybrid encoded indices
            byte[] indicesData = new byte[dataStream.available()];
            dataStream.read(indicesData);

            RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(
                    new ByteArrayInputStream(indicesData), bitWidth);

            int[] indices = new int[numNonNullValues];
            indexDecoder.readInts(indices, 0, numNonNullValues);

            // Map indices to dictionary values, applying definition levels
            if (definitionLevels != null) {
                int encodedIndex = 0;
                int maxDefLevel = column.getMaxDefinitionLevel();
                for (int i = 0; i < header.numValues(); i++) {
                    if (definitionLevels[i] == maxDefLevel) {
                        values[i] = dictionary[indices[encodedIndex++]];
                    }
                    // else values[i] stays null
                }
            }
            else {
                for (int i = 0; i < numNonNullValues; i++) {
                    values[i] = dictionary[indices[i]];
                }
            }
        }
        else {
            throw new UnsupportedOperationException(
                    "Encoding not yet supported: " + header.encoding());
        }

        return new Page(header.numValues(), definitionLevels, values);
    }

    private void parseDictionaryPage(dev.morling.hardwood.parquet.metadata.DictionaryPageHeader header, byte[] data)
            throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);

        // Dictionary values are encoded with PLAIN or PLAIN_DICTIONARY encoding
        if (header.encoding() != Encoding.PLAIN && header.encoding() != Encoding.PLAIN_DICTIONARY) {
            throw new UnsupportedOperationException(
                    "Dictionary encoding not yet supported: " + header.encoding());
        }

        // Read all dictionary values (both PLAIN and PLAIN_DICTIONARY use plain encoding for dictionary)
        dictionary = new Object[header.numValues()];
        PlainDecoder decoder = new PlainDecoder(dataStream, column.type());
        decoder.readValues(dictionary, 0, header.numValues());
    }

    private int getBitWidth(int maxValue) {
        if (maxValue == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }

    /**
     * InputStream wrapper for RandomAccessFile that tracks bytes read.
     */
    private static class RandomAccessFileInputStream extends InputStream {
        private final RandomAccessFile file;

        public RandomAccessFileInputStream(RandomAccessFile file) {
            this.file = file;
        }

        @Override
        public int read() throws IOException {
            return file.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return file.read(b, off, len);
        }
    }

    public static record Page(int numValues, int[] definitionLevels, Object[] values) {
    }
}
