/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.FileMetaData;

/**
 * Utility class for reading Parquet file metadata from a FileChannel.
 * <p>
 * This centralizes the metadata reading logic used by ParquetFileReader,
 * MultiFileRowReader, and FileManager.
 * </p>
 */
public final class ParquetMetadataReader {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    private ParquetMetadataReader() {
        // Utility class
    }

    /**
     * Reads file metadata from a memory-mapped buffer covering the entire file.
     *
     * @param fileMapping the memory-mapped buffer of the entire file
     * @param path the file path (used for error messages)
     * @return the parsed FileMetaData
     * @throws IOException if the file is not a valid Parquet file
     */
    public static FileMetaData readMetadata(MappedByteBuffer fileMapping, Path path) throws IOException {
        int fileSize = fileMapping.limit();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new IOException("File too small to be a valid Parquet file: " + path);
        }

        // Validate magic number at start
        byte[] startMagic = new byte[MAGIC_SIZE];
        fileMapping.get(0, startMagic);
        if (!Arrays.equals(startMagic, MAGIC)) {
            throw new IOException("Not a Parquet file (invalid magic number at start): " + path);
        }

        // Read footer size and magic number at end
        int footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer footerInfoBuf = fileMapping.slice(footerInfoPos, FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        footerInfoBuf.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = footerInfoBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        footerInfoBuf.get(endMagic);
        if (!Arrays.equals(endMagic, MAGIC)) {
            throw new IOException("Not a Parquet file (invalid magic number at end): " + path);
        }

        // Validate footer length
        int footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
        if (footerStart < MAGIC_SIZE) {
            throw new IOException("Invalid footer length: " + footerLength);
        }

        // Parse file metadata directly from the mapping (no copy needed)
        ByteBuffer footerBuffer = fileMapping.slice(footerStart, footerLength);
        ThriftCompactReader reader = new ThriftCompactReader(footerBuffer);
        return FileMetaDataReader.read(reader);
    }
}
