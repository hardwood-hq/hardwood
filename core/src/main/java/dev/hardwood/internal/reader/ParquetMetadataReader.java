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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import dev.hardwood.InputFile;
import dev.hardwood.internal.EncryptedParquetException;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.FileMetaData;

/// Utility class for reading Parquet file metadata from an [InputFile].
///
/// This centralizes the metadata reading logic used by ParquetFileReader,
/// MultiFileRowReader, and FileManager.
public final class ParquetMetadataReader {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    /// Magic written in place of [#MAGIC] when the footer itself is encrypted
    /// (Parquet Modular Encryption, encrypted-footer mode).
    private static final byte[] ENCRYPTED_MAGIC = "PARE".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    private ParquetMetadataReader() {
        // Utility class
    }

    /// Reads file metadata from an [InputFile].
    ///
    /// @param inputFile the input file to read metadata from
    /// @return the parsed FileMetaData
    /// @throws IOException if the file is not a valid Parquet file
    public static FileMetaData readMetadata(InputFile inputFile) throws IOException {
        long fileSize = inputFile.length();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new IOException(ExceptionContext.filePrefix(inputFile.name())
                    + "File too small to be a valid Parquet file");
        }

        // Validate magic number at start
        ByteBuffer startMagicBuf;
        try (FetchReason.Scope ignored = FetchReason.set("footer-magic-start")) {
            startMagicBuf = inputFile.readRange(0, MAGIC_SIZE);
        }
        byte[] startMagic = new byte[MAGIC_SIZE];
        startMagicBuf.get(startMagic);
        if (Arrays.equals(startMagic, ENCRYPTED_MAGIC)) {
            throw encrypted(inputFile);
        }
        if (!Arrays.equals(startMagic, MAGIC)) {
            throw new IOException(ExceptionContext.filePrefix(inputFile.name())
                    + "Not a Parquet file (invalid magic number at start)");
        }

        // Read footer size and magic number at end
        long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer footerInfoBuf;
        try (FetchReason.Scope ignored = FetchReason.set("footer-info")) {
            footerInfoBuf = inputFile.readRange(footerInfoPos, FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        }
        footerInfoBuf.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = footerInfoBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        footerInfoBuf.get(endMagic);
        if (Arrays.equals(endMagic, ENCRYPTED_MAGIC)) {
            throw encrypted(inputFile);
        }
        if (!Arrays.equals(endMagic, MAGIC)) {
            throw new IOException(ExceptionContext.filePrefix(inputFile.name())
                    + "Not a Parquet file (invalid magic number at end)");
        }

        // Validate footer length
        long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
        if (footerStart < MAGIC_SIZE) {
            throw new IOException("Invalid footer length: " + footerLength);
        }

        // Parse file metadata
        ByteBuffer footerBuffer;
        try (FetchReason.Scope ignored = FetchReason.set("footer-body")) {
            footerBuffer = inputFile.readRange(footerStart, footerLength);
        }
        ThriftCompactReader reader = new ThriftCompactReader(footerBuffer);
        try {
            return FileMetaDataReader.read(reader);
        }
        catch (EncryptedParquetException e) {
            // Plaintext-footer encryption: the footer parsed, but the data is
            // encrypted. Re-throw with file context for an attributable error.
            throw encrypted(inputFile);
        }
        catch (IOException e) {
            // Any failure parsing the footer (negative sizes/counts/offsets,
            // unknown field type, truncated/EOF mid-footer, ...) names the
            // problem but not the file; attach file context so the controlled
            // error stays attributable.
            throw new IOException(ExceptionContext.filePrefix(inputFile.name()) + e.getMessage(), e);
        }
    }

    private static EncryptedParquetException encrypted(InputFile inputFile) {
        return new EncryptedParquetException(
                ExceptionContext.filePrefix(inputFile.name()) + EncryptedParquetException.MESSAGE);
    }
}
