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
import dev.hardwood.internal.EncryptedFileException;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.FileMetaDataReader.ReadFooter;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParsedFooter;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.StaleMetadataException;

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

    /// Named once because two places raise it: the magic-byte check here, and the
    /// `encryption_algorithm` field a plaintext footer carries.
    public static final String ENCRYPTED_MESSAGE =
            "Encrypted Parquet files are not supported (Parquet Modular Encryption)";

    private ParquetMetadataReader() {
        // Utility class
    }

    /// Reads file metadata from an [InputFile].
    ///
    /// @param inputFile the input file to read metadata from
    /// @return the parsed FileMetaData
    /// @throws IOException if the file cannot be read
    /// @throws ParquetReadException if what it holds is not a Parquet file
    public static FileMetaData readMetadata(InputFile inputFile) throws IOException {
        return readFooter(inputFile).metaData();
    }

    /// Reads the footer of an [InputFile].
    ///
    /// @param inputFile the input file to read the footer from
    /// @return the parsed footer
    /// @throws IOException if the file cannot be read
    /// @throws ParquetReadException if what it holds is not a Parquet file
    public static ReadFooter readFooter(InputFile inputFile) throws IOException {
        long fileSize = inputFile.length();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
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
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
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
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "Not a Parquet file (invalid magic number at end)");
        }

        // Validate footer length
        long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
        if (footerStart < MAGIC_SIZE) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "Invalid footer length: " + footerLength);
        }

        // Parse file metadata
        ByteBuffer footerBuffer;
        try (FetchReason.Scope ignored = FetchReason.set("footer-body")) {
            footerBuffer = inputFile.readRange(footerStart, footerLength);
        }
        ThriftCompactReader reader = new ThriftCompactReader(footerBuffer);
        try {
            return FileMetaDataReader.readFooter(reader);
        }
        catch (EncryptedFileException e) {
            // Plaintext-footer encryption: the footer parsed, but the data is
            // encrypted. Re-throw with file context for an attributable error.
            throw encrypted(inputFile);
        }
        catch (ParquetReadException e) {
            // Negative sizes, an unknown field type, a struct that ends early —
            // the reader already types all of them as the file being wrong. What
            // it cannot name is the file, which only this frame knows.
            throw new ParquetReadException(
                    ExceptionContext.filePrefix(inputFile.name()) + e.getMessage(), e);
        }
    }

    /// An encrypted file is a correct file this library does not read, which is
    /// what [UnsupportedOperationException] says everywhere else the reader meets
    /// something it has not implemented — an absent codec library, an encoding it
    /// does not decode.
    private static UnsupportedOperationException encrypted(InputFile inputFile) {
        return new UnsupportedOperationException(
                ExceptionContext.filePrefix(inputFile.name()) + ENCRYPTED_MESSAGE);
    }

    /// Reads file metadata and the serialised Thrift footer length from an [InputFile].
    ///
    /// Delegates to [#readFooter(InputFile)] for all validation and parsing, then
    /// re-reads the 4-byte footer length from the trailer (warm in the page cache
    /// after readFooter). Used by [dev.hardwood.reader.ParsedFooter#readFrom(InputFile)].
    public static MetadataWithLength readMetadataWithLength(InputFile inputFile) throws IOException {
        // Parse the full footer (validates magic, trailer, and footer length).
        ReadFooter footer = readFooter(inputFile);
        // Derive the Thrift footer length from the trailer. readFooter already validated
        // it; this re-reads the same 4 bytes from the trailer, which are warm in the
        // page cache on any mapped file. Avoids changing ReadFooter's public API to
        // carry the length directly.
        long fileSize = inputFile.length();
        long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer lenBuf;
        try (FetchReason.Scope ignored = FetchReason.set("footer-length")) {
            lenBuf = inputFile.readRange(footerInfoPos, FOOTER_LENGTH_SIZE);
        }
        lenBuf.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = lenBuf.getInt();
        return new MetadataWithLength(footer, footerLength);
    }

    /// Reads the 8-byte trailer of an already-opened [InputFile] and validates it.
    ///
    /// Checks both the start and end Parquet magic, that the file has not been
    /// replaced with an encrypted file, and that the recorded footer length matches
    /// the trailer. Call this from a [dev.hardwood.reader.MetadataSource] branch to provide
    /// lightweight structural validation even when the full footer is not re-parsed.
    ///
    /// The check is ~8 bytes of I/O on a file whose data pages will be read anyway;
    /// on a warm page cache it is effectively free.
    ///
    /// @param inputFile     an [InputFile] on which [dev.hardwood.InputFile#open()] has been called
    /// @param parsedFooter  the cached footer to validate against this file
    /// @throws dev.hardwood.reader.StaleMetadataException if the trailer indicates the file has changed
    /// @throws dev.hardwood.reader.ParquetReadException if the file is not a valid Parquet file
    public static void validateTrailer(InputFile inputFile,
                                       ParsedFooter parsedFooter) throws IOException {
        long fileSize = inputFile.length();
        // Structural guard first: a malformed or truncated file is a read error,
        // not a staleness signal, and must not send the caller into a cache-refresh
        // retry loop.
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "File too small to be a valid Parquet file");
        }
        // Size mismatch after the structural guard: the file is a plausible Parquet
        // file but differs from what the cached footer describes.
        if (parsedFooter.fileLength() != fileSize) {
            throw new StaleMetadataException(
                    parsedFooter.sourceIdentity().orElse(null),
                    "Cached footer file length " + parsedFooter.fileLength()
                            + " does not match actual file length " + fileSize
                            + " for " + inputFile.name());
        }
        // Check start magic. readFooter() checks both ends; validateTrailer also
        // checks the start so that a same-length non-Parquet file does not slip
        // through on the end-magic check alone.
        ByteBuffer startMagicBuf;
        try (FetchReason.Scope ignored = FetchReason.set("trailer-validate-start")) {
            startMagicBuf = inputFile.readRange(0, MAGIC_SIZE);
        }
        byte[] startMagic = new byte[MAGIC_SIZE];
        startMagicBuf.get(startMagic);
        if (Arrays.equals(startMagic, ENCRYPTED_MAGIC)) {
            throw new UnsupportedOperationException(
                    ExceptionContext.filePrefix(inputFile.name()) + ENCRYPTED_MESSAGE);
        }
        if (!Arrays.equals(startMagic, MAGIC)) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "Not a Parquet file (invalid magic number at start)");
        }
        long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer trailerBuf;
        try (FetchReason.Scope ignored = FetchReason.set("trailer-validate")) {
            trailerBuf = inputFile.readRange(footerInfoPos, FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        }
        trailerBuf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int trailerFooterLength = trailerBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        trailerBuf.get(endMagic);
        if (java.util.Arrays.equals(endMagic, ENCRYPTED_MAGIC)) {
            throw new UnsupportedOperationException(
                    ExceptionContext.filePrefix(inputFile.name()) + ENCRYPTED_MESSAGE);
        }
        if (!java.util.Arrays.equals(endMagic, MAGIC)) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "Not a Parquet file (invalid magic number at end)");
        }
        if (trailerFooterLength != (int) parsedFooter.footerLength()) {
            throw new StaleMetadataException(
                    parsedFooter.sourceIdentity().orElse(null),
                    "Cached footer length " + parsedFooter.footerLength()
                            + " does not match trailer footer length " + trailerFooterLength
                            + " for " + inputFile.name());
        }
    }

    /// Result type for [#readMetadataWithLength(InputFile)].
    public record MetadataWithLength(ReadFooter readFooter, int footerLength) {}
}
