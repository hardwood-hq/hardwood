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
import dev.hardwood.internal.ExceptionContext.ReadContext.Region;
import dev.hardwood.internal.ReadScope;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetReadException;

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
        try (ReadScope.Scope file = ReadScope.file(inputFile.name())) {
            long fileSize = inputFile.length();
            if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
                throw new ParquetReadException(
                        "File too small to be a valid Parquet file (" + fileSize + " bytes)");
            }

            try (ReadScope.Scope magic = ReadScope.region(Region.MAGIC, 0)) {
                requireMagic(inputFile.readRange(0, MAGIC_SIZE), "at start");
            }

            long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
            int footerLength;
            try (ReadScope.Scope length = ReadScope.region(Region.FOOTER_LENGTH, footerInfoPos)) {
                footerLength = readFooterLength(inputFile, footerInfoPos, fileSize);
            }

            long footerStart = footerInfoPos - footerLength;
            try (ReadScope.Scope footer = ReadScope.region(Region.FOOTER, footerStart)) {
                return parseFooter(inputFile, footerStart, footerLength);
            }
        }
    }

    /// The declared footer length, checked against the space there is for it.
    ///
    /// The closing magic rides in the same read — four bytes in front of it —
    /// so it is checked here, in its own region: a file whose last four bytes
    /// are not `PAR1` is not a footer that is the wrong length, it is not a
    /// Parquet file.
    private static int readFooterLength(InputFile inputFile, long footerInfoPos, long fileSize)
            throws IOException {
        ByteBuffer footerInfo = inputFile.readRange(footerInfoPos, FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        footerInfo.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = footerInfo.getInt();

        byte[] endMagic = new byte[MAGIC_SIZE];
        footerInfo.get(endMagic);
        try (ReadScope.Scope magic = ReadScope.region(Region.MAGIC, fileSize - MAGIC_SIZE)) {
            requireMagic(endMagic, "at end");
        }

        if (footerInfoPos - footerLength < MAGIC_SIZE) {
            throw new ParquetReadException(footerLength
                    + " would start the footer in front of the file's opening magic");
        }
        return footerLength;
    }

    /// Parses the footer, whose failures are the file's.
    ///
    /// The one catch says what a failure *is* rather than where it was: a
    /// plaintext footer that parses and then declares its columns encrypted is
    /// a correct file this library does not read, and it must not leave as a
    /// malformed one.
    private static FileMetaData parseFooter(InputFile inputFile, long footerStart, int footerLength)
            throws IOException {
        ByteBuffer footer = inputFile.readRange(footerStart, footerLength);
        try {
            return FileMetaDataReader.read(new ThriftCompactReader(footer));
        }
        catch (EncryptedFileException e) {
            throw encrypted();
        }
    }

    /// Fails unless `PAR1` is where it has to be.
    ///
    /// `PARE` in either position is the encrypted-footer layout, which is a
    /// correct file this library does not read rather than a broken one.
    private static void requireMagic(ByteBuffer buffer, String where) {
        byte[] magic = new byte[MAGIC_SIZE];
        buffer.get(magic);
        requireMagic(magic, where);
    }

    private static void requireMagic(byte[] magic, String where) {
        if (Arrays.equals(magic, ENCRYPTED_MAGIC)) {
            throw encrypted();
        }
        if (!Arrays.equals(magic, MAGIC)) {
            throw new ParquetReadException("Not a Parquet file (invalid magic number " + where + ")");
        }
    }

    /// An encrypted file is a correct file this library does not read, which is
    /// what [UnsupportedOperationException] says everywhere else the reader meets
    /// something it has not implemented — an absent codec library, an encoding it
    /// does not decode.
    private static UnsupportedOperationException encrypted() {
        String fileName = ReadScope.current() == null ? null : ReadScope.current().fileName();
        return new UnsupportedOperationException(
                ExceptionContext.filePrefix(fileName) + ENCRYPTED_MESSAGE);
    }

}
