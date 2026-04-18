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

import dev.hardwood.AadPrefixProvider;
import dev.hardwood.DecryptionKeyProvider;
import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.FileCryptoMetaDataReader;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.EncryptionAlgorithm;
import dev.hardwood.metadata.FileCryptoMetaData;
import dev.hardwood.metadata.FileMetaData;

/// Utility class for reading Parquet file metadata from an [InputFile].
///
/// This centralizes the metadata reading logic used by ParquetFileReader,
/// MultiFileRowReader, and FileManager.
public final class ParquetMetadataReader {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MAGIC_ENCRYPTED = "PARE".getBytes(StandardCharsets.UTF_8);
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
    public static FileMetaData readMetadata1(InputFile inputFile) throws IOException {
        long fileSize = inputFile.length();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new IOException("File too small to be a valid Parquet file: " + inputFile.name());
        }

        // Validate magic number at start
        ByteBuffer startMagicBuf = inputFile.readRange(0, MAGIC_SIZE);
        byte[] startMagic = new byte[MAGIC_SIZE];
        startMagicBuf.get(startMagic);
        if (!Arrays.equals(startMagic, MAGIC) && !Arrays.equals(startMagic, MAGIC_ENCRYPTED)) {
            throw new IOException("Not a Parquet file (invalid magic number at start): " + inputFile.name());
        }

        // Read footer size and magic number at end
        long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer footerInfoBuf = inputFile.readRange(footerInfoPos, FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        footerInfoBuf.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = footerInfoBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        footerInfoBuf.get(endMagic);
        if (!Arrays.equals(endMagic, MAGIC)) {
            throw new IOException("Not a Parquet file (invalid magic number at end): " + inputFile.name());
        }

        // Validate footer length
        long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
        if (footerStart < MAGIC_SIZE) {
            throw new IOException("Invalid footer length: " + footerLength);
        }

        // Parse file metadata
        ByteBuffer footerBuffer = inputFile.readRange(footerStart, footerLength);
        ThriftCompactReader reader = new ThriftCompactReader(footerBuffer);
        return FileMetaDataReader.read(reader);
    }

    public static FileMetaData readMetadata(InputFile inputFile, DecryptionKeyProvider keyProvider,
                                                     AadPrefixProvider aadPrefixProvider) throws IOException {
        long fileSize = inputFile.length();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new IOException("File too small to be a valid Parquet file: " + inputFile.name());
        }

        // Validate magic number at start - accept both PAR1 and PARE
        ByteBuffer startMagicBuf = inputFile.readRange(0, MAGIC_SIZE);
        byte[] startMagic = new byte[MAGIC_SIZE];
        startMagicBuf.get(startMagic);
        if (!Arrays.equals(startMagic, MAGIC) && !Arrays.equals(startMagic, MAGIC_ENCRYPTED)) {
            throw new IOException("Not a Parquet file (invalid magic number at start): " + inputFile.name());
        }

        // Read combined/footer length and end magic
        long tailPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer tailBuf = inputFile.readRange(tailPos, FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        tailBuf.order(ByteOrder.LITTLE_ENDIAN);
        int combinedLength = tailBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        tailBuf.get(endMagic);

        // Calculate start position of footer/combined chunk
        long chunkStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - combinedLength;
        if (chunkStart < MAGIC_SIZE) {
            throw new IOException("Invalid footer length: " + combinedLength);
        }

        ByteBuffer chunkBuf = inputFile.readRange(chunkStart, combinedLength);

        if (Arrays.equals(endMagic, MAGIC)) {
            // PAR1 - plaintext footer, parse directly
            ThriftCompactReader reader = new ThriftCompactReader(chunkBuf);

            return FileMetaDataReader.read(reader);
        }
        else if (Arrays.equals(endMagic, MAGIC_ENCRYPTED)) {
            // PARE - read FileCryptoMetaData first, remaining bytes are encrypted FileMetaData
            ThriftCompactReader cryptoReader = new ThriftCompactReader(chunkBuf);
            FileCryptoMetaData cryptoMetaData = FileCryptoMetaDataReader.read(cryptoReader);
            ByteBuffer encryptedFooterBuf = chunkBuf.slice();

            EncryptionAlgorithm algo = cryptoMetaData.encryptionAlgorithm();
            byte[] aadPrefix;
            if (algo.supplyAadPrefix() && algo.aadPrefix() == null) {
                if (aadPrefixProvider == null) {
                    throw new IllegalArgumentException(
                            "File requires AAD prefix to be supplied by caller for decryption");
                }
                aadPrefix = aadPrefixProvider.provideAadPrefix();
                if (aadPrefix == null) {
                    throw new IllegalArgumentException(
                            "AAD prefix provider failed to provide AAD prefix for decryption");
                }
            } else {
                aadPrefix = algo.aadPrefix(); // may be null, that's fine
            }

            byte[] aad = ParquetCryptoHelper.buildFooterAad(aadPrefix, algo.aadFileUnique());
            ByteBuffer decryptedFooterBuf = ParquetCryptoHelper.decrypt(encryptedFooterBuf,
                    keyProvider.provideKey(cryptoMetaData.keyMetadata()), aad);
            ThriftCompactReader footerReader = new ThriftCompactReader(decryptedFooterBuf);
            FileMetaData fileMetaData = FileMetaDataReader.read(footerReader);

            // For PARE, we use crypto metadata in FileCryptoMetaData for decrypting column data
            return fileMetaData.withCrypto(cryptoMetaData.encryptionAlgorithm(), cryptoMetaData.keyMetadata());
        }
        else {
            throw new IOException("Not a Parquet file (invalid magic number at end): " + inputFile.name());
        }
    }
}
