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
import dev.hardwood.ParquetEncryptionException;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.thrift.FileCryptoMetaDataReader;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.EncryptionStrategy;
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
    /// @param keyProvider decryption key provider
    /// @param aadPrefixProvider AAD refix provider
    /// @return the parsed FileMetaData
    /// @throws IOException if the file is not a valid Parquet file
    public static FileMetaData readMetadata(InputFile inputFile, DecryptionKeyProvider keyProvider,
                                            AadPrefixProvider aadPrefixProvider) throws IOException {
        long fileSize = inputFile.length();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new IOException(ExceptionContext.filePrefix(inputFile.name())
                    + "File too small to be a valid Parquet file");
        }

        // Validate magic number at start - accept both PAR1 and PARE
        ByteBuffer startMagicBuf = inputFile.readRange(0, MAGIC_SIZE);
        byte[] startMagic = new byte[MAGIC_SIZE];
        startMagicBuf.get(startMagic);
        if (!Arrays.equals(startMagic, MAGIC) && !Arrays.equals(startMagic, MAGIC_ENCRYPTED)) {
            throw new IOException(ExceptionContext.filePrefix(inputFile.name())
                    + "Not a Parquet file (invalid magic number at start)");
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
            // PAR1 - it's a plaintext footer, so we parse it directly
            ThriftCompactReader reader = new ThriftCompactReader(chunkBuf);

            return FileMetaDataReader.read(reader, keyProvider, null);
        }
        else if (Arrays.equals(endMagic, MAGIC_ENCRYPTED)) {
            // PARE - read FileCryptoMetaData first, remaining bytes are encrypted FileMetaData

            // check key provider is not null
            if(keyProvider == null) {
                throw new ParquetEncryptionException(
                        ExceptionContext.filePrefix(inputFile.name())
                                + "File has encrypted footer but key provider not supplied");
            }
            ThriftCompactReader cryptoReader = new ThriftCompactReader(chunkBuf);
            FileCryptoMetaData cryptoMetaData = FileCryptoMetaDataReader.read(cryptoReader);
            chunkBuf.position(cryptoReader.getBytesRead());  // advance past FileCryptoMetaData bytes
            ByteBuffer encryptedFooterBuf = chunkBuf.slice();

            EncryptionStrategy encStrategy = cryptoMetaData.encryptionAlgorithm().activeEncryptionStrategy();
            byte[] aadPrefix;
            if (encStrategy.supplyAadPrefix()) {
                // prefix is not stored in file, hence caller needs to supply it
                if (aadPrefixProvider == null) {
                    throw new ParquetEncryptionException(
                            ExceptionContext.filePrefix(inputFile.name())
                                    + "File decryption requires user-supplied AAD prefix but no AadPrefixProvider was supplied");
                }
                aadPrefix = aadPrefixProvider.provideAadPrefix();
                if (aadPrefix == null || aadPrefix.length == 0) {
                    throw new ParquetEncryptionException(
                            ExceptionContext.filePrefix(inputFile.name())
                                    + "AadPrefixProvider returned null or empty AAD prefix");
                }
            }
            else {
                // prefix stored in file (or no prefix used) — read directly, aadPrefixProvider ignored
                aadPrefix = encStrategy.aadPrefix();
            }

            byte[] aad = ParquetCryptoHelper.buildFooterAad(aadPrefix, encStrategy.aadFileUnique());
            ByteBuffer decryptedFooterBuf;
            byte[] footerKey = keyProvider.provideKey(cryptoMetaData.keyMetadata());
            if (footerKey == null || footerKey.length == 0) {
                throw new ParquetEncryptionException(
                        ExceptionContext.filePrefix(inputFile.name())
                                + "DecryptionKeyProvider returned null or empty key for footer");
            }
            try {
                decryptedFooterBuf = ParquetCryptoHelper.decryptGcm(encryptedFooterBuf, footerKey, aad);
            }
            catch (ParquetEncryptionException e) {
                throw new ParquetEncryptionException(inputFile.name(), e);
            }

            ThriftCompactReader footerReader = new ThriftCompactReader(decryptedFooterBuf);
            FileMetaData fileMetaData = FileMetaDataReader.read(footerReader, keyProvider, cryptoMetaData.encryptionAlgorithm());

            // For PARE, we use crypto metadata from FileCryptoMetaData for decrypting column data
            return fileMetaData.withCrypto(cryptoMetaData.encryptionAlgorithm(), cryptoMetaData.keyMetadata());
        }
        else {
            throw new IOException("Not a Parquet file (invalid magic number at end): " + inputFile.name());
        }
    }
}
