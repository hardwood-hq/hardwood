/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

import dev.hardwood.AadPrefixProvider;
import dev.hardwood.DecryptionKeyProvider;
import dev.hardwood.ParquetEncryptionException;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnCryptoMetaData;
import dev.hardwood.metadata.EncryptionAlgorithm;
import dev.hardwood.metadata.EncryptionStrategy;
import dev.hardwood.metadata.FileMetaData;

/// Decryptor for a single column chunk's pages.
///
/// Holds all column-chunk-level crypto context (key, AAD ingredients, ordinals)
/// and exposes simple decrypt methods per page. One instance is created per column
/// chunk (i.e. per column per row group) and reused across all pages of that chunk.
///
/// Lifecycle:
/// <pre>
/// for each RowGroup:
///     for each ColumnChunk:
///         ColumnDecryptor decryptor = new ColumnDecryptor(...)
///         // dictionary page (optional, always first if present)
///         if hasDictionaryPage:
///             decryptor.decryptDictPageHeader(buf)
///             decryptor.decryptDictPageData(buf)
///         // data pages
///         int pageOrdinal = 0
///         for each DataPage:
///             decryptor.decryptPageHeader(buf, pageOrdinal)
///             decryptor.decryptPageData(buf, pageOrdinal)
///             pageOrdinal++
/// </pre>
///
/// Thread safety: instances are immutable and safe for concurrent use.
/// AAD is computed fresh on each call from the stored immutable context
/// plus the pageOrdinal passed by the caller — no state is mutated.
/// It is never stored in the file - see {@link ParquetCryptoHelper} for the full AAD structure.
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Encryption.md">Parquet Encryption Spec</a>
public class ColumnDecryptor {

    private final byte[] key;
    private final byte[] aadPrefix;
    private final byte[] aadFileUnique;
    private final int rowGroupOrdinal;
    private final int columnOrdinal;
    private final boolean useCtr;

    /// Creates a new ColumnDecryptor for a single column chunk.
    ///
    /// @param key resolved AES key for this column (footer key or column key)
    /// @param aadPrefix caller-supplied AAD prefix, or null if absent
    /// @param aadFileUnique per-file unique bytes from EncryptionStrategy
    /// @param rowGroupOrdinal 0-based row group index
    /// @param columnOrdinal 0-based column index within the row group
    /// @param useCtr true if file uses AES_GCM_CTR_V1 (page data uses CTR),
    ///               false if AES_GCM_V1 (page data uses GCM)
    public ColumnDecryptor(byte[] key, byte[] aadPrefix, byte[] aadFileUnique,
                           int rowGroupOrdinal, int columnOrdinal, boolean useCtr) {
        this.key = key;
        this.aadPrefix = aadPrefix;
        this.aadFileUnique = aadFileUnique;
        this.rowGroupOrdinal = rowGroupOrdinal;
        this.columnOrdinal = columnOrdinal;
        this.useCtr = useCtr;
    }

    /// Decrypts a data page header (always AES-GCM).
    ///
    /// @param buf encrypted buffer:
    /// <pre>[ 4-byte length | 12-byte nonce | ciphertext | 16-byte tag ]</pre>
    /// @param pageOrdinal 0-based page index within this column chunk
    /// @return decrypted page header bytes
    /// @throws ParquetEncryptionException if decryption or authentication fails
    public ByteBuffer decryptPageHeader(ByteBuffer buf, int pageOrdinal) throws ParquetEncryptionException {
        byte[] aad = ParquetCryptoHelper.buildPageAad(
                aadPrefix, aadFileUnique,
                ParquetModuleType.DATA_PAGE_HEADER,
                rowGroupOrdinal, columnOrdinal, pageOrdinal);
        return ParquetCryptoHelper.decryptGcm(buf, key, aad);
    }

    /// Decrypts a dictionary page header (always AES-GCM, no page ordinal).
    ///
    /// @param buf encrypted buffer:
    /// <pre>[ 4-byte length | 12-byte nonce | ciphertext | 16-byte tag ]</pre>
    /// @return decrypted dictionary page header bytes
    /// @throws ParquetEncryptionException if decryption or authentication fails
    public ByteBuffer decryptDictPageHeader(ByteBuffer buf) throws ParquetEncryptionException {
        byte[] aad = ParquetCryptoHelper.buildPageAad(
                aadPrefix, aadFileUnique,
                ParquetModuleType.DICT_PAGE_HEADER,
                rowGroupOrdinal, columnOrdinal, -1);
        return ParquetCryptoHelper.decryptGcm(buf, key, aad);
    }

    /// Decrypts data page data.
    ///
    /// Uses AES-CTR if the file uses AES_GCM_CTR_V1, otherwise AES-GCM.
    ///
    /// @param buf encrypted buffer:
    /// <pre>
    /// AES-GCM: [ 4-byte length | 12-byte nonce | ciphertext | 16-byte GCM tag ]
    /// AES-CTR: [ 4-byte length | 12-byte nonce | ciphertext ]
    /// </pre>
    /// @param pageOrdinal 0-based page index within this column chunk
    /// @return decrypted page data bytes
    /// @throws ParquetEncryptionException if decryption fails
    public ByteBuffer decryptPageData(ByteBuffer buf, int pageOrdinal) throws ParquetEncryptionException {
        if (useCtr) {
            return ParquetCryptoHelper.decryptCtr(buf, key);
        }
        byte[] aad = ParquetCryptoHelper.buildPageAad(
                aadPrefix, aadFileUnique,
                ParquetModuleType.DATA_PAGE,
                rowGroupOrdinal, columnOrdinal, pageOrdinal);
        return ParquetCryptoHelper.decryptGcm(buf, key, aad);
    }

    /// Decrypts dictionary page data (always AES-GCM, no page ordinal).
    ///
    /// @param buf encrypted buffer:
    /// <pre>[ 4-byte length | 12-byte nonce | ciphertext | 16-byte tag ]</pre>
    /// @return decrypted dictionary page data bytes
    /// @throws ParquetEncryptionException if decryption fails
    public ByteBuffer decryptDictPageData(ByteBuffer buf) throws ParquetEncryptionException {
        if (useCtr) {
            return ParquetCryptoHelper.decryptCtr(buf, key);
        }
        byte[] aad = ParquetCryptoHelper.buildPageAad(
                aadPrefix, aadFileUnique,
                ParquetModuleType.DICT_PAGE,
                rowGroupOrdinal, columnOrdinal, -1);
        return ParquetCryptoHelper.decryptGcm(buf, key, aad);
    }

    /// Creates a ColumnDecryptor for a single column chunk, resolving the correct key
    /// (footer key or column key) and AAD ingredients from the file metadata.
    ///
    /// Returns null if the file is not encrypted.
    ///
    /// @param fileMetaData file metadata containing encryption algorithm and footer key metadata
    /// @param columnChunk column chunk metadata containing optional column crypto metadata
    /// @param rowGroupOrdinal 0-based row group index
    /// @param columnOrdinal 0-based column index within the row group
    /// @param keyProvider provider for resolving AES keys from key metadata
    /// @param aadPrefixProvider provider for AAD prefix if required by the file, or null
    /// @return a ColumnDecryptor for this column chunk, or null if not encrypted
    /// @throws ParquetEncryptionException if key resolution or AAD prefix retrieval fails
    public static ColumnDecryptor forColumnChunk(
            FileMetaData fileMetaData,
            ColumnChunk columnChunk,
            int rowGroupOrdinal,
            int columnOrdinal,
            DecryptionKeyProvider keyProvider,
            AadPrefixProvider aadPrefixProvider) throws ParquetEncryptionException {
        // Not encrypted
        if (fileMetaData == null || fileMetaData.encryptionAlgorithm() == null) {
            return null;
        }

        // Column is not encrypted - no crypto metadata means plaintext column
        if (columnChunk.cryptoMetadata() == null) {
            return null;
        }

        EncryptionAlgorithm algo = fileMetaData.encryptionAlgorithm();
        boolean useCtr = algo.aesGcmCtrV1() != null;
        EncryptionStrategy strategy = useCtr ? algo.aesGcmCtrV1() : algo.aesGcmV1();

        // Resolve AAD prefix
        byte[] aadPrefix;
        if (strategy.supplyAadPrefix() && aadPrefixProvider != null) {
            aadPrefix = aadPrefixProvider.provideAadPrefix();
            if (aadPrefix == null) {
                throw new ParquetEncryptionException("AadPrefixProvider returned null AAD prefix");
            }
        } else {
            aadPrefix = strategy.aadPrefix();
        }

        // Get hold of key(column key takes precedence over footer key)
        byte[] key;
        ColumnCryptoMetaData cryptoMetaData = columnChunk.cryptoMetadata();
        if (cryptoMetaData.columnKey() != null) {
            byte[] keyMetadata = cryptoMetaData.columnKey().keyMetadata();
            key = keyProvider.provideKey(keyMetadata);
        } else if (cryptoMetaData.footerKey() != null) {
            byte[] keyMetadata = fileMetaData.footerSigningKeyMetadata();
            key = keyProvider.provideKey(keyMetadata);
        } else {
            throw new ParquetEncryptionException(
                    "ColumnCryptoMetaData has neither columnKey nor footerKey set for column "
                            + columnOrdinal + " in row group " + rowGroupOrdinal + " — file may be corrupt");
        }
        // check for key error scenarios(null/empty/wrong length)
        if (key == null || key.length == 0) {
            throw new ParquetEncryptionException(
                    "DecryptionKeyProvider returned null or empty key for column " + columnOrdinal
                            + " in row group " + rowGroupOrdinal);
        }
        if (key.length != 16 && key.length != 24 && key.length != 32) {
            throw new ParquetEncryptionException(
                    "DecryptionKeyProvider returned invalid key length " + key.length
                            + " for column " + columnOrdinal + " — must be 16, 24, or 32 bytes");
        }

        return new ColumnDecryptor(key, aadPrefix, strategy.aadFileUnique(),
                rowGroupOrdinal, columnOrdinal, useCtr);
    }

    /// Decrypts the offset index.
    ///
    /// The offset index is encrypted with the column's key using AES-GCM.
    /// The AAD is built using OFFSET_INDEX module type, row group ordinal,
    /// and column ordinal, ensuring the index can only be decrypted in the
    /// correct column/row-group context, preventing index swapping attacks.
    ///
    /// @param buf buffer containing the encrypted offset index blob
    /// @return plaintext offset index bytes ready for Thrift parsing
    /// @throws ParquetEncryptionException if decryption or GCM authentication fails
    public ByteBuffer decryptOffsetIndex(ByteBuffer buf) throws ParquetEncryptionException {
        byte[] aad = ParquetCryptoHelper.buildPageAad(
                aadPrefix, aadFileUnique,
                ParquetModuleType.OFFSET_INDEX,
                rowGroupOrdinal, columnOrdinal, -1);
        return ParquetCryptoHelper.decryptGcm(buf, key, aad);
    }
}
