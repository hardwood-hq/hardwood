/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import dev.hardwood.ParquetEncryptionException;

/// Utility class for Parquet decryption operations.
///
/// Parquet uses two AES cipher modes:
/// - AES-GCM: authenticated encryption, used for all modules except page data in AES_GCM_CTR_V1
/// - AES-CTR: unauthenticated encryption, used for page data only in AES_GCM_CTR_V1
///
/// Every encrypted module in a Parquet file is stored as:
/// <pre>
/// +----------+------------+---------------------------+----------+
/// | length   |   nonce    |        ciphertext         |   tag    |
/// | (4 bytes)| (12 bytes) |      (variable)           |(16 bytes)|
/// +----------+------------+---------------------------+----------+
///  little-    plaintext    encrypted data               GCM only,
///  endian                                               absent in CTR
/// </pre>
///
/// AAD (Additional Authenticated Data) is never stored in the file - it is
/// reconstructed by the reader from known context and used to verify integrity.
/// AAD structure:
/// <pre>
/// +------------------+----------------+------------+------------------+----------------+-------------+
/// |   aadPrefix      | aadFileUnique  | moduleType | rowGroupOrdinal  | columnOrdinal  | pageOrdinal |
/// | (optional,       |  (N bytes,     |  (1 byte)  | (2 bytes, LE,    | (2 bytes, LE,  | (2 bytes,   |
/// |  caller-supplied)|  from file)    |            |  excl. footer)   |  excl. footer) |  LE, pages  |
/// |                  |                |            |                  |                |  only)      |
/// +------------------+----------------+------------+------------------+----------------+-------------+
/// </pre>
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Encryption.md">Parquet Encryption Spec</a>
public final class ParquetCryptoHelper {

    private static final int NONCE_LENGTH = 12;
    private static final int TAG_LENGTH_BITS = 128;

    private ParquetCryptoHelper() {
        // Utility class
    }

    /// Holds the nonce and ciphertext extracted from an encrypted buffer.
    private record CipherInput(byte[] nonce, byte[] ciphertext) {}

    /// Reads nonce and ciphertext from an encrypted Parquet module buffer.
    ///
    /// Every encrypted module is stored as:
    /// <pre>[ 4-byte length (LE) | 12-byte nonce | ciphertext (+ 16-byte GCM tag if GCM) ]</pre>
    ///
    /// The 4-byte length field (nonce + ciphertext length) is used to read exactly the right
    /// number of ciphertext bytes, allowing multiple encrypted blobs to be read sequentially
    /// from the same buffer (e.g. encrypted page header followed by encrypted page data).
    private static CipherInput readCipherInput(ByteBuffer encryptedBuf) {
        encryptedBuf.order(ByteOrder.LITTLE_ENDIAN);
        int length = encryptedBuf.getInt(); // total length of nonce + ciphertext
        byte[] nonce = new byte[NONCE_LENGTH];
        encryptedBuf.get(nonce);
        int ciphertextLen = length - NONCE_LENGTH; // exact bytes belonging to this blob
        byte[] ciphertext = new byte[ciphertextLen];
        encryptedBuf.get(ciphertext);

        return new CipherInput(nonce, ciphertext);
    }

    /// Decrypts an AES-GCM encrypted Parquet module.
    ///
    /// Used for: footer, page headers, column metadata, and page data in AES_GCM_V1 files.
    /// GCM is authenticated - if the AAD does not match what the writer used, decryption fails.
    ///
    /// @param encryptedBuf buffer containing the encrypted module bytes:
    /// <pre>[ 4-byte length | 12-byte nonce | ciphertext ]</pre>
    /// When multiple blobs are present in the buffer (e.g. page header followed by page data),
    /// the buffer position advances past this blob after the call.
    /// @param key AES key bytes (128, 192, or 256 bits)
    /// @param aad additional authenticated data, or null if not used
    /// @return decrypted plaintext as a ByteBuffer
    /// @throws ParquetEncryptionException if decryption fails or authentication tag does not match
    public static ByteBuffer decryptGcm(ByteBuffer encryptedBuf, byte[] key, byte[] aad) throws ParquetEncryptionException {
        CipherInput input = readCipherInput(encryptedBuf);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(TAG_LENGTH_BITS, input.nonce()));
            if (aad != null) {
                cipher.updateAAD(aad);
            }

            return ByteBuffer.wrap(cipher.doFinal(input.ciphertext()));
        }
        catch (GeneralSecurityException e) {
            throw new ParquetEncryptionException("Failed to decrypt data (GCM)", e);
        }
    }

    /// Decrypts an AES-CTR encrypted Parquet page.
    ///
    /// Used for: page data only, in AES_GCM_CTR_V1 files.
    /// CTR is not authenticated - no AAD, no integrity check.
    ///
    /// The CTR IV is 16 bytes: [ 12-byte nonce | 0x00 0x00 0x00 0x01 ]
    /// The initial counter value is 1 (first 31 bits = 0, last bit = 1) per Parquet spec section 4.2.2.
    ///
    /// <pre>
    /// +------------------+------------------------------+
    /// |    nonce         |     initial counter          |
    /// |   (12 bytes)     |  0x00 | 0x00 | 0x00 | 0x01  |
    /// +------------------+------------------------------+
    ///  from encrypted buf  always this value per spec
    /// </pre>
    ///
    /// @param encryptedBuf buffer containing the encrypted module bytes:
    /// <pre>[ 4-byte length | 12-byte nonce | ciphertext ]</pre>
    /// When multiple blobs are present in the buffer (e.g. page header followed by page data),
    /// the buffer position advances past this blob after the call.
    /// @param key AES key bytes (128, 192, or 256 bits)
    /// @return decrypted plaintext as a ByteBuffer
    /// @throws ParquetEncryptionException if decryption fails
    public static ByteBuffer decryptCtr(ByteBuffer encryptedBuf, byte[] key) throws ParquetEncryptionException {
        CipherInput input = readCipherInput(encryptedBuf);
        try {
            byte[] iv = new byte[16];
            System.arraycopy(input.nonce(), 0, iv, 0, NONCE_LENGTH);
            iv[15] = 1; // counter starts at 1 per Parquet spec section 4.2.2
            Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(key, "AES"),
                    new IvParameterSpec(iv));

            return ByteBuffer.wrap(cipher.doFinal(input.ciphertext()));
        }
        catch (GeneralSecurityException e) {
            throw new ParquetEncryptionException("Failed to decrypt data (CTR)", e);
        }
    }

    /// Builds the AAD for the file footer module.
    ///
    /// Footer AAD structure:
    /// <pre>
    /// +------------------+----------------+------------+
    /// |   aadPrefix      | aadFileUnique  | moduleType |
    /// | (optional)       |  (N bytes)     |  (0x00)    |
    /// +------------------+----------------+------------+
    /// </pre>
    ///
    /// @param aadPrefix caller-supplied prefix, or null if absent
    /// @param aadFileUnique per-file unique bytes from FileCryptoMetaData
    /// @return AAD byte array for footer decryption
    public static byte[] buildFooterAad(byte[] aadPrefix, byte[] aadFileUnique) {
        int prefixLen = aadPrefix != null ? aadPrefix.length : 0;
        int uniqueLen = aadFileUnique != null ? aadFileUnique.length : 0;
        byte[] aad = new byte[prefixLen + uniqueLen + 1];
        int pos = 0;
        if (aadPrefix != null) {
            System.arraycopy(aadPrefix, 0, aad, pos, prefixLen);
            pos += prefixLen;
        }
        if (aadFileUnique != null) {
            System.arraycopy(aadFileUnique, 0, aad, pos, uniqueLen);
            pos += uniqueLen;
        }
        aad[pos] = ParquetModuleType.FOOTER;

        return aad;
    }

    /// Builds the AAD for a page module (data page, dict page, page headers).
    ///
    /// Page AAD structure:
    /// <pre>
    /// +------------------+----------------+------------+-----------------+---------------+-------------+
    /// |   aadPrefix      | aadFileUnique  | moduleType | rowGroupOrdinal | columnOrdinal | pageOrdinal |
    /// | (optional)       |  (N bytes)     |  (1 byte)  | (2 bytes, LE)   | (2 bytes, LE) | (2 bytes,   |
    /// |                  |                |            |                 |               |  LE, pages  |
    /// |                  |                |            |                 |               |  only)      |
    /// +------------------+----------------+------------+-----------------+---------------+-------------+
    /// </pre>
    ///
    /// pageOrdinal is only included for DATA_PAGE, DATA_PAGE_HEADER module types.
    /// For all other modules (DICT_PAGE, DICT_PAGE_HEADER, COLUMN_INDEX etc), pass pageOrdinal = -1.
    ///
    /// @param aadPrefix caller-supplied prefix, or null if absent
    /// @param aadFileUnique per-file unique bytes from FileCryptoMetaData
    /// @param moduleType one of the constants from {@link ParquetModuleType}
    /// @param rowGroupOrdinal 0-based row group index
    /// @param columnOrdinal 0-based column index within the row group
    /// @param pageOrdinal 0-based page index, or -1 if not applicable for this module type
    /// @return AAD byte array for page decryption
    public static byte[] buildPageAad(byte[] aadPrefix, byte[] aadFileUnique,
                                      byte moduleType,
                                      int rowGroupOrdinal, int columnOrdinal, int pageOrdinal) {
        int prefixLen = aadPrefix != null ? aadPrefix.length : 0;
        int uniqueLen = aadFileUnique != null ? aadFileUnique.length : 0;
        // moduleType(1) + rowGroupOrdinal(2) + columnOrdinal(2) = 5 bytes minimum
        // + pageOrdinal(2) if applicable
        boolean includePageOrdinal = pageOrdinal >= 0;
        int suffixLen = 5 + (includePageOrdinal ? 2 : 0);
        byte[] aad = new byte[prefixLen + uniqueLen + suffixLen];
        int pos = 0;
        if (aadPrefix != null) {
            System.arraycopy(aadPrefix, 0, aad, pos, prefixLen);
            pos += prefixLen;
        }
        if (aadFileUnique != null) {
            System.arraycopy(aadFileUnique, 0, aad, pos, uniqueLen);
            pos += uniqueLen;
        }
        aad[pos++] = moduleType;
        // little-endian shorts for ordinals
        aad[pos++] = (byte) (rowGroupOrdinal);
        aad[pos++] = (byte) (rowGroupOrdinal >> 8);
        aad[pos++] = (byte) (columnOrdinal);
        aad[pos++] = (byte) (columnOrdinal >> 8);
        if (includePageOrdinal) {
            aad[pos++] = (byte) (pageOrdinal);
            aad[pos]   = (byte) (pageOrdinal >> 8);
        }

        return aad;
    }
}
