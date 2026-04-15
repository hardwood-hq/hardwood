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
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class ParquetCryptoHelper {

    private static final int NONCE_LENGTH = 12;
    private static final int TAG_LENGTH_BITS = 128; // 16 bytes * 8

    public static ByteBuffer decrypt(ByteBuffer encryptedBuf, byte[] key, byte[] aad) throws IOException {
        // Step 1: read length (4 bytes) - we don't actually need it, just skip
        encryptedBuf.order(ByteOrder.LITTLE_ENDIAN);
        int length = encryptedBuf.getInt();

        // Step 2: read nonce (12 bytes plaintext)
        byte[] nonce = new byte[NONCE_LENGTH];
        encryptedBuf.get(nonce);

        // Step 3: remaining bytes are ciphertext + GCM tag
        byte[] ciphertext = new byte[encryptedBuf.remaining()];
        encryptedBuf.get(ciphertext);

        // Step 4: decrypt using javax.crypto
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
            GCMParameterSpec paramSpec = new GCMParameterSpec(TAG_LENGTH_BITS, nonce);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, paramSpec);
            if (aad != null) {
                cipher.updateAAD(aad);
            }
            byte[] decrypted = cipher.doFinal(ciphertext);
            return ByteBuffer.wrap(decrypted);
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to decrypt data", e);
        }
    }

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
        aad[pos] = 0x00; // module type = footer

        return aad;
    }
}
