/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

///  Encryption algorithm UNION, either of AesGcmV1 OR AesGcmCtrV1
public record EncryptionAlgorithm(
        AesGcmV1 aesGcmV1,
        AesGcmCtrV1 aesGcmCtrV1) {

    public byte[] aadPrefix() {
        if(aesGcmV1 != null)
            return aesGcmV1.aadPrefix();
        else
            return aesGcmCtrV1.aadPrefix();
    }

    public byte[] aadFileUnique() {
        if(aesGcmV1 != null)
            return aesGcmV1.aadFileUnique();
        else
            return aesGcmCtrV1.aadFileUnique();
    }

    public boolean supplyAadPrefix() {
        if(aesGcmV1 != null)
            return aesGcmV1.supplyAadPrefix();
        else
            return aesGcmCtrV1.supplyAadPrefix();
    }
}
