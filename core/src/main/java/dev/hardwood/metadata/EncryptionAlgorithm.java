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

    public EncryptionStrategy activeEncryptionStrategy() {
        if(aesGcmV1 != null)
            return aesGcmV1;
        else
            return aesGcmCtrV1;
    }
}
