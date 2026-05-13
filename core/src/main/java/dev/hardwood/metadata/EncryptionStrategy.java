/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

public sealed interface EncryptionStrategy
        permits AesGcmV1, AesGcmCtrV1 {

    byte[] aadPrefix();
    byte[] aadFileUnique();
    boolean supplyAadPrefix();
}
