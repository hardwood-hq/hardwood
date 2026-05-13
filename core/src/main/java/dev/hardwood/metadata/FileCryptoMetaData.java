/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Encryption meta data for footer
///
/// @param encryptionAlgorithm Encryption algorithm, This field is only used for files with encrypted footer.
///             Files with plaintext footer store algorithm id, inside footer (FileMetaData structure)
/// @param keyMetadata retrieval metadata of key used for encryption of footer,and (possibly) columns, or 'null' if absent
public record FileCryptoMetaData(
        EncryptionAlgorithm encryptionAlgorithm,
        byte[] keyMetadata) {
}
