/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

/// Retrieves decryption keys based on key metadata stored in a Parquet file.
/// Caller needs to provide its implementation to fetch keys from key management system.
@FunctionalInterface
public interface DecryptionKeyProvider {

    /// Provides the AES key bytes for the given key metadata.
    /// @param keyMetadata the key metadata bytes from the Parquet file, may be null
    /// @return the AES key bytes - must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256.
    ///         Used with either AES-GCM or AES-GCM-CTR depending on the file's encryption algorithm.
    byte[] provideKey(byte[] keyMetadata);
}
