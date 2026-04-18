/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Column level encryption metadata
///
/// @param pathInSchema column path in schema, required
/// @param keyMetadata retrieval metadata of column encryption key, optional
public record EncryptionWithColumnKey(
        FieldPath pathInSchema,
        byte[] keyMetadata) {
}
