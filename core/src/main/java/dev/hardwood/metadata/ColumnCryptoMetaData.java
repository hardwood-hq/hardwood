/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Column key encryption metadata
///
/// @param footerKey footer key encryption metadata, or 'null' if absent
/// @param columnKey column key encryption metadata, or 'null' if absent
public record ColumnCryptoMetaData(
        EncryptionWithFooterKey footerKey,
        EncryptionWithColumnKey columnKey) {
}
