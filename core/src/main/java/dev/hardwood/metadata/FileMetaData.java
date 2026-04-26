/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;
import java.util.Map;

/// Top-level file metadata for a Parquet file.
///
/// @param version Parquet format version (currently 1 or 2)
/// @param schema flattened schema elements as written in the file footer
/// @param numRows total number of rows across all row groups
/// @param rowGroups metadata for each row group in the file
/// @param keyValueMetadata application-defined key-value metadata, or an empty map if absent
/// @param createdBy identifier of the library that wrote the file, or `null` if absent
/// @param encryptionAlgorithm encryption algorithm, set only in encrypted files with plaintext footer,
///             Files with encrypted footer store algorithm id in FileCryptoMetaData structure
/// @param footerSigningKeyMetadata retrieval metadata of key used for signing the footer, used only in encrypted files
///        with plaintext footer
/// @see <a href="https://parquet.apache.org/docs/file-format/metadata/#file-metadata">File Format – File Metadata</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record FileMetaData(
        int version,
        List<SchemaElement> schema,
        long numRows,
        List<RowGroup> rowGroups,
        Map<String, String> keyValueMetadata,
        String createdBy,
        EncryptionAlgorithm encryptionAlgorithm,
        byte[] footerSigningKeyMetadata) {

    public FileMetaData withCrypto(EncryptionAlgorithm algo, byte[] footerSigningKeyMetadata) {
        return new FileMetaData(this.version, this.schema, this.numRows,
                this.rowGroups, this.keyValueMetadata, this.createdBy,
                algo, footerSigningKeyMetadata);
    }
}
