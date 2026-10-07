/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Objects;

import dev.hardwood.internal.thrift.FileMetaDataReader.ReadFooter;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.schema.FileSchema;

/// One file's footer as the reader uses it: the parse, the schema derived from it, and the two
/// lengths a supplied footer is checked against.
///
/// @param readFooter the parsed footer, with the leaves whose logical type was not read
/// @param schema the schema derived from `readFooter` by [ParquetMetadataReader#schemaOf]
/// @param footerLength the length of the serialized footer, as the file's trailer records it
/// @param fileLength the length of the file the footer was read from
public record FileFooter(ReadFooter readFooter, FileSchema schema, int footerLength, long fileLength) {

    public FileFooter {
        Objects.requireNonNull(readFooter, "readFooter");
        Objects.requireNonNull(schema, "schema");
    }

    public FileMetaData metaData() {
        return readFooter.metaData();
    }
}
