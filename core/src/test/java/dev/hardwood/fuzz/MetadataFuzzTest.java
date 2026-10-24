/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.fuzz;

import org.junit.jupiter.params.provider.MethodSource;

import com.code_intelligence.jazzer.junit.FuzzTest;

import dev.hardwood.internal.EncryptedFileException;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.thrift.BloomFilterReader;
import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;

/// Parses arbitrary bytes as each Thrift struct a file holds. A parse returns, or fails with
/// [ParquetReadException] or, for a union variant this version does not implement,
/// [UnsupportedOperationException]; any other throwable is a finding. The footer target also
/// accepts the two exceptions the reader restates where it reads a footer: the internal one an
/// encrypted footer raises, and the [IllegalArgumentException] of a schema the footer parsed that
/// [FileSchema] does not accept, whose checks it shares with the schema builder.
class MetadataFuzzTest {

    @MethodSource("dev.hardwood.fuzz.Seeds#footers")
    @FuzzTest
    void footer(byte[] footer) {
        FileMetaData metaData;
        try {
            metaData = FileMetaDataReader.readFooter(Seeds.reader(footer)).metaData();
        }
        catch (ParquetReadException | UnsupportedOperationException | EncryptedFileException e) {
            return;
        }
        try {
            ParquetMetadataReader.schemaOf(metaData);
        }
        catch (ParquetReadException | IllegalArgumentException e) {
            // a schema the reader does not accept
        }
    }

    @MethodSource("dev.hardwood.fuzz.Seeds#pageHeaders")
    @FuzzTest
    void pageHeader(byte[] header) {
        try {
            PageHeaderReader.read(Seeds.reader(header));
        }
        catch (ParquetReadException | UnsupportedOperationException e) {
            // malformed, or a variant this version does not implement
        }
    }

    @MethodSource("dev.hardwood.fuzz.Seeds#offsetIndexes")
    @FuzzTest
    void offsetIndex(byte[] index) {
        try {
            OffsetIndexReader.read(Seeds.reader(index));
        }
        catch (ParquetReadException | UnsupportedOperationException e) {
            // malformed, or a variant this version does not implement
        }
    }

    @MethodSource("dev.hardwood.fuzz.Seeds#columnIndexes")
    @FuzzTest
    void columnIndex(byte[] index) {
        try {
            ColumnIndexReader.read(Seeds.reader(index));
        }
        catch (ParquetReadException | UnsupportedOperationException e) {
            // malformed, or a variant this version does not implement
        }
    }

    @MethodSource("dev.hardwood.fuzz.Seeds#bloomFilters")
    @FuzzTest
    void bloomFilter(byte[] filter) {
        try {
            BloomFilterReader.read(Seeds.reader(filter));
        }
        catch (ParquetReadException | UnsupportedOperationException e) {
            // malformed, or a variant this version does not implement
        }
    }
}
