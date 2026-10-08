/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.ParsedFooter;

/// The [ParsedFooter] [ParsedFooter#readFrom(InputFile)] creates: the [FileFooter] a reader's
/// own open would have produced, and the identity of the file it was read from.
///
/// @param fileFooter the footer as [ParquetMetadataReader#read(InputFile)] produced it
/// @param sourceIdentity the [InputFile#identity()] of the file it was read from
public record ParsedFooterImpl(FileFooter fileFooter, Optional<String> sourceIdentity) implements ParsedFooter {

    public ParsedFooterImpl {
        Objects.requireNonNull(fileFooter, "fileFooter");
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
    }

    /// Reads the footer of an opened file.
    public static ParsedFooter readFrom(InputFile openFile) throws IOException {
        // Resolved before the read, so that a file not opened fails as the caller's error rather
        // than as a read failure of the file.
        Optional<String> identity = openFile.identity();
        return new ParsedFooterImpl(ParquetMetadataReader.read(openFile), identity);
    }

    @Override
    public FileMetaData metaData() {
        return fileFooter.metaData();
    }

    @Override
    public long footerLength() {
        return fileFooter.footerLength();
    }
}
