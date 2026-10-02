/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.io.IOException;
import java.util.Optional;

import dev.hardwood.Experimental;
import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.reader.ParsedFooterImpl;

/// The parsed footer of one file, as a [MetadataSource] hands it to a reader.
///
/// Instances come from [#readFrom(InputFile)], so each one carries the lengths and the
/// identity of the file it was read from. A `ParsedFooter` is immutable and can be shared by
/// any number of readers on any threads. It is held in memory and has no serialized form.
/// This interface is not meant to be implemented outside Hardwood.
@Experimental
public interface ParsedFooter {

    /// Reads and parses the footer of an opened file.
    ///
    /// @param openFile a file on which [InputFile#open()] has been called; it stays open
    /// @return the file's footer
    /// @throws IOException if the file cannot be read
    /// @throws dev.hardwood.reader.ParquetReadException if the file is not a valid Parquet file
    /// @throws IllegalStateException if the backend resolves the file's identity in
    ///         [InputFile#open()] and `openFile` has not been opened
    static ParsedFooter readFrom(InputFile openFile) throws IOException {
        return ParsedFooterImpl.readFrom(openFile);
    }

    /// The file's metadata as stated in its footer.
    FileMetaData metaData();

    /// The length in bytes of the serialized footer, excluding the trailing length field and
    /// magic number.
    long footerLength();

    /// The [InputFile#identity()] of the file the footer was read from; empty when that file
    /// had none.
    Optional<String> sourceIdentity();
}
