/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;

import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.reader.StaleMetadataException;

/// Supplies the parsed footer of a file a reader opens, in place of the reader reading it.
///
/// Installed on a context with [HardwoodContext.Builder#metadataSource(MetadataSource)], the
/// source is asked for the footer of every file a reader opened against that context reads:
/// the first file when the reader is opened, and each further file of a multi-file reader
/// when the reader first needs it.
///
/// The source owns acquisition. It returns a footer for every file, and on a miss reads one
/// with [ParsedFooter#readFrom(InputFile)]:
///
/// ```java
/// MetadataSource source = file -> {
///     ParsedFooter footer = cache.get(key(file));
///     if (footer == null) {
///         footer = ParsedFooter.readFrom(file);
///         cache.put(key(file), footer);
///     }
///     return footer;
/// };
/// ```
///
/// The reader checks every supplied footer against the file before using it, and raises
/// [StaleMetadataException] when the footer does not describe the file.
///
/// The reader calls [#footerOf(InputFile)] on the thread that opens it for the first file, and on
/// other threads for the further files of a multi-file read, several at once, so
/// implementations must be thread-safe. A call blocks the read waiting
/// on it; a source backed by a remote store keeps an in-process tier in front of it.
@FunctionalInterface
@Experimental
public interface MetadataSource {

    /// The parsed footer of `file`.
    ///
    /// `file` is open; the source reads from it only through [ParsedFooter#readFrom(InputFile)]
    /// and does not close it.
    ///
    /// @param file the opened file whose footer the reader needs
    /// @return the file's footer, never `null`
    /// @throws IOException if the footer cannot be obtained
    ParsedFooter footerOf(InputFile file) throws IOException;
}
