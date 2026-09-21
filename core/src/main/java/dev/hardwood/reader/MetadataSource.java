/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;

import dev.hardwood.InputFile;

/// Supplies a [ParsedFooter] for an [InputFile], enabling callers to reuse a
/// cached footer across many read operations without re-reading or re-parsing it
/// on every [ParquetFileReader#open] call.
///
/// Install a source on a [dev.hardwood.HardwoodContext] via
/// [dev.hardwood.HardwoodContext.Builder#metadataSource]:
///
/// ```java
/// Map<String, ParsedFooter> cache = new ConcurrentHashMap<>();
///
/// // ParsedFooter.readFrom throws IOException; wrap it for the lambda.
/// MetadataSource source = file -> {
///     try {
///         return cache.computeIfAbsent(file.name(), k -> {
///             try { return ParsedFooter.readFrom(file); }
///             catch (IOException e) { throw new UncheckedIOException(e); }
///         });
///     } catch (UncheckedIOException e) { throw e.getCause(); }
/// };
///
/// try (HardwoodContext ctx = HardwoodContext.builder()
///         .metadataSource(source)
///         .build()) {
///     try (ParquetFileReader reader = ParquetFileReader.open(myFile, ctx)) {
///         // footer is served from the cache, not re-read from the file
///     }
/// }
/// ```
///
/// hardwood calls [#footerOf(InputFile)] exactly once per `open` or `openAll` call
/// that needs the first file's footer. The file is already open when the call is
/// made. The source must not open or close the file.
///
/// If the returned [ParsedFooter] carries a [ParsedFooter#sourceIdentity()] and the
/// current [InputFile#identity()] of the file differs from it, hardwood throws
/// [StaleMetadataException] before reading any data. The caller should then
/// invalidate its cache entry and retry.
///
/// Implementations must be safe for concurrent use from multiple threads.
///
/// @see ParsedFooter
/// @see StaleMetadataException
/// @see dev.hardwood.HardwoodContext.Builder#metadataSource(MetadataSource)
@FunctionalInterface
public interface MetadataSource {

    /// Return the footer for `file`.
    ///
    /// The file is already open; the source must not call [InputFile#open()] or
    /// [InputFile#close()].
    ///
    /// @param file the open file whose footer is needed
    /// @return the parsed footer; must not be {@code null}
    /// @throws IOException if the footer cannot be obtained
    ParsedFooter footerOf(InputFile file) throws IOException;
}
