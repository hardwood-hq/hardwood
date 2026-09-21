/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.util.Optional;

/// Thrown when a [MetadataSource] returns a [ParsedFooter] whose
/// [ParsedFooter#sourceIdentity()] does not match the current
/// [dev.hardwood.InputFile#identity()] of the file being opened.
///
/// This indicates that the file content has changed since the cached footer was
/// read. The caller should invalidate the cache entry identified by
/// [#sourceIdentity()] and retry:
///
/// ```java
/// try {
///     read(file);
/// } catch (StaleMetadataException e) {
///     e.sourceIdentity().ifPresent(id -> cache.invalidate(file.name()));
///     read(file);  // MetadataSource will re-read the footer
/// }
/// ```
///
/// This exception is only thrown when both [ParsedFooter#sourceIdentity()] and
/// [dev.hardwood.InputFile#identity()] are present and disagree. If either is
/// empty no staleness check is performed and no exception is thrown.
public class StaleMetadataException extends IOException {

    private final Optional<String> sourceIdentity;

    /// @param sourceIdentity the identity token carried by the stale [ParsedFooter]
    /// @param message        a human-readable description of the conflict
    public StaleMetadataException(String sourceIdentity, String message) {
        super(message);
        this.sourceIdentity = Optional.ofNullable(sourceIdentity);
    }

    /// The content-identity token from the stale cached footer, if present.
    ///
    /// Pass this to your cache's invalidation method to evict precisely the entry
    /// that is out of date rather than clearing the whole cache.
    public Optional<String> sourceIdentity() {
        return sourceIdentity;
    }
}
