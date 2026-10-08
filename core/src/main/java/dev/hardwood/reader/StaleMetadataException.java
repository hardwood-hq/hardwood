/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import dev.hardwood.Experimental;
import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.ParsedFooter;

/// Raised when the footer a [MetadataSource] supplied does not describe the file being read.
///
/// The file itself is intact. Evicting the footer the source served for the file and opening a
/// new reader, so that the source reads the footer afresh, is the remedy. For a cache keyed on
/// the file's name and the identity its footer was read with:
///
/// ```java
/// try {
///     read(path);
/// }
/// catch (StaleMetadataException e) {
///     e.sourceIdentity().ifPresent(id -> cache.remove(new FooterKey(e.fileName(), id)));
///     read(path);
/// }
/// ```
///
/// The reader raises it before reading any data of the file.
@Experimental
public class StaleMetadataException extends IOException {

    /// The check the supplied footer failed.
    public enum Check {
        /// The footer's [ParsedFooter#sourceIdentity()] differs from the file's
        /// [InputFile#identity()]. Made only when both are present.
        IDENTITY,
        /// The file's length, or the footer length its trailer records, differs from those the
        /// footer was read with.
        TRAILER
    }

    private final String fileName;
    private final Check check;
    private final Optional<String> sourceIdentity;
    private final Optional<String> fileIdentity;

    /// @param fileName the [InputFile#name()] of the file being read
    /// @param check the check the footer failed
    /// @param sourceIdentity the footer's [ParsedFooter#sourceIdentity()]
    /// @param fileIdentity the file's [InputFile#identity()]
    /// @param detail what the check found
    public StaleMetadataException(String fileName, Check check, Optional<String> sourceIdentity,
            Optional<String> fileIdentity, String detail) {
        super(ExceptionContext.filePrefix(fileName)
                + "Footer from the MetadataSource does not describe the file: " + detail);
        this.fileName = Objects.requireNonNull(fileName, "fileName");
        this.check = Objects.requireNonNull(check, "check");
        this.sourceIdentity = Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        this.fileIdentity = Objects.requireNonNull(fileIdentity, "fileIdentity");
    }

    /// The [InputFile#name()] of the file being read.
    public String fileName() {
        return fileName;
    }

    /// The check the supplied footer failed.
    public Check check() {
        return check;
    }

    /// The identity of the file the supplied footer was read from; empty when it had none.
    public Optional<String> sourceIdentity() {
        return sourceIdentity;
    }

    /// The identity of the file being read; empty when it has none.
    public Optional<String> fileIdentity() {
        return fileIdentity;
    }
}
