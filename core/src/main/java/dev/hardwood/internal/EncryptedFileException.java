/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

/// Signals, from a footer parse that has no file name to hand, that the file is
/// encrypted.
///
/// Internal and never seen by a caller: [dev.hardwood.internal.reader.footer.ParquetMetadataReader]
/// catches it and raises the [UnsupportedOperationException] a caller gets, which
/// is what the reader says everywhere else about a file it cannot read.
public final class EncryptedFileException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EncryptedFileException() {
        super("encrypted");
    }
}
