/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.io.IOException;

/// Signals that a Parquet file uses Parquet Modular Encryption, which Hardwood
/// does not support.
///
/// Thrown while reading the file metadata so that callers fail fast with a
/// clear, file-attributed error, rather than reporting a misleading "invalid
/// magic number" (encrypted-footer mode) or crashing later with an
/// unattributable page-scan error (plaintext-footer mode).
public final class EncryptedParquetException extends IOException {

    /// Canonical, file-name-agnostic message. Callers prepend file context via
    /// [ExceptionContext#filePrefix].
    public static final String MESSAGE = "Encrypted Parquet files are not supported (Parquet Modular Encryption)";

    public EncryptedParquetException() {
        super(MESSAGE);
    }

    public EncryptedParquetException(String message) {
        super(message);
    }
}
