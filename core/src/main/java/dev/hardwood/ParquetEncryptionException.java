/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;

import dev.hardwood.internal.ExceptionContext;

/// This is exception class to represent an error scenario during Parquet decryption.
///
/// Separates decryption failures from general application failures,
/// thus enabling callers to handle them distinctly.
public class ParquetEncryptionException extends IOException {

    public ParquetEncryptionException(String message) {
        super(message);
    }

    public ParquetEncryptionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParquetEncryptionException(String fileName, ParquetEncryptionException cause) {
        super(ExceptionContext.filePrefix(fileName) + cause.getMessage(), cause.getCause());
    }
}
