/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A rejection of the record itself: a value the column cannot hold, or a `REQUIRED` field
/// left unset or set null. [dev.hardwood.writer.RowWriter#tryWriteRow] returns this as
/// [dev.hardwood.writer.RowWriteResult.Rejected] without failing the writer;
/// [dev.hardwood.writer.RowWriter#writeRow] still throws it and fails the writer.
///
/// Distinct from builder misuse — an unknown name, a field set twice, a setter that does
/// not fit the field — and from an exception the filler throws itself. Those remain
/// [IllegalArgumentException] or whatever the filler raised.
public final class RejectedRecordException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final String fieldPath;

    public RejectedRecordException(String fieldPath, String message) {
        super(message);
        this.fieldPath = fieldPath;
    }

    public RejectedRecordException(String fieldPath, String message, Throwable cause) {
        super(message, cause);
        this.fieldPath = fieldPath;
    }

    public String fieldPath() {
        return fieldPath;
    }
}
