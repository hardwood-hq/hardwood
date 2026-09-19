/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import dev.hardwood.Experimental;

/// The outcome of [RowWriter#tryWriteRow]: the record is in the batch, or it was rejected
/// without failing the writer.
///
/// `Staged` means the record is staged, not that it has been flushed. A later batch write
/// can still fail the writer.
///
/// **This API is [Experimental]:** the shape may change in future releases.
@Experimental
public sealed interface RowWriteResult permits RowWriteResult.Staged, RowWriteResult.Rejected {

    /// The record is in the batch.
    enum Staged implements RowWriteResult {
        INSTANCE
    }

    /// The record was not staged. `fieldPath` is the schema path already used in rejection
    /// messages; `message` is the full text [RowWriter#writeRow] throws for the same
    /// rejection.
    record Rejected(String fieldPath, String message) implements RowWriteResult {

        public Rejected {
            if (fieldPath == null) {
                throw new IllegalArgumentException("fieldPath must not be null");
            }
            if (message == null) {
                throw new IllegalArgumentException("message must not be null");
            }
        }
    }
}
