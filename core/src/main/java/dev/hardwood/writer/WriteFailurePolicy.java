/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

/// What [ParquetFileWriter#close()] does when a write has failed.
///
/// A writer that throws from [ColumnWriter#writeBatch] or from the flush a
/// [RowWriter#writeRow] triggers is marked as failed. This policy decides what
/// `close()` — and therefore `try-with-resources` — does with the output that
/// was produced before the failure.
///
/// The default is [#DISCARD]: a failure leaves nothing behind, and
/// `try-with-resources` is safe without any extra handling. [#COMMIT_PREFIX]
/// opts into the pre-1.1 behaviour, publishing whatever rows were flushed
/// before the failure as a valid, readable, but truncated file.
public enum WriteFailurePolicy {

    /// Discard the output when a write has failed.
    ///
    /// `close()` calls [dev.hardwood.OutputFile#discard()] and returns silently,
    /// leaving no file at the destination. This is the default, matching the
    /// documented contract: "a failure leaves nothing behind."
    ///
    /// A caller who catches the write failure and wants the successfully-written
    /// prefix should use [#COMMIT_PREFIX] instead.
    DISCARD,

    /// Commit whatever was successfully written before the failure.
    ///
    /// `close()` writes a valid footer over the rows that were flushed before
    /// the failure, publishing a well-formed but truncated file. The caller
    /// takes responsibility for the semantic correctness of the prefix.
    ///
    /// This is an opt-in: by default, a failure discards. It exists for
    /// callers who produce data in a streaming pipeline and can use whatever
    /// arrived before the break.
    COMMIT_PREFIX
}
