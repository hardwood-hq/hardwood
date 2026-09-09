/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.reader.ParquetReadException;

/// The buffer ended before the structure being parsed did.
///
/// Either the bytes ran out mid-field, or a field declared a length the buffer cannot hold —
/// both say the same thing about the buffer, so both raise this.
///
/// A [ParquetReadException] like any other parse failure, because for most
/// callers that is what it is: a footer or an index that stops mid-struct is a
/// file that does not describe itself, and no retry changes it.
///
/// It is nameable separately because one caller means something else by it. A
/// page header is read by peeking a guessed number of bytes in front of it —
/// `DataPageHeader.statistics` can carry bounds long enough to push the header
/// past any fixed guess — so running out of bytes there says the guess was too
/// small, not that the file is wrong. Those callers catch this, read more and
/// try again, and give up only once the peek has grown past what the chunk can
/// hold.
///
/// Anywhere else it is left to propagate, and reads as the truncation it is.
public class ThriftTruncatedException extends ParquetReadException {

    private static final long serialVersionUID = 1L;

    public ThriftTruncatedException(String message) {
        super(message);
    }

    /// The same, keeping `cause`.
    ///
    /// Needed by [dev.hardwood.internal.ExceptionContext#addReadContext], which restates a
    /// failure with the file and chunk in front of its message and reconstructs it through
    /// this constructor. Without one the type it hands back is a plain [RuntimeException],
    /// and a caller that catches [ParquetReadException] to report a broken file cleanly
    /// stops seeing it.
    public ThriftTruncatedException(String message, Throwable cause) {
        super(message, cause);
    }
}
