/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import dev.hardwood.reader.RowReader;

/// A [RowReader] that can name the file the row it is positioned on came from.
///
/// Readers enrich their own exceptions through
/// [dev.hardwood.internal.ExceptionContext]; this exposes the same file name to
/// layers built on top of a `RowReader`, which raise their own failures and would
/// otherwise report a row position with no file to attribute it to. A multi-file
/// reader serves rows from one file at a time, so the answer tracks the batch
/// currently being served rather than the reader as a whole.
public interface FileAwareRowReader extends RowReader {

    /// The name of the file the current row was read from.
    ///
    /// @return the file name, or `null` before the first batch is loaded and after
    ///         the reader is closed
    String currentFileName();
}
