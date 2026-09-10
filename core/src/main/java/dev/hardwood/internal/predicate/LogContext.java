/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.FieldPath;

/// Where in a file the filter layer is reading, for anything it has to say about what it found
/// there.
///
/// Built as far up as the position is known — a row group's evaluator knows the file and the
/// row group, but not which column a leaf will test, nor which page a scan will reach — and
/// narrowed with [#withColumn] and [#withPageIndex] as it descends. The parts still unknown are
/// left off the rendered prefix rather than guessed at.
///
/// @param fileName the file, or `null` where there is none to name, in which case nothing
///        locates the message and the whole position is left off
/// @param rowGroupIndex the row group, or [ExceptionContext#UNKNOWN_ROW_GROUP]
/// @param columnPath the column, or `null` before one is resolved
/// @param pageIndex the page, or [ExceptionContext#UNKNOWN_PAGE] where the subject is a whole
///        column chunk rather than one of its pages
public record LogContext(String fileName, int rowGroupIndex, FieldPath columnPath, int pageIndex) {

    /// A row group, before a column or page within it is known.
    public LogContext(String fileName, int rowGroupIndex) {
        this(fileName, rowGroupIndex, null, ExceptionContext.UNKNOWN_PAGE);
    }

    /// The same position, narrowed to one of the row group's columns.
    public LogContext withColumn(FieldPath columnPath) {
        return new LogContext(fileName, rowGroupIndex, columnPath, pageIndex);
    }

    /// The same position, narrowed to one of the column chunk's pages.
    public LogContext withPageIndex(int pageIndex) {
        return new LogContext(fileName, rowGroupIndex, columnPath, pageIndex);
    }

    /// The `[file: row group N, column 'X', page P] ` prefix for this position, dropping
    /// whichever parts are unknown, as the read pipeline marks its failures.
    String prefix() {
        return ExceptionContext.readPrefix(fileName, rowGroupIndex,
                columnPath != null ? columnPath.toString() : null, pageIndex);
    }
}
