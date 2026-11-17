/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetReadException;

/// A stored `INT(8)` or `INT(16)` value outside the range its annotation states, refused by
/// [LogicalTypeConverter#intWithinBitWidth].
///
/// A [ParquetReadException] like any other value a file holds that its type cannot: the file is
/// what is wrong. It is nameable separately for the CLI's viewing commands, which show such a
/// value as a marker and carry on, where every other read failure stops them; the value and the
/// annotation it carries spell that marker. It reaches library callers as a plain
/// [ParquetReadException] wherever a boundary names the file
/// ([dev.hardwood.internal.ExceptionContext#addReadContext]), which keeps it as the cause.
public class IntOutOfRangeException extends ParquetReadException {

    private static final long serialVersionUID = 1L;

    private final transient LogicalType.IntType annotation;
    private final long value;

    /// @param annotation the column's annotation
    /// @param value the stored value, read unsigned under an unsigned annotation
    /// @param min the smallest value the annotation admits
    /// @param max the largest value the annotation admits
    public IntOutOfRangeException(LogicalType.IntType annotation, long value, long min, long max) {
        super("The " + annotation + " value " + value + " lies outside its range, which holds " + min + " to " + max);
        this.annotation = annotation;
        this.value = value;
    }

    /// The annotation whose range the value lies outside.
    public LogicalType.IntType annotation() {
        return annotation;
    }

    /// The stored value, read unsigned under an unsigned annotation.
    public long value() {
        return value;
    }

    /// The refusal in `failure` or its causes, or `null` where it holds none. A boundary that
    /// names the file restates it as a plain [ParquetReadException] with this as the cause.
    public static IntOutOfRangeException in(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof IntOutOfRangeException refusal) {
                return refusal;
            }
        }
        return null;
    }
}
