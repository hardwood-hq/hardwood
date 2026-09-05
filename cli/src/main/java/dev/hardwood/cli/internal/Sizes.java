/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import dev.hardwood.metadata.ColumnMetaData;

public class Sizes {

    private Sizes() {
    }

    public static String columnPath(ColumnMetaData cmd) {
        return cmd.pathInSchema().toString();
    }

    /// Largest scaled value that still renders below `1024.0` at one
    /// decimal place. Anything at or above it rounds up to `1024.0`,
    /// which belongs in the next unit — so the branches switch here
    /// rather than at the exact power of two.
    private static final double ROUNDING_LIMIT = 1_023.95;

    public static String format(long bytes) {
        if (bytes < 1_024) {
            return bytes + " B";
        }
        double kib = bytes / 1_024.0;
        if (kib < ROUNDING_LIMIT) {
            return Fmt.fmt("%.1f KiB", kib);
        }
        double mib = kib / 1_024.0;
        if (mib < ROUNDING_LIMIT) {
            return Fmt.fmt("%.1f MiB", mib);
        }
        return Fmt.fmt("%.1f GiB", mib / 1_024.0);
    }

    /// Compressed size as a percentage of the uncompressed size. Every surface
    /// renders compression this way, so a figure read on one screen carries the
    /// same meaning on the next: lower is better, and the number is what
    /// survived the codec rather than the factor it divided by.
    ///
    /// @return the percentage, or [Strings#ABSENT_VALUE] when `uncompressed`
    ///         is not positive and there is nothing to divide by
    public static String compression(long compressed, long uncompressed) {
        if (uncompressed <= 0) {
            return Strings.ABSENT_VALUE;
        }
        return Fmt.fmt("%.1f%%", 100.0 * compressed / uncompressed);
    }

    /// Renders bytes as a human-readable form plus the raw byte count in
    /// parentheses (e.g. `"1.5 KiB  (1,536 B)"`). When the value is under
    /// 1 KiB the human form already shows the raw count, so the parenthesised
    /// form is dropped to avoid `"422 B  (422 B)"` duplication.
    public static String dualFormat(long bytes) {
        if (bytes < 1_024) {
            return format(bytes);
        }
        return format(bytes) + "  (" + Fmt.fmt("%,d", bytes) + " B)";
    }
}
