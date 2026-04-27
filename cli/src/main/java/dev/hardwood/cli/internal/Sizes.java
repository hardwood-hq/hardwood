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

    public static String format(long bytes) {
        if (bytes < 1_024) {
            return bytes + " B";
        }
        if (bytes < 1_024 * 1_024) {
            return Fmt.fmt("%.1f KB", bytes / 1_024.0);
        }
        if (bytes < 1_024L * 1_024 * 1_024) {
            return Fmt.fmt("%.1f MB", bytes / (1_024.0 * 1_024));
        }
        return Fmt.fmt("%.1f GB", bytes / (1_024.0 * 1_024 * 1_024));
    }

    /// Renders bytes as a human-readable form plus the raw byte count in
    /// parentheses (e.g. `"1.5 KB  (1,536 B)"`). When the value is under
    /// 1 KB the human form already shows the raw count, so the parenthesised
    /// form is dropped to avoid `"422 B  (422 B)"` duplication.
    public static String dualFormat(long bytes) {
        if (bytes < 1_024) {
            return format(bytes);
        }
        return format(bytes) + "  (" + Fmt.fmt("%,d", bytes) + " B)";
    }
}
