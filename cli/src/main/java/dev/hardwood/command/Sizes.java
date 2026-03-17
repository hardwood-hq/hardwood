/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import dev.hardwood.metadata.ColumnMetaData;

class Sizes {

    private Sizes() {
    }

    static String columnPath(ColumnMetaData cmd) {
        return cmd.pathInSchema().toString();
    }

    static String format(long bytes) {
        if (bytes < 1_024) {
            return bytes + " B";
        }
        if (bytes < 1_024 * 1_024) {
            return String.format("%.1f KB", bytes / 1_024.0);
        }
        if (bytes < 1_024L * 1_024 * 1_024) {
            return String.format("%.1f MB", bytes / (1_024.0 * 1_024));
        }
        return String.format("%.1f GB", bytes / (1_024.0 * 1_024 * 1_024));
    }
}
