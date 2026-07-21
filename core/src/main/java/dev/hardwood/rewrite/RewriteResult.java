/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.rewrite;

/// Summary of a completed byte-copy rewrite.
///
/// @param inputFiles number of source files
/// @param rowGroups number of copied row groups
/// @param rows number of copied rows
/// @param copiedBytes number of encoded column-chunk bytes copied
public record RewriteResult(int inputFiles, int rowGroups, long rows, long copiedBytes) {
}
