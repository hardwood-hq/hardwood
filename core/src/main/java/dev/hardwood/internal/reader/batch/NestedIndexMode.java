/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

/// Which derived structures a nested column computes for each batch as its cell completes,
/// selected by the constructing reader:
///
/// - `ALL_ITEMS` — the [dev.hardwood.internal.reader.row.NestedRowReader] path: the all-items
///   element-validity / multi-level-offset index and raw def/rep levels, which
///   [dev.hardwood.internal.reader.row.NestedBatchIndex] and its consumers read.
/// - `REAL_VIEW` — a [dev.hardwood.reader.ColumnReader] whose scan compacts nothing: the
///   real-items [NestedLevelComputer.RealView], built as the cell completes so the serial consumer
///   does no scan, and the values written at their real leaf positions. The raw levels, the record
///   offsets and the all-items index are dropped — nothing reads them.
/// - `REAL_VIEW_KEEP_LEVELS` — a `ColumnReader` whose scan compacts its batches: one with an
///   exact filter, an offset or a limit, or one that drops rows its read published before its
///   skip. The raw levels are kept so the scan can slice them per kept record, and the consumer
///   rebuilds the view lazily from the compacted levels. The all-items index is still skipped.
public enum NestedIndexMode {
    ALL_ITEMS,
    REAL_VIEW,
    REAL_VIEW_KEEP_LEVELS
}
