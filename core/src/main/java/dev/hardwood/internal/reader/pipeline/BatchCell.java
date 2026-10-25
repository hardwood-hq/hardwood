/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

/// The work of one [DecodedColumn] for one planned batch, as its publish order sees it: a
/// [Cell] of a flat column, a [NestedCell] of a column decoded through levels.
///
/// @param <B> the batch type
interface BatchCell<B> {

    /// The read's row of the batch's first record.
    long firstRow();

    /// Whether the cell is completed and waits to be published; read and set under the column
    /// lock.
    boolean isCompleted();

    void markCompleted();

    /// The batch, once the cell is completed.
    B batch();

    /// Drops what the cell references once it is published. A page usually spans several cells
    /// and reaches each through its segments until its last cell is published; a published cell
    /// that kept its batch would keep the batch's arrays alive as long as the page.
    void release();
}
