/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.writer.ColumnBatch;

/// One node of the row-oriented layer's plan: the schema tree as the record-shaped API sees
/// it, carrying the staging its part of a batch accumulates into.
///
/// The plan is built once per writer and reused for the whole file. A node is entered at most
/// once at a time — a Parquet schema cannot be recursive — so each node can double as the
/// builder handed to the caller for its scope.
abstract class RowNode {

    /// The dotted path from the root, used in messages. This is the schema path, so it
    /// includes the synthetic `list` / `element` / `key_value` segments the caller never
    /// spells; a message naming a list element therefore points at the column the writer
    /// produces.
    final String path;

    RowNode(String path) {
        this.path = path;
    }

    /// Stages a null instance of this field: the field was left unset in a record, or the
    /// caller asked for a null explicitly. Throws if the field is `REQUIRED`.
    abstract void appendNullInstance();

    /// Stages the placeholder an absent ancestor requires. Every leaf below a null struct
    /// still owns a slot per record — the columnar contract ignores the value at a slot its
    /// levels mark absent — and every nested group still advances its own scope so later
    /// records stay aligned.
    abstract void appendAbsentInstance();

    /// Adds this node and its descendants to the plan's flat node lists, over which staging
    /// is checkpointed, rolled back, filled and reset without walking the tree.
    abstract void collect(RowPlan.Nodes nodes);

    /// Hands this node's staging to the batch.
    abstract void fill(ColumnBatch batch);

    /// Describes this node's shape for a message about the wrong verb being used on it.
    abstract String kind();

    /// The rejection for a filler that re-enters the scope it is already inside — calling
    /// `writeRow` from within a record's filler, or `addStruct` from within its own. The
    /// staging would silently restart the scope, and the columnar layer would report the
    /// resulting mismatch at flush time, naming neither the record nor the offending call.
    final IllegalStateException reenteredScope() {
        return new IllegalStateException("Cannot start " + (path.isEmpty() ? "a record" : kind() + " " + path)
                + " while it is already being written; a filler must not re-enter the scope it is inside");
    }

    final RejectedRecordException requiredField() {
        return new RejectedRecordException(path, "Field " + path
                + " is REQUIRED; it must be set to a non-null value in every record");
    }
}
