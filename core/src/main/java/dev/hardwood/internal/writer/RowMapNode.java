/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.function.Consumer;

import dev.hardwood.Validity;
import dev.hardwood.row.PqMap;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.MapBuilder;
import dev.hardwood.writer.StructBuilder;

/// A `MAP` group in the row-oriented layer's plan, and the [MapBuilder] its scope hands to
/// the caller.
///
/// A map's entries are a repeated struct of `key` and `value`, so an entry is populated
/// through the [RowStructNode] of that group — the same builder a nested struct uses. The
/// map's offsets drive both leaves, which is what the columnar API's `map` setter records.
final class RowMapNode extends RowRepeatedNode implements MapBuilder {

    private final RowStructNode entry;

    RowMapNode(String path, boolean optional, RowStructNode entry) {
        super(path, optional);
        this.entry = entry;
    }

    @Override
    void collect(RowPlan.Nodes nodes) {
        nodes.repeated().add(this);
        entry.collect(nodes);
    }

    @Override
    void fill(ColumnBatch batch) {
        Validity validity = validity();
        if (validity == null) {
            batch.map(path, offsets());
        }
        else {
            batch.map(path, offsets(), validity);
        }
    }

    @Override
    String kind() {
        return "MAP group";
    }

    /// A MAP group takes a [PqMap], whose entries are copied in order into the entry group's
    /// key and value fields.
    @Override
    void writeValue(Object value) {
        if (value == null) {
            appendNullInstance();
            return;
        }
        if (!(value instanceof PqMap source)) {
            throw wrongValue(value, "a PqMap");
        }
        beginScope();
        for (PqMap.Entry sourceEntry : source.getEntries()) {
            entry.beginScope();
            entry.setValue(0, sourceEntry.getKey());
            if (entry.getFieldCount() > 1) {
                entry.setValue(1, sourceEntry.getValue());
            }
            entry.endScope();
            countEntry();
        }
        endScope();
    }

    @Override
    public MapBuilder addEntry(Consumer<StructBuilder> filler) {
        ensureActive();
        entry.beginScope();
        filler.accept(entry);
        entry.endScope();
        countEntry();
        return this;
    }
}
