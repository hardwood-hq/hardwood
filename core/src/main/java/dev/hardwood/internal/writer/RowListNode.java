/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.function.Consumer;

import dev.hardwood.Validity;
import dev.hardwood.row.PqFile;
import dev.hardwood.row.PqInterval;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.ListBuilder;
import dev.hardwood.writer.MapBuilder;
import dev.hardwood.writer.StructBuilder;

/// A `LIST` group in the row-oriented layer's plan, and the [ListBuilder] its scope hands to
/// the caller. The element it appends to is the list's logical element — the child of the
/// synthetic `repeated` group — which the caller never names.
final class RowListNode extends RowRepeatedNode implements ListBuilder {

    private final RowNode element;

    RowListNode(String path, boolean optional, RowNode element) {
        super(path, optional);
        this.element = element;
    }

    @Override
    void collect(RowPlan.Nodes nodes) {
        nodes.repeated().add(this);
        element.collect(nodes);
    }

    @Override
    void fill(ColumnBatch batch) {
        Validity validity = validity();
        if (validity == null) {
            batch.list(path, offsets());
        }
        else {
            batch.list(path, offsets(), validity);
        }
    }

    @Override
    String kind() {
        return "LIST group";
    }

    // ==================== ListBuilder ====================

    @Override
    public ListBuilder addInt(int value) {
        leaf("addInt").setInt(value);
        return counted();
    }

    @Override
    public ListBuilder addLong(long value) {
        leaf("addLong").setLong(value);
        return counted();
    }

    @Override
    public ListBuilder addFloat(float value) {
        leaf("addFloat").setFloat(value);
        return counted();
    }

    @Override
    public ListBuilder addDouble(double value) {
        leaf("addDouble").setDouble(value);
        return counted();
    }

    @Override
    public ListBuilder addBoolean(boolean value) {
        leaf("addBoolean").setBoolean(value);
        return counted();
    }

    @Override
    public ListBuilder addString(String value) {
        leaf("addString").setString(value);
        return counted();
    }

    @Override
    public ListBuilder addBinary(byte[] value) {
        leaf("addBinary").setBinary(value);
        return counted();
    }

    @Override
    public ListBuilder addDate(LocalDate value) {
        leaf("addDate").setDate(value);
        return counted();
    }

    @Override
    public ListBuilder addTime(LocalTime value) {
        leaf("addTime").setTime(value);
        return counted();
    }

    @Override
    public ListBuilder addTimestamp(Instant value) {
        leaf("addTimestamp").setTimestamp(value);
        return counted();
    }

    @Override
    public ListBuilder addLocalTimestamp(LocalDateTime value) {
        leaf("addLocalTimestamp").setLocalTimestamp(value);
        return counted();
    }

    @Override
    public ListBuilder addDecimal(BigDecimal value) {
        leaf("addDecimal").setDecimal(value);
        return counted();
    }

    @Override
    public ListBuilder addUuid(UUID value) {
        leaf("addUuid").setUuid(value);
        return counted();
    }

    @Override
    public ListBuilder addInterval(PqInterval value) {
        leaf("addInterval").setInterval(value);
        return counted();
    }

    @Override
    public ListBuilder addNull() {
        ensureActive();
        element.appendNullInstance();
        return counted();
    }

    @Override
    public ListBuilder addStruct(Consumer<StructBuilder> filler) {
        ensureActive();
        if (!(element instanceof RowStructNode struct) || struct.isFile()) {
            throw wrongVerb("addStruct", "a struct element");
        }
        struct.beginScope();
        filler.accept(struct);
        struct.endScope();
        return counted();
    }

    @Override
    public ListBuilder addFile(PqFile value) {
        ensureActive();
        if (!(element instanceof RowStructNode group) || !group.isFile()) {
            throw wrongVerb("addFile", "a FILE element");
        }
        if (value == null) {
            group.appendNullInstance();
        }
        else {
            group.writeFile(value);
        }
        return counted();
    }

    @Override
    public ListBuilder addList(Consumer<ListBuilder> filler) {
        ensureActive();
        if (!(element instanceof RowListNode list)) {
            throw wrongVerb("addList", "a LIST element");
        }
        list.beginScope();
        filler.accept(list);
        list.endScope();
        return counted();
    }

    @Override
    public ListBuilder addMap(Consumer<MapBuilder> filler) {
        ensureActive();
        if (!(element instanceof RowMapNode map)) {
            throw wrongVerb("addMap", "a MAP element");
        }
        map.beginScope();
        filler.accept(map);
        map.endScope();
        return counted();
    }

    private ListBuilder counted() {
        countEntry();
        return this;
    }

    private RowLeafNode leaf(String verb) {
        ensureActive();
        if (!(element instanceof RowLeafNode leafNode)) {
            throw wrongVerb(verb, "a leaf element");
        }
        return leafNode;
    }

    private IllegalArgumentException wrongVerb(String verb, String expected) {
        return new IllegalArgumentException("The element of list " + path + " is a " + element.kind()
                + "; " + verb + " applies to " + expected);
    }
}
