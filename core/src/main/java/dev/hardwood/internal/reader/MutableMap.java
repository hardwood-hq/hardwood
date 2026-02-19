/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.schema.FieldPath.PathStep;

/**
 * Mutable map container. Entries are MutableStruct objects containing key-value pairs.
 */
public final class MutableMap implements MutableContainer {

    private final List<MutableStruct> entries;

    public MutableMap() {
        this.entries = new ArrayList<>();
    }

    @Override
    public Object getChild(int index) {
        if (index >= entries.size()) {
            return null;
        }
        return entries.get(index);
    }

    @Override
    public void setChild(int index, Object value) {
        throw new UnsupportedOperationException("Map entries must be MutableStruct, use getOrCreateChild");
    }

    @Override
    public MutableContainer getOrCreateChild(int index, PathStep childStep) {
        ensureCapacity(index);
        MutableStruct existing = entries.get(index);
        if (existing != null) {
            return existing;
        }

        MutableStruct child = new MutableStruct(childStep.numChildren());
        entries.set(index, child);
        return child;
    }

    @Override
    public void ensureCapacity(int index) {
        while (entries.size() <= index) {
            entries.add(null);
        }
    }

    @Override
    public int size() {
        return entries.size();
    }

    /**
     * Returns an iterable over the entries for reading.
     */
    public Iterable<MutableStruct> entries() {
        return entries;
    }

    @Override
    public String toString() {
        return "MutableMap" + entries;
    }
}
