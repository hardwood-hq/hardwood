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
 * Mutable list container. Elements are either primitive values or MutableContainer objects.
 */
public final class MutableList implements MutableContainer {

    private final List<Object> elements;

    public MutableList() {
        this.elements = new ArrayList<>();
    }

    @Override
    public Object getChild(int index) {
        if (index >= elements.size()) {
            return null;
        }
        return elements.get(index);
    }

    @Override
    public void setChild(int index, Object value) {
        ensureCapacity(index);
        elements.set(index, value);
    }

    @Override
    public MutableContainer getOrCreateChild(int index, PathStep childStep) {
        ensureCapacity(index);
        Object existing = elements.get(index);
        if (existing != null) {
            return (MutableContainer) existing;
        }

        MutableContainer child = MutableContainer.create(childStep);
        elements.set(index, child);
        return child;
    }

    @Override
    public void ensureCapacity(int index) {
        while (elements.size() <= index) {
            elements.add(null);
        }
    }

    @Override
    public int size() {
        return elements.size();
    }

    /**
     * Returns the list of elements for reading.
     */
    public List<Object> elements() {
        return elements;
    }

    @Override
    public String toString() {
        return "MutableList" + elements;
    }
}
