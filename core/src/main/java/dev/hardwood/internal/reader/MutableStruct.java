/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;

import dev.hardwood.schema.FieldPath.PathStep;

/**
 * Mutable struct container. Children are either primitive values or MutableContainer objects.
 */
public final class MutableStruct implements MutableContainer {

    private final Object[] children;

    public MutableStruct(int numFields) {
        this.children = new Object[numFields];
    }

    @Override
    public Object getChild(int index) {
        return children[index];
    }

    @Override
    public void setChild(int index, Object value) {
        children[index] = value;
    }

    @Override
    public MutableContainer getOrCreateChild(int index, PathStep childStep) {
        Object existing = children[index];
        if (existing != null) {
            return (MutableContainer) existing;
        }

        MutableContainer child = MutableContainer.create(childStep);
        children[index] = child;
        return child;
    }

    @Override
    public void ensureCapacity(int index) {
        // No-op for structs - fixed size
    }

    @Override
    public int size() {
        return children.length;
    }

    @Override
    public String toString() {
        return "MutableStruct" + Arrays.toString(children);
    }
}
