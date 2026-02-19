/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.schema.FieldPath.PathStep;

/**
 * Unified interface for mutable containers used during record assembly.
 * Containers form a tree hierarchy - children are either primitive values or other MutableContainers.
 */
public sealed interface MutableContainer permits MutableStruct, MutableList, MutableMap {

    /**
     * Get the child at the given index.
     * Returns the raw value for primitives, or MutableContainer for nested structures.
     *
     * @param index the index to access
     * @return the child object (may be null)
     */
    Object getChild(int index);

    /**
     * Set a primitive value at the given index.
     *
     * @param index the index to set
     * @param value the primitive value to set
     */
    void setChild(int index, Object value);

    /**
     * Get or create a child container at the given index.
     * The type of container created depends on the childStep metadata.
     *
     * @param index the index to access/create
     * @param childStep the PathStep describing the child's structure
     * @return the existing or newly created child container
     */
    MutableContainer getOrCreateChild(int index, PathStep childStep);

    /**
     * Ensure capacity for the given index. No-op for fixed-size containers.
     *
     * @param index the index that needs to be accessible
     */
    void ensureCapacity(int index);

    /**
     * Returns the number of children/elements in this container.
     */
    int size();

    /**
     * Create the appropriate container type from a PathStep.
     *
     * @param step the PathStep describing the container structure
     * @return the appropriate MutableContainer implementation
     */
    static MutableContainer create(PathStep step) {
        if (step.isMap()) {
            return new MutableMap();
        }
        else if (step.isList()) {
            return new MutableList();
        }
        else if (step.isStruct()) {
            return new MutableStruct(step.numChildren());
        }
        else {
            throw new IllegalArgumentException("Cannot create container for primitive step");
        }
    }
}
