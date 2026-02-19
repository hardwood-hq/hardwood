/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

/**
 * Specialized list interface for INT64 values.
 * <p>
 * Provides primitive iteration without boxing overhead.
 * </p>
 */
public interface PqLongList {

    /**
     * Get the number of elements in this list.
     *
     * @return the element count
     */
    int size();

    /**
     * Check if this list is empty.
     *
     * @return true if the list has no elements
     */
    boolean isEmpty();

    /**
     * Get an element by index.
     *
     * @param index the element index (0-based)
     * @return the long value
     * @throws NullPointerException if the element is null
     * @throws IndexOutOfBoundsException if index is out of range
     */
    long get(int index);

    /**
     * Check if an element is null by index.
     *
     * @param index the element index (0-based)
     * @return true if the element is null
     */
    boolean isNull(int index);

    /**
     * Get a primitive iterator over the elements.
     *
     * @return a primitive long iterator
     */
    PrimitiveIterator.OfLong iterator();

    /**
     * Perform an action for each element.
     *
     * @param action the action to perform
     */
    void forEach(LongConsumer action);

    /**
     * Convert to a primitive array.
     *
     * @return a new array containing all elements
     */
    long[] toArray();
}
