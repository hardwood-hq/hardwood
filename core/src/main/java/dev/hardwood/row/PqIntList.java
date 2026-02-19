/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import java.util.PrimitiveIterator;
import java.util.function.IntConsumer;

/**
 * Specialized list interface for INT32 values.
 * <p>
 * Provides primitive iteration without boxing overhead.
 * </p>
 *
 * <pre>{@code
 * PqIntList scores = row.getIntList("scores");
 * for (var it = scores.iterator(); it.hasNext(); ) {
 *     int score = it.nextInt();
 * }
 *
 * // Or with forEach:
 * scores.forEach(score -> System.out.println(score));
 * }</pre>
 */
public interface PqIntList {

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
     * @return the int value
     * @throws NullPointerException if the element is null
     * @throws IndexOutOfBoundsException if index is out of range
     */
    int get(int index);

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
     * @return a primitive int iterator
     */
    PrimitiveIterator.OfInt iterator();

    /**
     * Perform an action for each element.
     *
     * @param action the action to perform
     */
    void forEach(IntConsumer action);

    /**
     * Convert to a primitive array.
     *
     * @return a new array containing all elements
     */
    int[] toArray();
}
