/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.row;

/**
 * Type-safe list interface for reading Parquet list values.
 * <p>
 * Provides access to list elements using {@link PqType} for compile-time type safety.
 * </p>
 *
 * <pre>{@code
 * // Simple list
 * PqList tags = row.getValue(PqType.LIST, "tags");
 * for (String tag : tags.getValues(PqType.STRING)) {
 *     System.out.println(tag);
 * }
 *
 * // Nested list (list<list<int>>)
 * PqList matrix = row.getValue(PqType.LIST, "matrix");
 * for (PqList innerList : matrix.getValues(PqType.LIST)) {
 *     for (Integer val : innerList.getValues(PqType.INT32)) {
 *         System.out.println(val);
 *     }
 * }
 *
 * // List of structs
 * PqList contacts = row.getValue(PqType.LIST, "contacts");
 * for (PqRow contact : contacts.getValues(PqType.ROW)) {
 *     String name = contact.getValue(PqType.STRING, "name");
 * }
 * }</pre>
 */
public interface PqList {

    /**
     * Get an iterable over the list elements with type safety.
     *
     * @param elementType the expected type of list elements
     * @param <T> the Java type corresponding to the PqType
     * @return an iterable over the elements (never null, may be empty)
     * @throws IllegalArgumentException if the requested element type doesn't match the schema
     */
    <T> Iterable<T> getValues(PqType<T> elementType);

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
}
