/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Type-safe list interface for reading Parquet list values.
 * <p>
 * Provides type-specific accessor methods for iterating over list elements.
 * For primitive int/long/double lists, use the dedicated types
 * ({@link PqIntList}, {@link PqLongList}, {@link PqDoubleList}) via
 * {@code rowReader.getListOfInts()}, etc.
 * </p>
 *
 * <pre>{@code
 * // String list
 * PqList tags = rowReader.getList("tags");
 * for (String tag : tags.strings()) {
 *     System.out.println(tag);
 * }
 *
 * // Nested struct list
 * PqList items = rowReader.getList("items");
 * for (PqStruct item : items.structs()) {
 *     String name = item.getString("name");
 * }
 *
 * // Nested list (2D matrix)
 * PqList matrix = rowReader.getList("matrix");
 * for (PqIntList innerList : matrix.intLists()) {
 *     for (var it = innerList.iterator(); it.hasNext(); ) {
 *         int value = it.nextInt();
 *     }
 * }
 *
 * // Triple nested list (3D cube)
 * PqList cube = rowReader.getList("cube");
 * for (PqList plane : cube.lists()) {
 *     for (PqIntList innerList : plane.intLists()) {
 *         // ...
 *     }
 * }
 * }</pre>
 */
public interface PqList {

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
     * Get a raw element by index without type conversion.
     *
     * @param index the element index (0-based)
     * @return the raw element value, or null if the element is null
     * @throws IndexOutOfBoundsException if index is out of range
     */
    Object get(int index);

    /**
     * Check if an element is null by index.
     *
     * @param index the element index (0-based)
     * @return true if the element is null
     */
    boolean isNull(int index);

    /**
     * Iterate over elements as raw objects without type conversion.
     */
    Iterable<Object> values();

    // ==================== Primitive Type Accessors ====================

    /**
     * Iterate over elements as int values.
     */
    Iterable<Integer> ints();

    /**
     * Iterate over elements as long values.
     */
    Iterable<Long> longs();

    /**
     * Iterate over elements as float values.
     */
    Iterable<Float> floats();

    /**
     * Iterate over elements as double values.
     */
    Iterable<Double> doubles();

    /**
     * Iterate over elements as boolean values.
     */
    Iterable<Boolean> booleans();

    // ==================== Object Type Accessors ====================

    /**
     * Iterate over elements as String values.
     */
    Iterable<String> strings();

    /**
     * Iterate over elements as binary (byte[]) values.
     */
    Iterable<byte[]> binaries();

    /**
     * Iterate over elements as LocalDate values.
     */
    Iterable<LocalDate> dates();

    /**
     * Iterate over elements as LocalTime values.
     */
    Iterable<LocalTime> times();

    /**
     * Iterate over elements as Instant (timestamp) values.
     */
    Iterable<Instant> timestamps();

    /**
     * Iterate over elements as BigDecimal values.
     */
    Iterable<BigDecimal> decimals();

    /**
     * Iterate over elements as UUID values.
     */
    Iterable<UUID> uuids();

    // ==================== Nested Type Accessors ====================

    /**
     * Iterate over elements as nested structs.
     */
    Iterable<PqStruct> structs();

    /**
     * Iterate over elements as nested lists.
     * Use this for list-of-list structures.
     */
    Iterable<PqList> lists();

    /**
     * Iterate over elements as nested int lists.
     * Use this for list-of-int-list structures (e.g., 2D int matrix).
     */
    Iterable<PqIntList> intLists();

    /**
     * Iterate over elements as nested long lists.
     * Use this for list-of-long-list structures.
     */
    Iterable<PqLongList> longLists();

    /**
     * Iterate over elements as nested double lists.
     * Use this for list-of-double-list structures.
     */
    Iterable<PqDoubleList> doubleLists();

    /**
     * Iterate over elements as nested maps.
     */
    Iterable<PqMap> maps();
}
