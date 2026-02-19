/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

/**
 * A simple open-addressed hash map with linear probing for String -> int mappings.
 * Optimized for small to medium-sized immutable maps (like column name lookups).
 * <p>
 * Uses open addressing for better cache locality compared to HashMap's separate chaining.
 */
final class StringToIntMap {

    private static final int EMPTY = -1;

    private final String[] keys;
    private final int[] values;
    private final int mask;

    /**
     * Create a map with the given capacity (will be rounded up to next power of 2).
     */
    StringToIntMap(int expectedSize) {
        // Use ~75% load factor, round up to power of 2
        int capacity = tableSizeFor(expectedSize + (expectedSize >> 1) + 1);
        this.keys = new String[capacity];
        this.values = new int[capacity];
        this.mask = capacity - 1;

        // Initialize values to EMPTY
        for (int i = 0; i < capacity; i++) {
            values[i] = EMPTY;
        }
    }

    /**
     * Put a key-value pair into the map.
     */
    void put(String key, int value) {
        int index = key.hashCode() & mask;

        while (keys[index] != null) {
            if (keys[index].equals(key)) {
                values[index] = value;
                return;
            }
            index = (index + 1) & mask;
        }

        keys[index] = key;
        values[index] = value;
    }

    /**
     * Get the value for a key, or EMPTY (-1) if not found.
     */
    int get(String key) {
        int index = key.hashCode() & mask;

        while (keys[index] != null) {
            if (keys[index].equals(key)) {
                return values[index];
            }
            index = (index + 1) & mask;
        }

        return EMPTY;
    }

    /**
     * Check if the map contains the given key.
     */
    boolean containsKey(String key) {
        return get(key) != EMPTY;
    }

    /**
     * Round up to the next power of 2.
     */
    private static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 16) ? 16 : (n >= 1 << 30) ? 1 << 30 : n + 1;
    }
}
