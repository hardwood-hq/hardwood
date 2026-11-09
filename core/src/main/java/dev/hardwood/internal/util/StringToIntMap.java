/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

/// A simple open-addressed hash map with linear probing for String -> int mappings.
/// Optimized for small to medium-sized immutable maps (like column name lookups).
///
/// Uses open addressing for better cache locality compared to HashMap's separate chaining.
public final class StringToIntMap {

    private static final int EMPTY = -1;

    /// The value [#putOrMarkAmbiguous] leaves for a key put more than once.
    public static final int AMBIGUOUS = -2;

    private final String[] keys;
    private final int[] values;
    private final int mask;

    /// Create a map with the given capacity (will be rounded up to next power of 2).
    public StringToIntMap(int expectedSize) {
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

    /// Put a key-value pair into the map.
    public void put(String key, int value) {
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

    /// Get the value for a key, or EMPTY (-1) if not found.
    public int get(String key) {
        int index = key.hashCode() & mask;

        while (keys[index] != null) {
            if (keys[index].equals(key)) {
                return values[index];
            }
            index = (index + 1) & mask;
        }

        return EMPTY;
    }

    /// Puts `key` with `value`, or marks it [#AMBIGUOUS] when the map already holds it: two fields
    /// of one name, which Parquet allows, leave neither reachable by that name.
    public void putOrMarkAmbiguous(String key, int value) {
        put(key, containsKey(key) ? AMBIGUOUS : value);
    }

    /// The message of the exception a by-name accessor throws for a name [#AMBIGUOUS] in its map.
    public static String ambiguousFieldMessage(String name) {
        return "Field name '" + name + "' is ambiguous: more than one field has it; access the field by index";
    }

    /// Check if the map contains the given key.
    public boolean containsKey(String key) {
        return get(key) != EMPTY;
    }

    /// Round up to the next power of 2.
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
