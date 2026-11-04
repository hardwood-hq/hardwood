/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

/// A map from a fixed set of names to their indexes, for names a caller looks up per value, such
/// as the field names of a row reader's named accessors.
///
/// Each name is held interned, so a name the caller passes as a string literal (or any interned
/// string) is the very instance the table holds and is found by identity, without comparing
/// characters. A name's slot is the low bits of its hash. Since the names are known when the map
/// is built, it doubles the table, up to [#MAX_SLOTS_PER_NAME] slots a name, until every name has
/// a slot of its own: a lookup is then one mask and one comparison. When no size separates them
/// (two names of one hash code), the table keeps the largest size, where the fewest names share a
/// slot; such a name moves on to the next free one, and a lookup probes on. Above
/// [#MAX_PERFECT_NAMES] names the table stays at its smallest size. Another instance than the
/// interned one is found along the same probe sequence by equality.
///
/// A name given more than once maps to its last index, so that of sibling fields of one name,
/// which Parquet does not forbid, a lookup by name reaches the last.
public final class InternedNameMap {

    /// What [#get] answers for a name that is not in the map.
    public static final int NOT_FOUND = -1;

    /// The most names for which the map grows its table: the table that gives every name a slot
    /// of its own grows with the square of the names, and past this rarely fits in
    /// [#MAX_SLOTS_PER_NAME] slots a name.
    private static final int MAX_PERFECT_NAMES = 128;
    private static final int MAX_SLOTS_PER_NAME = 64;
    private static final int MIN_SLOTS_PER_NAME = 4;

    private final String[] keys;
    private final int[] values;
    private final int mask;

    /// Maps each of `names` to its index in the array; a name given more than once to its last.
    public InternedNameMap(String... names) {
        String[] interned = new String[names.length];
        for (int i = 0; i < names.length; i++) {
            interned[i] = names[i].intern();
        }
        int minSlots = tableSizeFor(Math.multiplyExact(Math.max(1, names.length), MIN_SLOTS_PER_NAME));
        int maxSlots = names.length <= MAX_PERFECT_NAMES
                ? tableSizeFor(Math.max(minSlots, names.length * MAX_SLOTS_PER_NAME))
                : minSlots;
        Table table = separated(interned, minSlots, maxSlots);
        if (table == null) {
            table = probed(interned, maxSlots);
        }
        this.keys = table.keys();
        this.values = table.values();
        this.mask = keys.length - 1;
    }

    /// A filled table: each name and its index, at the same slot.
    private record Table(String[] keys, int[] values) {
    }

    /// Gets the index of `name`, or [#NOT_FOUND] if it is not in the map.
    public int get(String name) {
        int slot = slotOf(name);
        if (keys[slot] == name) {
            return values[slot];
        }
        return probe(name, slot);
    }

    private int slotOf(String name) {
        return name.hashCode() & mask;
    }

    /// Walks the probe sequence from `slot` for a name past the slot its hash selects, or for
    /// another instance than the interned one: [String#equals] tests identity before it compares.
    private int probe(String name, int slot) {
        while (keys[slot] != null) {
            if (keys[slot].equals(name)) {
                return values[slot];
            }
            slot = (slot + 1) & mask;
        }
        return NOT_FOUND;
    }

    /// The table of the smallest size, from `minSlots` up to `maxSlots`, in which every distinct
    /// name of `interned` has the slot its hash selects to itself; `null` if there is none. The
    /// placement that finds the size is the table.
    private static Table separated(String[] interned, int minSlots, int maxSlots) {
        for (int slots = minSlots; slots <= maxSlots; slots <<= 1) {
            Table table = placedApart(interned, slots);
            if (table != null) {
                return table;
            }
        }
        return null;
    }

    /// `interned` placed in a table of `slots` slots, each name at the slot its hash selects, or
    /// `null` at the first two distinct names that select one slot. A name given more than once
    /// maps to its last index.
    private static Table placedApart(String[] interned, int slots) {
        String[] keys = new String[slots];
        int[] values = new int[slots];
        int mask = slots - 1;
        for (int i = 0; i < interned.length; i++) {
            String name = interned[i];
            int slot = name.hashCode() & mask;
            if (keys[slot] == null) {
                keys[slot] = name;
                values[slot] = i;
            }
            else if (keys[slot] == name) {
                values[slot] = i;
            }
            else {
                return null;
            }
        }
        return new Table(keys, values);
    }

    /// `interned` placed in a table of `slots` slots, a name whose slot is taken moving on to the
    /// next free one. A name given more than once maps to its last index.
    private static Table probed(String[] interned, int slots) {
        String[] keys = new String[slots];
        int[] values = new int[slots];
        int mask = slots - 1;
        for (int i = 0; i < interned.length; i++) {
            String name = interned[i];
            int slot = name.hashCode() & mask;
            while (keys[slot] != null && keys[slot] != name) {
                slot = (slot + 1) & mask;
            }
            keys[slot] = name;
            values[slot] = i;
        }
        return new Table(keys, values);
    }

    /// The power of two at or above `cap`, and at least 16.
    private static int tableSizeFor(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        return Math.max(16, n + 1);
    }
}
