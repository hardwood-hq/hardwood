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
/// characters. A name's slot is the low bits of its hash. The table starts at
/// [#MIN_SLOTS_PER_NAME] slots a name, which leaves most names a slot of their own, and since
/// the names are known when the map is built, it doubles the table until every name has one: a
/// lookup is then one mask and one comparison. It doubles up to [#MAX_SLOTS_PER_NAME] slots a
/// name and to no more than [#MAX_GROWN_SLOTS] slots, so a map that grows stays small, and a
/// map that starts larger than that does not grow. Both bounds rise with the number of names,
/// so a map of more names is never given a smaller table to work with. When no size separates
/// the names, the table keeps the largest size tried, where the fewest names share a slot; such a
/// name moves on to the next free one, and a lookup probes on. Another instance than the
/// interned one is found along the same probe sequence by equality.
///
/// A name given more than once maps to [#AMBIGUOUS]: of sibling fields of one name, which
/// Parquet does not forbid, none is reachable by that name. Both answers that are not an index
/// are negative, so a caller tells them apart only on its miss branch.
public final class InternedNameMap {

    /// What [#get] answers for a name that is not in the map.
    public static final int NOT_FOUND = -1;

    /// What [#get] answers for a name given more than once, the same marker
    /// [StringToIntMap#putOrMarkAmbiguous] leaves.
    public static final int AMBIGUOUS = StringToIntMap.AMBIGUOUS;

    /// The table's size to start from, in slots a name. At this size four names in five or more
    /// already have a slot of their own.
    private static final int MIN_SLOTS_PER_NAME = 4;

    /// The most slots a name the table grows to. Each doubling halves the names that share a slot
    /// and doubles the memory, and past this it buys a name or two of a hundred.
    private static final int MAX_SLOTS_PER_NAME = 16;

    /// The most slots the table grows to, 32 KB of keys and values. Past a few hundred names no
    /// size up to this separates them all, so a larger map keeps the size it starts at.
    private static final int MAX_GROWN_SLOTS = 4096;

    private final String[] keys;
    private final int[] values;
    private final int mask;

    /// Maps each of `names` to its index in the array; a name given more than once to [#AMBIGUOUS].
    public InternedNameMap(String... names) {
        String[] interned = new String[names.length];
        for (int i = 0; i < names.length; i++) {
            interned[i] = names[i].intern();
        }
        int minSlots = minSlots(names.length);
        int maxSlots = maxSlots(names.length);
        Table table = separated(interned, minSlots, maxSlots);
        if (table == null) {
            table = probed(interned, maxSlots);
        }
        this.keys = table.keys();
        this.values = table.values();
        this.mask = keys.length - 1;
    }

    /// The table size the map starts at for `names` names.
    static int minSlots(int names) {
        return tableSizeFor(Math.multiplyExact(Math.max(1, names), MIN_SLOTS_PER_NAME));
    }

    /// The largest table size the map grows to for `names` names: [#MAX_SLOTS_PER_NAME] slots a
    /// name, no more than [#MAX_GROWN_SLOTS], and never less than [#minSlots].
    static int maxSlots(int names) {
        int grown = tableSizeFor(Math.multiplyExact(Math.max(1, names), MAX_SLOTS_PER_NAME));
        return Math.max(minSlots(names), Math.min(grown, MAX_GROWN_SLOTS));
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
    /// maps to [#AMBIGUOUS].
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
                values[slot] = AMBIGUOUS;
            }
            else {
                return null;
            }
        }
        return new Table(keys, values);
    }

    /// `interned` placed in a table of `slots` slots, a name whose slot is taken moving on to the
    /// next free one. A name given more than once maps to [#AMBIGUOUS].
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
            values[slot] = keys[slot] == name ? AMBIGUOUS : i;
            keys[slot] = name;
        }
        return new Table(keys, values);
    }

    /// The power of two at or above `cap`, and at least 16.
    private static int tableSizeFor(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        return Math.max(16, n + 1);
    }
}
