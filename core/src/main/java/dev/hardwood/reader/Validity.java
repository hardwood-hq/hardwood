/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.BitSet;

import dev.hardwood.Experimental;

/// Per-item null bitmap at a [ColumnReader] scope (a `STRUCT` /
/// `REPEATED` layer or the leaf).
///
/// A `Validity` is one of two shapes:
///
/// - [NoNulls] — every item at that scope is non-null in the current
///   batch. The [#NO_NULLS] singleton, returned for the no-nulls fast
///   path; no per-batch allocation.
/// - [Backed] — a wrapper over a [BitSet] whose bit `i` is set iff
///   item `i` is **not null** (set-bit-= -present storage polarity).
///
/// Consumer-side predicates (`isNull(i)` / `isNotNull(i)` / `hasNulls()`)
/// describe nullability; the storage uses set-bit-= -present internally
/// to match Arrow's layout. The sealed-type shape makes the no-nulls
/// fast path explicit:
/// ```java
/// switch (validity) {
///     case NoNulls n -> // tight loop, skip per-item check
///     case Backed b  -> // checked loop
/// }
/// ```
///
/// **This API is [Experimental]:** the shape may change in future releases.
@Experimental
public sealed interface Validity permits Validity.NoNulls, Validity.Backed {

    /// Singleton signalling "no item at this scope is null in the
    /// current batch." Identity-stable across calls.
    Validity NO_NULLS = new NoNulls();

    /// Wraps a backing [BitSet] (set-bit-= -present storage). Returns
    /// [#NO_NULLS] when `bits` is `null` (the sparse "no nulls"
    /// representation produced by the internal pipeline); otherwise
    /// returns a fresh [Backed] holding the given bitmap. The wrapper
    /// does not copy — callers must not mutate the bitmap after handing
    /// it to a `Validity`.
    static Validity of(BitSet bits) {
        return bits == null ? NO_NULLS : new Backed(bits);
    }

    /// `true` iff at least one item at this scope is null in the current
    /// batch. O(1). May help on hot loops as a per-batch fast-path gate:
    /// ```java
    /// if (!validity.hasNulls()) {
    ///     // tight loop, no per-item check
    /// } else {
    ///     // checked loop
    /// }
    /// ```
    boolean hasNulls();

    /// `true` iff the item at index `i` is null.
    boolean isNull(int i);

    /// `true` iff the item at index `i` is not null.
    boolean isNotNull(int i);

    /// Number of null items in this batch. `count` is the total item
    /// count at this scope — required because [NoNulls] has no
    /// intrinsic length.
    int nullCount(int count);

    /// Index of the next null item in `[from, count)`, or `-1` if every
    /// item in that range is non-null.
    int nextNull(int from, int count);

    /// Index of the next non-null item in `[from, count)`, or `-1` if
    /// every item in that range is null. `count` is the total item count
    /// at this scope — required because [NoNulls] has no intrinsic
    /// length.
    int nextNotNull(int from, int count);

    /// Every item at this scope is non-null in the current batch. Use
    /// [#NO_NULLS] — the constructor is private so identity comparison
    /// against the singleton is stable.
    final class NoNulls implements Validity {
        private NoNulls() {}

        @Override
        public boolean hasNulls() {
            return false;
        }

        @Override
        public boolean isNull(int i) {
            return false;
        }

        @Override
        public boolean isNotNull(int i) {
            return true;
        }

        @Override
        public int nullCount(int count) {
            return 0;
        }

        @Override
        public int nextNull(int from, int count) {
            return -1;
        }

        @Override
        public int nextNotNull(int from, int count) {
            return from < count ? from : -1;
        }
    }

    /// A `Validity` whose per-item nullability is stored in a [BitSet]
    /// (set bit = item is present). Constructed by [Validity#of] when
    /// at least one item is null in the batch. The backing bitmap is
    /// not exposed; consumers query through `isNull` / `isNotNull` /
    /// `nullCount` / `nextNull` / `nextNotNull`.
    final class Backed implements Validity {
        private final BitSet bits;

        Backed(BitSet bits) {
            this.bits = bits;
        }

        @Override
        public boolean hasNulls() {
            return true;
        }

        @Override
        public boolean isNull(int i) {
            return !bits.get(i);
        }

        @Override
        public boolean isNotNull(int i) {
            return bits.get(i);
        }

        @Override
        public int nullCount(int count) {
            return count - bits.cardinality();
        }

        @Override
        public int nextNull(int from, int count) {
            int n = bits.nextClearBit(from);
            return n >= count ? -1 : n;
        }

        @Override
        public int nextNotNull(int from, int count) {
            int n = bits.nextSetBit(from);
            return n >= count ? -1 : n;
        }
    }
}
