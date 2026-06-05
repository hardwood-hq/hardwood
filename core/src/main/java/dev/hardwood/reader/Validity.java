/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.Experimental;
import dev.hardwood.internal.reader.BackedValidity;
import dev.hardwood.internal.reader.NoNullsValidity;

/// Per-item null bitmap at a [ColumnReader] scope (a `STRUCT` /
/// `REPEATED` layer or the leaf).
///
/// A `Validity` is one of two shapes:
///
/// - **No nulls** — every item at that scope is non-null in the current
///   batch. The [#NO_NULLS] singleton, returned for the no-nulls fast
///   path; no per-batch allocation.
/// - **Backed** — a packed `long[]` bitmap with **set-bit = present**
///   polarity: bit `i` is set iff item `i` is present (non-null). Word
///   `w` covers items `[w*64, w*64+64)`, low bit = lowest item.
///
/// Consumer-side predicates (`isNull(i)` / `isNotNull(i)` / `hasNulls()`)
/// describe nullability; the storage uses set-bit = present internally.
/// [#hasNulls()] makes the no-nulls fast path explicit:
/// ```java
/// if (!validity.hasNulls()) {
///     // tight loop, skip per-item check
/// } else {
///     // checked loop
/// }
/// ```
///
/// **This API is [Experimental]:** the shape may change in future releases.
@Experimental
public interface Validity {

    /// Singleton signalling "no item at this scope is null in the
    /// current batch." Identity-stable across calls.
    Validity NO_NULLS = NoNullsValidity.INSTANCE;

    /// Wraps a packed `long[]` bitmap (set-bit = present storage).
    /// Returns [#NO_NULLS] when `words` is `null` (the sparse "no nulls"
    /// representation produced by the internal pipeline); otherwise
    /// returns a fresh backed instance holding the given bitmap. The
    /// wrapper does not copy — callers must not mutate the bitmap after
    /// handing it to a `Validity`.
    ///
    /// The caller is responsible for sizing the array to at least
    /// `(count + 63) >>> 6` words for any `count` they later pass to
    /// [#nullCount] / [#nextNull] / [#nextNotNull], and for keeping
    /// indices into [#isNull] / [#isNotNull] within the same bound.
    static Validity of(long[] words) {
        return words == null ? NO_NULLS : new BackedValidity(words);
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
    /// count at this scope — required because the no-nulls shape has no
    /// intrinsic length.
    int nullCount(int count);

    /// Index of the next null item in `[from, count)`, or `-1` if every
    /// item in that range is non-null.
    int nextNull(int from, int count);

    /// Index of the next non-null item in `[from, count)`, or `-1` if
    /// every item in that range is null. `count` is the total item count
    /// at this scope — required because the no-nulls shape has no
    /// intrinsic length.
    int nextNotNull(int from, int count);

    /// The word array (set-bit = present polarity). Returns `null` when
    /// there are no nulls. Otherwise returns the backing array directly —
    /// no copy. Callers must not mutate it; mirroring the inbound contract
    /// on [#of(long[])], the `Validity` owns the bitmap once handed in.
    /// Bits at indices `>= count` are undefined and must not be read.
    long[] words();
}
