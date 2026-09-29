/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Marker for `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` typed [ColumnBatchMatcher]s. Implementations
/// cast `batch.values` to [dev.hardwood.internal.reader.BinaryBatchValues] and compare each value's
/// `bytes[offsets[i], offsets[i + 1])` slice against the literal in place, without materialising a
/// `byte[]` per row.
///
/// A null row's slot holds no value — an empty slice for `BYTE_ARRAY`, undefined scratch bytes for
/// `FIXED_LEN_BYTE_ARRAY` — so its answer must never reach the output. The byte-wise matchers skip
/// null rows before comparing; the short-value matcher compares every slot and clears the null rows'
/// bits afterward. Either way, a null row's bit ends clear.
///
/// The literal's [ResolvedPredicate.BinaryPredicate.Comparison] decides both the order the bytes
/// compare in and whether equality may be byte equality; implementations take it at construction.
public non-sealed interface BinaryBatchMatcher extends ColumnBatchMatcher {

    /// Tests one non-null value in `bytes[from, to)` with the same semantics as
    /// [#test]. Dictionary-aware evaluation uses this operation once per
    /// referenced entry; rows without a dictionary entry use it on their packed
    /// batch slice.
    boolean testValue(byte[] bytes, int from, int to);
}
