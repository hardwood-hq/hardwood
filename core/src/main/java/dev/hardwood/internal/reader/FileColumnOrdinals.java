/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.ResolvedPredicate;

/// Where each leaf of the reference schema sits in one specific file.
///
/// A Parquet footer stores `RowGroup.columns` as a list positionally aligned with
/// the flattened schema leaves, so a leaf's ordinal is a property of the file it
/// was written into, not of the column. A multi-file read resolves projections and
/// predicates once against the first file (the reference schema) and must translate
/// those reference ordinals before indexing into any other file's metadata.
///
/// The translation is by field path: two files agree on a column when the same path
/// carries a compatible leaf, wherever that leaf happens to sit.
///
/// Pruning one file's row groups and pages needs two things in that file's own ordinals, so
/// both travel here: the filter, translated, and which of the file's leaves carry bounds in
/// an order this reader can read — a property of the file that wrote them.
public final class FileColumnOrdinals {

    private final int[] fileOrdinals;
    private final ResolvedPredicate filter;
    private final BoundsReadability boundsReadability;

    private FileColumnOrdinals(int[] fileOrdinals, ResolvedPredicate filter,
            BoundsReadability boundsReadability) {
        this.fileOrdinals = fileOrdinals;
        this.filter = filter;
        this.boundsReadability = boundsReadability;
    }

    /// Mapping for the reference file itself, whose leaf ordinals are the reference
    /// ordinals. Every other file resolves its own mapping through [#of].
    ///
    /// @param referenceLeafCount number of leaf columns in the reference schema
    /// @param filter the filter predicate resolved against the reference schema, or `null`
    /// @param boundsReadability the reference file's bounds readability
    public static FileColumnOrdinals identity(int referenceLeafCount, ResolvedPredicate filter,
            BoundsReadability boundsReadability) {
        int[] ordinals = new int[referenceLeafCount];
        for (int i = 0; i < referenceLeafCount; i++) {
            ordinals[i] = i;
        }
        return new FileColumnOrdinals(ordinals, filter, boundsReadability);
    }

    /// Mapping for a file whose leaf order may differ from the reference schema.
    ///
    /// @param fileOrdinals this file's leaf ordinal per reference leaf ordinal;
    ///        `-1` for reference leaves the file does not carry
    /// @param filter the filter predicate resolved against the reference schema, or `null`
    /// @param boundsReadability this file's bounds readability, by its own leaf ordinals
    static FileColumnOrdinals of(int[] fileOrdinals, ResolvedPredicate filter,
            BoundsReadability boundsReadability) {
        return new FileColumnOrdinals(fileOrdinals,
                filter == null ? null : ResolvedPredicate.remapColumns(filter, fileOrdinals),
                boundsReadability);
    }

    /// This file's leaf ordinal for a reference schema leaf ordinal — the index to
    /// use against `RowGroup.columns` and this file's [dev.hardwood.schema.FileSchema].
    ///
    /// @throws IllegalStateException if the file does not carry that leaf, which
    ///         means it was never validated as a projected column
    public int fileOrdinal(int referenceOrdinal) {
        int ordinal = fileOrdinals[referenceOrdinal];
        if (ordinal < 0) {
            throw new IllegalStateException(
                    "Column " + referenceOrdinal + " of the reference schema is absent from this file");
        }
        return ordinal;
    }

    /// The filter predicate with its column indices translated to this file's
    /// ordinals, or `null` when no filter is set.
    ResolvedPredicate filter() {
        return filter;
    }

    /// Which of this file's leaves carry bounds this reader can read, by this file's ordinals —
    /// the ones [#filter] and [#fileOrdinal] speak in.
    BoundsReadability boundsReadability() {
        return boundsReadability;
    }
}
