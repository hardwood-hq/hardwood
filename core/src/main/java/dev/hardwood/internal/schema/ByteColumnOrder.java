/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

/// The order the values of a byte-stored column sort in, as `parquet.thrift`'s `ColumnOrder`
/// gives it per annotation. It is the one representation of a byte order: [AnnotationKind] states
/// it per annotation, [AnnotationPairings#byteColumnOrder] picks it for a column, the writer
/// collects bounds in it, a predicate's `Comparison` names one, and `BinaryComparator` compares
/// byte slices in each that has a slice comparison.
public enum ByteColumnOrder {
    /// Unsigned byte-wise: the stored bytes order as the values do.
    BYTES,
    /// The number a big-endian two's complement encodes, the shorter value sign-extended: a
    /// `DECIMAL`.
    SIGNED_BIG_ENDIAN,
    /// The count a little-endian two's complement of one width encodes: a
    /// `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`.
    SIGNED_LITTLE_ENDIAN,
    /// The IEEE half a `FLOAT16` encodes, compared as that float rather than as a slice.
    HALF_FLOAT,
    /// The instant a legacy `INT96` timestamp encodes: whole days, then nanoseconds.
    INT96_INSTANT,
    /// No order: the annotation names none, as [AnnotationKind#namesAnOrder()] answers.
    NONE
}
