/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.schema.TextColumns;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.SchemaNode;

/// How a leaf decodes to the value a generic accessor hands back, as its physical
/// type and its annotation decide between them.
///
/// Both read paths classify through [#of(PhysicalType, LogicalType)]: `FlatRowReader` once per
/// column at construction, [LeafDecoder] per leaf. Stating the rules here is what keeps them from
/// drifting apart, and it is what lets the string gate on the recording side
/// ([dev.hardwood.internal.reader.batch.BatchExchange]) and the one on the consumer side ask the
/// same question.
enum LeafKind {

    /// A `BYTE_ARRAY` annotated `UTF8`, `ENUM` or `JSON`. Served from the per-chunk
    /// interned-`String` cache rather than decoded per value.
    ///
    /// `ENUM` carries a UTF-8 payload that the Parquet specification tells readers
    /// without a native enum type to interpret as a string, so it decodes exactly
    /// like `UTF8`.
    STRING,

    /// An `INT96` carrying no annotation, which the format leaves unannotated and
    /// readers conventionally treat as a timestamp.
    ///
    /// Keyed on the annotation's absence: one that survives into the schema is one
    /// no physical type is imposed on, since `FileSchema` has already dropped any
    /// that `INT96` cannot carry, so it decides the decode as it does on any other
    /// column.
    INT96_TIMESTAMP,

    /// An unannotated leaf, whose physical value is already the value to hand back.
    RAW,

    /// An annotated leaf, decoded through `LogicalTypeConverter`.
    CONVERT,

    /// Not a leaf at all: a struct, list or map node, whose value the flyweights build
    /// themselves. Named rather than left as a `null` return so a caller switching on
    /// the classification stays exhaustive.
    GROUP;

    /// How a leaf of this physical type and annotation decodes.
    static LeafKind of(PhysicalType type, LogicalType logicalType) {
        if (TextColumns.isAnnotatedText(type, logicalType)) {
            return STRING;
        }
        if (LogicalTypeConverter.isLegacyInt96Timestamp(type, logicalType)) {
            return INT96_TIMESTAMP;
        }
        if (logicalType == null) {
            return RAW;
        }
        return CONVERT;
    }

    /// How the leaf `schema` describes decodes, or [#GROUP] where `schema` is a group
    /// and so carries no leaf decode at all.
    static LeafKind of(SchemaNode schema) {
        return schema instanceof SchemaNode.PrimitiveNode primitive
                ? of(primitive.type(), primitive.logicalType())
                : GROUP;
    }
}
