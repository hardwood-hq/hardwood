/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.SchemaElement;

/// The annotation fields a schema node writes to its [SchemaElement]: the modern `LogicalType`
/// union and, where one exists, the legacy `converted_type` with the sibling `scale` and
/// `precision` a `DECIMAL` needs.
///
/// parquet-format requires both: a writer must always write the union where applicable, and
/// must also write the corresponding `converted_type` so pre-union readers still see the
/// annotation. Deriving one from the other here is what makes that a single declaration for the
/// caller. This is the inverse of [LeafAnnotation#effective], which collapses a legacy
/// annotation into a `LogicalType` when a file is read.
///
/// @param union the `LogicalType` union member to write, or `null` if the annotation has none
/// @param convertedType the legacy annotation to write, or `null` if the annotation has none
/// @param scale a `DECIMAL`'s scale, `null` otherwise
/// @param precision a `DECIMAL`'s precision, `null` otherwise
public record LogicalTypeAnnotations(LogicalType union, ConvertedType convertedType, Integer scale,
                                     Integer precision) {

    /// An unannotated node.
    public static final LogicalTypeAnnotations NONE = new LogicalTypeAnnotations(null, null, null, null);

    /// The annotations written for a primitive column of `physicalType` carrying `logicalType`.
    ///
    /// A legacy annotation is written only over a physical type [AnnotationPairings#checkConverted]
    /// defines it on, which leaves a `TIMESTAMP` over a `FIXED_LEN_BYTE_ARRAY(12)` union-only.
    public static LogicalTypeAnnotations of(PhysicalType physicalType, LogicalType logicalType) {
        LogicalTypeAnnotations annotations = of(logicalType);
        if (annotations.convertedType() != null
                && AnnotationPairings.checkConverted(physicalType, annotations.convertedType()) instanceof Pairing.Illegal) {
            return unionOnly(logicalType);
        }
        return annotations;
    }

    /// The annotations written for a logical type, whatever physical type carries it: the union
    /// member where the union has one and the legacy annotation [AnnotationKind#convertedType]
    /// names. `INTERVAL` writes only the legacy annotation, because parquet.thrift reserves union
    /// field 9 for it without ever defining the member struct.
    private static LogicalTypeAnnotations of(LogicalType logicalType) {
        if (logicalType == null) {
            return NONE;
        }
        AnnotationKind kind = AnnotationKind.of(logicalType);
        LogicalType union = kind.hasUnionMember() ? logicalType : null;
        ConvertedType convertedType = kind.convertedType(logicalType);
        if (logicalType instanceof LogicalType.DecimalType decimal) {
            return new LogicalTypeAnnotations(union, convertedType, decimal.scale(), decimal.precision());
        }
        return new LogicalTypeAnnotations(union, convertedType, null, null);
    }

    /// The annotations written for a group, which the schema model may hold in either
    /// representation: a group built by the writer carries a `LogicalType`, while one read from
    /// a file may carry only the legacy `LIST` / `MAP`. Either way both are written back out.
    ///
    /// The deprecated `MAP_KEY_VALUE` has no union member and is passed through unchanged.
    ///
    /// @throws IllegalArgumentException if either annotation is one the format does not define
    ///         over a group ([AnnotationKind#annotatesGroup()],
    ///         [AnnotationPairings#annotatesGroup(ConvertedType)]), which the reader drops and the
    ///         writer never writes
    public static LogicalTypeAnnotations ofGroup(ConvertedType convertedType, LogicalType logicalType) {
        if (logicalType != null) {
            if (!AnnotationKind.of(logicalType).annotatesGroup()) {
                throw new IllegalArgumentException(logicalType + " annotates a primitive, not a group");
            }
            return of(logicalType);
        }
        if (convertedType == null) {
            return NONE;
        }
        if (!AnnotationPairings.annotatesGroup(convertedType)) {
            throw new IllegalArgumentException(convertedType + " annotates a primitive, not a group");
        }
        return switch (convertedType) {
            case LIST -> both(LogicalType.list(), ConvertedType.LIST);
            case MAP -> both(LogicalType.map(), ConvertedType.MAP);
            default -> new LogicalTypeAnnotations(null, convertedType, null, null);
        };
    }

    private static LogicalTypeAnnotations both(LogicalType union, ConvertedType convertedType) {
        return new LogicalTypeAnnotations(union, convertedType, null, null);
    }

    private static LogicalTypeAnnotations unionOnly(LogicalType union) {
        return new LogicalTypeAnnotations(union, null, null, null);
    }
}
