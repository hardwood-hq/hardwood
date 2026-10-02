/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.SchemaElement;

/// The annotation a footer gives a primitive [SchemaElement], and whether the reader drops it.
///
/// `FileSchema` builds each column from these, and `BoundsReadability` asks the same question of
/// the footer to find the columns whose bounds were recorded in the order of a dropped
/// annotation. Both answer from here, so they cannot disagree about which columns those are.
public final class LeafAnnotation {

    private LeafAnnotation() {
    }

    /// Resolve the effective logical type of a primitive element, falling back
    /// to the legacy `converted_type` annotation when the modern logical-type
    /// union is absent. Older writers (parquet-mr, Spark, Hive) only set
    /// `converted_type`, which would otherwise leave the column read as a bare
    /// physical column and decoded wrong or not at all (e.g. a `DECIMAL` read as
    /// an unscaled integer, a `DATE` as a raw `INT32`).
    ///
    /// When both annotations are present the modern `logicalType()` wins, except for
    /// `UNKNOWN`: beside a `converted_type` it yields to it. `UNKNOWN` states that the column
    /// stores only nulls, and a converted type beside it is a writer's stand-in for an
    /// annotation the union cannot express; parquet-java writes `INTERVAL` this way. Only
    /// primitive-level annotations are mapped here; the group-level `LIST`, `MAP`,
    /// and `MAP_KEY_VALUE` map to `null`.
    public static LogicalType effective(SchemaElement element) {
        if (!convertedTypeDecides(element)) {
            return element.logicalType();
        }
        return switch (element.convertedType()) {
            case UTF8 -> LogicalType.string();
            case ENUM -> LogicalType.enumType();
            case JSON -> LogicalType.json();
            case BSON -> LogicalType.bson();
            case INTERVAL -> LogicalType.interval();
            case DATE -> LogicalType.date();
            case DECIMAL -> decimalFromElement(element);
            // The parquet-format backward-compatibility rule maps the legacy
            // TIME_*/TIMESTAMP_* converted types to isAdjustedToUTC=true; these
            // annotations always denoted UTC-normalized values.
            case TIME_MILLIS -> LogicalType.time(true, LogicalType.TimeUnit.MILLIS);
            case TIME_MICROS -> LogicalType.time(true, LogicalType.TimeUnit.MICROS);
            case TIMESTAMP_MILLIS -> LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS);
            case TIMESTAMP_MICROS -> LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS);
            case INT_8 -> LogicalType.intType(8, true);
            case INT_16 -> LogicalType.intType(16, true);
            case INT_32 -> LogicalType.intType(32, true);
            case INT_64 -> LogicalType.intType(64, true);
            case UINT_8 -> LogicalType.intType(8, false);
            case UINT_16 -> LogicalType.intType(16, false);
            case UINT_32 -> LogicalType.intType(32, false);
            case UINT_64 -> LogicalType.intType(64, false);
            // Group-level annotations are handled on GroupNode, not here.
            case LIST, MAP, MAP_KEY_VALUE -> null;
        };
    }

    /// Why the element's physical type cannot carry its [effective][#effective(SchemaElement)]
    /// annotation, which the reader then drops; `null` where the element has no annotation or
    /// its physical type carries it.
    public static String dropFault(SchemaElement element) {
        LogicalType annotation = effective(element);
        if (annotation == null) {
            return null;
        }
        if (convertedTypeDecides(element)
                && AnnotationPairings.checkConverted(element.type(), element.convertedType())
                        instanceof Pairing.Illegal(Pairing.Fault.WrongPhysicalType wrong)) {
            return LogicalTypeConverter.readsFrom(element.convertedType().name(), wrong.allowed(), element.type());
        }
        return LogicalTypeConverter.conversionFault(element.type(), element.typeLength(), annotation);
    }

    /// Whether the element's `converted_type`, rather than its `logicalType`, decides its annotation.
    private static boolean convertedTypeDecides(SchemaElement element) {
        return convertedTypeDecides(element.convertedType(), element.logicalType());
    }

    /// Whether a `converted_type` decides an element's annotation beside this `LogicalType` union:
    /// where the union is absent, holds a member this release does not recognize (which reads as
    /// absent), or is `UNKNOWN`, which yields to a converted type as [#effective] describes.
    ///
    /// @param convertedType the element's `converted_type`, `null` where it has none
    /// @param union the element's union member as read, `null` where it is absent or unrecognized
    public static boolean convertedTypeDecides(ConvertedType convertedType, LogicalType union) {
        return convertedType != null && (union == null || union instanceof LogicalType.NullType);
    }

    /// Build a [LogicalType.DecimalType] from a legacy `DECIMAL` converted-type
    /// element, reading `scale`/`precision` off the schema element. A missing
    /// scale defaults to `0`; a missing precision is a malformed schema.
    private static LogicalType.DecimalType decimalFromElement(SchemaElement element) {
        if (element.precision() == null) {
            throw new IllegalArgumentException(
                    "DECIMAL converted type requires a precision: " + element.name());
        }
        int scale = element.scale() != null ? element.scale() : 0;
        return LogicalType.decimal(element.precision(), scale);
    }
}
