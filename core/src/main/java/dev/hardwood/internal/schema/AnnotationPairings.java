/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.List;

import dev.hardwood.internal.conversion.Flba12Timestamps;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

import static dev.hardwood.internal.schema.Pairing.Illegal;
import static dev.hardwood.internal.schema.Pairing.Legal;
import static dev.hardwood.internal.schema.Pairing.Fault.GroupAnnotation;
import static dev.hardwood.internal.schema.Pairing.Fault.PrecisionTooLarge;
import static dev.hardwood.internal.schema.Pairing.Fault.WrongPhysicalType;
import static dev.hardwood.internal.schema.Pairing.Fault.WrongWidth;

/// Which pairings of physical type, annotation and width parquet-format defines, and which
/// annotations name an order over their column's values.
///
/// Each question is answered here and nowhere else. The writer refuses a pairing [#check] calls illegal
/// ([LogicalTypeValidator]) and the reader drops the annotation of one
/// (`LogicalTypeConverter.conversionFault`), which is what parquet-format requires of a reader:
/// "readers should ignore both the logical type annotation and column order for that column. Only
/// the physical type information should be used to process the column's data." A column whose
/// annotation [#namesAnOrder] denies has no bounds recorded for it, none read from it, and no
/// ordered predicate admitted on it.
///
/// Both switches are exhaustive with no `default`, so a new annotation does not compile until it is
/// answered, and a pairing the format newly defines is one cell of the first one's grid.
///
/// Every cell is taken from the specification, and each arm cites the sentence that settles it: `LogicalTypes.md` and `parquet.thrift` at
/// parquet-format `bf099392`. `FILE` (union field 19) is absent because [LogicalType] does not model
/// it (#1413); a footer carrying it decodes to an unrecognized union member and the annotation is
/// dropped, which is what the format asks of a reader that does not recognize one.
public final class AnnotationPairings {

    /// The widths parquet-format fixes for an annotation over a `FIXED_LEN_BYTE_ARRAY`.
    private static final int UUID_WIDTH = 16;
    private static final int INTERVAL_WIDTH = 12;
    private static final int FLOAT16_WIDTH = 2;

    private static final List<PhysicalType> BYTE_ARRAY_ONLY = List.of(PhysicalType.BYTE_ARRAY);
    private static final List<PhysicalType> FIXED_ONLY = List.of(PhysicalType.FIXED_LEN_BYTE_ARRAY);
    private static final List<PhysicalType> INT32_ONLY = List.of(PhysicalType.INT32);
    private static final List<PhysicalType> INT64_ONLY = List.of(PhysicalType.INT64);
    private static final List<PhysicalType> TIMESTAMP_TYPES =
            List.of(PhysicalType.INT64, PhysicalType.FIXED_LEN_BYTE_ARRAY);
    private static final List<PhysicalType> DECIMAL_TYPES = List.of(PhysicalType.INT32, PhysicalType.INT64,
            PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);

    private static final Pairing LEGAL = new Legal();

    private AnnotationPairings() {
    }

    /// Whether parquet-format defines an order over the values of a column annotated `annotation`.
    ///
    /// It defines none for `INTERVAL`, `UNKNOWN`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, `LIST` and
    /// `MAP`, and states for `INTERVAL` that no `min` / `max` should be written at all. The writer
    /// records no bounds for such a column and the reader reads none from one, since a bound in an
    /// order a reader cannot know would prune away live rows, and the ordered operators are refused
    /// on it. One answer serves all three, the format stating one rule.
    ///
    /// The switch is exhaustive rather than a list of the annotations without an order, so one
    /// added later has to say which side it falls on.
    ///
    /// @param annotation the column's annotation, `null` for an unannotated column, whose physical
    ///        type names its order
    public static boolean namesAnOrder(LogicalType annotation) {
        if (annotation == null) {
            return true;
        }
        return switch (annotation) {
            case LogicalType.StringType ignored -> true;
            case LogicalType.EnumType ignored -> true;
            case LogicalType.JsonType ignored -> true;
            case LogicalType.BsonType ignored -> true;
            case LogicalType.UuidType ignored -> true;
            case LogicalType.DateType ignored -> true;
            case LogicalType.TimeType ignored -> true;
            case LogicalType.TimestampType ignored -> true;
            case LogicalType.IntType ignored -> true;
            case LogicalType.DecimalType ignored -> true;
            case LogicalType.Float16Type ignored -> true;
            case LogicalType.IntervalType ignored -> false;
            case LogicalType.NullType ignored -> false;
            case LogicalType.VariantType ignored -> false;
            case LogicalType.GeometryType ignored -> false;
            case LogicalType.GeographyType ignored -> false;
            case LogicalType.ListType ignored -> false;
            case LogicalType.MapType ignored -> false;
        };
    }

    /// Whether the format defines `annotation` over a column of this physical type and width.
    ///
    /// A `FIXED_LEN_BYTE_ARRAY` that declares no width is not the annotation's fault: that column
    /// cannot be decoded at all and [FixedWidthValidator] refuses it by name, so a width-fixing
    /// annotation over it is classified as though the width matched.
    ///
    /// @param type the column's physical type
    /// @param typeLength the `FIXED_LEN_BYTE_ARRAY` width, `null` for any other type
    /// @param annotation the column's annotation, `null` for an unannotated column
    public static Pairing check(PhysicalType type, Integer typeLength, LogicalType annotation) {
        if (annotation == null) {
            return LEGAL;
        }
        return switch (annotation) {
            // "may only be used to annotate the BYTE_ARRAY primitive type"
            case LogicalType.StringType ignored -> byteArray(type);
            // "annotates the BYTE_ARRAY primitive type"
            case LogicalType.EnumType ignored -> byteArray(type);
            // "must annotate a BYTE_ARRAY primitive type"
            case LogicalType.JsonType ignored -> byteArray(type);
            case LogicalType.BsonType ignored -> byteArray(type);
            // "Allowed for physical type: BYTE_ARRAY." on both structs, the payload being WKB.
            case LogicalType.GeometryType ignored -> byteArray(type);
            case LogicalType.GeographyType ignored -> byteArray(type);
            // "annotates a 16-byte FIXED_LEN_BYTE_ARRAY primitive type"
            case LogicalType.UuidType ignored -> fixedWidth(type, typeLength, UUID_WIDTH);
            // "must annotate a FIXED_LEN_BYTE_ARRAY of length 12"
            case LogicalType.IntervalType ignored ->
                    fixedWidth(type, typeLength, INTERVAL_WIDTH);
            // "The primitive type is a 2-byte FIXED_LEN_BYTE_ARRAY."
            case LogicalType.Float16Type ignored ->
                    fixedWidth(type, typeLength, FLOAT16_WIDTH);
            // "must annotate an int32 that stores the number of days from the Unix epoch"
            case LogicalType.DateType ignored -> only(type, PhysicalType.INT32, INT32_ONLY);
            // MILLIS "must annotate an int32"; MICROS and NANOS "must annotate an int64".
            case LogicalType.TimeType time -> time.unit() == LogicalType.TimeUnit.MILLIS
                    ? only(type, PhysicalType.INT32, INT32_ONLY)
                    : only(type, PhysicalType.INT64, INT64_ONLY);
            case LogicalType.IntType integer -> integerPairing(type, integer);
            case LogicalType.TimestampType ignored -> timestampPairing(type, typeLength);
            case LogicalType.DecimalType decimal -> decimalPairing(type, typeLength, decimal);
            // "allowed for any physical type, only null values stored": no physical type
            // contradicts it, and the schema alone cannot disprove the claim.
            case LogicalType.NullType ignored -> LEGAL;
            // `LIST` and `MAP` annotate a multi-level structure and `VARIANT` "must annotate a
            // group", so none of the three is a pairing a primitive column can hold.
            case LogicalType.ListType ignored -> new Illegal(new GroupAnnotation());
            case LogicalType.MapType ignored -> new Illegal(new GroupAnnotation());
            case LogicalType.VariantType ignored -> new Illegal(new GroupAnnotation());
        };
    }

    private static Pairing byteArray(PhysicalType type) {
        return only(type, PhysicalType.BYTE_ARRAY, BYTE_ARRAY_ONLY);
    }

    private static Pairing only(PhysicalType actual, PhysicalType required, List<PhysicalType> allowed) {
        return actual == required ? LEGAL : new Illegal(new WrongPhysicalType(allowed));
    }

    /// An annotation parquet-format fixes to one `FIXED_LEN_BYTE_ARRAY` width.
    private static Pairing fixedWidth(PhysicalType type, Integer typeLength, int width) {
        if (type != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return new Illegal(new WrongPhysicalType(FIXED_ONLY));
        }
        return typeLength == null || typeLength == width ? LEGAL : new Illegal(new WrongWidth(width));
    }

    /// "INT(8, true), INT(16, true), and INT(32, true) must annotate an int32 primitive type and
    /// INT(64, true) must annotate an int64", and the same for the unsigned forms. Signedness decides
    /// the order the column's values compare in, not which type carries them.
    private static Pairing integerPairing(PhysicalType type, LogicalType.IntType integer) {
        boolean wide = integer.bitWidth() == 64;
        PhysicalType required = wide ? PhysicalType.INT64 : PhysicalType.INT32;
        return type == required ? LEGAL : new Illegal(new WrongPhysicalType(wide ? INT64_ONLY : INT32_ONLY));
    }

    /// "each value is an int64 or a 12-byte FIXED_LEN_BYTE_ARRAY", the latter a little-endian count
    /// whose range the `INT64` form cannot hold.
    private static Pairing timestampPairing(PhysicalType type, Integer typeLength) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return typeLength == null || typeLength == Flba12Timestamps.WIDTH
                    ? LEGAL
                    : new Illegal(new WrongWidth(Flba12Timestamps.WIDTH));
        }
        return only(type, PhysicalType.INT64, TIMESTAMP_TYPES);
    }

    /// "DECIMAL can be used to annotate the following types: int32, for 1 <= precision <= 9;
    /// int64, for 1 <= precision <= 18; fixed_len_byte_array, precision is limited by the array
    /// size, length n can store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits; byte_array,
    /// precision is not limited". [LogicalTypeValidator#maxDecimalPrecision] counts those digits.
    private static Pairing decimalPairing(PhysicalType type, Integer typeLength,
            LogicalType.DecimalType decimal) {
        boolean held = switch (type) {
            case INT32, INT64, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> true;
            case BOOLEAN, INT96, FLOAT, DOUBLE -> false;
        };
        if (!held) {
            return new Illegal(new WrongPhysicalType(DECIMAL_TYPES));
        }
        // A FIXED_LEN_BYTE_ARRAY with no declared width has no digits to count, and is
        // FixedWidthValidator's to refuse.
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY && (typeLength == null || typeLength <= 0)) {
            return LEGAL;
        }
        long maxPrecision = LogicalTypeValidator.maxDecimalPrecision(type, typeLength);
        return decimal.precision() > maxPrecision ? new Illegal(new PrecisionTooLarge(decimal.precision(), maxPrecision)) : LEGAL;
    }
}
