/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.math.BigDecimal;
import java.util.List;

import dev.hardwood.internal.conversion.FixedWidths;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

import static dev.hardwood.internal.schema.Pairing.Illegal;
import static dev.hardwood.internal.schema.Pairing.Legal;
import static dev.hardwood.internal.schema.Pairing.Fault.GroupAnnotation;
import static dev.hardwood.internal.schema.Pairing.Fault.PrecisionTooLarge;
import static dev.hardwood.internal.schema.Pairing.Fault.WrongPhysicalType;
import static dev.hardwood.internal.schema.Pairing.Fault.WrongWidth;

/// The rules parquet-format states about an annotation together with a column's physical type,
/// its width or a legacy `converted_type`: which pairings it defines, which byte order a
/// byte-stored column sorts in, whether a file's `column_orders` entry names an order a column
/// has, how many digits a `DECIMAL` carrier holds, and which annotations a group keeps.
///
/// A fact about an annotation alone (whether it names an order, annotates a group, holds text or
/// compares unsigned) is [AnnotationKind]'s, and the rules here read it from there. Each rule is
/// answered here and nowhere else. The writer refuses a pairing [#check] calls illegal
/// ([LogicalTypeValidator]) and the reader drops the annotation of one
/// (`LogicalTypeConverter.conversionFault`), which is what parquet-format requires of a reader:
/// "readers should ignore both the logical type annotation and column order for that column. Only
/// the physical type information should be used to process the column's data."
///
/// Every cell of the grid is taken from the specification, and each rule cites the sentence that
/// settles it: `LogicalTypes.md` and `parquet.thrift` at parquet-format `bf099392`. Each
/// annotation's constant in [AnnotationKind] names the rule it is defined by.
public final class AnnotationPairings {

    private static final List<PhysicalType> TIMESTAMP_TYPES =
            List.of(PhysicalType.INT64, PhysicalType.FIXED_LEN_BYTE_ARRAY);
    private static final List<PhysicalType> DECIMAL_TYPES = List.of(PhysicalType.INT32, PhysicalType.INT64,
            PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);

    static final Pairing LEGAL = new Legal();

    /// `log10(2)` to forty places, which makes [#maxDecimalPrecision(int)] exact for every width an
    /// `i32` can declare: `(8 * length - 1) * log10(2)` never comes within `1e-11` of an
    /// integer there, and this constant's error at the widest width is below `2e-30`.
    private static final BigDecimal LOG10_2 = new BigDecimal("0.3010299956639811952137388947244930267681");

    private AnnotationPairings() {
    }

    /// The order the values of a column stored as `INT96`, `BYTE_ARRAY` or
    /// `FIXED_LEN_BYTE_ARRAY` sort in. The writer collects a byte column's bounds in it and the
    /// resolver compares a byte literal in it, so the two cannot disagree about a column. An
    /// annotated column's order is the one [AnnotationKind] states for its annotation.
    ///
    /// @param type the column's physical type, one stored as bytes
    /// @param annotation the column's annotation, `null` for an unannotated column
    /// @throws IllegalArgumentException if `type` is not stored as bytes, or `annotation` is one
    ///         defined over no byte-stored type
    public static ByteColumnOrder byteColumnOrder(PhysicalType type, LogicalType annotation) {
        switch (type) {
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
            }
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE ->
                    throw new IllegalArgumentException(type + " is not stored as bytes");
        }
        // An INT96's twelve bytes are the legacy timestamp's, whatever else annotates it: NULL is
        // the one annotation the grid keeps on one, and it changes nothing a byte literal compares.
        if (type == PhysicalType.INT96) {
            return ByteColumnOrder.INT96_INSTANT;
        }
        if (annotation == null) {
            return ByteColumnOrder.BYTES;
        }
        ByteColumnOrder order = AnnotationKind.of(annotation).byteOrder();
        if (order == null) {
            throw new IllegalArgumentException(annotation + " is not defined over " + type);
        }
        return order;
    }

    /// Whether a file's `column_orders` entry names an order the values of a column of this
    /// physical type and annotation have, so that the bounds recorded in it can be read.
    ///
    /// `TYPE_ORDER` is the order [AnnotationKind#namesAnOrder(LogicalType)] answers for. `IEEE_754_TOTAL_ORDER` is defined
    /// for "columns of physical type FLOAT or DOUBLE, or logical type FLOAT16" alone, and an
    /// order this release does not recognize names nothing it can read.
    ///
    /// @param order the column's entry, [ColumnOrder#TYPE_DEFINED_ORDER] where the file has none
    public static boolean namesAnOrder(ColumnOrder order, PhysicalType type, LogicalType annotation) {
        return switch (order) {
            case TYPE_DEFINED_ORDER -> AnnotationKind.namesAnOrder(annotation);
            case IEEE754_TOTAL_ORDER -> annotation == null
                    ? type == PhysicalType.FLOAT || type == PhysicalType.DOUBLE
                    : annotation instanceof LogicalType.Float16Type;
            case UNKNOWN -> false;
        };
    }

    /// Whether the format defines `annotation` over a column of this physical type and width.
    ///
    /// A `FIXED_LEN_BYTE_ARRAY` that declares no width, or one that is not positive, is not the
    /// annotation's fault: that column cannot be decoded at all and [FixedWidthValidator] refuses
    /// it by name, so a width-fixing annotation over it is classified as though the width matched.
    ///
    /// @param type the column's physical type
    /// @param typeLength the `FIXED_LEN_BYTE_ARRAY` width, `null` for any other type
    /// @param annotation the column's annotation, `null` for an unannotated column
    public static Pairing check(PhysicalType type, Integer typeLength, LogicalType annotation) {
        if (annotation == null) {
            return LEGAL;
        }
        return AnnotationKind.of(annotation).pairing(type, typeLength, annotation);
    }

    /// Whether the format defines the legacy `converted_type` over a column of this physical type,
    /// beyond what [#check] answers for the logical type it stands for.
    ///
    /// Every converted type is defined over the physical types of its logical counterpart but two:
    /// `TIMESTAMP_MILLIS` and `TIMESTAMP_MICROS` "must annotate an int64", while the `TIMESTAMP`
    /// they stand for is also carried by a `FIXED_LEN_BYTE_ARRAY(12)`. The reader drops such a
    /// legacy annotation standing alone on anything else, and the writer writes a `TIMESTAMP` over
    /// a `FIXED_LEN_BYTE_ARRAY(12)` without one.
    ///
    /// @param type the column's physical type
    /// @param converted the column's converted type
    public static Pairing checkConverted(PhysicalType type, ConvertedType converted) {
        return switch (converted) {
            // "Like the logical type counterpart, it must annotate an int64."
            case TIMESTAMP_MILLIS, TIMESTAMP_MICROS -> only(type, PhysicalType.INT64);
            case UTF8, MAP, MAP_KEY_VALUE, LIST, ENUM, DECIMAL, DATE, TIME_MILLIS, TIME_MICROS,
                 UINT_8, UINT_16, UINT_32, UINT_64, INT_8, INT_16, INT_32, INT_64, JSON, BSON,
                 INTERVAL -> LEGAL;
        };
    }

    /// Whether the format defines the legacy `converted` over a group: `LIST` and `MAP` on the
    /// outer group, and `MAP_KEY_VALUE` on the repeated group inside a legacy `MAP`.
    public static boolean annotatesGroup(ConvertedType converted) {
        return switch (converted) {
            case LIST, MAP, MAP_KEY_VALUE -> true;
            case UTF8, ENUM, DECIMAL, DATE, TIME_MILLIS, TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS,
                 UINT_8, UINT_16, UINT_32, UINT_64, INT_8, INT_16, INT_32, INT_64, JSON, BSON,
                 INTERVAL -> false;
        };
    }

    /// Whether a group's legacy `converted` states the structure its `annotation` does, the
    /// one a writer writes beside it. Where they differ, the annotation decides, as it does on a
    /// primitive, and the converted type is dropped.
    public static boolean convertedAgrees(LogicalType annotation, ConvertedType converted) {
        return annotation instanceof LogicalType.ListType && converted == ConvertedType.LIST
                || annotation instanceof LogicalType.MapType && converted == ConvertedType.MAP;
    }

    /// A group's annotation as the reader keeps it: `annotation` where
    /// [AnnotationKind#annotatesGroup()] holds, `null` otherwise. `FileSchema` and
    /// `BareRepeatedGroups` both read a group through this and [#readableGroupConvertedType], so
    /// they cannot disagree about which structure it is.
    public static LogicalType readableGroupAnnotation(LogicalType annotation) {
        return annotation != null && AnnotationKind.of(annotation).annotatesGroup() ? annotation : null;
    }

    /// A group's converted type as the reader keeps it: `converted` where it annotates a group
    /// and, beside a readable annotation, names the same structure; `null` otherwise.
    ///
    /// @param readableAnnotation the group's annotation as [#readableGroupAnnotation] keeps it
    public static ConvertedType readableGroupConvertedType(LogicalType readableAnnotation, ConvertedType converted) {
        if (converted == null || !annotatesGroup(converted)) {
            return null;
        }
        return readableAnnotation == null || convertedAgrees(readableAnnotation, converted) ? converted : null;
    }

    /// An annotation defined over a `BYTE_ARRAY` alone.
    static Pairing byteArray(PhysicalType type) {
        return only(type, PhysicalType.BYTE_ARRAY);
    }

    /// An annotation defined over `required` alone.
    static Pairing only(PhysicalType actual, PhysicalType required) {
        return actual == required ? LEGAL : new Illegal(new WrongPhysicalType(List.of(required)));
    }

    /// An annotation of a group, which no primitive column holds.
    static Pairing groupAnnotation() {
        return new Illegal(new GroupAnnotation());
    }

    /// Whether a `FIXED_LEN_BYTE_ARRAY` declares a width its values can have. One that does not is
    /// [FixedWidthValidator]'s to refuse, whatever annotates it.
    private static boolean hasUsableWidth(Integer typeLength) {
        return typeLength != null && typeLength > 0;
    }

    /// An annotation parquet-format fixes to one `FIXED_LEN_BYTE_ARRAY` width.
    static Pairing fixedWidth(PhysicalType type, Integer typeLength, int width) {
        if (type != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return new Illegal(new WrongPhysicalType(List.of(PhysicalType.FIXED_LEN_BYTE_ARRAY)));
        }
        return !hasUsableWidth(typeLength) || typeLength == width ? LEGAL : new Illegal(new WrongWidth(width));
    }

    /// "INT(8, true), INT(16, true), and INT(32, true) must annotate an int32 primitive type and
    /// INT(64, true) must annotate an int64", and the same for the unsigned forms. Signedness decides
    /// the order the column's values compare in, not which type carries them.
    static Pairing integerPairing(PhysicalType type, LogicalType.IntType integer) {
        return only(type, integer.bitWidth() == 64 ? PhysicalType.INT64 : PhysicalType.INT32);
    }

    /// "each value is an int64 or a 12-byte FIXED_LEN_BYTE_ARRAY", the latter a little-endian count
    /// whose range the `INT64` form cannot hold.
    static Pairing timestampPairing(PhysicalType type, Integer typeLength) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return !hasUsableWidth(typeLength) || typeLength == FixedWidths.FLBA12_TIMESTAMP
                    ? LEGAL
                    : new Illegal(new WrongWidth(FixedWidths.FLBA12_TIMESTAMP));
        }
        return type == PhysicalType.INT64 ? LEGAL : new Illegal(new WrongPhysicalType(TIMESTAMP_TYPES));
    }

    /// "DECIMAL can be used to annotate the following types: int32, for 1 <= precision <= 9;
    /// int64, for 1 <= precision <= 18; fixed_len_byte_array, precision is limited by the array
    /// size, length n can store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits; byte_array,
    /// precision is not limited". [#maxDecimalPrecision(PhysicalType)] and [#maxDecimalPrecision(int)]
    /// count those digits.
    static Pairing decimalPairing(PhysicalType type, Integer typeLength,
            LogicalType.DecimalType decimal) {
        boolean held = switch (type) {
            case INT32, INT64, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> true;
            case BOOLEAN, INT96, FLOAT, DOUBLE -> false;
        };
        if (!held) {
            return new Illegal(new WrongPhysicalType(DECIMAL_TYPES));
        }
        // A FIXED_LEN_BYTE_ARRAY with no usable width has no digits to count.
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY && !hasUsableWidth(typeLength)) {
            return LEGAL;
        }
        long maxPrecision = type == PhysicalType.FIXED_LEN_BYTE_ARRAY
                ? maxDecimalPrecision(typeLength.intValue())
                : maxDecimalPrecision(type);
        return decimal.precision() > maxPrecision ? new Illegal(new PrecisionTooLarge(decimal.precision(), maxPrecision)) : LEGAL;
    }

    /// The most digits a `DECIMAL` stored in `type` can have: 9 for an `INT32` and 18 for an
    /// `INT64`; a `BYTE_ARRAY` is unbounded. A `FIXED_LEN_BYTE_ARRAY`'s depend on its width, which
    /// [#maxDecimalPrecision(int)] counts.
    ///
    /// @param type the physical type the `DECIMAL` is stored in
    /// @return the largest precision `type` holds; `Long.MAX_VALUE` for a `BYTE_ARRAY`
    /// @throws IllegalArgumentException if `type` does not store a `DECIMAL`, or is a
    ///         `FIXED_LEN_BYTE_ARRAY`
    public static long maxDecimalPrecision(PhysicalType type) {
        return switch (type) {
            case INT32 -> 9;
            case INT64 -> 18;
            case BYTE_ARRAY -> Long.MAX_VALUE;
            case FIXED_LEN_BYTE_ARRAY -> throw new IllegalArgumentException(
                    "A FIXED_LEN_BYTE_ARRAY DECIMAL holds the digits of its width, not of its type");
            case BOOLEAN, INT96, FLOAT, DOUBLE ->
                    throw new IllegalArgumentException("DECIMAL is not stored in " + type);
        };
    }

    /// The most digits a `DECIMAL` stored in a `FIXED_LEN_BYTE_ARRAY` of `fixedWidth` bytes can
    /// have, the largest precision a two's-complement value of that width represents:
    /// `floor(log10(2^(8 * fixedWidth - 1) - 1))`. No power of two is a power of ten, so that is
    /// `floor((8 * fixedWidth - 1) * log10(2))`, which [#LOG10_2] computes without building the
    /// power, whose size a footer's `type_length` would otherwise set.
    ///
    /// @throws IllegalArgumentException if `fixedWidth` is not positive
    public static long maxDecimalPrecision(int fixedWidth) {
        if (fixedWidth <= 0) {
            throw new IllegalArgumentException(
                    "A FIXED_LEN_BYTE_ARRAY DECIMAL needs a positive width, not " + fixedWidth);
        }
        return new BigDecimal(8L * fixedWidth - 1).multiply(LOG10_2).longValue();
    }
}
