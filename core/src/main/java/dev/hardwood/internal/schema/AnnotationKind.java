/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.function.Function;

import dev.hardwood.internal.conversion.FixedWidths;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

/// The facts parquet-format states about each annotation, one constant per member of
/// [LogicalType]: its `LogicalType` union field, whether it annotates a group, whether its bytes
/// are text, the order its values sort in, the physical types and widths it is defined over, the
/// legacy `converted_type` a writer writes beside it, and the literal a filter predicate takes on
/// its column.
///
/// Each fact is a constructor argument, so a member added to [LogicalType] states all of them in
/// its constant, and [#of] is the one switch that maps a member to its constant. The facts that
/// depend on an annotation's parameters (a `TIME`'s unit, an `INT`'s width and sign, a
/// `TIMESTAMP`'s UTC flag, a `DECIMAL`'s digits) are functions of the record.
///
/// [AnnotationPairings] answers the questions that combine these facts with a column's physical
/// type, width or legacy converted type, and its pairing rules cite the specification: `LogicalTypes.md` and `parquet.thrift` at
/// parquet-format `bf099392`. `FILE` (union field 19) has no constant because [LogicalType] does
/// not model it (#1413); a footer carrying it decodes to an unrecognized union member and the
/// annotation is dropped, which is what the format asks of a reader that does not recognize one.
public enum AnnotationKind {

    STRING(1, Target.PRIMITIVE, Content.TEXT, ByteColumnOrder.BYTES,
            // "may only be used to annotate the BYTE_ARRAY primitive type"
            (type, typeLength, annotation) -> AnnotationPairings.byteArray(type),
            annotation -> ConvertedType.UTF8,
            annotation -> "String"),

    MAP(2, Target.GROUP, Content.OTHER, ByteColumnOrder.NONE,
            (type, typeLength, annotation) -> AnnotationPairings.groupAnnotation(),
            annotation -> ConvertedType.MAP,
            annotation -> null),

    LIST(3, Target.GROUP, Content.OTHER, ByteColumnOrder.NONE,
            (type, typeLength, annotation) -> AnnotationPairings.groupAnnotation(),
            annotation -> ConvertedType.LIST,
            annotation -> null),

    ENUM(4, Target.PRIMITIVE, Content.TEXT, ByteColumnOrder.BYTES,
            // "annotates the BYTE_ARRAY primitive type"
            (type, typeLength, annotation) -> AnnotationPairings.byteArray(type),
            annotation -> ConvertedType.ENUM,
            annotation -> "String"),

    /// "DECIMAL - signed comparison of the represented value", its bytes big-endian.
    DECIMAL(5, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.SIGNED_BIG_ENDIAN,
            (type, typeLength, annotation) ->
                    AnnotationPairings.decimalPairing(type, typeLength, (LogicalType.DecimalType) annotation),
            annotation -> ConvertedType.DECIMAL,
            annotation -> "BigDecimal"),

    DATE(6, Target.PRIMITIVE, Content.OTHER, null,
            // "must annotate an int32 that stores the number of days from the Unix epoch"
            (type, typeLength, annotation) -> AnnotationPairings.only(type, PhysicalType.INT32),
            annotation -> ConvertedType.DATE,
            annotation -> "LocalDate"),

    TIME(7, Target.PRIMITIVE, Content.OTHER, null,
            // MILLIS "must annotate an int32"; MICROS and NANOS "must annotate an int64".
            (type, typeLength, annotation) ->
                    ((LogicalType.TimeType) annotation).unit() == LogicalType.TimeUnit.MILLIS
                            ? AnnotationPairings.only(type, PhysicalType.INT32)
                            : AnnotationPairings.only(type, PhysicalType.INT64),
            annotation -> switch (((LogicalType.TimeType) annotation).unit()) {
                case MILLIS -> ConvertedType.TIME_MILLIS;
                case MICROS -> ConvertedType.TIME_MICROS;
                case NANOS -> null;
            },
            annotation -> "LocalTime"),

    /// "signed two's-complement comparison of the represented value", which over a
    /// `FIXED_LEN_BYTE_ARRAY(12)` is a little-endian count.
    TIMESTAMP(8, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.SIGNED_LITTLE_ENDIAN,
            (type, typeLength, annotation) -> AnnotationPairings.timestampPairing(type, typeLength),
            annotation -> switch (((LogicalType.TimestampType) annotation).unit()) {
                case MILLIS -> ConvertedType.TIMESTAMP_MILLIS;
                case MICROS -> ConvertedType.TIMESTAMP_MICROS;
                case NANOS -> null;
            },
            annotation -> ((LogicalType.TimestampType) annotation).isAdjustedToUTC() ? "Instant" : "LocalDateTime"),

    /// parquet.thrift reserves union field 9 for `INTERVAL` without ever defining the member
    /// struct, so the annotation is written as the legacy `converted_type` alone, and field 9 reads
    /// as an unrecognized member. The format defines no order over its values and states that no
    /// `min` / `max` should be written for it.
    INTERVAL(AnnotationKind.NO_UNION_FIELD, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.NONE,
            // "must annotate a FIXED_LEN_BYTE_ARRAY of length 12"
            (type, typeLength, annotation) -> AnnotationPairings.fixedWidth(type, typeLength, FixedWidths.INTERVAL),
            annotation -> ConvertedType.INTERVAL,
            annotation -> "PqInterval"),

    INT(10, Target.PRIMITIVE, Content.OTHER, null,
            (type, typeLength, annotation) ->
                    AnnotationPairings.integerPairing(type, (LogicalType.IntType) annotation),
            annotation -> intConvertedType((LogicalType.IntType) annotation),
            annotation -> null),

    /// The format's `UNKNOWN`, which names no order over a column holding only nulls.
    NULL(11, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.NONE,
            // "allowed for any physical type, only null values stored": no physical type
            // contradicts it, and the schema alone cannot disprove the claim.
            (type, typeLength, annotation) -> AnnotationPairings.LEGAL,
            annotation -> null,
            annotation -> null),

    JSON(12, Target.PRIMITIVE, Content.TEXT, ByteColumnOrder.BYTES,
            // "must annotate a BYTE_ARRAY primitive type"
            (type, typeLength, annotation) -> AnnotationPairings.byteArray(type),
            annotation -> ConvertedType.JSON,
            annotation -> "String"),

    BSON(13, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.BYTES,
            // "must annotate a BYTE_ARRAY primitive type"
            (type, typeLength, annotation) -> AnnotationPairings.byteArray(type),
            annotation -> ConvertedType.BSON,
            annotation -> null),

    /// Not in `ColumnOrder`'s list, so the `FIXED_LEN_BYTE_ARRAY`'s own unsigned byte order.
    UUID(14, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.BYTES,
            // "annotates a 16-byte FIXED_LEN_BYTE_ARRAY primitive type"
            (type, typeLength, annotation) -> AnnotationPairings.fixedWidth(type, typeLength, FixedWidths.UUID),
            annotation -> null,
            annotation -> "UUID"),

    /// "FLOAT16 - signed comparison of the represented value", compared as the half float.
    FLOAT16(15, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.HALF_FLOAT,
            // "The primitive type is a 2-byte FIXED_LEN_BYTE_ARRAY."
            (type, typeLength, annotation) -> AnnotationPairings.fixedWidth(type, typeLength, FixedWidths.FLOAT16),
            annotation -> null,
            annotation -> "float"),

    /// "must annotate a group".
    VARIANT(16, Target.GROUP, Content.OTHER, ByteColumnOrder.NONE,
            (type, typeLength, annotation) -> AnnotationPairings.groupAnnotation(),
            annotation -> null,
            annotation -> null),

    GEOMETRY(17, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.NONE,
            // "Allowed for physical type: BYTE_ARRAY.", the payload being WKB.
            (type, typeLength, annotation) -> AnnotationPairings.byteArray(type),
            annotation -> null,
            annotation -> null),

    GEOGRAPHY(18, Target.PRIMITIVE, Content.OTHER, ByteColumnOrder.NONE,
            // "Allowed for physical type: BYTE_ARRAY.", the payload being WKB.
            (type, typeLength, annotation) -> AnnotationPairings.byteArray(type),
            annotation -> null,
            annotation -> null);

    /// The [#unionField] of an annotation the `LogicalType` union has no member for.
    public static final int NO_UNION_FIELD = 0;

    private static final AnnotationKind[] BY_UNION_FIELD = byUnionField();

    /// What an annotation is defined over: a primitive column, or the group of a structure.
    private enum Target {
        PRIMITIVE,
        GROUP
    }

    /// Whether an annotation says its stored bytes are the UTF-8 encoding of a string.
    private enum Content {
        TEXT,
        OTHER
    }

    /// Whether the format defines an annotation over a column of a physical type and width.
    @FunctionalInterface
    private interface PairingRule {
        Pairing check(PhysicalType type, Integer typeLength, LogicalType annotation);
    }

    private final int unionField;
    private final Target target;
    private final Content content;
    private final ByteColumnOrder byteOrder;
    private final PairingRule pairing;
    private final Function<LogicalType, ConvertedType> convertedType;
    private final Function<LogicalType, String> literal;

    /// @param unionField the `LogicalType` union field id, [#NO_UNION_FIELD] where it has none
    /// @param target what the annotation is defined over
    /// @param content whether its bytes are text
    /// @param byteOrder the order its values sort in when stored as bytes: [ByteColumnOrder#NONE]
    ///        where the format names no order over them, `null` where the annotation is defined
    ///        over no byte-stored type and its values order as the integers that store them
    /// @param pairing the physical types and widths it is defined over
    /// @param convertedType the legacy annotation written beside it, `null` where none is
    /// @param literal the Java type of its column's logical accessor, as a refusal names it,
    ///        `null` where only the physical accessor reads the column
    AnnotationKind(int unionField, Target target, Content content, ByteColumnOrder byteOrder,
            PairingRule pairing, Function<LogicalType, ConvertedType> convertedType,
            Function<LogicalType, String> literal) {
        this.unionField = unionField;
        this.target = target;
        this.content = content;
        this.byteOrder = byteOrder;
        this.pairing = pairing;
        this.convertedType = convertedType;
        this.literal = literal;
    }

    /// The constant of `annotation`'s member.
    ///
    /// @param annotation a non-null annotation
    public static AnnotationKind of(LogicalType annotation) {
        return switch (annotation) {
            case LogicalType.StringType ignored -> STRING;
            case LogicalType.MapType ignored -> MAP;
            case LogicalType.ListType ignored -> LIST;
            case LogicalType.EnumType ignored -> ENUM;
            case LogicalType.DecimalType ignored -> DECIMAL;
            case LogicalType.DateType ignored -> DATE;
            case LogicalType.TimeType ignored -> TIME;
            case LogicalType.TimestampType ignored -> TIMESTAMP;
            case LogicalType.IntervalType ignored -> INTERVAL;
            case LogicalType.IntType ignored -> INT;
            case LogicalType.NullType ignored -> NULL;
            case LogicalType.JsonType ignored -> JSON;
            case LogicalType.BsonType ignored -> BSON;
            case LogicalType.UuidType ignored -> UUID;
            case LogicalType.Float16Type ignored -> FLOAT16;
            case LogicalType.VariantType ignored -> VARIANT;
            case LogicalType.GeometryType ignored -> GEOMETRY;
            case LogicalType.GeographyType ignored -> GEOGRAPHY;
        };
    }

    /// The constant whose `LogicalType` union member is field `fieldId`, `null` for a field this
    /// release does not recognize, which includes the reserved field 9 of `INTERVAL`.
    public static AnnotationKind ofUnionField(int fieldId) {
        return fieldId > NO_UNION_FIELD && fieldId < BY_UNION_FIELD.length ? BY_UNION_FIELD[fieldId] : null;
    }

    /// Whether the `LogicalType` union has a member for this annotation.
    public boolean hasUnionMember() {
        return unionField != NO_UNION_FIELD;
    }

    /// The field id of this annotation's `LogicalType` union member.
    ///
    /// @throws IllegalStateException if the union has none, which [#hasUnionMember] answers
    public int unionField() {
        if (!hasUnionMember()) {
            throw new IllegalStateException(this + " has no LogicalType union member");
        }
        return unionField;
    }

    /// Whether the format defines this annotation over a group: `LIST` and `MAP` annotate the
    /// outer group of their structure and `VARIANT` "must annotate a group"; every other
    /// annotation is defined over a primitive alone.
    public boolean annotatesGroup() {
        return target == Target.GROUP;
    }

    /// Whether this annotation says the stored bytes are the UTF-8 encoding of a string.
    public boolean holdsText() {
        return content == Content.TEXT;
    }

    /// Whether parquet-format defines an order over the values of a column carrying this
    /// annotation. It defines none for `INTERVAL`, `UNKNOWN`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`,
    /// `LIST` and `MAP`.
    public boolean namesAnOrder() {
        return byteOrder != ByteColumnOrder.NONE;
    }

    /// Whether parquet-format defines an order over the values of a column annotated
    /// `annotation`, as [#namesAnOrder()] states it per annotation.
    ///
    /// The writer records no bounds for a column without one and the reader reads none from one,
    /// since a bound in an order a reader cannot know would prune away live rows, and the ordered
    /// operators are refused on it. One answer serves all three, the format stating one rule.
    ///
    /// @param annotation the column's annotation, `null` for an unannotated column, whose physical
    ///        type names its order
    public static boolean namesAnOrder(LogicalType annotation) {
        return annotation == null || of(annotation).namesAnOrder();
    }

    /// Whether an integer column's values compare unsigned: only the unsigned `INT` annotations
    /// do. The narrower ones never diverge from the signed order over the values they hold, but
    /// take the unsigned form too, so the annotation alone decides.
    ///
    /// @param annotation the column's annotation, `null` for an unannotated column, which
    ///        compares signed
    public static boolean ordersUnsigned(LogicalType annotation) {
        return annotation instanceof LogicalType.IntType integer && !integer.isSigned();
    }

    /// The order this annotation's values sort in over a byte-stored column, as `parquet.thrift`'s
    /// `ColumnOrder` gives it; [ByteColumnOrder#NONE] where it names none, and `null` where the
    /// annotation is defined over no byte-stored type.
    ByteColumnOrder byteOrder() {
        return byteOrder;
    }

    /// Whether the format defines `annotation`, of this kind, over a column of this physical type
    /// and width, as [AnnotationPairings#check] answers it.
    ///
    /// @throws IllegalArgumentException if `annotation` is not of this kind
    Pairing pairing(PhysicalType type, Integer typeLength, LogicalType annotation) {
        return pairing.check(type, typeLength, requireOwn(annotation));
    }

    /// The legacy `converted_type` a writer writes beside `annotation`, of this kind, so that a
    /// reader predating the union still sees it; `null` where the annotation has none.
    ///
    /// `TIME` and `TIMESTAMP` derive it from the unit alone, ignoring `isAdjustedToUTC`: the
    /// legacy annotations denoted UTC-normalized values, but parquet-format requires writers to
    /// annotate local times with them too, for forward compatibility with the libraries that did
    /// so before the union existed. Nanosecond units have no legacy counterpart.
    ///
    /// @throws IllegalArgumentException if `annotation` is not of this kind
    public ConvertedType convertedType(LogicalType annotation) {
        return convertedType.apply(requireOwn(annotation));
    }

    /// The Java type of the value the logical accessor of a column carrying `annotation`, of this
    /// kind, returns, as a refusal names it; `null` where only the physical accessor reads the
    /// column.
    ///
    /// @throws IllegalArgumentException if `annotation` is not of this kind
    public String literal(LogicalType annotation) {
        return literal.apply(requireOwn(annotation));
    }

    /// `annotation`, checked to be of this kind, so that a fact of one kind is never read off
    /// the record of another.
    private LogicalType requireOwn(LogicalType annotation) {
        AnnotationKind kind = of(annotation);
        if (kind != this) {
            throw new IllegalArgumentException(annotation + " is of kind " + kind + ", not " + this);
        }
        return annotation;
    }

    private static ConvertedType intConvertedType(LogicalType.IntType integer) {
        boolean signed = integer.isSigned();
        return switch (integer.bitWidth()) {
            case 8 -> signed ? ConvertedType.INT_8 : ConvertedType.UINT_8;
            case 16 -> signed ? ConvertedType.INT_16 : ConvertedType.UINT_16;
            case 32 -> signed ? ConvertedType.INT_32 : ConvertedType.UINT_32;
            case 64 -> signed ? ConvertedType.INT_64 : ConvertedType.UINT_64;
            default -> throw new IllegalArgumentException("Invalid integer bit width: " + integer.bitWidth());
        };
    }

    private static AnnotationKind[] byUnionField() {
        int max = 0;
        for (AnnotationKind kind : values()) {
            max = Math.max(max, kind.unionField);
        }
        AnnotationKind[] byField = new AnnotationKind[max + 1];
        for (AnnotationKind kind : values()) {
            if (kind.hasUnionMember()) {
                if (byField[kind.unionField] != null) {
                    throw new IllegalStateException(byField[kind.unionField] + " and " + kind
                            + " both claim union field " + kind.unionField);
                }
                byField[kind.unionField] = kind;
            }
        }
        return byField;
    }
}
