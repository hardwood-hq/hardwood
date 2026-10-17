/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Converts the primitive top-level columns a projection selects into an Arrow [Schema] and
/// the [ArrowPlan] that fills it.
///
/// The mapping follows the metadata-free conversion of Arrow C++'s Parquet reader: no
/// `ARROW:schema` entry is consulted and no extension type is inferred, so a `UUID` column is
/// a `FixedSizeBinary(16)`, a `JSON` column a `Utf8`, an `ENUM` column a `Binary`.
public final class ArrowSchemaConverter {

    /// The field-metadata key Arrow C++ records a Parquet field id under.
    public static final String FIELD_ID_KEY = "PARQUET:field_id";

    /// The widest decimal Arrow holds: `Decimal256` carries 76 digits.
    private static final int MAX_DECIMAL_PRECISION = 76;
    private static final int DECIMAL_128_PRECISION = 38;

    private record Mapping(ArrowType type, ScalarKind kind) {
    }

    private ArrowSchemaConverter() {
    }

    /// Resolves `projection` against `schema` and converts every column it selects.
    ///
    /// @param schema the file schema
    /// @param elements the file's schema elements as written in its footer, the source of
    ///        field ids, which [FileSchema] does not carry
    /// @param projection the requested columns
    /// @return the schema and plan, in the projection's request order
    /// @throws IllegalArgumentException if a projected name is not in the schema
    /// @throws UnsupportedOperationException if a selected column is nested, repeated, of a
    ///         type the module does not map, or a `DECIMAL` wider than Arrow's decimals
    /// @throws dev.hardwood.reader.SchemaIncompatibleException if a `FIXED_LEN_BYTE_ARRAY`
    ///         column declares no positive width
    public static ArrowPlan plan(FileSchema schema, List<SchemaElement> elements, ColumnProjection projection) {
        ProjectedSchema projected = ProjectedSchema.create(schema, projection);
        Map<String, Integer> fieldIds = topLevelFieldIds(elements);
        int count = projected.requestedColumnCount();
        List<ArrowPlan.Entry> entries = new ArrayList<>(count);
        List<Field> fields = new ArrayList<>(count);
        for (int position = 0; position < count; position++) {
            ColumnSchema column = schema.getColumn(projected.toOriginalIndex(projected.requestedColumn(position)));
            requireTopLevelScalar(column);
            Mapping mapping = mapping(column);
            Field field = field(column, mapping.type(), fieldIds.get(column.name()));
            entries.add(new ArrowPlan.Entry(column, field, mapping.kind()));
            fields.add(field);
        }
        return new ArrowPlan(new Schema(fields), List.copyOf(entries));
    }

    private static void requireTopLevelScalar(ColumnSchema column) {
        if (column.fieldPath().elements().size() != 1) {
            throw new UnsupportedOperationException("Column '" + column.fieldPath()
                    + "' is nested: hardwood-arrow reads non-repeated primitive columns directly below the root");
        }
        if (column.repetitionType() == RepetitionType.REPEATED) {
            throw new UnsupportedOperationException("Column '" + column.fieldPath()
                    + "' is repeated: hardwood-arrow reads non-repeated primitive columns directly below the root");
        }
    }

    /// The field ids of the root's direct children, by name, leaving out negative ids as Arrow
    /// C++ does, for which an id below zero means none. Elements are in depth-first order, so
    /// a group's subtree is skipped by counting its children down.
    private static Map<String, Integer> topLevelFieldIds(List<SchemaElement> elements) {
        Map<String, Integer> ids = new HashMap<>();
        if (elements.isEmpty()) {
            return ids;
        }
        Deque<Integer> remaining = new ArrayDeque<>();
        remaining.push(childCount(elements.get(0)));
        for (int i = 1; i < elements.size() && !remaining.isEmpty(); i++) {
            SchemaElement element = elements.get(i);
            boolean topLevel = remaining.size() == 1;
            remaining.push(remaining.pop() - 1);
            if (topLevel && element.fieldId() != null && element.fieldId() >= 0) {
                ids.put(element.name(), element.fieldId());
            }
            int children = childCount(element);
            if (children > 0) {
                remaining.push(children);
            }
            while (!remaining.isEmpty() && remaining.peek() == 0) {
                remaining.pop();
            }
        }
        return ids;
    }

    private static int childCount(SchemaElement element) {
        return element.numChildren() == null ? 0 : element.numChildren();
    }

    /// A field carries its Parquet field id as `PARQUET:field_id` metadata, as Arrow C++ records it.
    private static Field field(ColumnSchema column, ArrowType type, Integer fieldId) {
        boolean nullable = column.repetitionType() == RepetitionType.OPTIONAL;
        Map<String, String> metadata = fieldId == null ? null : Map.of(FIELD_ID_KEY, Integer.toString(fieldId));
        return new Field(column.name(), new FieldType(nullable, type, null, metadata), null);
    }

    private static Mapping mapping(ColumnSchema column) {
        LogicalType annotation = column.logicalType();
        if (annotation instanceof LogicalType.NullType) {
            return new Mapping(ArrowType.Null.INSTANCE, ScalarKind.NULL);
        }
        return switch (column.type()) {
            case BOOLEAN -> unannotated(column, ArrowType.Bool.INSTANCE, ScalarKind.BOOLEAN);
            case INT32 -> int32(column, annotation);
            case INT64 -> int64(column, annotation);
            case INT96 -> unannotated(column, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null),
                    ScalarKind.INT96_TIMESTAMP);
            case FLOAT -> unannotated(column, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                    ScalarKind.FLOAT);
            case DOUBLE -> unannotated(column, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                    ScalarKind.DOUBLE);
            case BYTE_ARRAY -> byteArray(column, annotation);
            case FIXED_LEN_BYTE_ARRAY -> fixedLenByteArray(column, annotation);
        };
    }

    private static Mapping unannotated(ColumnSchema column, ArrowType type, ScalarKind kind) {
        if (column.logicalType() != null) {
            throw unsupported(column);
        }
        return new Mapping(type, kind);
    }

    private static Mapping int32(ColumnSchema column, LogicalType annotation) {
        return switch (annotation) {
            case null -> new Mapping(new ArrowType.Int(32, true), ScalarKind.INT32);
            case LogicalType.IntType type -> int32(column, type);
            case LogicalType.DateType ignored -> new Mapping(new ArrowType.Date(DateUnit.DAY), ScalarKind.INT32);
            case LogicalType.TimeType type -> new Mapping(new ArrowType.Time(timeUnit(type.unit()), 32),
                    ScalarKind.INT32);
            case LogicalType.DecimalType type -> new Mapping(decimal(column, type), ScalarKind.DECIMAL_INT32);
            default -> throw unsupported(column);
        };
    }

    private static Mapping int32(ColumnSchema column, LogicalType.IntType type) {
        boolean signed = type.isSigned();
        return switch (type.bitWidth()) {
            case 8 -> new Mapping(new ArrowType.Int(8, signed), signed ? ScalarKind.INT8 : ScalarKind.UINT8);
            case 16 -> new Mapping(new ArrowType.Int(16, signed), signed ? ScalarKind.INT16 : ScalarKind.UINT16);
            case 32 -> new Mapping(new ArrowType.Int(32, signed), ScalarKind.INT32);
            default -> throw unsupported(column);
        };
    }

    private static Mapping int64(ColumnSchema column, LogicalType annotation) {
        return switch (annotation) {
            case null -> new Mapping(new ArrowType.Int(64, true), ScalarKind.INT64);
            case LogicalType.IntType type when type.bitWidth() == 64 ->
                    new Mapping(new ArrowType.Int(64, type.isSigned()), ScalarKind.INT64);
            case LogicalType.TimeType type -> new Mapping(new ArrowType.Time(timeUnit(type.unit()), 64),
                    ScalarKind.INT64);
            case LogicalType.TimestampType type -> new Mapping(
                    new ArrowType.Timestamp(timeUnit(type.unit()), type.isAdjustedToUTC() ? "UTC" : null),
                    ScalarKind.INT64);
            case LogicalType.DecimalType type -> new Mapping(decimal(column, type), ScalarKind.DECIMAL_INT64);
            default -> throw unsupported(column);
        };
    }

    private static Mapping byteArray(ColumnSchema column, LogicalType annotation) {
        return switch (annotation) {
            case null -> new Mapping(ArrowType.Binary.INSTANCE, ScalarKind.BINARY);
            case LogicalType.StringType ignored -> new Mapping(ArrowType.Utf8.INSTANCE, ScalarKind.BINARY);
            case LogicalType.JsonType ignored -> new Mapping(ArrowType.Utf8.INSTANCE, ScalarKind.BINARY);
            case LogicalType.EnumType ignored -> new Mapping(ArrowType.Binary.INSTANCE, ScalarKind.BINARY);
            case LogicalType.BsonType ignored -> new Mapping(ArrowType.Binary.INSTANCE, ScalarKind.BINARY);
            case LogicalType.GeometryType ignored -> new Mapping(ArrowType.Binary.INSTANCE, ScalarKind.BINARY);
            case LogicalType.GeographyType ignored -> new Mapping(ArrowType.Binary.INSTANCE, ScalarKind.BINARY);
            case LogicalType.DecimalType type -> new Mapping(decimal(column, type), ScalarKind.DECIMAL_BYTES);
            default -> throw unsupported(column);
        };
    }

    private static Mapping fixedLenByteArray(ColumnSchema column, LogicalType annotation) {
        int width = FixedWidthValidator.requireWidth(null, column);
        return switch (annotation) {
            case null -> new Mapping(new ArrowType.FixedSizeBinary(width), ScalarKind.FIXED_BINARY);
            case LogicalType.UuidType ignored ->
                    new Mapping(new ArrowType.FixedSizeBinary(width), ScalarKind.FIXED_BINARY);
            case LogicalType.IntervalType ignored ->
                    new Mapping(new ArrowType.FixedSizeBinary(width), ScalarKind.FIXED_BINARY);
            case LogicalType.TimestampType ignored ->
                    new Mapping(new ArrowType.FixedSizeBinary(width), ScalarKind.FIXED_BINARY);
            case LogicalType.Float16Type ignored ->
                    new Mapping(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), ScalarKind.FIXED_BINARY);
            case LogicalType.DecimalType type -> new Mapping(fixedDecimal(column, type, width),
                    ScalarKind.DECIMAL_BYTES);
            default -> throw unsupported(column);
        };
    }

    /// A fixed-width decimal must fit the Arrow decimal its precision selects, as Arrow C++
    /// requires: a writer may declare a wider `FIXED_LEN_BYTE_ARRAY` than the precision needs,
    /// and one wider than 16 or 32 bytes has no Arrow counterpart.
    private static ArrowType fixedDecimal(ColumnSchema column, LogicalType.DecimalType type, int width) {
        ArrowType.Decimal decimal = decimal(column, type);
        int arrowWidth = decimal.getBitWidth() / Byte.SIZE;
        if (width > arrowWidth) {
            throw new UnsupportedOperationException("Column '" + column.fieldPath() + "' is a DECIMAL("
                    + type.precision() + ", " + type.scale() + ") over " + width + " bytes, wider than the "
                    + arrowWidth + "-byte Arrow decimal of that precision");
        }
        return decimal;
    }

    private static ArrowType.Decimal decimal(ColumnSchema column, LogicalType.DecimalType type) {
        int precision = type.precision();
        if (precision > MAX_DECIMAL_PRECISION) {
            throw new UnsupportedOperationException("Column '" + column.fieldPath() + "' is a DECIMAL("
                    + precision + ", " + type.scale() + "): an Arrow decimal holds at most "
                    + MAX_DECIMAL_PRECISION + " digits");
        }
        int bitWidth = precision <= DECIMAL_128_PRECISION ? 128 : 256;
        return new ArrowType.Decimal(precision, type.scale(), bitWidth);
    }

    private static TimeUnit timeUnit(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> TimeUnit.MILLISECOND;
            case MICROS -> TimeUnit.MICROSECOND;
            case NANOS -> TimeUnit.NANOSECOND;
        };
    }

    private static UnsupportedOperationException unsupported(ColumnSchema column) {
        return new UnsupportedOperationException("Column '" + column.fieldPath() + "' is a " + column.type()
                + " annotated " + column.logicalType() + ", which hardwood-arrow does not map to an Arrow type");
    }
}
