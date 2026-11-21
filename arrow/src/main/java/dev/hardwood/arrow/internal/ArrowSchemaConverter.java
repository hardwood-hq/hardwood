/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

import java.util.ArrayList;
import java.util.Arrays;
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

import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Converts projected Parquet columns into an Arrow [Schema] and [ArrowPlan], following Arrow
/// C++'s metadata-free mapping (no `ARROW:schema` restoration, no extension types).
public final class ArrowSchemaConverter {

    /// Field-metadata key under which Arrow C++ records a Parquet field id.
    public static final String FIELD_ID_KEY = "PARQUET:field_id";

    private static final String ENTRIES = "entries";
    private static final String ARROW_SCHEMA_KEY = "ARROW:schema";
    private static final int MAX_DECIMAL_PRECISION = 76;
    private static final int DECIMAL_128_PRECISION = 38;

    private record Mapping(ArrowType type, ScalarKind kind) {
    }

    private record Context(ProjectedSchema projected, int[] firstPosition,
            NestedLevelComputer.Layers[] expectedLayers) {
    }

    private ArrowSchemaConverter() {
    }

    /// Resolves `projection` against `schema` and converts every projected node into an
    /// [ArrowPlan], ordered by first occurrence in the projection.
    public static ArrowPlan plan(FileSchema schema, Map<String, String> keyValueMetadata,
            ColumnProjection projection) {
        ProjectedSchema projected = ProjectedSchema.create(schema, projection);
        Context context = new Context(projected, firstPositions(projected),
                NestedLevelComputer.computeLayers(schema));
        List<ArrowPlan.Node> roots = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (SchemaNode child : projected.projectedChildren(schema.getRootNode())) {
            ArrowPlan.Node root = convert(context, child, List.of(child.name()), false, List.of());
            roots.add(root);
            fields.add(root.field());
        }
        return new ArrowPlan(new Schema(fields, schemaMetadata(keyValueMetadata)), List.copyOf(roots));
    }

    private static Map<String, String> schemaMetadata(Map<String, String> keyValueMetadata) {
        Map<String, String> metadata = new HashMap<>();
        for (Map.Entry<String, String> entry : keyValueMetadata.entrySet()) {
            if (!ARROW_SCHEMA_KEY.equals(entry.getKey())) {
                metadata.put(entry.getKey(), entry.getValue() == null ? "" : entry.getValue());
            }
        }
        return metadata.isEmpty() ? null : metadata;
    }

    private static int[] firstPositions(ProjectedSchema projected) {
        int[] first = new int[projected.getProjectedColumnCount()];
        Arrays.fill(first, -1);
        for (int position = 0; position < projected.requestedColumnCount(); position++) {
            int projectedIndex = projected.requestedColumn(position);
            if (first[projectedIndex] < 0) {
                first[projectedIndex] = position;
            }
        }
        return first;
    }

    // ==================== The tree ====================

    /// Converts `node` following `NestedLevelComputer.walkLayers`: a node contributes a
    /// `REPEATED` layer when repeated outside a `LIST`/`MAP` scaffold or when it is a `LIST`/`MAP`,
    /// and a `STRUCT` layer when it is an optional group outside a scaffold.
    private static ArrowPlan.Node convert(Context context, SchemaNode node, List<String> path, boolean parentIsListOrMap,
            List<LayerKind> layers) {
        boolean bareRepeated = !parentIsListOrMap && node.repetitionType() == RepetitionType.REPEATED;
        List<LayerKind> own = bareRepeated ? append(layers, LayerKind.REPEATED) : layers;
        ArrowPlan.Node inner = switch (node) {
            case SchemaNode.PrimitiveNode primitive -> leaf(context, primitive, own, !bareRepeated);
            case SchemaNode.GroupNode group -> group(context, group, path, parentIsListOrMap, own);
        };
        if (!bareRepeated) {
            return inner;
        }
        // Bare repeated node -> non-nullable list; list carries the id, primitive element does not.
        Field list = new Field(node.name(), new FieldType(false, ArrowType.List.INSTANCE, null, metadata(node)),
                List.of(inner.field()));
        return ArrowPlan.Node.group(ArrowPlan.Shape.LIST, list, inner.position(), layers.size(), List.of(inner));
    }

    private static ArrowPlan.Node leaf(Context context, SchemaNode.PrimitiveNode primitive,
            List<LayerKind> layers, boolean carriesId) {
        ColumnSchema column = primitive.column();
        requireLayers(context, column, layers);
        Mapping mapping = mapping(column);
        boolean nullable = primitive.repetitionType() == RepetitionType.OPTIONAL;
        Map<String, String> metadata = carriesId ? metadata(primitive) : null;
        int position = context.firstPosition()[context.projected().toProjectedIndex(primitive.columnIndex())];
        Field field = new Field(primitive.name(), new FieldType(nullable, mapping.type(), null, metadata), null);
        return ArrowPlan.Node.leaf(field, mapping.kind(), position);
    }

    private static void requireLayers(Context context, ColumnSchema column, List<LayerKind> layers) {
        LayerKind[] expected = context.expectedLayers()[column.columnIndex()].kinds();
        if (!Arrays.asList(expected).equals(layers)) {
            throw new IllegalStateException("Column '" + column.fieldPath() + "' reports layers " + Arrays.toString(expected)
                    + " but the Arrow plan derived " + layers);
        }
    }

    private static ArrowPlan.Node group(Context context, SchemaNode.GroupNode group, List<String> path,
            boolean parentIsListOrMap, List<LayerKind> layers) {
        if (group.isList()) {
            return list(context, group, path, layers);
        }
        if (group.isMap()) {
            return map(context, group, path, layers);
        }
        boolean ownLayer = !parentIsListOrMap && group.repetitionType() == RepetitionType.OPTIONAL;
        List<LayerKind> below = ownLayer ? append(layers, LayerKind.STRUCT) : layers;
        List<ArrowPlan.Node> children = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (SchemaNode child : context.projected().projectedChildren(group)) {
            ArrowPlan.Node node = convert(context, child, append(path, child.name()), false, below);
            children.add(node);
            fields.add(node.field());
        }
        boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
        Field field = new Field(group.name(), new FieldType(nullable, ArrowType.Struct.INSTANCE, null,
                metadata(group)), fields);
        return ArrowPlan.Node.group(ArrowPlan.Shape.STRUCT, field, children.get(0).position(),
                ownLayer ? layers.size() : -1, children);
    }

    private static ArrowPlan.Node list(Context context, SchemaNode.GroupNode group, List<String> path,
            List<LayerKind> layers) {
        SchemaNode element = group.getListElement();
        if (element == null) {
            throw new SchemaIncompatibleException("Column '" + dotted(path) + "' is a LIST group with no element");
        }
        SchemaNode repeated = group.children().get(0);
        boolean twoLevel = element == repeated;
        List<String> elementPath = twoLevel ? append(path, element.name())
                : append(append(path, repeated.name()), element.name());
        ArrowPlan.Node elementNode = convert(context, element, elementPath, twoLevel,
                append(layers, LayerKind.REPEATED));
        boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
        Field field = new Field(group.name(), new FieldType(nullable, ArrowType.List.INSTANCE, null,
                metadata(group)), List.of(elementNode.field()));
        return ArrowPlan.Node.group(ArrowPlan.Shape.LIST, field, elementNode.position(), layers.size(),
                List.of(elementNode));
    }

    private static ArrowPlan.Node map(Context context, SchemaNode.GroupNode group, List<String> path,
            List<LayerKind> layers) {
        SchemaNode key = group.getMapKey();
        SchemaNode value = group.getMapValue();
        if (key == null) {
            throw new SchemaIncompatibleException("Column '" + dotted(path) + "' is a MAP group with no key");
        }
        if (key.repetitionType() == RepetitionType.OPTIONAL) {
            throw new SchemaIncompatibleException("Column '" + dotted(path) + "' is a MAP group whose key '"
                    + key.name() + "' is OPTIONAL; MAP keys must be REQUIRED");
        }
        if (value == null) {
            throw new UnsupportedOperationException("Column '" + dotted(path)
                    + "' is a MAP group with no value, which an Arrow map cannot represent");
        }
        SchemaNode.GroupNode keyValue = (SchemaNode.GroupNode) group.children().get(0);
        List<SchemaNode> projected = context.projected().projectedChildren(keyValue);
        if (!projected.contains(key) || !projected.contains(value)) {
            throw new UnsupportedOperationException("Column '" + dotted(path)
                    + "' is a MAP group of which only the key or only the value is projected: project the map as a whole");
        }
        List<LayerKind> below = append(layers, LayerKind.REPEATED);
        List<String> keyValuePath = append(path, keyValue.name());
        ArrowPlan.Node keyNode = convert(context, key, append(keyValuePath, key.name()), false, below);
        if (keyNode.kind() == ScalarKind.NULL) {
            throw new UnsupportedOperationException("Column '" + dotted(path)
                    + "' is a MAP group whose key is annotated UNKNOWN, which an Arrow map cannot represent");
        }
        ArrowPlan.Node valueNode = convert(context, value, append(keyValuePath, value.name()), false, below);
        Field entries = new Field(ENTRIES, new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
                List.of(keyNode.field(), valueNode.field()));
        boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
        Field field = new Field(group.name(), new FieldType(nullable, new ArrowType.Map(false), null,
                metadata(group)), List.of(entries));
        return ArrowPlan.Node.group(ArrowPlan.Shape.MAP, field, keyNode.position(), layers.size(),
                List.of(keyNode, valueNode));
    }

    private static <T> List<T> append(List<T> list, T element) {
        List<T> result = new ArrayList<>(list.size() + 1);
        result.addAll(list);
        result.add(element);
        return List.copyOf(result);
    }

    private static String dotted(List<String> path) {
        return String.join(".", path);
    }

    private static Map<String, String> metadata(SchemaNode node) {
        Integer fieldId = node.fieldId();
        return fieldId == null || fieldId < 0 ? null : Map.of(FIELD_ID_KEY, Integer.toString(fieldId));
    }

    // ==================== Primitive mapping ====================

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
