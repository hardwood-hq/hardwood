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
import java.util.Arrays;
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

import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Converts the columns a projection selects into an Arrow [Schema] and the [ArrowPlan] that
/// fills it.
///
/// The mapping follows the metadata-free conversion of Arrow C++'s Parquet reader: no
/// `ARROW:schema` entry is consulted and no extension type is inferred, so a `UUID` column is
/// a `FixedSizeBinary(16)`, a `JSON` column a `Utf8`, an `ENUM` column a `Binary`. Groups map to
/// `Struct`, `List` and `Map`; a Variant group is refused.
///
/// A group's validity and offsets come from a layer of the leaf columns beneath it, so the
/// conversion derives, for every node, which layer of the layer model it owns, by the rule
/// `NestedLevelComputer` applies per leaf, and checks the result against that computation
/// for every leaf.
public final class ArrowSchemaConverter {

    /// The field-metadata key Arrow C++ records a Parquet field id under.
    public static final String FIELD_ID_KEY = "PARQUET:field_id";

    /// The name Arrow gives the struct between a map and its key and value.
    private static final String ENTRIES = "entries";

    /// The key-value entry Arrow writers store their own schema under, and which this module
    /// neither reads nor carries.
    private static final String ARROW_SCHEMA_KEY = "ARROW:schema";

    /// The widest decimal Arrow holds: `Decimal256` carries 76 digits.
    private static final int MAX_DECIMAL_PRECISION = 76;
    private static final int DECIMAL_128_PRECISION = 38;

    private record Mapping(ArrowType type, ScalarKind kind) {
    }

    /// What a conversion carries down the tree: the resolved projection, the first reader
    /// position of every projected column, the field ids by path, and the root for the
    /// per-leaf layer check.
    ///
    /// A path is its components, never their dotted rendering: a field's name may itself hold
    /// a dot, so a top-level `account.id` and an `id` inside `account` are different paths
    /// with the same rendering.
    private record Context(FileSchema schema, ProjectedSchema projected, int[] firstPosition,
            Map<List<String>, Integer> fieldIds) {
    }

    private ArrowSchemaConverter() {
    }

    /// Resolves `projection` against `schema` and converts every node it selects.
    ///
    /// @param schema the file schema
    /// @param elements the file's schema elements as written in its footer, the source of
    ///        field ids, which [FileSchema] does not carry
    /// @param keyValueMetadata the file's key-value metadata, which becomes the schema's, as
    ///        Arrow C++ records it, except an `ARROW:schema` entry, which Arrow C++ consumes and
    ///        drops and this module leaves unread
    /// @param projection the requested columns
    /// @return the schema and plan, one root per projected top-level field in request order
    /// @throws IllegalArgumentException if a projected name is not in the schema
    /// @throws UnsupportedOperationException if a selected node is a Variant group, a map
    ///         with only one of its key and value selected or with no value, a `LIST` group
    ///         with no element, a column of a type the module does not map, or a `DECIMAL`
    ///         wider than Arrow's decimals
    /// @throws dev.hardwood.reader.SchemaIncompatibleException if a `FIXED_LEN_BYTE_ARRAY`
    ///         column declares no positive width
    public static ArrowPlan plan(FileSchema schema, List<SchemaElement> elements, Map<String, String> keyValueMetadata,
            ColumnProjection projection) {
        ProjectedSchema projected = ProjectedSchema.create(schema, projection);
        Context context = new Context(schema, projected, firstPositions(projected), fieldIds(elements));
        List<ArrowPlan.Node> roots = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (SchemaNode child : projected.projectedChildren(schema.getRootNode())) {
            ArrowPlan.Node root = convert(context, child, List.of(child.name()), false, List.of());
            roots.add(root);
            fields.add(root.field());
        }
        return new ArrowPlan(new Schema(fields, schemaMetadata(keyValueMetadata)), List.copyOf(roots));
    }

    /// The file's key-value metadata as Arrow schema metadata, without the `ARROW:schema`
    /// entry, as Arrow C++ leaves it after consuming it; a key the file wrote without a value
    /// carries an empty one, since Arrow's metadata holds no null.
    private static Map<String, String> schemaMetadata(Map<String, String> keyValueMetadata) {
        Map<String, String> metadata = new HashMap<>();
        for (Map.Entry<String, String> entry : keyValueMetadata.entrySet()) {
            if (!ARROW_SCHEMA_KEY.equals(entry.getKey())) {
                metadata.put(entry.getKey(), entry.getValue() == null ? "" : entry.getValue());
            }
        }
        return metadata.isEmpty() ? null : metadata;
    }

    /// The first position, in request order, each projected column is served at.
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

    /// The field ids of every element, by path, leaving out negative ids as Arrow C++ does,
    /// for which an id below zero means none. Elements are in depth-first order, so the path
    /// is kept as a stack of the groups still open.
    private static Map<List<String>, Integer> fieldIds(List<SchemaElement> elements) {
        Map<List<String>, Integer> ids = new HashMap<>();
        if (elements.isEmpty()) {
            return ids;
        }
        Deque<Integer> remaining = new ArrayDeque<>();
        Deque<List<String>> path = new ArrayDeque<>();
        remaining.push(childCount(elements.get(0)));
        for (int i = 1; i < elements.size() && !remaining.isEmpty(); i++) {
            SchemaElement element = elements.get(i);
            remaining.push(remaining.pop() - 1);
            List<String> current = path.isEmpty() ? List.of(element.name()) : child(path.peekLast(), element.name());
            if (element.fieldId() != null && element.fieldId() >= 0) {
                ids.put(current, element.fieldId());
            }
            int children = childCount(element);
            if (children > 0) {
                remaining.push(children);
                path.addLast(current);
            }
            while (!remaining.isEmpty() && remaining.peek() == 0) {
                remaining.pop();
                if (!path.isEmpty()) {
                    path.removeLast();
                }
            }
        }
        return ids;
    }

    private static int childCount(SchemaElement element) {
        return element.numChildren() == null ? 0 : element.numChildren();
    }

    // ==================== The tree ====================

    /// Converts `node`, reached by `path`, into its output node.
    ///
    /// `parentIsListOrMap` and `layers` follow `NestedLevelComputer.walkLayersToLeaf`: a node
    /// contributes a `REPEATED` layer when it is repeated outside a `LIST` or `MAP` scaffold,
    /// another when it is itself a `LIST` or `MAP`, and a `STRUCT` layer when it is an optional
    /// group outside a scaffold. A required group, and the repeated child of a `LIST` or `MAP`
    /// group (the synthetic wrapper, or the element itself in the legacy two-level encoding),
    /// contributes none. `layers` holds the kinds contributed above `node`, so its size is the
    /// index the next layer takes.
    private static ArrowPlan.Node convert(Context context, SchemaNode node, List<String> path, boolean parentIsListOrMap,
            List<LayerKind> layers) {
        boolean bareRepeated = !parentIsListOrMap && node.repetitionType() == RepetitionType.REPEATED;
        List<LayerKind> own = bareRepeated ? append(layers, LayerKind.REPEATED) : layers;
        ArrowPlan.Node inner = switch (node) {
            case SchemaNode.PrimitiveNode primitive -> leaf(context, primitive, path, own, !bareRepeated);
            case SchemaNode.GroupNode group -> group(context, group, path, parentIsListOrMap, own);
        };
        if (!bareRepeated) {
            return inner;
        }
        // A repeated node outside any scaffold is a list of its own elements, each present. The
        // list carries the field's id; a primitive element does not, as Arrow C++ leaves it,
        // while a group element keeps it on its struct too.
        Field element = inner.field();
        Field list = new Field(node.name(), new FieldType(false, ArrowType.List.INSTANCE, null, metadata(context, path)),
                List.of(element));
        return new ArrowPlan.Node(ArrowPlan.Shape.LIST, list, null, inner.position(), layers.size(),
                List.of(inner));
    }

    private static ArrowPlan.Node leaf(Context context, SchemaNode.PrimitiveNode primitive, List<String> path,
            List<LayerKind> layers, boolean carriesId) {
        ColumnSchema column = context.schema().getColumn(primitive.columnIndex());
        requireLayers(context, column, layers);
        Mapping mapping = mapping(column);
        boolean nullable = primitive.repetitionType() == RepetitionType.OPTIONAL;
        Map<String, String> metadata = carriesId ? metadata(context, path) : null;
        Field field = new Field(primitive.name(), new FieldType(nullable, mapping.type(), null, metadata), null);
        int position = context.firstPosition()[context.projected().toProjectedIndex(primitive.columnIndex())];
        return new ArrowPlan.Node(ArrowPlan.Shape.LEAF, field, mapping.kind(), position, -1, List.of());
    }

    /// The layers the tree derived for a leaf must be the ones its reader reports; a
    /// difference would misalign every group above it, so it is a defect to fail on.
    private static void requireLayers(Context context, ColumnSchema column, List<LayerKind> layers) {
        LayerKind[] expected = NestedLevelComputer.computeLayers(context.schema().getRootNode(),
                column.columnIndex()).kinds();
        if (!Arrays.asList(expected).equals(layers)) {
            throw new IllegalStateException("Column '" + column.fieldPath() + "' reports layers " + Arrays.toString(expected)
                    + " but the Arrow plan derived " + layers);
        }
    }

    private static ArrowPlan.Node group(Context context, SchemaNode.GroupNode group, List<String> path,
            boolean parentIsListOrMap, List<LayerKind> layers) {
        if (group.isVariant()) {
            throw new UnsupportedOperationException("Column '" + path
                    + "' is a Variant group, which hardwood-arrow does not serve");
        }
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
            ArrowPlan.Node node = convert(context, child, child(path, child.name()), false, below);
            children.add(node);
            fields.add(node.field());
        }
        boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
        Field field = new Field(group.name(), new FieldType(nullable, ArrowType.Struct.INSTANCE, null,
                metadata(context, path)), fields);
        return new ArrowPlan.Node(ArrowPlan.Shape.STRUCT, field, null, children.get(0).position(),
                ownLayer ? layers.size() : -1, List.copyOf(children));
    }

    /// A `LIST` group: one `REPEATED` layer of its own, then its element, which in the
    /// three-level encoding sits inside a synthetic wrapper and in the legacy two-level one is
    /// the repeated child itself.
    private static ArrowPlan.Node list(Context context, SchemaNode.GroupNode group, List<String> path,
            List<LayerKind> layers) {
        SchemaNode element = group.getListElement();
        if (element == null) {
            throw new UnsupportedOperationException("Column '" + dotted(path) + "' is a LIST group with no element");
        }
        SchemaNode repeated = group.children().get(0);
        boolean twoLevel = element == repeated;
        List<String> elementPath = twoLevel ? child(path, element.name())
                : child(child(path, repeated.name()), element.name());
        ArrowPlan.Node elementNode = convert(context, element, elementPath, twoLevel,
                append(layers, LayerKind.REPEATED));
        boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
        Field field = new Field(group.name(), new FieldType(nullable, ArrowType.List.INSTANCE, null,
                metadata(context, path)), List.of(elementNode.field()));
        return new ArrowPlan.Node(ArrowPlan.Shape.LIST, field, null, elementNode.position(), layers.size(),
                List.of(elementNode));
    }

    /// A `MAP` group: one `REPEATED` layer of its own, then the key and the value beneath the
    /// repeated `key_value` group, which Arrow carries as a non-nullable struct named
    /// `entries`, whatever the Parquet group is called and whatever its field id, as Arrow C++
    /// names it and leaves it without metadata.
    private static ArrowPlan.Node map(Context context, SchemaNode.GroupNode group, List<String> path,
            List<LayerKind> layers) {
        SchemaNode key = group.getMapKey();
        SchemaNode value = group.getMapValue();
        if (key == null) {
            throw new UnsupportedOperationException("Column '" + dotted(path) + "' is a MAP group with no key");
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
        List<String> keyValuePath = child(path, keyValue.name());
        ArrowPlan.Node keyNode = convert(context, key, child(keyValuePath, key.name()), false, below);
        ArrowPlan.Node valueNode = convert(context, value, child(keyValuePath, value.name()), false, below);
        Field entries = new Field(ENTRIES, new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
                List.of(keyNode.field(), valueNode.field()));
        boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
        Field field = new Field(group.name(), new FieldType(nullable, new ArrowType.Map(false), null,
                metadata(context, path)), List.of(entries));
        return new ArrowPlan.Node(ArrowPlan.Shape.MAP, field, null, keyNode.position(), layers.size(),
                List.of(keyNode, valueNode));
    }

    private static List<LayerKind> append(List<LayerKind> layers, LayerKind kind) {
        List<LayerKind> result = new ArrayList<>(layers);
        result.add(kind);
        return List.copyOf(result);
    }

    private static List<String> child(List<String> path, String name) {
        List<String> result = new ArrayList<>(path);
        result.add(name);
        return List.copyOf(result);
    }

    private static String dotted(List<String> path) {
        return String.join(".", path);
    }

    /// A field carries its Parquet field id as `PARQUET:field_id` metadata, as Arrow C++ records it.
    private static Map<String, String> metadata(Context context, List<String> path) {
        Integer fieldId = context.fieldIds().get(path);
        return fieldId == null ? null : Map.of(FIELD_ID_KEY, Integer.toString(fieldId));
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
