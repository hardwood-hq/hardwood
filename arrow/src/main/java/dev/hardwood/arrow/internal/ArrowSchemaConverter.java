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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.SchemaIncompatibleException;
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
/// `Struct`, `List` and `Map`; a Variant group is the struct of the fields it stores, which is
/// the storage of Arrow's Variant extension type, applied by nobody here.
///
/// A group's validity and offsets come from a layer of the leaf columns beneath it, so the
/// conversion derives, for every node, which layer of the layer model it owns, by the rule
/// `NestedLevelComputer` applies per leaf, and checks the result against that computation
/// for every leaf.
///
/// When dictionary output is requested (either automatically from the file's row-group encodings
/// or explicitly by column path), eligible `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96`
/// leaves receive an Arrow [DictionaryEncoding] with signed 32-bit indices and are collected in
/// [ArrowPlan#dictionaryNodes()].
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

    /// Signed 32-bit dictionary index type, matching [dev.hardwood.reader.ColumnReader#getDictionaryIds()].
    private static final ArrowType.Int DICTIONARY_INDEX_TYPE = new ArrowType.Int(32, true);

    private record Mapping(ArrowType type, ScalarKind kind) {
    }

    /// What a conversion carries down the tree: the resolved projection, the first reader
    /// position of every projected column, the per-leaf layers core computes, the leaf columns
    /// selected for dictionary output, and the collector for dictionary nodes.
    private record Context(ProjectedSchema projected, int[] firstPosition,
            NestedLevelComputer.Layers[] expectedLayers, BitSet dictionaryColumns,
            List<ArrowPlan.Node> dictionaryNodes) {
    }

    private ArrowSchemaConverter() {
    }

    /// Resolves `projection` against `schema` and converts every node it selects to plain
    /// (non-dictionary) Arrow fields.
    public static ArrowPlan plan(FileSchema schema, Map<String, String> keyValueMetadata,
            ColumnProjection projection) {
        return plan(schema, keyValueMetadata, projection, false, Set.of(), List.of());
    }

    /// Resolves `projection` against `schema` and converts every node it selects, optionally
    /// marking eligible binary columns as Arrow dictionary-encoded.
    ///
    /// @param schema the file schema (carrying `field_id` on every [SchemaNode])
    /// @param keyValueMetadata the file's key-value metadata, which becomes the schema's, as
    ///        Arrow C++ records it, except an `ARROW:schema` entry, which Arrow C++ consumes and
    ///        drops and this module leaves unread
    /// @param projection the requested columns
    /// @param autoDictionaries when `true`, marks every projected dictionary-eligible column
    ///        whose metadata in `rowGroups` uses dictionary encoding
    /// @param explicitDictionaryColumns Parquet field paths of leaf columns to emit as
    ///        dictionary-encoded Arrow fields
    /// @param rowGroups the file's row groups, consulted only when `autoDictionaries` is `true`
    /// @return the schema and plan, one root per projected top-level field in request order
    /// @throws IllegalArgumentException if a projected name or explicit dictionary column is not
    ///         in the projected schema
    /// @throws SchemaIncompatibleException if a `FIXED_LEN_BYTE_ARRAY` column declares no positive
    ///         width, a `LIST` group has no element, or a `MAP` group has no key or an `OPTIONAL` key
    /// @throws UnsupportedOperationException if a selected node is a map with only one of its
    ///         key and value selected or with no value, a column of a type the module does not
    ///         map, a `DECIMAL` wider than Arrow's decimals, or an explicit dictionary column
    ///         whose physical type does not support dictionary output
    public static ArrowPlan plan(FileSchema schema, Map<String, String> keyValueMetadata,
            ColumnProjection projection, boolean autoDictionaries, Set<String> explicitDictionaryColumns,
            List<RowGroup> rowGroups) {
        ProjectedSchema projected = ProjectedSchema.create(schema, projection);
        BitSet dictionaryColumns = resolveDictionaryColumns(schema, projected, autoDictionaries,
                explicitDictionaryColumns, rowGroups);
        List<ArrowPlan.Node> dictionaryNodes = new ArrayList<>();
        Context context = new Context(projected, firstPositions(projected),
                NestedLevelComputer.computeLayers(schema), dictionaryColumns, dictionaryNodes);
        List<ArrowPlan.Node> roots = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (SchemaNode child : projected.projectedChildren(schema.getRootNode())) {
            ArrowPlan.Node root = convert(context, child, List.of(child.name()), false, List.of());
            roots.add(root);
            fields.add(root.field());
        }
        return new ArrowPlan(new Schema(fields, schemaMetadata(keyValueMetadata)), List.copyOf(roots),
                List.copyOf(dictionaryNodes));
    }

    /// Resolves which original leaf column indices should be emitted as Arrow dictionary-encoded
    /// fields from `autoDictionaries` (footer metadata) and `explicitDictionaryColumns`.
    private static BitSet resolveDictionaryColumns(FileSchema schema, ProjectedSchema projected,
            boolean autoDictionaries, Set<String> explicitDictionaryColumns, List<RowGroup> rowGroups) {
        BitSet selected = new BitSet();
        if (autoDictionaries && rowGroups != null) {
            for (RowGroup rg : rowGroups) {
                List<ColumnChunk> chunks = rg.columns();
                for (int i = 0; i < chunks.size(); i++) {
                    ColumnMetaData meta = chunks.get(i).metaData();
                    if (meta == null || !usesDictionaryEncoding(meta)) {
                        continue;
                    }
                    int colIdx = columnIndexFor(schema, i, meta);
                    if (colIdx >= 0 && projected.toProjectedIndex(colIdx) >= 0) {
                        ColumnSchema column = schema.getColumn(colIdx);
                        if (isDictionaryEligible(column)) {
                            selected.set(colIdx);
                        }
                    }
                }
            }
        }
        if (explicitDictionaryColumns != null && !explicitDictionaryColumns.isEmpty()) {
            Map<String, ColumnSchema> projectedByPath = new HashMap<>();
            for (ColumnSchema column : projected.getProjectedColumns()) {
                projectedByPath.put(column.fieldPath().toString(), column);
            }
            for (String path : explicitDictionaryColumns) {
                ColumnSchema column = projectedByPath.get(path);
                if (column == null) {
                    throw new IllegalArgumentException("Dictionary column '" + path
                            + "' is not a projected leaf column");
                }
                if (!isDictionaryEligible(column)) {
                    throw new UnsupportedOperationException("Column '" + column.fieldPath() + "' ("
                            + column.type() + (column.logicalType() != null ? ", " + column.logicalType() : "")
                            + ") does not support Arrow dictionary output; only BYTE_ARRAY, "
                            + "FIXED_LEN_BYTE_ARRAY and INT96 columns without an UNKNOWN annotation do");
                }
                selected.set(column.columnIndex());
            }
        }
        return selected;
    }

    private static boolean usesDictionaryEncoding(ColumnMetaData meta) {
        if (meta.dictionaryPageOffset() != null) {
            return true;
        }
        List<Encoding> encodings = meta.encodings();
        if (encodings != null) {
            for (Encoding encoding : encodings) {
                if (encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY) {
                    return true;
                }
            }
        }
        return false;
    }

    private static int columnIndexFor(FileSchema schema, int hintIndex, ColumnMetaData meta) {
        if (hintIndex < schema.getColumnCount()
                && schema.getColumn(hintIndex).fieldPath().equals(meta.pathInSchema())) {
            return hintIndex;
        }
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (schema.getColumn(i).fieldPath().equals(meta.pathInSchema())) {
                return i;
            }
        }
        return -1;
    }

    /// Whether `ColumnReader#getDictionaryIds()` can produce dictionary indices for `column`.
    private static boolean isDictionaryEligible(ColumnSchema column) {
        if (column.logicalType() instanceof LogicalType.NullType) {
            return false;
        }
        PhysicalType type = column.type();
        return type == PhysicalType.BYTE_ARRAY || type == PhysicalType.FIXED_LEN_BYTE_ARRAY
                || type == PhysicalType.INT96;
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

    // ==================== The tree ====================

    /// Converts `node`, reached by `path`, into its output node.
    ///
    /// `parentIsListOrMap` and `layers` follow `NestedLevelComputer.walkLayers`: a node
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
            case SchemaNode.PrimitiveNode primitive -> leaf(context, primitive, own, !bareRepeated);
            case SchemaNode.GroupNode group -> group(context, group, path, parentIsListOrMap, own);
        };
        if (!bareRepeated) {
            return inner;
        }
        // A repeated node outside any scaffold is a list of its own elements, each present. The
        // list carries the field's id; a primitive element does not, as Arrow C++ leaves it,
        // while a group element keeps it on its struct too.
        Field element = inner.field();
        Field list = new Field(node.name(), new FieldType(false, ArrowType.List.INSTANCE, null, metadata(node)),
                List.of(element));
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
        if (context.dictionaryColumns().get(primitive.columnIndex())) {
            int slot = context.dictionaryNodes().size();
            DictionaryEncoding encoding = new DictionaryEncoding(slot, false, DICTIONARY_INDEX_TYPE);
            Field field = new Field(primitive.name(), new FieldType(nullable, mapping.type(), encoding, metadata),
                    null);
            ArrowPlan.Node node = ArrowPlan.Node.leaf(field, mapping.kind(), position, slot);
            context.dictionaryNodes().add(node);
            return node;
        }
        Field field = new Field(primitive.name(), new FieldType(nullable, mapping.type(), null, metadata), null);
        return ArrowPlan.Node.leaf(field, mapping.kind(), position, -1);
    }

    /// The layers the tree derived for a leaf must be the ones its reader reports; a
    /// difference would misalign every group above it, so it is a defect to fail on.
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

    /// A `LIST` group: one `REPEATED` layer of its own, then its element, which in the
    /// three-level encoding sits inside a synthetic wrapper and in the legacy two-level one is
    /// the repeated child itself.
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

    /// A `MAP` group: one `REPEATED` layer of its own, then the key and the value beneath the
    /// repeated `key_value` group, which Arrow carries as a non-nullable struct named
    /// `entries`, whatever the Parquet group is called and whatever its field id, as Arrow C++
    /// names it and leaves it without metadata.
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

    /// A field carries its non-negative Parquet field id as `PARQUET:field_id` metadata, as
    /// Arrow C++ records it; a negative id is treated as none.
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
