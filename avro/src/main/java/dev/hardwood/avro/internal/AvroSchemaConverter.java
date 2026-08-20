/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import dev.hardwood.avro.internal.AvroPlanNode.Kind;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Converts a Hardwood [FileSchema] to an Avro [Schema].
///
/// The mapping follows the same conventions as parquet-java's
/// `AvroSchemaConverter`, producing Avro schemas that are compatible
/// with standard Avro tools and libraries.
///
/// It diverges in one place, and only by accepting more: a map key annotated
/// `ENUM` or `JSON` converts to a string-keyed Avro map, where parquet-java
/// requires `STRING`. Both annotations describe UTF-8 text the string accessor
/// serves, and `ENUM` / `JSON` values already convert to Avro strings. So a
/// file that converts here does not necessarily convert under parquet-java —
/// never the reverse.
///
/// Conversion yields a decode plan — see [#plan] — pairing each converted value
/// position with the Parquet node it comes from, for readers that need the
/// Parquet-side annotations the Avro schema does not carry. The converted schema
/// on its own is `plan(...).avro()`.
public final class AvroSchemaConverter {

    /// Marker property on an Avro `LONG` schema recording that the source column is
    /// physically `INT32` with the `UINT_32` logical type — a distinction Avro's type
    /// system cannot carry, since it has no unsigned types. Informational: it describes
    /// the converted schema for consumers that hold the schema alone.
    public static final String UNSIGNED_INT32_PROP = "hardwood.unsignedInt32";

    /// Marker property recording the original Parquet name when Avro rewrites it.
    public static final String PARQUET_NAME_PROP = "hardwood.parquetName";

    private final FileSchema fileSchema;

    /// The projection to narrow to, or `null` to retain every field.
    private final ProjectedSchema projected;

    private final AvroNames names;

    private AvroSchemaConverter(FileSchema fileSchema, ProjectedSchema projected) {
        this.fileSchema = fileSchema;
        this.projected = projected;
        this.names = AvroNames.forSchema(fileSchema);
    }

    /// Convert a Hardwood FileSchema to a decode plan, narrowed to the given column
    /// projection. Only projected fields appear in the result, with pruning applied
    /// recursively through structs, list elements, and map values — so `address.city`
    /// yields an `address` record carrying only `city`, and `items.list.element.quantity`
    /// yields a list whose element record carries only `quantity`, mirroring the partial
    /// rows the row reader serves.
    ///
    /// The plan's [AvroPlanNode#avro] is the converted schema; its [AvroPlanNode#kind]
    /// tree carries the per-value accessor decisions taken from the Parquet schema.
    ///
    /// @param fileSchema the Parquet file schema
    /// @param projection the columns to retain
    /// @return the root of the decode plan, restricted to projected fields
    public static AvroPlanNode plan(FileSchema fileSchema, ColumnProjection projection) {
        ProjectedSchema projected = projection.projectsAll()
                ? null
                : ProjectedSchema.create(fileSchema, projection);
        return new AvroSchemaConverter(fileSchema, projected).convertRoot();
    }

    private AvroPlanNode convertRoot() {
        rejectCanonicalRootConflict();
        return convertGroup(fileSchema.getRootNode(), "");
    }

    /// Convert a struct group (or the schema root) to an Avro record, retaining
    /// only children that contain a projected leaf when a projection is active.
    private AvroPlanNode convertGroup(SchemaNode.GroupNode group, String path) {
        List<Schema.Field> fields = new ArrayList<>();
        List<AvroPlanNode> children = new ArrayList<>();
        for (SchemaNode child : group.children()) {
            if (projected != null && !hasProjectedLeaf(child, projected)) {
                continue;
            }
            AvroPlanNode childNode = convertNode(child, childPath(path, child.name()));
            Schema.Field field = new Schema.Field(names.fieldName(child), fieldSchema(childNode, child), null, null);
            applyParquetName(field, child);
            fields.add(field);
            children.add(childNode);
        }
        AvroNames.TypeName typeName = names.typeName(group);
        Schema record = Schema.createRecord(typeName.name(), null, typeName.namespace(), false, fields);
        applyParquetName(record, group);
        return AvroPlanNode.record(record, group, children);
    }

    /// The dotted path a converted node is reported under when conversion rejects
    /// it. Names the value positions of the converted schema, so the synthetic
    /// `list` and `key_value` levels of the Parquet encoding do not appear: a map
    /// nested in `holder` is `holder.nested`, a map at a list element position is
    /// `items.element`, and a map at a map value position is `outer.value`.
    private static String childPath(String path, String name) {
        return path.isEmpty() ? name : path + "." + name;
    }

    /// The schema a converted node takes as a field, list element, or map value:
    /// OPTIONAL positions are wrapped in `[null, T]` — unless T is already the NULL
    /// type, since Avro unions disallow duplicate branches.
    private static Schema fieldSchema(AvroPlanNode node, SchemaNode source) {
        Schema schema = node.avro();
        if (source.repetitionType() == RepetitionType.OPTIONAL && schema.getType() != Schema.Type.NULL) {
            return nullable(schema);
        }
        return schema;
    }

    /// True if `node` is, or transitively contains, a projected leaf column.
    private static boolean hasProjectedLeaf(SchemaNode node, ProjectedSchema projected) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> projected.toProjectedIndex(prim.columnIndex()) >= 0;
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    if (hasProjectedLeaf(child, projected)) {
                        yield true;
                    }
                }
                yield false;
            }
        };
    }

    private AvroPlanNode convertNode(SchemaNode node, String path) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> convertPrimitive(prim);
            case SchemaNode.GroupNode group -> convertGroupNode(group, path);
        };
    }

    /// Classify a group the way the row reader does. [SchemaNode.GroupNode#isStruct]
    /// is narrower than "not a Variant, list or map": a group carrying some other
    /// annotation is a struct to neither. Converting it to an Avro RECORD anyway
    /// would yield a record the reader cannot fill, since the accessors would serve
    /// the group's leaf instead — so reject it here, once, rather than per value.
    private AvroPlanNode convertGroupNode(SchemaNode.GroupNode group, String path) {
        if (group.isVariant()) {
            return convertVariant(group);
        }
        if (group.isList()) {
            return convertList(group, path);
        }
        if (group.isMap()) {
            return convertMap(group, path);
        }
        if (!group.isStruct()) {
            throw new IllegalArgumentException("Group '" + path
                    + "' carries an annotation Avro conversion does not recognise: "
                    + groupAnnotation(group));
        }
        // Plain struct — prune unprojected children recursively.
        return convertGroup(group, path);
    }

    private static String groupAnnotation(SchemaNode.GroupNode group) {
        if (group.logicalType() != null) {
            return "logical type " + group.logicalType();
        }
        return "converted type " + group.convertedType();
    }

    /// Emit a two-field Avro RECORD carrying the canonical Variant bytes.
    /// Matches parquet-java's [org.apache.parquet.avro.AvroParquetReader]
    /// output shape so tooling that already consumes the parquet-java Avro
    /// surface works unchanged. Callers who want typed access to the Variant
    /// payload use [dev.hardwood.reader.RowReader#getVariant] on the file
    /// reader and the [dev.hardwood.row.PqVariant] API.
    ///
    /// The record is a plan leaf: its two fields are read through the Variant
    /// accessor, not as fields of a struct. An ordinary group of the same shape
    /// converts to the same Avro record, so only the plan tells them apart.
    private AvroPlanNode convertVariant(SchemaNode.GroupNode group) {
        List<Schema.Field> fields = List.of(
                new Schema.Field("metadata", Schema.create(Schema.Type.BYTES), null, null),
                new Schema.Field("value", Schema.create(Schema.Type.BYTES), null, null));
        AvroNames.TypeName typeName = names.typeName(group);
        Schema schema = Schema.createRecord(typeName.name(), null, typeName.namespace(),
                false, new ArrayList<>(fields));
        applyParquetName(schema, group);
        return AvroPlanNode.leaf(schema, Kind.VARIANT, group);
    }

    private AvroPlanNode convertList(SchemaNode.GroupNode listGroup, String path) {
        SchemaNode element = listGroup.getListElement();
        if (element == null) {
            throw new IllegalArgumentException("LIST group '" + path
                    + "' has no element");
        }
        // The list column is only reached when it has a projected leaf; prune the
        // element subtree so a list<struct> with a sub-field projection narrows to
        // the served fields.
        AvroPlanNode elementNode = convertNode(element, childPath(path, element.name()));
        return AvroPlanNode.container(
                Schema.createArray(fieldSchema(elementNode, element)), Kind.LIST, listGroup, elementNode);
    }

    private AvroPlanNode convertMap(SchemaNode.GroupNode mapGroup, String path) {
        // MAP -> key_value (repeated) -> key, value
        SchemaNode keyNode = mapGroup.getMapKey();
        SchemaNode valueNode = mapGroup.getMapValue();
        if (keyNode == null) {
            throw new IllegalArgumentException("MAP group '" + path
                    + "' must contain a repeated key/value group with a key");
        }
        requireAvroStringKey(keyNode, path);
        if (valueNode == null) {
            Schema nullSchema = Schema.create(Schema.Type.NULL);
            AvroPlanNode value = AvroPlanNode.leaf(nullSchema, Kind.NULL, mapGroup);
            return AvroPlanNode.container(
                    Schema.createMap(nullSchema), Kind.MAP, mapGroup, value);
        }
        // Prune the value subtree so a map<_, struct> with a sub-field
        // projection narrows to the served fields (the key is always read).
        AvroPlanNode value = convertNode(valueNode, childPath(path, valueNode.name()));
        return AvroPlanNode.container(
                Schema.createMap(fieldSchema(value, valueNode)), Kind.MAP, mapGroup, value);
    }

    /// An Avro `MAP` carries no key schema — the spec fixes keys as strings — so a
    /// Parquet map is only representable when its key converts to an Avro `STRING`.
    /// The reader reads the key through [dev.hardwood.row.PqMap.Entry#getStringKey],
    /// which a key of any other type cannot serve; reject the whole map here rather
    /// than let a schema claiming string keys fail per value during materialization.
    ///
    /// "Converts to an Avro `STRING`" is [Kind#STRING], as decided by
    /// [#convertLogicalType] — a `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`.
    /// A `UUID` key also converts to an Avro string but is [Kind#UUID], carrying
    /// bytes the string accessor would hand back as mojibake, so it is rejected too.
    private void requireAvroStringKey(SchemaNode keyNode, String path) {
        if (!(keyNode instanceof SchemaNode.PrimitiveNode key)
                || convertPrimitive(key).kind() != Kind.STRING) {
            throw new IllegalArgumentException("Map '" + path
                    + "' key must be a BYTE_ARRAY annotated as STRING, ENUM or JSON"
                    + " — Avro map keys are strings — but is " + mapKeyType(keyNode));
        }
    }

    /// Describe a map key for the rejection message. A `BYTE_ARRAY` key is rejected
    /// for the annotation it lacks rather than its physical type, so name the missing
    /// annotation instead of repeating the type the message just asked for.
    private static String mapKeyType(SchemaNode keyNode) {
        if (!(keyNode instanceof SchemaNode.PrimitiveNode key)) {
            return "group '" + keyNode.name() + "'";
        }
        if (key.logicalType() != null) {
            return key.type() + " (" + key.logicalType() + ")";
        }
        return key.type() == PhysicalType.BYTE_ARRAY
                ? "BYTE_ARRAY with no logical annotation"
                : key.type().toString();
    }

    private AvroPlanNode convertPrimitive(SchemaNode.PrimitiveNode prim) {
        LogicalType logicalType = prim.logicalType();

        if (logicalType != null) {
            return annotate(convertLogicalType(prim.type(), logicalType, prim));
        }

        return annotate(convertPhysicalType(prim.type(), prim));
    }

    /// Record on the converted schema what Avro's type system cannot express, for
    /// consumers that see the schema without the plan. The annotation is derived
    /// from the node's [Kind] and is never read back: readers take the [Kind].
    private static AvroPlanNode annotate(AvroPlanNode node) {
        if (node.kind() == Kind.UNSIGNED_INT32) {
            node.avro().addProp(UNSIGNED_INT32_PROP, true);
        }
        return node;
    }

    private AvroPlanNode convertLogicalType(PhysicalType physicalType, LogicalType logicalType,
            SchemaNode.PrimitiveNode prim) {
        return switch (logicalType) {
            case LogicalType.StringType s -> string(prim);
            case LogicalType.EnumType e -> string(prim);
            case LogicalType.JsonType j -> string(prim);
            case LogicalType.BsonType b -> binary(prim);
            case LogicalType.UuidType u -> AvroPlanNode.leaf(
                    LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)), Kind.UUID, prim);
            case LogicalType.DateType dt -> AvroPlanNode.leaf(
                    LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), Kind.INT, prim);
            case LogicalType.TimeType t -> convertTimeType(t, prim);
            case LogicalType.TimestampType t -> convertTimestampType(t, prim);
            case LogicalType.DecimalType d -> convertDecimalType(physicalType, d, prim);
            case LogicalType.IntType i -> convertIntType(i, prim);
            case LogicalType.IntervalType iv -> AvroPlanNode.leaf(
                    Schema.createFixed("interval", null, null, 12), Kind.FIXED, prim);
            case LogicalType.Float16Type f -> AvroPlanNode.leaf(
                    Schema.createFixed("float16", null, null, 2), Kind.FIXED, prim);
            case LogicalType.ListType l -> convertPhysicalType(physicalType, prim);
            case LogicalType.MapType m -> convertPhysicalType(physicalType, prim);
            case LogicalType.VariantType v -> throw new IllegalStateException(
                    "VariantType is a group-level annotation; encountered on primitive column " + prim.name());
            // Avro has no geospatial type — round-trip the raw WKB payload as bytes.
            case LogicalType.GeometryType g -> binary(prim);
            case LogicalType.GeographyType g -> binary(prim);
            case LogicalType.NullType n -> AvroPlanNode.leaf(
                    Schema.create(Schema.Type.NULL), Kind.NULL, prim);
        };
    }

    private static AvroPlanNode string(SchemaNode.PrimitiveNode prim) {
        return AvroPlanNode.leaf(Schema.create(Schema.Type.STRING), Kind.STRING, prim);
    }

    private static AvroPlanNode binary(SchemaNode.PrimitiveNode prim) {
        return AvroPlanNode.leaf(Schema.create(Schema.Type.BYTES), Kind.BINARY, prim);
    }

    private static AvroPlanNode convertTimeType(LogicalType.TimeType t, SchemaNode.PrimitiveNode prim) {
        return switch (t.unit()) {
            case MILLIS -> AvroPlanNode.leaf(
                    LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)), Kind.INT, prim);
            case MICROS -> AvroPlanNode.leaf(
                    LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)), Kind.LONG, prim);
            case NANOS -> AvroPlanNode.leaf(Schema.create(Schema.Type.LONG), Kind.LONG, prim);
        };
    }

    private static AvroPlanNode convertTimestampType(LogicalType.TimestampType t,
            SchemaNode.PrimitiveNode prim) {
        Schema schema = switch (t.unit()) {
            case MILLIS -> t.isAdjustedToUTC()
                    ? LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
                    : LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case MICROS -> t.isAdjustedToUTC()
                    ? LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
                    : LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case NANOS -> Schema.create(Schema.Type.LONG);
        };
        return AvroPlanNode.leaf(schema, Kind.LONG, prim);
    }

    /// A `FIXED_LEN_BYTE_ARRAY`-backed decimal materializes as Avro `fixed` and is
    /// read as the raw on-disk bytes; every other backing type materializes as
    /// `bytes` carrying the unscaled magnitude, which only the decimal accessor
    /// recovers from an `INT32`/`INT64` column.
    private AvroPlanNode convertDecimalType(PhysicalType physicalType, LogicalType.DecimalType d,
            SchemaNode.PrimitiveNode prim) {
        org.apache.avro.LogicalType decimal = LogicalTypes.decimal(d.precision(), d.scale());
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return AvroPlanNode.leaf(decimal.addToSchema(fixedSchema(prim, fixedByteLength(prim))),
                    Kind.FIXED, prim);
        }
        return AvroPlanNode.leaf(decimal.addToSchema(Schema.create(Schema.Type.BYTES)), Kind.DECIMAL, prim);
    }

    private static AvroPlanNode convertIntType(LogicalType.IntType i, SchemaNode.PrimitiveNode prim) {
        // UINT_32 widens to LONG so the unsigned magnitude fits; the kind records that
        // the column is still int[]-backed and must be read as an int and widened.
        if (!i.isSigned() && i.bitWidth() == 32) {
            return AvroPlanNode.leaf(Schema.create(Schema.Type.LONG), Kind.UNSIGNED_INT32, prim);
        }
        if (i.bitWidth() <= 32) {
            return AvroPlanNode.leaf(Schema.create(Schema.Type.INT), Kind.INT, prim);
        }
        return AvroPlanNode.leaf(Schema.create(Schema.Type.LONG), Kind.LONG, prim);
    }

    private AvroPlanNode convertPhysicalType(PhysicalType type, SchemaNode.PrimitiveNode prim) {
        return switch (type) {
            case BOOLEAN -> AvroPlanNode.leaf(Schema.create(Schema.Type.BOOLEAN), Kind.BOOLEAN, prim);
            case INT32 -> AvroPlanNode.leaf(Schema.create(Schema.Type.INT), Kind.INT, prim);
            case INT64 -> AvroPlanNode.leaf(Schema.create(Schema.Type.LONG), Kind.LONG, prim);
            case FLOAT -> AvroPlanNode.leaf(Schema.create(Schema.Type.FLOAT), Kind.FLOAT, prim);
            case DOUBLE -> AvroPlanNode.leaf(Schema.create(Schema.Type.DOUBLE), Kind.DOUBLE, prim);
            case BYTE_ARRAY -> binary(prim);
            case FIXED_LEN_BYTE_ARRAY -> AvroPlanNode.leaf(
                    fixedSchema(prim, fixedByteLength(prim)), Kind.FIXED, prim);
            // INT96 has a fixed 12-byte width that the schema does not carry a length for.
            case INT96 -> AvroPlanNode.leaf(fixedSchema(prim, 12), Kind.FIXED, prim);
        };
    }

    private Schema fixedSchema(SchemaNode.PrimitiveNode prim, int size) {
        AvroNames.TypeName typeName = names.typeName(prim);
        Schema schema = Schema.createFixed(typeName.name(), null, typeName.namespace(), size);
        applyParquetName(schema, prim);
        return schema;
    }

    private void applyParquetName(Schema schema, SchemaNode node) {
        String raw = names.rewrittenFrom(node);
        if (raw != null) {
            schema.addProp(PARQUET_NAME_PROP, raw);
        }
    }

    private void applyParquetName(Schema.Field field, SchemaNode node) {
        String raw = names.rewrittenFrom(node);
        if (raw != null) {
            field.addProp(PARQUET_NAME_PROP, raw);
        }
    }

    private void rejectCanonicalRootConflict() {
        String fullName = names.typeName(fileSchema.getRootNode()).fullName();
        if (("interval".equals(fullName) && containsLogicalType(fileSchema.getRootNode(), LogicalType.IntervalType.class))
                || ("float16".equals(fullName)
                && containsLogicalType(fileSchema.getRootNode(), LogicalType.Float16Type.class))) {
            throw new IllegalArgumentException(
                    "Root named '" + fullName + "' conflicts with canonical fixed type '" + fullName + "'");
        }
    }

    private static boolean containsLogicalType(SchemaNode node,
            Class<? extends LogicalType> logicalTypeClass) {
        if (node instanceof SchemaNode.PrimitiveNode primitive
                && logicalTypeClass.isInstance(primitive.logicalType())) {
            return true;
        }
        if (node instanceof SchemaNode.GroupNode group) {
            for (SchemaNode child : group.children()) {
                if (containsLogicalType(child, logicalTypeClass)) {
                    return true;
                }
            }
        }
        return false;
    }

    /// Resolve the declared byte length of a [PhysicalType#FIXED_LEN_BYTE_ARRAY]
    /// column, looked up from its [dev.hardwood.schema.ColumnSchema] by leaf index.
    /// A fixed-length column with no `type_length` is malformed; fail early rather
    /// than emit a bogus zero-width Avro `fixed`, matching the decoders that reject
    /// the same condition.
    private int fixedByteLength(SchemaNode.PrimitiveNode prim) {
        Integer typeLength = fileSchema.getColumn(prim.columnIndex()).typeLength();
        if (typeLength == null) {
            throw new IllegalArgumentException(
                    "FIXED_LEN_BYTE_ARRAY column '" + prim.name() + "' is missing its type_length");
        }
        return typeLength;
    }

    private static Schema nullable(Schema schema) {
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
}
