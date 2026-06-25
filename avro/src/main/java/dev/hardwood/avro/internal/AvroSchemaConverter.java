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
public final class AvroSchemaConverter {

    /// Marker property on an Avro `LONG` schema indicating that the source
    /// column is physically `INT32` with the `UINT_32` logical type. Readers
    /// must call the `getInt` accessor and apply [Integer#toUnsignedLong] to
    /// recover the unsigned magnitude.
    public static final String UNSIGNED_INT32_PROP = "hardwood.unsignedInt32";

    private final FileSchema fileSchema;

    /// The projection to narrow to, or `null` to retain every field.
    private final ProjectedSchema projected;

    private AvroSchemaConverter(FileSchema fileSchema, ProjectedSchema projected) {
        this.fileSchema = fileSchema;
        this.projected = projected;
    }

    /// Convert a Hardwood FileSchema to an Avro record Schema.
    ///
    /// @param fileSchema the Parquet file schema
    /// @return the equivalent Avro record schema
    public static Schema convert(FileSchema fileSchema) {
        return new AvroSchemaConverter(fileSchema, null).convertRoot();
    }

    /// Convert a Hardwood FileSchema to an Avro record Schema, narrowed to the
    /// given column projection. Only projected fields appear in the result, with
    /// pruning applied recursively through structs, list elements, and map values
    /// — so `address.city` yields an `address` record carrying only `city`, and
    /// `items.list.element.quantity` yields a list whose element record carries
    /// only `quantity`, mirroring the partial rows the row reader serves.
    ///
    /// @param fileSchema the Parquet file schema
    /// @param projection the columns to retain
    /// @return the equivalent Avro record schema, restricted to projected fields
    public static Schema convert(FileSchema fileSchema, ColumnProjection projection) {
        if (projection.projectsAll()) {
            return convert(fileSchema);
        }
        ProjectedSchema projected = ProjectedSchema.create(fileSchema, projection);
        return new AvroSchemaConverter(fileSchema, projected).convertRoot();
    }

    private Schema convertRoot() {
        return convertGroup(fileSchema.getRootNode(), fileSchema.getName());
    }

    /// Convert a struct group (or the schema root) to an Avro record, retaining
    /// only children that contain a projected leaf when a projection is active.
    private Schema convertGroup(SchemaNode.GroupNode group, String recordName) {
        List<Schema.Field> fields = new ArrayList<>();
        for (SchemaNode child : group.children()) {
            if (projected != null && !hasProjectedLeaf(child, projected)) {
                continue;
            }
            Schema fieldSchema = convertNode(child);
            // Wrap OPTIONAL fields in [null, T] — unless T is already the
            // NULL type, since Avro unions disallow duplicate branches.
            if (child.repetitionType() == RepetitionType.OPTIONAL
                    && fieldSchema.getType() != Schema.Type.NULL) {
                fieldSchema = nullable(fieldSchema);
            }
            fields.add(new Schema.Field(child.name(), fieldSchema, null, null));
        }
        return Schema.createRecord(recordName, null, null, false, fields);
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

    private Schema convertNode(SchemaNode node) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> convertPrimitive(prim);
            case SchemaNode.GroupNode group -> convertGroupNode(group);
        };
    }

    private Schema convertGroupNode(SchemaNode.GroupNode group) {
        if (group.isVariant()) {
            return convertVariant(group);
        }
        if (group.isList()) {
            return convertList(group);
        }
        if (group.isMap()) {
            return convertMap(group);
        }
        // Plain struct — prune unprojected children recursively.
        return convertGroup(group, group.name());
    }

    /// Emit a two-field Avro RECORD carrying the canonical Variant bytes.
    /// Matches parquet-java's [org.apache.parquet.avro.AvroParquetReader]
    /// output shape so tooling that already consumes the parquet-java Avro
    /// surface works unchanged. Callers who want typed access to the Variant
    /// payload use [dev.hardwood.reader.RowReader#getVariant] on the file
    /// reader and the [dev.hardwood.row.PqVariant] API.
    private static Schema convertVariant(SchemaNode.GroupNode group) {
        List<Schema.Field> fields = List.of(
                new Schema.Field("metadata", Schema.create(Schema.Type.BYTES), null, null),
                new Schema.Field("value", Schema.create(Schema.Type.BYTES), null, null));
        return Schema.createRecord(group.name(), null, null, false, new ArrayList<>(fields));
    }

    private Schema convertList(SchemaNode.GroupNode listGroup) {
        SchemaNode element = listGroup.getListElement();
        if (element == null) {
            // Fallback for malformed list
            return Schema.createArray(Schema.create(Schema.Type.NULL));
        }
        // The list column is only reached when it has a projected leaf; prune the
        // element subtree so a list<struct> with a sub-field projection narrows to
        // the served fields.
        Schema elementSchema = convertNode(element);
        if (element.repetitionType() == RepetitionType.OPTIONAL
                && elementSchema.getType() != Schema.Type.NULL) {
            elementSchema = nullable(elementSchema);
        }
        return Schema.createArray(elementSchema);
    }

    private Schema convertMap(SchemaNode.GroupNode mapGroup) {
        // MAP -> key_value (repeated) -> key, value
        if (mapGroup.children().isEmpty()) {
            return Schema.createMap(Schema.create(Schema.Type.NULL));
        }
        SchemaNode inner = mapGroup.children().get(0);
        if (inner instanceof SchemaNode.GroupNode kvGroup && kvGroup.children().size() >= 2) {
            SchemaNode valueNode = kvGroup.children().get(1);
            // Prune the value subtree so a map<_, struct> with a sub-field
            // projection narrows to the served fields (the key is always read).
            Schema valueSchema = convertNode(valueNode);
            if (valueNode.repetitionType() == RepetitionType.OPTIONAL
                    && valueSchema.getType() != Schema.Type.NULL) {
                valueSchema = nullable(valueSchema);
            }
            return Schema.createMap(valueSchema);
        }
        return Schema.createMap(Schema.create(Schema.Type.NULL));
    }

    private Schema convertPrimitive(SchemaNode.PrimitiveNode prim) {
        LogicalType logicalType = prim.logicalType();

        if (logicalType != null) {
            return convertLogicalType(prim.type(), logicalType, prim);
        }

        return convertPhysicalType(prim.type(), prim);
    }

    private Schema convertLogicalType(PhysicalType physicalType, LogicalType logicalType,
            SchemaNode.PrimitiveNode prim) {
        return switch (logicalType) {
            case LogicalType.StringType s -> Schema.create(Schema.Type.STRING);
            case LogicalType.EnumType e -> Schema.create(Schema.Type.STRING);
            case LogicalType.JsonType j -> Schema.create(Schema.Type.STRING);
            case LogicalType.BsonType b -> Schema.create(Schema.Type.BYTES);
            case LogicalType.UuidType u -> LogicalTypes.uuid()
                    .addToSchema(Schema.create(Schema.Type.STRING));
            case LogicalType.DateType dt -> LogicalTypes.date()
                    .addToSchema(Schema.create(Schema.Type.INT));
            case LogicalType.TimeType t -> convertTimeType(t);
            case LogicalType.TimestampType t -> convertTimestampType(t);
            case LogicalType.DecimalType d -> convertDecimalType(physicalType, d, prim);
            case LogicalType.IntType i -> convertIntType(i);
            case LogicalType.IntervalType iv ->
                    Schema.createFixed("interval", null, null, 12);
            case LogicalType.Float16Type f ->
                    Schema.createFixed("float16", null, null, 2);
            case LogicalType.ListType l -> convertPhysicalType(physicalType, prim);
            case LogicalType.MapType m -> convertPhysicalType(physicalType, prim);
            case LogicalType.VariantType v -> throw new IllegalStateException(
                    "VariantType is a group-level annotation; encountered on primitive column " + prim.name());
            // Avro has no geospatial type — round-trip the raw WKB payload as bytes.
            case LogicalType.GeometryType g -> Schema.create(Schema.Type.BYTES);
            case LogicalType.GeographyType g -> Schema.create(Schema.Type.BYTES);
            case LogicalType.NullType n -> Schema.create(Schema.Type.NULL);
        };
    }

    private static Schema convertTimeType(LogicalType.TimeType t) {
        return switch (t.unit()) {
            case MILLIS -> LogicalTypes.timeMillis()
                    .addToSchema(Schema.create(Schema.Type.INT));
            case MICROS -> LogicalTypes.timeMicros()
                    .addToSchema(Schema.create(Schema.Type.LONG));
            case NANOS -> Schema.create(Schema.Type.LONG);
        };
    }

    private static Schema convertTimestampType(LogicalType.TimestampType t) {
        return switch (t.unit()) {
            case MILLIS -> t.isAdjustedToUTC()
                    ? LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
                    : LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case MICROS -> t.isAdjustedToUTC()
                    ? LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
                    : LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case NANOS -> Schema.create(Schema.Type.LONG);
        };
    }

    private Schema convertDecimalType(PhysicalType physicalType, LogicalType.DecimalType d,
            SchemaNode.PrimitiveNode prim) {
        org.apache.avro.LogicalType decimal = LogicalTypes.decimal(d.precision(), d.scale());
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return decimal.addToSchema(Schema.createFixed(
                    prim.name(), null, null, fixedByteLength(prim)));
        }
        return decimal.addToSchema(Schema.create(Schema.Type.BYTES));
    }

    private static Schema convertIntType(LogicalType.IntType i) {
        if (!i.isSigned() && i.bitWidth() == 32) {
            Schema schema = Schema.create(Schema.Type.LONG);
            schema.addProp(UNSIGNED_INT32_PROP, true);
            return schema;
        }
        if (i.bitWidth() <= 32) {
            return Schema.create(Schema.Type.INT);
        }
        return Schema.create(Schema.Type.LONG);
    }

    private Schema convertPhysicalType(PhysicalType type, SchemaNode.PrimitiveNode prim) {
        return switch (type) {
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case INT32 -> Schema.create(Schema.Type.INT);
            case INT64 -> Schema.create(Schema.Type.LONG);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case BYTE_ARRAY -> Schema.create(Schema.Type.BYTES);
            case FIXED_LEN_BYTE_ARRAY -> Schema.createFixed(prim.name(), null, null, fixedByteLength(prim));
            // INT96 has a fixed 12-byte width that the schema does not carry a length for.
            case INT96 -> Schema.createFixed(prim.name(), null, null, 12);
        };
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
