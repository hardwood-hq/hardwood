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

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Converts a Hardwood [FileSchema] to an Avro [Schema].
///
/// The mapping follows the same conventions as parquet-java's
/// `AvroSchemaConverter`, producing Avro schemas that are compatible
/// with standard Avro tools and libraries.
public final class AvroSchemaConverter {

    private AvroSchemaConverter() {
    }

    /// Convert a Hardwood FileSchema to an Avro record Schema.
    ///
    /// @param fileSchema the Parquet file schema
    /// @return the equivalent Avro record schema
    public static Schema convert(FileSchema fileSchema) {
        SchemaNode.GroupNode root = fileSchema.getRootNode();
        return convertGroup(root, fileSchema.getName());
    }

    private static Schema convertGroup(SchemaNode.GroupNode group, String recordName) {
        List<Schema.Field> fields = new ArrayList<>();
        for (SchemaNode child : group.children()) {
            Schema fieldSchema = convertNode(child);
            if (child.repetitionType() == RepetitionType.OPTIONAL) {
                fieldSchema = nullable(fieldSchema);
            }
            fields.add(new Schema.Field(child.name(), fieldSchema, null, null));
        }
        return Schema.createRecord(recordName, null, null, false, fields);
    }

    private static Schema convertNode(SchemaNode node) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> convertPrimitive(prim);
            case SchemaNode.GroupNode group -> convertGroupNode(group);
        };
    }

    private static Schema convertGroupNode(SchemaNode.GroupNode group) {
        if (group.isList()) {
            return convertList(group);
        }
        if (group.isMap()) {
            return convertMap(group);
        }
        // Plain struct
        return convertGroup(group, group.name());
    }

    private static Schema convertList(SchemaNode.GroupNode listGroup) {
        SchemaNode element = listGroup.getListElement();
        if (element == null) {
            // Fallback for malformed list
            return Schema.createArray(Schema.create(Schema.Type.NULL));
        }
        Schema elementSchema = convertNode(element);
        if (element.repetitionType() == RepetitionType.OPTIONAL) {
            elementSchema = nullable(elementSchema);
        }
        return Schema.createArray(elementSchema);
    }

    private static Schema convertMap(SchemaNode.GroupNode mapGroup) {
        // MAP -> key_value (repeated) -> key, value
        if (mapGroup.children().isEmpty()) {
            return Schema.createMap(Schema.create(Schema.Type.NULL));
        }
        SchemaNode inner = mapGroup.children().get(0);
        if (inner instanceof SchemaNode.GroupNode kvGroup && kvGroup.children().size() >= 2) {
            SchemaNode valueNode = kvGroup.children().get(1);
            Schema valueSchema = convertNode(valueNode);
            if (valueNode.repetitionType() == RepetitionType.OPTIONAL) {
                valueSchema = nullable(valueSchema);
            }
            return Schema.createMap(valueSchema);
        }
        return Schema.createMap(Schema.create(Schema.Type.NULL));
    }

    private static Schema convertPrimitive(SchemaNode.PrimitiveNode prim) {
        LogicalType logicalType = prim.logicalType();

        if (logicalType != null) {
            return convertLogicalType(prim.type(), logicalType, prim);
        }

        return convertPhysicalType(prim.type(), prim);
    }

    private static Schema convertLogicalType(PhysicalType physicalType, LogicalType logicalType,
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
            case LogicalType.ListType l -> convertPhysicalType(physicalType, prim);
            case LogicalType.MapType m -> convertPhysicalType(physicalType, prim);
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

    private static Schema convertDecimalType(PhysicalType physicalType, LogicalType.DecimalType d,
            SchemaNode.PrimitiveNode prim) {
        org.apache.avro.LogicalType decimal = LogicalTypes.decimal(d.precision(), d.scale());
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return decimal.addToSchema(Schema.createFixed(
                    prim.name(), null, null, prim.columnIndex()));
        }
        return decimal.addToSchema(Schema.create(Schema.Type.BYTES));
    }

    private static Schema convertIntType(LogicalType.IntType i) {
        if (!i.isSigned() && i.bitWidth() == 32) {
            return Schema.create(Schema.Type.LONG);
        }
        if (i.bitWidth() <= 32) {
            return Schema.create(Schema.Type.INT);
        }
        return Schema.create(Schema.Type.LONG);
    }

    private static Schema convertPhysicalType(PhysicalType type, SchemaNode.PrimitiveNode prim) {
        return switch (type) {
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case INT32 -> Schema.create(Schema.Type.INT);
            case INT64 -> Schema.create(Schema.Type.LONG);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case BYTE_ARRAY -> Schema.create(Schema.Type.BYTES);
            case FIXED_LEN_BYTE_ARRAY -> Schema.createFixed(prim.name(), null, null, 12);
            case INT96 -> Schema.createFixed(prim.name(), null, null, 12);
        };
    }

    private static Schema nullable(Schema schema) {
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
}
