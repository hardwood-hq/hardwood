/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.SchemaNode;

/// One node in the Variant shredding tree. Every shredded level carries an
/// optional `value` binary column; the `typed` component — when present —
/// describes the shredded projection (primitive, array, or object). Built
/// once at schema-construction time by [#build(SchemaNode.GroupNode, ProjectedSchema)].
///
/// @param valueCol projected column index of this level's `value` binary, or `-1` if the column was pruned from the projection
/// @param valueDefLevel def level at or above which `value` is non-null for this level
/// @param typed the shredded-projection component, or `null` when this level is purely untyped (just `value`)
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/VariantShredding.md">Parquet Variant Shredding</a>
public record ShredLevel(int valueCol, int valueDefLevel, Typed typed) {

    /// The shredded projection at a given level. Sealed union with three
    /// variants matching the spec's three shredding shapes.
    public sealed interface Typed {

        /// typed_value is a primitive Parquet column.
        ///
        /// @param col projected column index of the typed_value column
        /// @param defLevel def level at or above which typed_value is non-null
        /// @param physicalType Parquet physical type of typed_value
        /// @param logicalType Parquet logical type on typed_value (may be null)
        /// @param carrier the Variant type typed_value encodes to, resolved from the two types
        record Primitive(int col, int defLevel,
                         PhysicalType physicalType,
                         LogicalType logicalType,
                         Carrier carrier) implements Typed {

            /// Resolves the [Carrier] from the physical and logical type.
            ///
            /// @throws ParquetReadException if the Variant shredding spec does not allow the
            ///         pair as a `typed_value`
            public Primitive(int col, int defLevel, PhysicalType physicalType, LogicalType logicalType) {
                this(col, defLevel, physicalType, logicalType, Carrier.of(physicalType, logicalType));
            }
        }

        /// typed_value is a LIST group whose element is itself a shredded level
        /// (so elements may carry their own `value` / `typed_value` pair).
        ///
        /// @param listDefLevel def level at or above which the list group is non-null
        /// @param elementDefLevel def level at or above which an actual element exists (vs. empty list)
        /// @param elementRepLevel rep level used to terminate element runs
        /// @param element child shredded level for each array element
        record Array(int listDefLevel, int elementDefLevel, int elementRepLevel,
                     ShredLevel element) implements Typed {}

        /// typed_value is a struct whose children are named shredded levels.
        ///
        /// @param structDefLevel def level at or above which the struct group is non-null
        /// @param fieldNames names of the shredded fields, in dictionary order emitted by the schema
        /// @param fields matching shredded levels (indices align with `fieldNames`)
        record Object(int structDefLevel, String[] fieldNames, ShredLevel[] fields) implements Typed {}
    }

    /// The Parquet carriers a primitive `typed_value` may use, each naming the Variant
    /// type it encodes to: the carriers the Variant shredding spec defines, with
    /// `INT(32, signed)` and `INT(64, signed)` read as the unannotated `INT32` and `INT64`
    /// they are equal to.
    public enum Carrier {
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        /// `INT32` `DECIMAL`.
        DECIMAL4,
        /// `INT64` `DECIMAL`.
        DECIMAL8,
        /// `BYTE_ARRAY` `DECIMAL`.
        DECIMAL_BYTE_ARRAY,
        /// `FIXED_LEN_BYTE_ARRAY` `DECIMAL`, encoded at the width its precision selects.
        DECIMAL_FIXED_LEN_BYTE_ARRAY,
        DATE,
        /// `INT64` `TIME(MICROS)`, not adjusted to UTC.
        TIME_MICROS,
        TIMESTAMP_MICROS,
        TIMESTAMP_NANOS,
        BINARY,
        /// `BYTE_ARRAY` `STRING`.
        STRING,
        UUID;

        /// Resolve the carrier of a `typed_value` column.
        ///
        /// @throws ParquetReadException if the Variant shredding spec does not allow the pair
        ///         as a `typed_value`: shredded values must use the types it lists
        static Carrier of(PhysicalType physicalType, LogicalType logicalType) {
            Carrier carrier = switch (physicalType) {
                case BOOLEAN -> logicalType == null ? BOOLEAN : null;
                case FLOAT -> logicalType == null ? FLOAT : null;
                case DOUBLE -> logicalType == null ? DOUBLE : null;
                case INT32 -> ofInt32(logicalType);
                case INT64 -> ofInt64(logicalType);
                case BYTE_ARRAY -> ofByteArray(logicalType);
                case FIXED_LEN_BYTE_ARRAY -> ofFixedLenByteArray(logicalType);
                case INT96 -> null;
            };
            if (carrier == null) {
                String type = logicalType == null ? physicalType.name() : physicalType + " " + logicalType;
                throw new ParquetReadException("Shredded Variant typed_value has type " + type
                        + ", which the Variant shredding specification does not allow");
            }
            return carrier;
        }

        private static Carrier ofInt32(LogicalType logicalType) {
            return switch (logicalType) {
                case null -> INT32;
                case LogicalType.IntType i when i.isSigned() && i.bitWidth() == 8 -> INT8;
                case LogicalType.IntType i when i.isSigned() && i.bitWidth() == 16 -> INT16;
                case LogicalType.IntType i when i.isSigned() && i.bitWidth() == 32 -> INT32;
                case LogicalType.DecimalType d -> DECIMAL4;
                case LogicalType.DateType d -> DATE;
                default -> null;
            };
        }

        private static Carrier ofInt64(LogicalType logicalType) {
            return switch (logicalType) {
                case null -> INT64;
                case LogicalType.IntType i when i.isSigned() && i.bitWidth() == 64 -> INT64;
                case LogicalType.DecimalType d -> DECIMAL8;
                case LogicalType.TimeType t when !t.isAdjustedToUTC() && t.unit() == LogicalType.TimeUnit.MICROS ->
                        TIME_MICROS;
                case LogicalType.TimestampType t when t.unit() == LogicalType.TimeUnit.MICROS -> TIMESTAMP_MICROS;
                case LogicalType.TimestampType t when t.unit() == LogicalType.TimeUnit.NANOS -> TIMESTAMP_NANOS;
                default -> null;
            };
        }

        private static Carrier ofByteArray(LogicalType logicalType) {
            return switch (logicalType) {
                case null -> BINARY;
                case LogicalType.StringType s -> STRING;
                case LogicalType.DecimalType d -> DECIMAL_BYTE_ARRAY;
                default -> null;
            };
        }

        /// A `FIXED_LEN_BYTE_ARRAY` without a logical type is no Variant binary: the spec
        /// carries binary as `BYTE_ARRAY` only.
        private static Carrier ofFixedLenByteArray(LogicalType logicalType) {
            return switch (logicalType) {
                case LogicalType.UuidType u -> UUID;
                case LogicalType.DecimalType d -> DECIMAL_FIXED_LEN_BYTE_ARRAY;
                case null, default -> null;
            };
        }
    }

    // ==================== Builders ====================

    /// Build the shredding tree for a Variant-annotated group. The group's
    /// first child (`metadata`) is owned by the caller; we consume `value`
    /// plus the optional `typed_value` sibling.
    public static ShredLevel build(SchemaNode.GroupNode variantGroup, ProjectedSchema projectedSchema) {
        // The Variant group's children are [metadata, value[, typed_value]].
        List<SchemaNode> kids = variantGroup.children();
        SchemaNode valueNode = kids.get(1);
        SchemaNode typedValueNode = kids.size() >= 3 ? kids.get(2) : null;
        return buildFromComponents(valueNode, typedValueNode, projectedSchema);
    }

    /// Build a nested shredded level from a group whose direct children are
    /// any combination of `value` and `typed_value` (at least one must be
    /// present) — the shape used for array elements and object fields within a
    /// Variant shred tree.
    static ShredLevel buildNested(SchemaNode.GroupNode group, ProjectedSchema projectedSchema) {
        SchemaNode valueNode = null;
        SchemaNode typedValueNode = null;
        for (SchemaNode child : group.children()) {
            switch (child.name()) {
                case "value" -> valueNode = child;
                case "typed_value" -> typedValueNode = child;
                default -> throw new ParquetReadException(
                        "Unexpected shredded-Variant child '" + child.name() + "' in group '" + group.name() + "'");
            }
        }
        if (valueNode == null && typedValueNode == null) {
            throw new ParquetReadException(
                    "Shredded-Variant group '" + group.name() + "' must contain at least one of 'value' or 'typed_value'");
        }
        return buildFromComponents(valueNode, typedValueNode, projectedSchema);
    }

    private static ShredLevel buildFromComponents(SchemaNode valueNode, SchemaNode typedValueNode,
                                                  ProjectedSchema projectedSchema) {
        int valueCol = -1;
        int valueDefLevel = 0;
        if (valueNode instanceof SchemaNode.PrimitiveNode valuePrim) {
            if (valuePrim.type() != PhysicalType.BYTE_ARRAY) {
                throw new ParquetReadException(
                        "Shredded-Variant 'value' column must be BYTE_ARRAY, got " + valuePrim.type());
            }
            valueCol = projectedSchema.toProjectedIndex(valuePrim.columnIndex());
            valueDefLevel = valuePrim.maxDefinitionLevel();
        }
        else if (valueNode != null) {
            throw new ParquetReadException("Shredded-Variant 'value' child must be a primitive");
        }
        Typed typed = typedValueNode == null ? null : buildTyped(typedValueNode, projectedSchema);
        return new ShredLevel(valueCol, valueDefLevel, typed);
    }

    private static Typed buildTyped(SchemaNode typedValueNode, ProjectedSchema projectedSchema) {
        return switch (typedValueNode) {
            case SchemaNode.PrimitiveNode prim -> new Typed.Primitive(
                    projectedSchema.toProjectedIndex(prim.columnIndex()),
                    prim.maxDefinitionLevel(),
                    prim.type(),
                    prim.logicalType());
            case SchemaNode.GroupNode group -> buildTypedGroup(group, projectedSchema);
        };
    }

    private static Typed buildTypedGroup(SchemaNode.GroupNode group, ProjectedSchema projectedSchema) {
        if (group.isList()) {
            // Spec: typed_value is a 3-level LIST whose single element is a
            // nested shredded group. `getListElement()` peels the intermediate
            // `list`/`array` group for us.
            SchemaNode elementNode = group.getListElement();
            if (!(elementNode instanceof SchemaNode.GroupNode elementGroup)) {
                throw new ParquetReadException(
                        "Shredded-Variant array typed_value must have a group element, got " + elementNode);
            }
            ShredLevel element = buildNested(elementGroup, projectedSchema);
            return new Typed.Array(
                    group.maxDefinitionLevel(),
                    elementGroup.maxDefinitionLevel(),
                    elementGroup.maxRepetitionLevel(),
                    element);
        }
        // Plain struct: children are named shredded levels.
        List<SchemaNode> kids = group.children();
        String[] names = new String[kids.size()];
        ShredLevel[] fields = new ShredLevel[kids.size()];
        for (int i = 0; i < kids.size(); i++) {
            SchemaNode child = kids.get(i);
            if (!(child instanceof SchemaNode.GroupNode childGroup)) {
                throw new ParquetReadException(
                        "Shredded-Variant object field '" + child.name() + "' must be a group");
            }
            names[i] = child.name();
            fields[i] = buildNested(childGroup, projectedSchema);
        }
        return new Typed.Object(group.maxDefinitionLevel(), names, fields);
    }

    // ==================== Convenience ====================

    /// All projected column indices referenced by this shredded subtree,
    /// in an unspecified but deterministic order. Useful for diagnostics.
    public int[] allProjectedCols() {
        List<Integer> out = new ArrayList<>();
        collectCols(this, out);
        int[] arr = new int[out.size()];
        for (int i = 0; i < out.size(); i++) {
            arr[i] = out.get(i);
        }
        return arr;
    }

    private static void collectCols(ShredLevel level, List<Integer> out) {
        if (level.valueCol >= 0) {
            out.add(level.valueCol);
        }
        if (level.typed instanceof Typed.Primitive p && p.col >= 0) {
            out.add(p.col);
        }
        else if (level.typed instanceof Typed.Array a) {
            collectCols(a.element, out);
        }
        else if (level.typed instanceof Typed.Object o) {
            for (ShredLevel f : o.fields) {
                collectCols(f, out);
            }
        }
    }

}
