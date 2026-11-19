/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.util.List;

import org.apache.avro.Schema;

import dev.hardwood.schema.SchemaNode;

/// One value position of the decode plan produced by [AvroSchemaConverter#plan].
///
/// A node pairs the Hardwood [SchemaNode] a value is read from with the Avro
/// [Schema] it is materialized into, and carries the [Kind] that pairing implies.
///
/// Children are positional: a [Kind#STRUCT] node has one child per field of its
/// record, at that field's Avro position; a [Kind#LIST] node has the element plan;
/// a [Kind#MAP] node has the value plan; every other node is a leaf.
public final class AvroPlanNode {

    /// How a value is read from the row reader and represented in Avro. Resolved
    /// once per value position while the schema is converted, so materialization
    /// never re-derives it from the Avro shape.
    public enum Kind {
        BOOLEAN,
        INT,
        LONG,
        /// Physically `INT32` with the `UINT_32` logical type: read as an int and
        /// widened through [Integer#toUnsignedLong].
        UNSIGNED_INT32,
        FLOAT,
        DOUBLE,
        STRING,
        UUID,
        BINARY,
        DECIMAL,
        FIXED,
        STRUCT,
        VARIANT,
        LIST,
        MAP,
        /// The Parquet `UNKNOWN` logical type. A non-null value at this position is
        /// always a materialization invariant failure.
        NULL
    }

    private static final AvroPlanNode[] NO_CHILDREN = new AvroPlanNode[0];

    private final Schema avro;
    private final Kind kind;
    private final SchemaNode source;
    private final AvroPlanNode[] children;

    private AvroPlanNode(Schema avro, Kind kind, SchemaNode source, AvroPlanNode[] children) {
        this.avro = avro;
        this.kind = kind;
        this.source = source;
        this.children = children;
    }

    /// A node with no children: a primitive, or a Variant group whose two byte
    /// fields are read through the Variant accessor rather than as fields.
    static AvroPlanNode leaf(Schema avro, Kind kind, SchemaNode source) {
        return new AvroPlanNode(avro, kind, source, NO_CHILDREN);
    }

    /// A record node whose children are its Avro fields, in field order. The counts
    /// must agree: a plan that has drifted from the record it describes would read
    /// each field past the drift through a neighbouring field's accessor.
    static AvroPlanNode record(Schema avro, SchemaNode source, List<AvroPlanNode> children) {
        if (children.size() != avro.getFields().size()) {
            throw new IllegalStateException("Decode plan for record '" + avro.getFullName() + "' has "
                    + children.size() + " children but the record has " + avro.getFields().size() + " fields");
        }
        return new AvroPlanNode(avro, Kind.STRUCT, source, children.toArray(NO_CHILDREN));
    }

    /// A list or map node carrying the plan for its element or value.
    static AvroPlanNode container(Schema avro, Kind kind, SchemaNode source, AvroPlanNode contained) {
        return new AvroPlanNode(avro, kind, source, new AvroPlanNode[] {contained});
    }

    /// The Avro schema this value materializes into, with any `[null, T]` union of
    /// the enclosing field already resolved to `T`.
    ///
    /// @return the resolved Avro schema
    public Schema avro() {
        return avro;
    }

    /// @return how to read and represent this value
    public Kind kind() {
        return kind;
    }

    /// The Parquet schema node this value is read from, for diagnostics. Decisions
    /// are taken from [#kind], which is derived from this node when the plan is built.
    ///
    /// @return the source schema node
    public SchemaNode source() {
        return source;
    }

    /// The plan for the record field at `index`, matching the Avro field position.
    ///
    /// @param index the Avro field position
    /// @return the field's plan
    public AvroPlanNode child(int index) {
        return children[index];
    }

    /// @return the plan for this list's elements
    public AvroPlanNode listElement() {
        if (kind != Kind.LIST) {
            throw new IllegalStateException("Not a list node: " + kind);
        }
        return children[0];
    }

    /// @return the plan for this map's values
    public AvroPlanNode mapValue() {
        if (kind != Kind.MAP) {
            throw new IllegalStateException("Not a map node: " + kind);
        }
        return children[0];
    }
}
