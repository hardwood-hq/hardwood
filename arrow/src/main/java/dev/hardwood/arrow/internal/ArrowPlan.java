/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/// The decode plan produced by [ArrowSchemaConverter#plan]: the Arrow schema and, one tree per
/// top-level field, how each field and its descendants are filled from the column readers.
///
/// @param schema the converted schema, one field per root node
/// @param roots the tree of each top-level field, in field order
public record ArrowPlan(Schema schema, List<Node> roots) {

    /// What an output field is, which decides the buffers it carries.
    public enum Shape {
        /// A primitive column: validity and the data buffers its [ScalarKind] names.
        LEAF,
        /// A struct: validity only; the children carry their own buffers.
        STRUCT,
        /// A list: validity and offsets, then one child.
        LIST,
        /// A map: validity and offsets, then an entries struct holding the key and value children.
        MAP
    }

    /// One output field.
    ///
    /// A group's validity and offsets are those of a layer on a leaf column beneath it: the
    /// layer model of [dev.hardwood.reader.ColumnReader] reports each nesting scope on every
    /// leaf it encloses, and every leaf reports the same bitmap and offsets for a shared scope,
    /// so one leaf, the `anchor`, serves the group.
    ///
    /// @param shape what the field is
    /// @param field the Arrow field
    /// @param kind how a leaf's values reach its buffers; `null` on a group
    /// @param position for a leaf, its index among the requested columns, the index
    ///        [dev.hardwood.reader.ColumnReaders#getColumnReader(int)] serves it at; for a
    ///        group, the position of its anchor leaf
    /// @param layer for a group, the index of the layer on the anchor leaf that carries the
    ///        group's validity and offsets, or `-1` for a group with neither (a `REQUIRED`
    ///        struct, the element struct of a bare repeated group) and for a leaf
    /// @param children the struct's fields, the list's element, or the map's key and value
    public record Node(Shape shape, Field field, ScalarKind kind, int position, int layer, List<Node> children) {

        /// Whether a group reads a validity bitmap, and for a list or map its offsets, from
        /// its layer.
        public boolean hasLayer() {
            return layer >= 0;
        }
    }
}
