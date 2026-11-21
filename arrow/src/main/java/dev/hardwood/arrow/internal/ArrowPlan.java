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

/// The decode plan produced by [ArrowSchemaConverter#plan]: the Arrow schema (in IPC message
/// format, carrying value types alongside any [org.apache.arrow.vector.types.pojo.DictionaryEncoding]),
/// one tree per top-level field describing how each field and its descendants are filled from the
/// column readers, and the flat list of dictionary-encoded leaf nodes.
///
/// @param schema the converted schema, one field per root node
/// @param roots the tree of each top-level field, in field order
/// @param dictionaryNodes the dictionary-encoded leaf nodes, indexed by [Node#dictionarySlot()]
public record ArrowPlan(Schema schema, List<Node> roots, List<Node> dictionaryNodes) {

    /// What an output field is, which decides the buffers it carries.
    public enum Shape {
        /// A primitive column: validity and the data buffers its [ScalarKind] (or dictionary
        /// index type) names.
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
    /// @param field the Arrow field (for a dictionary-encoded leaf, carries the value type and
    ///        its [org.apache.arrow.vector.types.pojo.DictionaryEncoding])
    /// @param kind how a leaf's values reach its buffers; `null` on a group
    /// @param position for a leaf, its index among the requested columns, the index
    ///        [dev.hardwood.reader.ColumnReaders#getColumnReader(int)] serves it at; for a
    ///        group, the position of its anchor leaf
    /// @param layer for a group, the index of the layer on the anchor leaf that carries the
    ///        group's validity and offsets, or `-1` for a group with neither (a `REQUIRED`
    ///        struct, the element struct of a bare repeated group) and for a leaf
    /// @param dictionarySlot the index of a dictionary-encoded leaf in [ArrowPlan#dictionaryNodes()]
    ///        (also used as its Arrow dictionary id), or `-1` for plain leaves and groups
    /// @param children the struct's fields, the list's element, or the map's key and value
    public record Node(Shape shape, Field field, ScalarKind kind, int position, int layer,
            int dictionarySlot, List<Node> children) {

        static Node leaf(Field field, ScalarKind kind, int position, int dictionarySlot) {
            return new Node(Shape.LEAF, field, kind, position, -1, dictionarySlot, List.of());
        }

        static Node group(Shape shape, Field field, int position, int layer, List<Node> children) {
            return new Node(shape, field, null, position, layer, -1, List.copyOf(children));
        }

        /// Whether a group reads a validity bitmap, and for a list or map its offsets, from
        /// its layer.
        public boolean hasLayer() {
            return layer >= 0;
        }

        /// Whether this leaf emits Arrow dictionary-encoded indices backed by a dictionary vector.
        public boolean isDictionary() {
            return dictionarySlot >= 0;
        }

        /// The Arrow dictionary id for a dictionary-encoded leaf, or `-1` otherwise.
        public long dictionaryId() {
            return dictionarySlot;
        }
    }
}
