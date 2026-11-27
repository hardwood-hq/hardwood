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

/// Decode plan built once by [ArrowSchemaConverter#plan]: the Arrow schema and one tree per
/// top-level field describing how each node is populated from [dev.hardwood.reader.ColumnReaders].
public record ArrowPlan(Schema schema, List<Node> roots) {

    /// Output field shape, determining the Arrow buffers it carries.
    public enum Shape {
        LEAF,
        STRUCT,
        LIST,
        MAP
    }

    /// One output field. A group reads its validity and offsets from `layer` on its anchor leaf
    /// (`position`), or uses `-1` when it has no layer of its own (e.g. a `REQUIRED` struct).
    public record Node(Shape shape, Field field, ScalarKind kind, int position, int layer, List<Node> children) {

        static Node leaf(Field field, ScalarKind kind, int position) {
            return new Node(Shape.LEAF, field, kind, position, -1, List.of());
        }

        static Node group(Shape shape, Field field, int position, int layer, List<Node> children) {
            return new Node(shape, field, null, position, layer, List.copyOf(children));
        }

        public boolean hasLayer() {
            return layer >= 0;
        }
    }
}
