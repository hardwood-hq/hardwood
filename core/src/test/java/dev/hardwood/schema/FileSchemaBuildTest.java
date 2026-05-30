/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;

/// Characterization test for [FileSchema#fromSchemaElements]: pins the exact
/// leaf ordering, column indices, and Dremel levels produced when walking the
/// flat depth-first `SchemaElement` list into a tree.
///
/// The schema mixes struct-in-struct nesting, a repeated primitive, and a
/// repeated group, and ends with a top-level primitive **after** a nested
/// subtree — so it exercises that the read cursor resumes at the right element
/// once a group's whole subtree has been consumed (the invariant behind the
/// `columnIndex` ⇄ `ColumnChunk`-order coupling the reader relies on).
class FileSchemaBuildTest {

    private static SchemaElement group(String name, RepetitionType rep, int numChildren) {
        return new SchemaElement(name, null, null, rep, numChildren, null, null, null, null, null);
    }

    private static SchemaElement prim(String name, PhysicalType type, RepetitionType rep) {
        return new SchemaElement(name, type, null, rep, null, null, null, null, null, null);
    }

    private static void assertColumn(ColumnSchema c, String path, int index, int maxDef, int maxRep) {
        assertThat(c.fieldPath().toString()).isEqualTo(path);
        assertThat(c.columnIndex()).isEqualTo(index);
        assertThat(c.maxDefinitionLevel()).isEqualTo(maxDef);
        assertThat(c.maxRepetitionLevel()).isEqualTo(maxRep);
    }

    /// ```
    /// message user {                       def rep
    ///   required int64   id;                0   0   col0
    ///   optional binary  name;              1   0   col1
    ///   optional group address {
    ///     optional binary street;           2   0   col2
    ///     optional group geo {
    ///       optional double lat;            3   0   col3
    ///       required double lon;            2   0   col4
    ///     }
    ///     optional int32 zip;               2   0   col5
    ///   }
    ///   repeated int32   scores;            1   1   col6
    ///   repeated group   tags {
    ///     required binary value;            1   1   col7
    ///   }
    ///   required int32   footer;            0   0   col8   (after a nested subtree)
    /// }
    /// ```
    @Test
    void leafOrderingIndicesAndLevels() {
        List<SchemaElement> elements = List.of(
                group("user", null, 6),
                prim("id", PhysicalType.INT64, RepetitionType.REQUIRED),
                prim("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL),
                group("address", RepetitionType.OPTIONAL, 3),
                prim("street", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL),
                group("geo", RepetitionType.OPTIONAL, 2),
                prim("lat", PhysicalType.DOUBLE, RepetitionType.OPTIONAL),
                prim("lon", PhysicalType.DOUBLE, RepetitionType.REQUIRED),
                prim("zip", PhysicalType.INT32, RepetitionType.OPTIONAL),
                prim("scores", PhysicalType.INT32, RepetitionType.REPEATED),
                group("tags", RepetitionType.REPEATED, 1),
                prim("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                prim("footer", PhysicalType.INT32, RepetitionType.REQUIRED));

        FileSchema schema = FileSchema.fromSchemaElements(elements);

        assertThat(schema.getColumnCount()).isEqualTo(9);
        assertColumn(schema.getColumn(0), "id", 0, 0, 0);
        assertColumn(schema.getColumn(1), "name", 1, 1, 0);
        assertColumn(schema.getColumn(2), "address.street", 2, 2, 0);
        assertColumn(schema.getColumn(3), "address.geo.lat", 3, 3, 0);
        assertColumn(schema.getColumn(4), "address.geo.lon", 4, 2, 0);
        assertColumn(schema.getColumn(5), "address.zip", 5, 2, 0);
        assertColumn(schema.getColumn(6), "scores", 6, 1, 1);
        assertColumn(schema.getColumn(7), "tags.value", 7, 1, 1);
        assertColumn(schema.getColumn(8), "footer", 8, 0, 0);
    }

    /// The tree shape must mirror the leaf list: a trailing top-level primitive
    /// after a nested subtree proves the cursor lands on the right sibling.
    @Test
    void treeStructureMirrorsNesting() {
        List<SchemaElement> elements = List.of(
                group("user", null, 6),
                prim("id", PhysicalType.INT64, RepetitionType.REQUIRED),
                prim("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL),
                group("address", RepetitionType.OPTIONAL, 3),
                prim("street", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL),
                group("geo", RepetitionType.OPTIONAL, 2),
                prim("lat", PhysicalType.DOUBLE, RepetitionType.OPTIONAL),
                prim("lon", PhysicalType.DOUBLE, RepetitionType.REQUIRED),
                prim("zip", PhysicalType.INT32, RepetitionType.OPTIONAL),
                prim("scores", PhysicalType.INT32, RepetitionType.REPEATED),
                group("tags", RepetitionType.REPEATED, 1),
                prim("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                prim("footer", PhysicalType.INT32, RepetitionType.REQUIRED));

        List<SchemaNode> top = FileSchema.fromSchemaElements(elements).getRootNode().children();

        assertThat(top).hasSize(6);
        assertThat(top.get(0)).isInstanceOf(SchemaNode.PrimitiveNode.class);   // id
        assertThat(top.get(1)).isInstanceOf(SchemaNode.PrimitiveNode.class);   // name

        SchemaNode.GroupNode address = (SchemaNode.GroupNode) top.get(2);
        assertThat(address.name()).isEqualTo("address");
        assertThat(address.children()).hasSize(3);                              // street, geo, zip
        SchemaNode.GroupNode geo = (SchemaNode.GroupNode) address.children().get(1);
        assertThat(geo.name()).isEqualTo("geo");
        assertThat(geo.children()).hasSize(2);                                  // lat, lon

        assertThat(top.get(3)).isInstanceOf(SchemaNode.PrimitiveNode.class);    // scores
        SchemaNode.GroupNode tags = (SchemaNode.GroupNode) top.get(4);
        assertThat(tags.children()).hasSize(1);                                 // value

        assertThat(top.get(5)).isInstanceOf(SchemaNode.PrimitiveNode.class);    // footer (resumed correctly)
        assertThat((top.get(5)).name()).isEqualTo("footer");
    }
}
