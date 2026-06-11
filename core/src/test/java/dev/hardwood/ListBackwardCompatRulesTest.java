/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.SchemaNode;
import dev.hardwood.schema.SchemaNode.GroupNode;
import dev.hardwood.schema.SchemaNode.PrimitiveNode;

import static org.assertj.core.api.Assertions.assertThat;

/// Exhaustively pins [GroupNode#getListElement()] against every example in the
/// Parquet `LIST` [backward-compatibility rules](https://parquet.apache.org/docs/file-format/types/logicaltypes/#backward-compatibility-rules).
///
/// Element resolution is purely structural — it never depends on the repeated
/// group being named `list`, which is why the spec's examples (named `element`,
/// `array`, `<list>_tuple`) must all resolve correctly. The levels passed to the
/// node constructors are irrelevant to resolution and set to zero.
class ListBackwardCompatRulesTest {

    private static PrimitiveNode prim(String name, RepetitionType rep) {
        return new PrimitiveNode(name, PhysicalType.BYTE_ARRAY, rep, null, 0, 0, 0);
    }

    private static GroupNode group(String name, RepetitionType rep, ConvertedType ct,
                                   SchemaNode... children) {
        return new GroupNode(name, rep, ct, null, List.of(children), 0, 0);
    }

    /// Wraps `inner` in an `optional group my_list (LIST) { ... }`.
    private static GroupNode myList(SchemaNode inner) {
        return group("my_list", RepetitionType.OPTIONAL, ConvertedType.LIST, inner);
    }

    /// Rule 1 — `repeated int32 element`: the repeated primitive is the element.
    @Test
    void rule1_repeatedPrimitiveIsTheElement() {
        GroupNode list = myList(prim("element", RepetitionType.REPEATED));

        SchemaNode element = list.getListElement();
        assertThat(element).isInstanceOf(PrimitiveNode.class);
        assertThat(element.name()).isEqualTo("element");
    }

    /// Rule 2 — `repeated group element { str; num }`: the multi-field repeated
    /// group is itself the element (a tuple), not a wrapper to unwrap.
    @Test
    void rule2_multiFieldRepeatedGroupIsTheElement() {
        GroupNode element = group("element", RepetitionType.REPEATED, null,
                prim("str", RepetitionType.REQUIRED),
                prim("num", RepetitionType.REQUIRED));
        GroupNode list = myList(element);

        assertThat(list.getListElement()).isSameAs(element);
    }

    /// Rule 3 — `repeated group array (LIST) { repeated int32 array }`: a single
    /// field that is itself repeated makes the repeated group a genuine element
    /// (a list whose element is a list), not a synthetic wrapper.
    @Test
    void rule3_repeatedChildMakesGroupTheElement() {
        GroupNode element = group("array", RepetitionType.REPEATED, ConvertedType.LIST,
                prim("array", RepetitionType.REPEATED));
        GroupNode list = myList(element);

        assertThat(list.getListElement()).isSameAs(element);
    }

    /// Rule 4 — `repeated group array { required binary str }`: a single-field
    /// group named `array` is the element.
    @Test
    void rule4_singleFieldGroupNamedArrayIsTheElement() {
        GroupNode element = group("array", RepetitionType.REPEATED, null,
                prim("str", RepetitionType.REQUIRED));
        GroupNode list = myList(element);

        assertThat(list.getListElement()).isSameAs(element);
    }

    /// Rule 4 — `repeated group my_list_tuple { required binary str }`: a
    /// single-field group named `<listName>_tuple` is the element.
    @Test
    void rule4_singleFieldGroupNamedTupleIsTheElement() {
        GroupNode element = group("my_list_tuple", RepetitionType.REPEATED, null,
                prim("str", RepetitionType.REQUIRED));
        GroupNode list = myList(element);

        assertThat(list.getListElement()).isSameAs(element);
    }

    /// Rule 5 — `repeated group element { optional binary str }`: the standard
    /// 3-level encoding. The single non-repeated child is the element; the
    /// repeated group is the synthetic wrapper that gets unwrapped.
    @Test
    void rule5_standardThreeLevelUnwrapsToChild() {
        PrimitiveNode str = prim("str", RepetitionType.OPTIONAL);
        GroupNode list = myList(group("element", RepetitionType.REPEATED, null, str));

        assertThat(list.getListElement()).isSameAs(str);
    }

    /// The spec's lead-in example: a 3-level list whose repeated group is named
    /// `element` (not `list`) with a `required` child still unwraps to that child
    /// — element resolution does not require the `list` name.
    @Test
    void leadInExampleNonStandardWrapperNameUnwrapsToChild() {
        PrimitiveNode str = prim("str", RepetitionType.REQUIRED);
        GroupNode list = myList(group("element", RepetitionType.REPEATED, null, str));

        SchemaNode element = list.getListElement();
        assertThat(element).isSameAs(str);
        assertThat(element.name()).isEqualTo("str");
    }
}
