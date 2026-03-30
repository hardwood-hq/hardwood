/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.FilterPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResolvedPredicateTest {

    @Test
    void testAndWithEmptyChildrenThrows() {
        assertThatThrownBy(() -> new ResolvedPredicate.And(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("AND requires at least one child predicate");
    }

    @Test
    void testOrWithEmptyChildrenThrows() {
        assertThatThrownBy(() -> new ResolvedPredicate.Or(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("OR requires at least one child predicate");
    }

    @Test
    void testAndWithSingleChildSucceeds() {
        ResolvedPredicate child = new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.EQ, 42);
        ResolvedPredicate.And and = new ResolvedPredicate.And(List.of(child));
        assertThat(and.children()).hasSize(1);
    }

    @Test
    void testOrWithSingleChildSucceeds() {
        ResolvedPredicate child = new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.EQ, 42);
        ResolvedPredicate.Or or = new ResolvedPredicate.Or(List.of(child));
        assertThat(or.children()).hasSize(1);
    }
}
