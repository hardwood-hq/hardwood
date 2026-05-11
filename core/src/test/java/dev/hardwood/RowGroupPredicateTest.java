/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.RowGroupPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowGroupPredicateTest {

    @Test
    void byteRangeFactoryProducesByteRangeRecord() {
        RowGroupPredicate p = RowGroupPredicate.byteRange(10L, 100L);
        assertThat(p).isInstanceOf(RowGroupPredicate.ByteRange.class);
        RowGroupPredicate.ByteRange b = (RowGroupPredicate.ByteRange) p;
        assertThat(b.startInclusive()).isEqualTo(10L);
        assertThat(b.endExclusive()).isEqualTo(100L);
    }

    @Test
    void byteRangeAcceptsEmptyRange() {
        // endExclusive < startInclusive is documented as an empty range — used by callers
        // that pass splitStart + splitLength and tolerate long overflow.
        RowGroupPredicate p = RowGroupPredicate.byteRange(100L, 50L);
        assertThat(p).isInstanceOf(RowGroupPredicate.ByteRange.class);
    }

    @Test
    void andFactoryProducesAndRecord() {
        RowGroupPredicate b1 = RowGroupPredicate.byteRange(0, 100);
        RowGroupPredicate b2 = RowGroupPredicate.byteRange(50, 150);
        RowGroupPredicate p = RowGroupPredicate.and(b1, b2);
        assertThat(p).isInstanceOf(RowGroupPredicate.And.class);
        RowGroupPredicate.And and = (RowGroupPredicate.And) p;
        assertThat(and.children()).containsExactly(b1, b2);
    }

    @Test
    void andRejectsEmptyChildren() {
        assertThatThrownBy(RowGroupPredicate::and)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one child");
    }

    @Test
    void andRejectsNullChildren() {
        assertThatThrownBy(() -> RowGroupPredicate.and((RowGroupPredicate[]) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one child");
    }
}
