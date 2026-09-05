/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.RowGroupPredicate;

import static org.assertj.core.api.Assertions.assertThat;

class RowGroupPredicateRendererTest {

    @Test
    void rendersByteRangeBoundsInFull() {
        assertThat(RowGroupPredicateRenderer.render(RowGroupPredicate.byteRange(10, 100)))
                .isEqualTo("byteRange(10, 100)");
    }

    @Test
    void rendersCompoundByteRanges() {
        RowGroupPredicate predicate = RowGroupPredicate.and(
                RowGroupPredicate.byteRange(10, 100),
                RowGroupPredicate.byteRange(20, 80));

        assertThat(RowGroupPredicateRenderer.render(predicate))
                .isEqualTo("and(byteRange(10, 100), byteRange(20, 80))");
    }
}
