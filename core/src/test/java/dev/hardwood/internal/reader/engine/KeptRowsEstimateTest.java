/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeptRowsEstimateTest {

    private final KeptRowsEstimate estimate = new KeptRowsEstimate(100);

    @Test
    void countsEveryRowBeforeAnyUnitIsPlanned() {
        assertThat(estimate.of(1_000_000)).isEqualTo(1_000_000);
    }

    @Test
    void takesTheFractionTheUnitPlannedLastKept() {
        estimate.planned(8_000, 1_000_000);

        assertThat(estimate.of(500_000)).isEqualTo(4_000);
    }

    /// A unit that keeps no row would let every unit after it be planned at once.
    @Test
    void takesAtLeastTheFloorWhereTheLastUnitKeptNoRow() {
        estimate.planned(0, 1_000_000);

        assertThat(estimate.of(1_000_000)).isEqualTo(100);
    }

    @Test
    void takesNoMoreThanTheUnitsRows() {
        estimate.planned(0, 1_000_000);

        assertThat(estimate.of(40)).isEqualTo(40);
    }

    /// Selectivity changing from high to low: the next estimate follows the unit planned last.
    @Test
    void followsAChangeFromFewKeptRowsToMany() {
        estimate.planned(8_000, 1_000_000);
        estimate.planned(1_000_000, 1_000_000);

        assertThat(estimate.of(1_000_000)).isEqualTo(1_000_000);
    }

    /// Selectivity changing from low to high.
    @Test
    void followsAChangeFromManyKeptRowsToFew() {
        estimate.planned(1_000_000, 1_000_000);
        estimate.planned(8_000, 1_000_000);

        assertThat(estimate.of(1_000_000)).isEqualTo(8_000);
    }

    /// Planned units count the rows they keep, so the look-ahead skips the rows their filters
    /// drop: two units of 1,000 rows keeping 20 each, 30 rows from the first end 10 into the second.
    @Test
    void keyPastCountsThePlannedUnitsByTheRowsTheyKeep() {
        long key = estimate.keyPast(0, 2, i -> i * 1000L, i -> 1000, i -> 20, 2000, 30);

        assertThat(key).isEqualTo(1010);
    }

    /// Units not yet planned count the rows they are taken to keep: after a unit planned keeping 20
    /// of 1,000 rows, two such units count 100 each (the floor), so 150 rows end 50 into the
    /// second, not 150 into the first.
    @Test
    void keyPastCountsTheUnitsNotYetPlannedByTheirEstimate() {
        estimate.planned(20, 1000);

        long key = estimate.keyPast(0, 2, i -> i * 1000L, i -> 1000, i -> -1, 2000, 150);

        assertThat(key).isEqualTo(1050);
    }

    /// Past the units listed, every row counts from the end of the rows listed.
    @Test
    void keyPastCountsEveryRowPastTheUnitsListed() {
        long key = estimate.keyPast(0, 1, i -> 0, i -> 1000, i -> 20, 1000, 50);

        assertThat(key).isEqualTo(1030);
    }
}
