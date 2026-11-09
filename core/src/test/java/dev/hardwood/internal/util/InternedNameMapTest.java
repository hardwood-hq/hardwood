/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class InternedNameMapTest {

    @Test
    void findsANameByAnyInstanceOfIt() {
        InternedNameMap map = new InternedNameMap(new String("passenger_count".toCharArray()),
                new String("trip_distance".toCharArray()), new String("fare_amount".toCharArray()));

        assertThat(map.get("passenger_count")).isEqualTo(0);
        assertThat(map.get("trip_distance")).isEqualTo(1);
        assertThat(map.get(new String("fare_amount".toCharArray()))).isEqualTo(2);
    }

    @Test
    void findsNamesThatShareTheLowBitsOfTheirHash() {
        // "zone_ia" shares the slot of "id" in a 16-slot table indexed by the low bits of the hash.
        assertThat("zone_ia".hashCode() & 15).isEqualTo("id".hashCode() & 15);
        InternedNameMap map = new InternedNameMap("id", "distance", "passengers", "zone_ia");

        assertThat(map.get("id")).isEqualTo(0);
        assertThat(map.get("zone_ia")).isEqualTo(3);
        assertThat(map.get(new String("zone_ia".toCharArray()))).isEqualTo(3);
    }

    @Test
    void findsNamesOfOneHashCode() {
        // "Aa" and "BB" have the same hash code, which no table size separates.
        assertThat("Aa".hashCode()).isEqualTo("BB".hashCode());
        InternedNameMap map = new InternedNameMap("Aa", "BB");

        assertThat(map.get("Aa")).isEqualTo(0);
        assertThat(map.get("BB")).isEqualTo(1);
        assertThat(map.get(new String("BB".toCharArray()))).isEqualTo(1);
    }

    @Test
    void findsEveryNameBesideTwoNamesOfOneHashCode() {
        // No table size separates "Aa" and "BB", so the map falls back to probing.
        String[] names = names(32);
        names[7] = "Aa";
        names[23] = "BB";
        InternedNameMap map = new InternedNameMap(names);

        for (int i = 0; i < names.length; i++) {
            assertThat(map.get(names[i].intern())).isEqualTo(i);
            assertThat(map.get(new String(names[i].toCharArray()))).isEqualTo(i);
        }
    }

    @Test
    void findsEveryNameOfAWideProjection() {
        String[] names = names(1_000);
        InternedNameMap map = new InternedNameMap(names);

        for (int i = 0; i < names.length; i++) {
            assertThat(map.get(names[i].intern())).isEqualTo(i);
            assertThat(map.get(new String(names[i].toCharArray()))).isEqualTo(i);
        }
        assertThat(map.get("col_1000")).isEqualTo(-1);
    }

    @Test
    void answersMinusOneForAMissingName() {
        InternedNameMap map = new InternedNameMap("id");

        assertThat(map.get("ID")).isEqualTo(-1);
        assertThat(map.get("")).isEqualTo(-1);
        assertThat(new InternedNameMap().get("id")).isEqualTo(-1);
    }

    @Test
    void answersNotFoundForAMissingNameThatProbes() {
        // "C#" shares the hash code of "Aa" and "BB", so its lookup starts at an occupied slot of
        // the probing table and walks past both names to the next free slot.
        assertThat("C#".hashCode()).isEqualTo("Aa".hashCode());
        for (int count : new int[] { 2, 32, 129, 2_000 }) {
            String[] names = names(count);
            names[0] = "Aa";
            names[1] = "BB";
            InternedNameMap map = new InternedNameMap(names);

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                assertThat(map.get("C#")).isEqualTo(InternedNameMap.NOT_FOUND);
                assertThat(map.get("col_missing")).isEqualTo(InternedNameMap.NOT_FOUND);
            });
        }
    }

    @Test
    void marksANameGivenTwiceAmbiguous() {
        InternedNameMap map = new InternedNameMap("id", "name", new String("id".toCharArray()));

        assertThat(map.get("id")).isEqualTo(InternedNameMap.AMBIGUOUS);
        assertThat(map.get(new String("id".toCharArray()))).isEqualTo(InternedNameMap.AMBIGUOUS);
        assertThat(map.get("name")).isEqualTo(1);
    }

    @Test
    void marksANameGivenTwiceAmbiguousBesideTwoNamesOfOneHashCode() {
        // "Aa" and "BB" force the probing table, which marks a repeated name ambiguous as well.
        // Each repeat is an instance of its own, as the names decoded from a footer are.
        InternedNameMap map = new InternedNameMap("Aa", new String("id".toCharArray()), "BB",
                new String("id".toCharArray()), new String("id".toCharArray()));

        assertThat(map.get("id")).isEqualTo(InternedNameMap.AMBIGUOUS);
        assertThat(map.get("Aa")).isEqualTo(0);
        assertThat(map.get("BB")).isEqualTo(2);
    }

    @Test
    void neverGivesMoreNamesASmallerTableToWorkWith() {
        for (int names = 1; names <= 5_000; names++) {
            assertThat(InternedNameMap.minSlots(names)).isGreaterThanOrEqualTo(InternedNameMap.minSlots(names - 1));
            assertThat(InternedNameMap.maxSlots(names)).isGreaterThanOrEqualTo(InternedNameMap.maxSlots(names - 1));
            assertThat(InternedNameMap.maxSlots(names)).isGreaterThanOrEqualTo(InternedNameMap.minSlots(names));
        }
    }

    @Test
    void growsToSixteenSlotsANameUpToTheCeiling() {
        assertThat(InternedNameMap.maxSlots(20)).isEqualTo(512);
        assertThat(InternedNameMap.maxSlots(128)).isEqualTo(2_048);
        assertThat(InternedNameMap.maxSlots(129)).isEqualTo(4_096);
        assertThat(InternedNameMap.maxSlots(300)).isEqualTo(4_096);
    }

    @Test
    void keepsTheTableItStartsAtPastTheCeiling() {
        assertThat(InternedNameMap.maxSlots(1_000)).isEqualTo(InternedNameMap.minSlots(1_000)).isEqualTo(4_096);
        assertThat(InternedNameMap.maxSlots(2_000)).isEqualTo(InternedNameMap.minSlots(2_000)).isEqualTo(8_192);
        assertThat(InternedNameMap.maxSlots(100_000)).isEqualTo(InternedNameMap.minSlots(100_000)).isEqualTo(524_288);
    }

    @Test
    void findsEveryNameOfOneHashCodeBesideManyAtTheCeiling() {
        // "Aa" and "BB" share a hash code, so no size separates the names and the map probes in
        // the largest table it may grow to.
        for (int count : new int[] { 128, 129, 300, 2_000 }) {
            String[] names = names(count);
            names[0] = "Aa";
            names[1] = "BB";
            InternedNameMap map = new InternedNameMap(names);
            for (int i = 0; i < count; i++) {
                assertThat(map.get(names[i].intern())).isEqualTo(i);
                assertThat(map.get(new String(names[i].toCharArray()))).isEqualTo(i);
            }
        }
    }

    private static String[] names(int count) {
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            names[i] = new String(("col_" + i).toCharArray());
        }
        return names;
    }
}
