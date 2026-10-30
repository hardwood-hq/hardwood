/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// [LongSet] against `HashSet`, on keys that exercise its empty-slot marker and its probe chain.
class LongSetTest {

    @Test
    void agreesWithHashSetOnRandomKeysAndProbes() {
        Random random = new Random(1594L);
        for (int size : new int[]{ 1, 9, 100, 4096 }) {
            Set<Long> expected = new HashSet<>();
            while (expected.size() < size) {
                expected.add(random.nextLong());
            }
            long[] keys = expected.stream().mapToLong(Long::longValue).toArray();
            LongSet set = new LongSet(keys);
            for (long key : keys) {
                assertThat(set.contains(key)).as("key %d of %d", key, size).isTrue();
            }
            for (int i = 0; i < 100_000; i++) {
                long probe = random.nextLong();
                assertThat(set.contains(probe)).as("probe %d of %d keys", probe, size).isEqualTo(expected.contains(probe));
            }
        }
    }

    @Test
    void zeroIsAKeyLikeAnyOther() {
        // 0 is the marker's first candidate; with 0 among the keys another marker is chosen, and a
        // probe for 0 finds it.
        LongSet withZero = new LongSet(new long[]{ 0L, 5L, 10L });
        assertThat(withZero.contains(0L)).isTrue();
        assertThat(withZero.contains(1L)).isFalse();

        // Without 0 among the keys, a probe for 0 meets the marker and reports no key.
        LongSet withoutZero = new LongSet(new long[]{ 5L, 10L });
        assertThat(withoutZero.contains(0L)).isFalse();
        assertThat(withoutZero.contains(5L)).isTrue();
    }

    @Test
    void consecutiveKeysProbePastTheirHomeSlots() {
        // Keys whose slots collide chain along the table and wrap at its end; every one is found.
        long[] keys = new long[1024];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = Long.MAX_VALUE - i;
        }
        LongSet set = new LongSet(keys);
        for (long key : keys) {
            assertThat(set.contains(key)).isTrue();
        }
        assertThat(set.contains(Long.MAX_VALUE - keys.length)).isFalse();
        assertThat(set.contains(Long.MIN_VALUE)).isFalse();
    }
}
