/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// [BinarySet] against `HashSet`, probing slices of a shared buffer as a dictionary's entries are.
class BinarySetTest {

    @Test
    void agreesWithHashSetOnRandomKeysAndProbes() {
        Random random = new Random(1594L);
        // Key lengths cover every arm of the hash: under four bytes, four to seven, one word,
        // several words and a word that overlaps the one before it.
        for (int size : new int[]{ 1, 9, 100, 4096 }) {
            Set<ByteBuffer> expected = new HashSet<>();
            while (expected.size() < size) {
                expected.add(ByteBuffer.wrap(randomBytes(random, random.nextInt(40))));
            }
            byte[][] keys = expected.stream().map(ByteBuffer::array).toArray(byte[][]::new);
            BinarySet set = new BinarySet(keys);
            for (byte[] key : keys) {
                assertThat(set.contains(key, 0, key.length)).as("key of %d", size).isTrue();
            }
            // Probes as slices of one buffer, each preceded and followed by other bytes.
            for (int i = 0; i < 20_000; i++) {
                byte[] probe = randomBytes(random, random.nextInt(40));
                byte[] buffer = new byte[probe.length + 7];
                random.nextBytes(buffer);
                System.arraycopy(probe, 0, buffer, 3, probe.length);
                assertThat(set.contains(buffer, 3, 3 + probe.length))
                        .as("probe of %d keys", size)
                        .isEqualTo(expected.contains(ByteBuffer.wrap(probe)));
            }
        }
    }

    @Test
    void lengthsBeyondTheMaskShareOneBit() {
        // A 70-byte and an 80-byte key both set bit 63; a 90-byte probe passes the length check
        // and is decided by the hash and the bytes.
        byte[] seventy = new byte[70];
        byte[] eighty = new byte[80];
        eighty[0] = 1;
        BinarySet set = new BinarySet(new byte[][]{ seventy, eighty });
        assertThat(set.contains(new byte[70], 0, 70)).isTrue();
        assertThat(set.contains(new byte[80], 0, 80)).isFalse();
        assertThat(set.contains(eighty, 0, 80)).isTrue();
        assertThat(set.contains(new byte[90], 0, 90)).isFalse();
    }

    private static byte[] randomBytes(Random random, int length) {
        byte[] bytes = new byte[length];
        // Few distinct byte values, so probes often share a prefix with a key.
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) random.nextInt(3);
        }
        return bytes;
    }
}
