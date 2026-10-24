/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// [KeptRuns] maps the kept positions of a masked page to its page positions, run by run.
class KeptRunsTest {

    /// Runs `[2, 4)`, `[5, 6)` and `[8, 11)` of a page of 11 values.
    private static KeptRuns threeRuns() {
        return KeptRuns.of(new int[]{ 2, 5, 8 }, new int[]{ 2, 1, 3 }, 3, 11);
    }

    @Test
    void mapsEachKeptPositionToItsRunAndPagePosition() {
        KeptRuns runs = threeRuns();
        int[] expectedRuns = { 0, 0, 1, 2, 2, 2 };
        int[] expectedPositions = { 2, 3, 5, 8, 9, 10 };
        int[] expectedRemaining = { 2, 1, 1, 3, 2, 1 };

        for (int kept = 0; kept < 6; kept++) {
            int run = runs.runOf(kept);
            assertThat(run).as("run of %d", kept).isEqualTo(expectedRuns[kept]);
            assertThat(runs.pagePosition(run, kept)).as("page position of %d", kept).isEqualTo(expectedPositions[kept]);
            assertThat(runs.remaining(run, kept)).as("remaining from %d", kept).isEqualTo(expectedRemaining[kept]);
        }
        assertThat(runs.count()).isEqualTo(3);
    }

    @Test
    void keepsTheLastValueOnlyWhenTheLastRunEndsAtThePageEnd() {
        assertThat(threeRuns().keepsLastValue()).isTrue();
        assertThat(KeptRuns.of(new int[]{ 2, 5, 8 }, new int[]{ 2, 1, 3 }, 3, 12).keepsLastValue()).isFalse();
        assertThat(KeptRuns.of(new int[]{ 0, 9 }, new int[]{ 3, 3 }, 1, 12).keepsLastValue())
                .as("only the first `count` runs count").isFalse();
    }

    /// Plain bytes summed over the runs a range of kept positions crosses, a null's slot holding
    /// none.
    @Test
    void sumsThePlainBytesOfARangeOverItsRuns() {
        // Page values: "a", "bb", null, "ccc", "dddd", "e", "ff", "ggg", "h", "ii", "jjj"
        int[] def = { 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1 };
        byte[] plain = plain("a", "bb", "ccc", "dddd", "e", "ff", "ggg", "h", "ii", "jjj");
        BinaryPageValues values = BinaryPageValues.plain(PhysicalType.BYTE_ARRAY, 0, def, 1, def.length, plain, 0,
                plain.length, new PageDecoder.LevelScratch());
        KeptRuns runs = threeRuns();

        // Kept positions: null, "ccc" | "e" | "h", "ii", "jjj"
        assertThat(runs.bytes(values, 0, 6)).isEqualTo(3 + 1 + 1 + 2 + 3);
        assertThat(runs.bytes(values, 1, 4)).isEqualTo(3 + 1 + 1);
        assertThat(runs.bytes(values, 0, 1)).isZero();
        assertThat(runs.bytes(values, 4, 6)).isEqualTo(2 + 3);
    }

    /// `PLAIN` byte-array values, each behind its 4-byte length.
    private static byte[] plain(String... values) {
        ByteBuffer plain = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        for (String value : values) {
            plain.putInt(value.length()).put(value.getBytes(StandardCharsets.UTF_8));
        }
        return Arrays.copyOf(plain.array(), plain.position());
    }
}
