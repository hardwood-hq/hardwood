/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.reader.LayerKind;

import static org.assertj.core.api.Assertions.assertThat;

/// [NestedLevelComputer#computeRealView] against [ReferenceRealView], on random levels over random
/// layer chains of up to six layers, including the `null` level arrays of a column without
/// repetition or definition levels, level arrays longer than the batch, and the all-zero
/// repetition levels a filtered read carries for a chain without a `REPEATED` layer.
///
/// Both are functions of the levels and the layer descriptor alone, so the levels need not
/// describe a well-formed record: any `def` in `[0, maxDef]` and `rep` in `[0, maxRep]` has to
/// give the same view. Each chain is read with levels drawn uniformly and with levels skewed
/// towards the maximum definition level, where the absent items that decide whether a validity is
/// built at all are rare.
class RealViewEquivalenceTest {

    private static final int CHAINS = 400;

    private static final int[] SIZES = { 0, 1, 2, 63, 64, 65, 127, 128, 129, 1_000, 4_097 };

    @ParameterizedTest
    @ValueSource(longs = { 1L, 2L, 3L })
    void matchesReference(long seed) {
        Random random = new Random(seed);
        for (int chain = 0; chain < CHAINS; chain++) {
            int layerCount = random.nextInt(7);
            LayerKind[] kinds = new LayerKind[layerCount];
            int[] defThresholds = new int[layerCount];
            int def = 0;
            int maxRep = 0;
            for (int k = 0; k < layerCount; k++) {
                kinds[k] = random.nextBoolean() ? LayerKind.REPEATED : LayerKind.STRUCT;
                def++;
                defThresholds[k] = def;
                if (kinds[k] == LayerKind.REPEATED) {
                    def++;
                    maxRep++;
                }
            }
            int maxDef = def + (random.nextBoolean() ? 1 : 0);
            NestedLevelComputer.Layers layers = NestedLevelComputer.Layers.of(kinds, defThresholds);

            for (int n : SIZES) {
                for (boolean skewed : new boolean[]{ false, true }) {
                    int[] defs = levels(random, n, maxDef, skewed);
                    int[] reps = maxRep > 0 ? levels(random, n, maxRep, false) : null;
                    if (reps != null && n > 0) {
                        reps[0] = 0;
                    }
                    String shape = "layers %s, thresholds %s, maxDef %d, n %d, skewed %b".formatted(
                            Arrays.toString(kinds), Arrays.toString(defThresholds),
                            maxDef, n, skewed);
                    assertSameView(shape, defs, reps, n, maxDef, layers);
                    // Scratch level arrays are longer than the batch, past which the levels are
                    // stale
                    assertSameView(shape + ", stale levels past n", withStaleTail(random, defs),
                            withStaleTail(random, reps), n, maxDef, layers);
                    if (reps == null) {
                        // A filtered read carries repetition levels for every nested column, all
                        // zero where the chain has no REPEATED layer
                        assertSameView(shape + ", zero repetition levels", defs, new int[n], n, maxDef, layers);
                    }
                    else if (n > 0) {
                        int[] anyFirst = reps.clone();
                        anyFirst[0] = random.nextInt(maxRep + 1);
                        assertSameView(shape + ", first repetition level " + anyFirst[0], defs, anyFirst, n,
                                maxDef, layers);
                    }
                    if (maxDef > 0 && n > 0) {
                        // A batch whose levels are all at the maximum, read with and without them.
                        int[] full = levels(random, n, maxDef, true);
                        Arrays.fill(full, maxDef);
                        assertSameView(shape + ", all at max", full, reps, n, maxDef, layers);
                        assertSameView(shape + ", no definition levels", null, reps, n, maxDef, layers);
                    }
                }
            }
        }
    }

    private static void assertSameView(String shape, int[] defs, int[] reps, int n, int maxDef,
                                       NestedLevelComputer.Layers layers) {
        NestedLevelComputer.RealView expected = ReferenceRealView.computeRealView(defs, reps, n, n, maxDef, layers);
        NestedLevelComputer.RealView actual = NestedLevelComputer.computeRealView(defs, reps, n, n, maxDef, layers);
        assertThat(actual.valueCount()).as("value count, %s", shape).isEqualTo(expected.valueCount());
        assertThat(actual.layerOffsets()).as("layer offsets, %s", shape).isEqualTo(expected.layerOffsets());
        assertThat(actual.layerValidity()).as("layer validity, %s", shape).isEqualTo(expected.layerValidity());
        assertThat(actual.leafValidity()).as("leaf validity, %s", shape).isEqualTo(expected.leafValidity());
        assertThat(actual.realToRawLeaf()).as("real-to-raw map, %s", shape).isEqualTo(expected.realToRawLeaf());
    }

    /// `levels` followed by up to 70 entries out of any level's range, `null` for `null`.
    private static int[] withStaleTail(Random random, int[] levels) {
        if (levels == null) {
            return null;
        }
        int[] longer = Arrays.copyOf(levels, levels.length + 1 + random.nextInt(70));
        for (int i = levels.length; i < longer.length; i++) {
            longer[i] = random.nextBoolean() ? -1 : 99;
        }
        return longer;
    }

    private static int[] levels(Random random, int n, int max, boolean skewed) {
        int[] levels = new int[n];
        for (int i = 0; i < n; i++) {
            levels[i] = skewed && random.nextInt(16) != 0 ? max : random.nextInt(max + 1);
        }
        return levels;
    }
}
