/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.metadata.PhysicalType;

import static dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport.PASS_LIMIT;
import static dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport.SPARSE_PASS_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

/// The membership check's three kernels on synthetic dictionaries: one pass per literal up to
/// [DictionaryFilterSupport#PASS_LIMIT] literals, and above it a [RangeBitmask] for integer
/// literals that lie close together or a [LongSet] / [BinarySet] otherwise; integer literals too
/// spread out for a bitmask run passes up to [DictionaryFilterSupport#SPARSE_PASS_LIMIT] before
/// they are hashed. Each kernel is driven to both decisions, with the present value at the
/// dictionary's first and last entry.
///
/// `DictionaryPushDownTest` covers the row-group decision over real fixtures.
class DictionaryFilterSupportTest {

    /// One more literal than the pass kernel takes.
    private static final int INDEXED = PASS_LIMIT + 1;

    /// One more literal than spread-out integer literals run passes for.
    private static final int HASHED = SPARSE_PASS_LIMIT + 1;

    /// `count` literals from `first` upwards, `step` apart.
    private static long[] longs(int count, long first, long step) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = first + i * step;
        }
        return values;
    }

    private static int[] ints(int count, int first, int step) {
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = first + i * step;
        }
        return values;
    }

    /// Random dictionaries and lists above the threshold, decided against the single-value check
    /// once per literal. Values are drawn from narrow pools so that both decisions occur, the
    /// bitmask and the set are both taken, and the values that need care (`0`, the extremes, every
    /// `NaN` payload, both zeroes, duplicate literals) recur.
    @Nested
    class RandomisedAgainstSingleValueChecks {

        private static final int ROUNDS = 300;

        private final Random random = new Random(1594L);

        private long randomLong() {
            return switch (random.nextInt(4)) {
                case 0 -> random.nextLong();
                case 1 -> random.nextInt(64) - 8;
                case 2 -> random.nextInt(1 << 22);
                default -> new long[]{ 0L, Long.MIN_VALUE, Long.MAX_VALUE, -1L }[random.nextInt(4)];
            };
        }

        private int randomInt() {
            return switch (random.nextInt(4)) {
                case 0 -> random.nextInt();
                case 1 -> random.nextInt(64) - 8;
                case 2 -> random.nextInt(1 << 22);
                default -> new int[]{ 0, Integer.MIN_VALUE, Integer.MAX_VALUE, -1 }[random.nextInt(4)];
            };
        }

        /// Random bits give every `NaN` payload, both zeroes and denormals; the small pool at
        /// `offset` gives matches. Entries draw at offset 0; literals draw at 0 or, in half the
        /// rounds, at an offset no entry has, so rounds in which every literal is absent occur
        /// and the specials (which match any `NaN` entry) stay rare.
        private double randomDouble(double offset) {
            return switch (random.nextInt(8)) {
                case 0 -> Double.longBitsToDouble(random.nextLong());
                case 1 -> new double[]{ 0.0, -0.0, Double.NaN, Double.longBitsToDouble(0x7ff8000000000001L) }[random.nextInt(4)];
                default -> offset + random.nextInt(64) * 0.5 - 2.0;
            };
        }

        private float randomFloat(float offset) {
            return switch (random.nextInt(8)) {
                case 0 -> Float.intBitsToFloat(random.nextInt());
                case 1 -> new float[]{ 0.0f, -0.0f, Float.NaN, Float.intBitsToFloat(0x7fc00001) }[random.nextInt(4)];
                default -> offset + random.nextInt(64) * 0.5f - 2.0f;
            };
        }

        /// `0` or `1000`, the latter in half the rounds.
        private int literalOffset() {
            return random.nextBoolean() ? 0 : 1000;
        }

        private byte[] randomBytes() {
            byte[] bytes = new byte[random.nextInt(6)];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) random.nextInt(3);
            }
            return bytes;
        }

        private int listLength() {
            return INDEXED + random.nextInt(64);
        }

        /// The rounds must have reached both decisions, or the comparison tested one path only.
        private static void requireBothDecisions(int[] decisions) {
            assertThat(decisions[0]).as("rounds deciding present").isPositive();
            assertThat(decisions[1]).as("rounds deciding absent").isPositive();
        }

        @Test
        void longs() {
            int[] decisions = new int[2];
            for (int round = 0; round < ROUNDS; round++) {
                long[] entries = new long[1 + random.nextInt(500)];
                for (int i = 0; i < entries.length; i++) {
                    entries[i] = randomLong();
                }
                long[] literals = new long[listLength()];
                for (int i = 0; i < literals.length; i++) {
                    literals[i] = randomLong();
                }
                Dictionary dictionary = new Dictionary.LongDictionary(entries);
                boolean expected = true;
                for (long literal : literals) {
                    expected &= DictionaryFilterSupport.valueAbsent(dictionary, literal);
                }
                assertThat(DictionaryFilterSupport.absentAll(dictionary, literals)).isEqualTo(expected);
                decisions[expected ? 1 : 0]++;
            }
            requireBothDecisions(decisions);
        }

        @Test
        void ints() {
            int[] decisions = new int[2];
            for (int round = 0; round < ROUNDS; round++) {
                int[] entries = new int[1 + random.nextInt(500)];
                for (int i = 0; i < entries.length; i++) {
                    entries[i] = randomInt();
                }
                int[] literals = new int[listLength()];
                for (int i = 0; i < literals.length; i++) {
                    literals[i] = randomInt();
                }
                Dictionary dictionary = new Dictionary.IntDictionary(entries);
                boolean expected = true;
                for (int literal : literals) {
                    expected &= DictionaryFilterSupport.valueAbsent(dictionary, literal);
                }
                assertThat(DictionaryFilterSupport.absentAll(dictionary, literals)).isEqualTo(expected);
                decisions[expected ? 1 : 0]++;
            }
            requireBothDecisions(decisions);
        }

        @Test
        void doubles() {
            int[] decisions = new int[2];
            for (int round = 0; round < ROUNDS; round++) {
                double[] entries = new double[1 + random.nextInt(500)];
                for (int i = 0; i < entries.length; i++) {
                    entries[i] = randomDouble(0);
                }
                double offset = literalOffset();
                double[] literals = new double[listLength()];
                for (int i = 0; i < literals.length; i++) {
                    literals[i] = randomDouble(offset);
                }
                Dictionary dictionary = new Dictionary.DoubleDictionary(entries);
                boolean expected = true;
                for (double literal : literals) {
                    expected &= DictionaryFilterSupport.valueAbsent(dictionary, literal);
                }
                assertThat(DictionaryFilterSupport.absentAll(dictionary, literals)).isEqualTo(expected);
                decisions[expected ? 1 : 0]++;
            }
            requireBothDecisions(decisions);
        }

        @Test
        void floats() {
            int[] decisions = new int[2];
            for (int round = 0; round < ROUNDS; round++) {
                float[] entries = new float[1 + random.nextInt(500)];
                for (int i = 0; i < entries.length; i++) {
                    entries[i] = randomFloat(0);
                }
                float offset = literalOffset();
                float[] literals = new float[listLength()];
                for (int i = 0; i < literals.length; i++) {
                    literals[i] = randomFloat(offset);
                }
                Dictionary dictionary = new Dictionary.FloatDictionary(entries);
                boolean expected = true;
                for (float literal : literals) {
                    expected &= DictionaryFilterSupport.valueAbsent(dictionary, literal);
                }
                assertThat(DictionaryFilterSupport.absentAll(dictionary, literals)).isEqualTo(expected);
                decisions[expected ? 1 : 0]++;
            }
            requireBothDecisions(decisions);
        }

        @Test
        void binaries() {
            int[] decisions = new int[2];
            for (int round = 0; round < ROUNDS; round++) {
                byte[][] entries = new byte[1 + random.nextInt(200)][];
                for (int i = 0; i < entries.length; i++) {
                    entries[i] = randomBytes();
                }
                byte[][] literals = new byte[listLength()][];
                for (int i = 0; i < literals.length; i++) {
                    literals[i] = randomBytes();
                }
                Dictionary dictionary = byteArrayDictionary(entries);
                boolean expected = true;
                for (byte[] literal : literals) {
                    expected &= DictionaryFilterSupport.valueAbsent(dictionary, literal);
                }
                assertThat(DictionaryFilterSupport.absentAll(dictionary, literals)).isEqualTo(expected);
                decisions[expected ? 1 : 0]++;
            }
            requireBothDecisions(decisions);
        }
    }

    /// A `BYTE_ARRAY` dictionary of `entries`, parsed from their plain encoding as a page's would be.
    private static Dictionary byteArrayDictionary(byte[][] entries) {
        int encodedSize = 0;
        for (byte[] entry : entries) {
            encodedSize += Integer.BYTES + entry.length;
        }
        ByteBuffer encoded = ByteBuffer.allocate(encodedSize).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] entry : entries) {
            encoded.putInt(entry.length);
            encoded.put(entry);
        }
        return Dictionary.parse(encoded.array(), encodedSize, entries.length, PhysicalType.BYTE_ARRAY, null);
    }

    @Nested
    class IntegerLists {

        private final Dictionary ints = new Dictionary.IntDictionary(new int[]{ 0, 3, 6, 9, 1000 });
        private final Dictionary longs = new Dictionary.LongDictionary(new long[]{ 0L, 1000L, 2000L, 3000L, Long.MIN_VALUE });

        @Test
        void shortListsRunOnePassPerLiteral() {
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(PASS_LIMIT, 10, 1))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(PASS_LIMIT, 1000, 1))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(PASS_LIMIT, 10L, 1L))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(PASS_LIMIT, 3000L, 1L))).isFalse();
        }

        @Test
        void closeLiteralsAreIndexedAsABitmask() {
            // Literals 1..9 and 10..18 span far less than RangeBitmask.SPAN_LIMIT.
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(INDEXED, 10, 1))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(INDEXED, 1, 1))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(INDEXED, 1L, 1L))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(INDEXED, 2992L, 1L))).isFalse();
        }

        @Test
        void bitmaskRejectsEntriesOnEitherSideOfTheLiteralsRange() {
            // Entries below the literals' minimum (0, Long.MIN_VALUE) and above their maximum
            // (1000, 3000) fall to the range check, not to a bit.
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(INDEXED, 10, 1))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(INDEXED, 10L, 1L))).isTrue();
            // The dictionary's first (0) and last (1000) entries are found at the range's ends.
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(INDEXED, 0, 1))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(INDEXED, 1000 - INDEXED + 1, 1))).isFalse();
        }

        @Test
        void spreadLiteralsBeyondThePassLimitStillRunPasses() {
            // Literals a span of SPAN_LIMIT apart cannot share a bitmask; up to the sparse pass
            // limit they are scanned for one by one.
            long step = RangeBitmask.SPAN_LIMIT;
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(INDEXED, 10L, step))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(INDEXED, 3000L - (INDEXED - 1) * step, step))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(SPARSE_PASS_LIMIT, 10L, step))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(SPARSE_PASS_LIMIT, 1000L, step))).isFalse();
            int intStep = Math.toIntExact(RangeBitmask.SPAN_LIMIT);
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(INDEXED, 10, intStep))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(SPARSE_PASS_LIMIT, 9, intStep))).isFalse();
        }

        @Test
        void spreadLiteralsAreIndexedAsAHashSet() {
            long step = RangeBitmask.SPAN_LIMIT;
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(HASHED, 10L, step))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(HASHED, 1000L, step))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(HASHED, 3000L - (HASHED - 1) * step, step))).isFalse();
            int intStep = Math.toIntExact(RangeBitmask.SPAN_LIMIT);
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(HASHED, 10, intStep))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(HASHED, 9, intStep))).isFalse();
        }

        @Test
        void literalsSpanningTheWholeRangeAreIndexedAsAHashSet() {
            // max - min overflows a long, so no bitmask can be sized; the set decides instead.
            long[] values = longs(HASHED, 10L, 1L);
            values[0] = Long.MIN_VALUE + 1;
            values[HASHED - 1] = Long.MAX_VALUE;
            assertThat(DictionaryFilterSupport.absentAll(longs, values)).isTrue();
            values[1] = Long.MIN_VALUE;
            assertThat(DictionaryFilterSupport.absentAll(longs, values)).isFalse();
        }

        @Test
        void anEntryEqualToTheSetsEmptyMarkerIsNotMistakenForALiteral() {
            // The set marks empty slots with 0 when 0 is not a literal; both dictionaries hold 0.
            long step = RangeBitmask.SPAN_LIMIT;
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(HASHED, 10L, step))).isTrue();
            // And 0 as a literal picks another marker and is found.
            assertThat(DictionaryFilterSupport.absentAll(longs, longs(HASHED, 0L, step))).isFalse();
            int intStep = Math.toIntExact(RangeBitmask.SPAN_LIMIT);
            assertThat(DictionaryFilterSupport.absentAll(ints, ints(HASHED, 0, intStep))).isFalse();
        }

        @Test
        void wrongArmOrNoDictionaryProvesNothing() {
            assertThat(DictionaryFilterSupport.absentAll(null, ints(INDEXED, 10, 1))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(longs, ints(INDEXED, 10, 1))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(ints, longs(INDEXED, 10L, 1L))).isFalse();
        }
    }

    @Nested
    class FloatingPointLists {

        /// A `NaN` with a payload other than the canonical one, as a writer may store it.
        private static final float FLOAT_NAN_PAYLOAD = Float.intBitsToFloat(0x7fc00001);
        private static final double DOUBLE_NAN_PAYLOAD = Double.longBitsToDouble(0x7ff8000000000001L);

        private final Dictionary floats = new Dictionary.FloatDictionary(new float[]{ -0.0f, 1.5f, FLOAT_NAN_PAYLOAD, 4.5f });
        private final Dictionary doubles = new Dictionary.DoubleDictionary(new double[]{ -0.0, 1.5, DOUBLE_NAN_PAYLOAD, 4.5 });

        private static float[] floats(int count, float first) {
            float[] values = new float[count];
            for (int i = 0; i < count; i++) {
                values[i] = first + i;
            }
            return values;
        }

        private static double[] doubles(int count, double first) {
            double[] values = new double[count];
            for (int i = 0; i < count; i++) {
                values[i] = first + i;
            }
            return values;
        }

        @Test
        void shortListsRunOnePassPerLiteral() {
            assertThat(DictionaryFilterSupport.absentAll(floats, floats(PASS_LIMIT, 10f))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(floats, floats(PASS_LIMIT, 4.5f))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(doubles, doubles(PASS_LIMIT, 10))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(doubles, doubles(PASS_LIMIT, 4.5))).isFalse();
        }

        @Test
        void longListsAreIndexedByTheirBits() {
            assertThat(DictionaryFilterSupport.absentAll(floats, floats(INDEXED, 10f))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(floats, floats(INDEXED, 1.5f))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(doubles, doubles(INDEXED, 10))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(doubles, doubles(INDEXED, 1.5))).isFalse();
        }

        @Test
        void everyNaNIsOneValueOnBothKernels() {
            // Float.compare treats every NaN as equal, so a canonical NaN literal finds the stored
            // payload NaN, in a pass and in the indexed set alike.
            float[] shortFloats = floats(PASS_LIMIT, 10f);
            shortFloats[PASS_LIMIT - 1] = Float.NaN;
            assertThat(DictionaryFilterSupport.absentAll(floats, shortFloats)).isFalse();
            float[] longFloats = floats(INDEXED, 10f);
            longFloats[0] = Float.NaN;
            assertThat(DictionaryFilterSupport.absentAll(floats, longFloats)).isFalse();

            double[] shortDoubles = doubles(PASS_LIMIT, 10);
            shortDoubles[PASS_LIMIT - 1] = Double.NaN;
            assertThat(DictionaryFilterSupport.absentAll(doubles, shortDoubles)).isFalse();
            double[] longDoubles = doubles(INDEXED, 10);
            longDoubles[0] = Double.NaN;
            assertThat(DictionaryFilterSupport.absentAll(doubles, longDoubles)).isFalse();
        }

        @Test
        void signedZeroesStayApartOnBothKernels() {
            // The dictionaries hold -0.0 only: a +0.0 literal is absent, a -0.0 literal present.
            float[] shortFloats = floats(PASS_LIMIT, 10f);
            shortFloats[0] = 0.0f;
            assertThat(DictionaryFilterSupport.absentAll(floats, shortFloats)).isTrue();
            shortFloats[0] = -0.0f;
            assertThat(DictionaryFilterSupport.absentAll(floats, shortFloats)).isFalse();
            float[] longFloats = floats(INDEXED, 10f);
            longFloats[INDEXED - 1] = 0.0f;
            assertThat(DictionaryFilterSupport.absentAll(floats, longFloats)).isTrue();
            longFloats[INDEXED - 1] = -0.0f;
            assertThat(DictionaryFilterSupport.absentAll(floats, longFloats)).isFalse();

            double[] shortDoubles = doubles(PASS_LIMIT, 10);
            shortDoubles[0] = 0.0;
            assertThat(DictionaryFilterSupport.absentAll(doubles, shortDoubles)).isTrue();
            shortDoubles[0] = -0.0;
            assertThat(DictionaryFilterSupport.absentAll(doubles, shortDoubles)).isFalse();
            double[] longDoubles = doubles(INDEXED, 10);
            longDoubles[INDEXED - 1] = 0.0;
            assertThat(DictionaryFilterSupport.absentAll(doubles, longDoubles)).isTrue();
            longDoubles[INDEXED - 1] = -0.0;
            assertThat(DictionaryFilterSupport.absentAll(doubles, longDoubles)).isFalse();
        }

        @Test
        void wrongArmOrNoDictionaryProvesNothing() {
            assertThat(DictionaryFilterSupport.absentAll(null, floats(INDEXED, 10f))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(doubles, floats(INDEXED, 10f))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(floats, doubles(INDEXED, 10))).isFalse();
        }
    }

    @Nested
    class BinaryLists {

        private final Dictionary dictionary = byteArrayDictionary(
                "cat_0", "cat_1", "cat_2", "cat_3", "", "a much longer category name than the others");

        private static Dictionary byteArrayDictionary(String... entries) {
            byte[][] values = new byte[entries.length][];
            for (int i = 0; i < entries.length; i++) {
                values[i] = entries[i].getBytes(StandardCharsets.UTF_8);
            }
            return DictionaryFilterSupportTest.byteArrayDictionary(values);
        }

        /// `count` literals `<prefix>0`, `<prefix>1`, …
        private static byte[][] literals(int count, String prefix) {
            byte[][] values = new byte[count][];
            for (int i = 0; i < count; i++) {
                values[i] = (prefix + i).getBytes(StandardCharsets.UTF_8);
            }
            return values;
        }

        @Test
        void shortListsRunOnePassPerLiteral() {
            assertThat(DictionaryFilterSupport.absentAll(dictionary, literals(PASS_LIMIT, "dog_"))).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(dictionary, literals(PASS_LIMIT, "cat_"))).isFalse();
        }

        @Test
        void longListsAreIndexedAsAHashSet() {
            assertThat(DictionaryFilterSupport.absentAll(dictionary, literals(INDEXED, "dog_"))).isTrue();
            // Found at the dictionary's first entry, and at its last.
            assertThat(DictionaryFilterSupport.absentAll(dictionary, literals(INDEXED, "cat_"))).isFalse();
            byte[][] withLast = literals(INDEXED, "dog_");
            withLast[INDEXED - 1] = "a much longer category name than the others".getBytes(StandardCharsets.UTF_8);
            assertThat(DictionaryFilterSupport.absentAll(dictionary, withLast)).isFalse();
        }

        @Test
        void entriesOfALengthNoLiteralHasAreNeverHashed() {
            // Every literal is 5 bytes; the empty entry and the long one fail the length check and
            // the 5-byte entries miss in the set.
            byte[][] fiveBytes = literals(INDEXED, "dog_");
            assertThat(Arrays.stream(fiveBytes).allMatch(value -> value.length == 5)).isTrue();
            assertThat(DictionaryFilterSupport.absentAll(dictionary, fiveBytes)).isTrue();
            // The empty entry is found once an empty literal is in the list.
            fiveBytes[0] = new byte[0];
            assertThat(DictionaryFilterSupport.absentAll(dictionary, fiveBytes)).isFalse();
        }

        @Test
        void literalsSharingALengthAndAPrefixAreToldApart() {
            // dog_0..dog_8 and cat_0..cat_3 are all 5 bytes with a common suffix shape; only an
            // exact byte match counts.
            byte[][] values = literals(INDEXED, "dog_");
            values[INDEXED - 1] = "cat_9".getBytes(StandardCharsets.UTF_8);
            assertThat(DictionaryFilterSupport.absentAll(dictionary, values)).isTrue();
            values[INDEXED - 1] = "cat_3".getBytes(StandardCharsets.UTF_8);
            assertThat(DictionaryFilterSupport.absentAll(dictionary, values)).isFalse();
        }

        @Test
        void wrongArmOrNoDictionaryProvesNothing() {
            assertThat(DictionaryFilterSupport.absentAll(null, literals(INDEXED, "dog_"))).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(new Dictionary.IntDictionary(new int[]{ 1 }),
                    literals(INDEXED, "dog_"))).isFalse();
        }
    }
}
