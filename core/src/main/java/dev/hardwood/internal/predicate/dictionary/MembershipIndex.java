/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.util.Arrays;

import dev.hardwood.internal.predicate.NaNLiterals;

/// An `IN` list's literals, kept as they are or indexed, prepared once for the dictionary check that
/// [DictionaryFilterSupport] runs per row group: the kernel for the list is chosen here, and the
/// index it probes, if any, is built here, so a read shares one across all of its row groups.
///
/// A short list is kept as it is and decided by one pass over the dictionary per literal; every
/// branch of such a pass goes the same way until a match, so the passes stay cheap. A longer list
/// is indexed: a [RangeBitmask] where integer literals lie close together, else a [LongSet] over
/// the values' raw bits or a [BinarySet] over byte strings. Integer literals too spread out for a
/// bitmask, and floating-point literals, stay on passes a while longer, since a pass over
/// primitives is cheap next to a hash probe; a pass over byte strings is not, so a byte-string list
/// is indexed from a few literals on. A `FLOAT16` list always takes a bitmask over the halves' raw
/// bits, which span at most 2^16 values.
///
/// Each list type has its own index type, so every scan stays monomorphic and primitive. An index
/// is never changed once built. It holds the literal arrays it was built from as they are, in the
/// passes form and inside a [BinarySet], so those arrays must not change afterwards either; the
/// resolved predicate that carries both is shared across a read and never mutated.
public final class MembershipIndex {

    /// Literal count up to which a membership check runs one pass per literal. Tuning: #1594.
    static final int PASS_LIMIT = 8;

    /// Literal count up to which a membership check over primitives that takes no [RangeBitmask]
    /// still runs one pass per literal: integer literals too far apart for one, and `FLOAT` /
    /// `DOUBLE` literals, whose passes compare raw bits. A pass over primitives costs far less per
    /// entry than a hash probe, so the hash set only wins from this count. Tuning: #1594.
    static final int SPARSE_PASS_LIMIT = 16;

    /// Literal count up to which a byte-string membership check runs one pass per literal. A pass
    /// compares bytes per entry, so it costs several times a primitive pass and the [BinarySet]
    /// wins from a few literals on. Tuning: #1594.
    static final int BINARY_PASS_LIMIT = 2;

    private MembershipIndex() {
    }

    /// An `INT32` list: its literals for passes, or a [RangeBitmask] or [LongSet] over them
    /// widened to `long`. Exactly one of the three is set.
    public static final class Ints {
        final int[] passes;
        final RangeBitmask bitmask;
        final LongSet set;

        private Ints(int[] passes, RangeBitmask bitmask, LongSet set) {
            this.passes = passes;
            this.bitmask = bitmask;
            this.set = set;
        }

        public static Ints of(int[] values) {
            if (values.length <= PASS_LIMIT) {
                return new Ints(values, null, null);
            }
            long[] keys = widened(values);
            RangeBitmask bitmask = RangeBitmask.of(keys);
            if (bitmask != null) {
                return new Ints(null, bitmask, null);
            }
            if (values.length <= SPARSE_PASS_LIMIT) {
                return new Ints(values, null, null);
            }
            return new Ints(null, null, new LongSet(keys));
        }
    }

    /// An `INT64` list: its literals for passes, or a [RangeBitmask] or [LongSet] over them.
    /// Exactly one of the three is set.
    public static final class Longs {
        final long[] passes;
        final RangeBitmask bitmask;
        final LongSet set;

        private Longs(long[] passes, RangeBitmask bitmask, LongSet set) {
            this.passes = passes;
            this.bitmask = bitmask;
            this.set = set;
        }

        public static Longs of(long[] values) {
            if (values.length <= PASS_LIMIT) {
                return new Longs(values, null, null);
            }
            RangeBitmask bitmask = RangeBitmask.of(values);
            if (bitmask != null) {
                return new Longs(null, bitmask, null);
            }
            if (values.length <= SPARSE_PASS_LIMIT) {
                return new Longs(values, null, null);
            }
            return new Longs(null, null, new LongSet(values));
        }
    }

    /// A `FLOAT` list: whether it holds a `NaN`, and either its literals for passes or a [LongSet]
    /// over the raw bits of the others, `null` when there are none. A value that is not `NaN` has one
    /// bit pattern that no `NaN` shares, so raw-bit equality is `Float.compare` equality for it.
    public static final class Floats {
        final float[] passes;
        final boolean nan;
        final LongSet set;

        private Floats(float[] passes, boolean nan, LongSet set) {
            this.passes = passes;
            this.nan = nan;
            this.set = set;
        }

        public static Floats of(float[] values) {
            boolean nan = NaNLiterals.containsNaN(values);
            if (values.length <= SPARSE_PASS_LIMIT) {
                return new Floats(values, nan, null);
            }
            long[] keys = rawBits(values);
            return new Floats(null, nan, keys.length == 0 ? null : new LongSet(keys));
        }
    }

    /// A `DOUBLE` list. See [Floats].
    public static final class Doubles {
        final double[] passes;
        final boolean nan;
        final LongSet set;

        private Doubles(double[] passes, boolean nan, LongSet set) {
            this.passes = passes;
            this.nan = nan;
            this.set = set;
        }

        public static Doubles of(double[] values) {
            boolean nan = NaNLiterals.containsNaN(values);
            if (values.length <= SPARSE_PASS_LIMIT) {
                return new Doubles(values, nan, null);
            }
            long[] keys = rawBits(values);
            return new Doubles(null, nan, keys.length == 0 ? null : new LongSet(keys));
        }
    }

    /// A `FLOAT16` list: whether it holds a `NaN`, and a [RangeBitmask] over the raw half bits of
    /// the literals binary16 represents exactly, `null` when there are none. A half that is not
    /// `NaN` has one bit pattern, so each two-byte entry is probed by its raw bits; a literal
    /// binary16 cannot represent matches no entry and is left out.
    public static final class Float16s {
        final boolean nan;
        final RangeBitmask bitmask;

        private Float16s(boolean nan, RangeBitmask bitmask) {
            this.nan = nan;
            this.bitmask = bitmask;
        }

        public static Float16s of(float[] values) {
            boolean nan = NaNLiterals.containsNaN(values);
            long[] keys = halfBits(values);
            if (keys.length == 0) {
                return new Float16s(nan, null);
            }
            // Half bits span at most 2^16 values, within RangeBitmask.SPAN_LIMIT. A null bitmask
            // means no literal can match, so a declined one must not pass for it.
            RangeBitmask bitmask = RangeBitmask.of(keys);
            if (bitmask == null) {
                throw new IllegalStateException("A bitmask declined the half bits of " + keys.length + " FLOAT16 literals");
            }
            return new Float16s(nan, bitmask);
        }
    }

    /// A byte-string list: its literals for passes, or a [BinarySet] over them. Exactly one of the
    /// two is set.
    public static final class Binaries {
        final byte[][] passes;
        final BinarySet set;

        private Binaries(byte[][] passes, BinarySet set) {
            this.passes = passes;
            this.set = set;
        }

        public static Binaries of(byte[][] values) {
            if (values.length <= BINARY_PASS_LIMIT) {
                return new Binaries(values, null);
            }
            return new Binaries(null, new BinarySet(values));
        }
    }

    /// The values widened to `long`.
    private static long[] widened(int[] values) {
        long[] keys = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            keys[i] = values[i];
        }
        return keys;
    }

    /// The raw bits of the values that are not `NaN`.
    private static long[] rawBits(float[] values) {
        int count = 0;
        for (float value : values) {
            if (!Float.isNaN(value)) {
                count++;
            }
        }
        long[] keys = new long[count];
        int i = 0;
        for (float value : values) {
            if (!Float.isNaN(value)) {
                keys[i++] = Float.floatToRawIntBits(value);
            }
        }
        return keys;
    }

    /// The raw bits of the values that are not `NaN`.
    private static long[] rawBits(double[] values) {
        int count = 0;
        for (double value : values) {
            if (!Double.isNaN(value)) {
                count++;
            }
        }
        long[] keys = new long[count];
        int i = 0;
        for (double value : values) {
            if (!Double.isNaN(value)) {
                keys[i++] = Double.doubleToRawLongBits(value);
            }
        }
        return keys;
    }

    /// The raw half bits of the values that are neither `NaN` nor beyond binary16's exact values.
    private static long[] halfBits(float[] values) {
        long[] keys = new long[values.length];
        int count = 0;
        for (float value : values) {
            short half = Float.floatToFloat16(value);
            if (!Float.isNaN(value)
                    && Float.floatToRawIntBits(Float.float16ToFloat(half)) == Float.floatToRawIntBits(value)) {
                keys[count++] = Short.toUnsignedLong(half);
            }
        }
        return Arrays.copyOf(keys, count);
    }
}
