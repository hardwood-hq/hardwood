/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// Estimates the distinct values a chunk stored after giving its dictionary up at a verdict, to tell
/// at flush whether the dictionary might have won over the whole chunk after all.
///
/// A HyperLogLog sketch: the top [#PRECISION] bits of a value's 64-bit hash pick one of
/// [#REGISTERS] registers, and each register keeps the longest run of leading zeros, plus one, seen
/// in the remaining bits. A value hashes the same at every occurrence, so the estimate depends on
/// the set of values offered and not on their order or how often each repeats. It is read with
/// Ertl's improved estimator (*New cardinality estimation algorithms for HyperLogLog sketches*,
/// 2017), which works from the histogram of register values and needs no switch between formulas
/// for small and large counts, so its relative error is about `1.04 / sqrt(REGISTERS)`, 0.8%, at
/// every distinct count.
///
/// The estimate only decides whether to intern the chunk again, where the exact comparison decides
/// the encoding. A wrong estimate costs a re-intern that is then rejected, or a dictionary left
/// unrecovered, and never a different file for the same values.
final class HyperLogLog {

    static final int PRECISION = 14;
    static final int REGISTERS = 1 << PRECISION;

    /// The largest register value: the remaining bits all zero, bounded by the sentinel bit.
    private static final int MAX_RANK = Long.SIZE - PRECISION + 1;

    /// Two standard errors of the estimate, which [#dictionaryMightWin] gives the dictionary.
    private static final double MARGIN = 2 * 1.04 / Math.sqrt(REGISTERS);

    private final byte[] registers = new byte[REGISTERS];

    /// Offers a value by its 64-bit hash.
    void offer(long hash) {
        int register = (int) (hash >>> (Long.SIZE - PRECISION));
        // The sentinel bit bounds the rank where the remaining bits are all zero.
        byte rank = (byte) (Long.numberOfLeadingZeros((hash << PRECISION) | (1L << (PRECISION - 1))) + 1);
        // Stored unconditionally: on a chunk not much longer than the sketch has registers, whether
        // a value raises its register is a coin toss a branch would mispredict.
        registers[register] = (byte) Math.max(registers[register], rank);
    }

    /// The distinct values offered, estimated.
    long estimatedDistinct() {
        int[] histogram = new int[MAX_RANK + 1];
        for (byte rank : registers) {
            histogram[rank]++;
        }
        double z = REGISTERS * tau(1 - (double) histogram[MAX_RANK] / REGISTERS);
        for (int rank = MAX_RANK - 1; rank >= 1; rank--) {
            z = 0.5 * (z + histogram[rank]);
        }
        z += REGISTERS * sigma((double) histogram[0] / REGISTERS);
        return Math.round(REGISTERS * (REGISTERS / (2 * Math.log(2))) / z);
    }

    /// Ertl's `sigma`, the correction for registers still empty: `x + sum(x^(2^k) * 2^(k-1))`.
    private static double sigma(double x) {
        if (x == 1) {
            return Double.POSITIVE_INFINITY;
        }
        double y = 1;
        double z = x;
        double previous;
        do {
            x *= x;
            previous = z;
            z += x * y;
            y += y;
        }
        while (z != previous);
        return z;
    }

    /// Ertl's `tau`, the correction for registers at [#MAX_RANK].
    private static double tau(double x) {
        if (x == 0 || x == 1) {
            return 0;
        }
        double y = 1;
        double z = 1 - x;
        double previous;
        do {
            x = Math.sqrt(x);
            previous = z;
            y *= 0.5;
            z -= (1 - x) * (1 - x) * y;
        }
        while (z != previous);
        return z / 3;
    }

    /// Whether a dictionary over all `presentValues` might come out smaller than `plainBytes`,
    /// each distinct value costing an average stored value. It is judged at the estimate less two
    /// standard errors, so a chunk near the break-even is interned again and the exact comparison
    /// decides it.
    boolean dictionaryMightWin(long presentValues, long plainBytes) {
        long distinct = Math.min((long) (estimatedDistinct() * (1 - MARGIN)), presentValues);
        long dictionaryBytes = Math.ceilDiv(distinct * plainBytes, presentValues)
                + Math.ceilDiv(presentValues * bitWidth(distinct), Byte.SIZE);
        return dictionaryBytes < plainBytes;
    }

    private static int bitWidth(long distinct) {
        return distinct <= 1 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(distinct - 1);
    }
}
