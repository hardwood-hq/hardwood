/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// The layout constants and the decode arithmetic of ALP, shared by [AlpDecoder] and the
/// encoder, which keeps a value only where this arithmetic gives its bits back.
///
/// The decode is normative: `encoded * 10^factor * 10^-exponent`, two multiplications in the
/// column's own precision by correctly rounded power-of-ten constants. The constants are literals,
/// which Java rounds correctly; the format forbids computing them, since `Math.pow` is not
/// guaranteed to round correctly.
final class AlpFormat {

    static final int HEADER_SIZE = 7;
    static final int OFFSET_SIZE = Integer.BYTES;
    /// Exponent, factor and exception count, ahead of the frame of reference.
    static final int ALP_INFO_SIZE = 4;
    static final int POSITION_SIZE = Short.BYTES;
    static final int MIN_LOG_VECTOR_SIZE = 3;
    static final int MAX_LOG_VECTOR_SIZE = 15;

    static final float[] FLOAT_POW10 = {
            1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f };
    static final float[] FLOAT_NEG_POW10 = {
            1e0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f };
    static final double[] DOUBLE_POW10 = {
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18 };
    static final double[] DOUBLE_NEG_POW10 = {
            1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
            1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18 };

    static final int MAX_FLOAT_EXPONENT = FLOAT_POW10.length - 1;
    static final int MAX_DOUBLE_EXPONENT = DOUBLE_POW10.length - 1;

    private AlpFormat() {
    }

    /// The bytes of a vector header: `AlpInfo`, then a frame of reference as wide as a value and
    /// a one-byte bit width.
    static int vectorHeaderSize(int valueWidth) {
        return ALP_INFO_SIZE + valueWidth + 1;
    }

    /// The bytes `count` deltas at `bitWidth` bits occupy, the last byte zero-padded.
    static long packedBytes(int count, int bitWidth) {
        return ((long) count * bitWidth + 7) / 8;
    }

    static double decodeDouble(long encoded, int exponent, int factor) {
        return (double) encoded * DOUBLE_POW10[factor] * DOUBLE_NEG_POW10[exponent];
    }

    static float decodeFloat(int encoded, int exponent, int factor) {
        return (float) encoded * FLOAT_POW10[factor] * FLOAT_NEG_POW10[exponent];
    }
}
