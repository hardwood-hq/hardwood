/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.internal.reader.ReferencePageDecoder;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// [AlpEncoder] through [AlpDecoder], as [ReferencePageDecoder] drives it: every value has to come
/// back bit for bit whatever pair the
/// encoder picks, so each case below is a round trip, and the size checks pin down that the pair
/// it picks is a good one where the data has few decimal digits.
class AlpEncoderTest {

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 7, 8, 1023, 1024, 1025, 5000 })
    void roundTripsDoublesAtEveryVectorBoundary(int count) {
        double[] values = new double[count];
        Random random = new Random(count);
        for (int i = 0; i < count; i++) {
            values[i] = Math.round(random.nextDouble() * 2000 - 1000) / 100.0;
        }

        assertRoundTrip(values);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 7, 8, 1023, 1024, 1025, 5000 })
    void roundTripsFloatsAtEveryVectorBoundary(int count) {
        float[] values = new float[count];
        Random random = new Random(count);
        for (int i = 0; i < count; i++) {
            values[i] = Math.round(random.nextFloat() * 2000 - 1000) / 100.0f;
        }

        assertRoundTrip(values);
    }

    /// Values no pair turns into integers, which become exceptions with their bits intact.
    @Test
    void roundTripsDoublesThatAreAllSpecial() {
        double[] values = {
                Double.NaN, Double.longBitsToDouble(0x7FF0_0000_0000_0001L), Double.longBitsToDouble(0xFFF8_0000_0000_1234L),
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, Double.MIN_VALUE, Double.MAX_VALUE,
                -Double.MAX_VALUE, Double.MIN_NORMAL, 1e300, -1e-300 };

        assertRoundTrip(values);
    }

    @Test
    void roundTripsFloatsThatAreAllSpecial() {
        float[] values = {
                Float.NaN, Float.intBitsToFloat(0x7F80_0001), Float.intBitsToFloat(0xFFC0_1234),
                Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f, Float.MIN_VALUE, Float.MAX_VALUE,
                -Float.MAX_VALUE, Float.MIN_NORMAL, 1e30f, -1e-30f };

        assertRoundTrip(values);
    }

    /// Random doubles carry all of their significant digits, so they are mostly exceptions; what
    /// matters is that every one comes back.
    @Test
    void roundTripsDoublesWithFullPrecision() {
        Random random = new Random(42);
        double[] values = new double[3000];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextDouble() * 1e6 - 5e5;
        }

        assertRoundTrip(values);
    }

    @Test
    void roundTripsFloatsWithFullPrecision() {
        Random random = new Random(42);
        float[] values = new float[3000];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextFloat() * 1e6f - 5e5f;
        }

        assertRoundTrip(values);
    }

    /// Integers spanning the whole range of a long encode at exponent 0 and pack at 64 bits.
    @Test
    void roundTripsDoublesSpanningTheLongRange() {
        double[] values = { -9.2e18, 9.2e18, 0, 1, -1, 4.5e18, -4.5e18, 123456789 };

        assertRoundTrip(values);
    }

    /// The float form: integers spanning more than the signed range of an int encode at exponent
    /// 0 and pack at 32 bits, their distances computed with wrapping int arithmetic.
    @Test
    void roundTripsFloatsSpanningTheIntRange() {
        float[] values = { -2.0e9f, 2.0e9f, 0, 1, -1, 1.0e9f, -1.0e9f, 12345 };

        assertRoundTrip(values);
    }

    /// Decimals mixed with values of another scale and with specials, in the same vector.
    @Test
    void roundTripsMixedVectors() {
        double[] values = new double[2500];
        for (int i = 0; i < values.length; i++) {
            values[i] = switch (i % 7) {
                case 0 -> Double.NaN;
                case 1 -> i * 1e10;
                case 2 -> -0.0;
                case 3 -> Math.PI * i;
                default -> i / 4.0;
            };
        }

        assertRoundTrip(values);
    }

    @Test
    void packsIdenticalValuesInNoBits() {
        double[] values = new double[1024];
        Arrays.fill(values, 12.5);

        byte[] page = assertRoundTrip(values);

        // Header and one offset, then the vector header alone: no packed bytes, no exceptions
        assertThat(page).hasSize(7 + 4 + 13);
    }

    /// Two decimal digits in a range of 20: about 2000 distinct integers, so 11 bits a value
    /// where `PLAIN` takes 64.
    @Test
    void packsTwoDecimalDoublesAtTheirRangesWidth() {
        Random random = new Random(1);
        double[] values = new double[8192];
        for (int i = 0; i < values.length; i++) {
            values[i] = Math.round(random.nextDouble() * 2000 - 1000) / 100.0;
        }

        byte[] page = assertRoundTrip(values);

        assertThat(page.length).as("ALP bytes against PLAIN's %d", values.length * 8)
                .isLessThan(values.length * 12 / 8 + 200);
    }

    /// Binary32 rounding leaves about 4% of two-decimal floats without an integer that decodes
    /// back to them at any pair, so these pack at 11 bits plus those exceptions: under half of
    /// `PLAIN`'s 32.
    @Test
    void packsTwoDecimalFloatsInUnderHalfTheirPlainSize() {
        Random random = new Random(1);
        float[] values = new float[8192];
        for (int i = 0; i < values.length; i++) {
            values[i] = Math.round(random.nextFloat() * 2000 - 1000) / 100.0f;
        }

        byte[] page = assertRoundTrip(values);

        assertThat(page.length).as("ALP bytes against PLAIN's %d", values.length * 4)
                .isLessThan(values.length * 4 / 2);
    }

    /// Every other value has full precision. A vector sample taken at an even step sees only one
    /// kind and picks a pair that keeps the random values at 57 bits, dragging the decimals up to
    /// that width as well; the right pair keeps the decimals at 11 bits and stores the random
    /// values as exceptions.
    @Test
    void choosesThePairForAVectorOfAlternatingKinds() {
        Random random = new Random(3);
        double[] values = new double[1024];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 2 == 0
                    ? random.nextDouble() * 20 - 10
                    : Math.round(random.nextDouble() * 2000 - 1000) / 100.0;
        }

        byte[] page = assertRoundTrip(values);

        // 512 exceptions of 10 bytes, 1024 values at 11 bits, and the headers
        assertThat(page.length).isLessThanOrEqualTo(512 * 10 + 1024 * 11 / 8 + 7 + 4 + 13);
    }

    /// Two values kept far apart widen the frame to 64 bits for the whole vector, the rest being
    /// exceptions on top; storing every value as an exception is smaller, and keeps the vector
    /// within the bound page cuts charge.
    @Test
    void storesAVectorAsExceptionsWhereThatIsSmaller() {
        Random random = new Random(5);
        double[] values = new double[1024];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextDouble();
        }
        values[0] = -9.0e18;
        values[1] = 9.0e18;

        byte[] page = assertRoundTrip(values);

        assertThat(page).hasSize(7 + 4 + 13 + 1024 * 10);
    }

    /// The float form. Random floats keep too few digits to be all exceptions, so the rest of
    /// the vector is NaNs, each with a payload of its own, which no pair turns into an integer.
    @Test
    void storesAFloatVectorAsExceptionsWhereThatIsSmaller() {
        float[] values = new float[1024];
        for (int i = 0; i < values.length; i++) {
            values[i] = Float.intBitsToFloat(0x7FC0_0000 | i);
        }
        values[0] = -2.0e9f;
        values[1] = 2.0e9f;

        byte[] page = assertRoundTrip(values);

        assertThat(page).hasSize(7 + 4 + 9 + 1024 * 6);
    }

    /// One encoder serves every page of a column, its buffer and scratch arrays carried from page
    /// to page. Nothing a page leaves behind may reach the next: a long page full of exceptions
    /// followed by a short one, and back, has to encode as fresh encoders do.
    @Test
    void encodesEachPageAsAFreshEncoderWouldWhenReused() {
        Random random = new Random(11);
        double[] wide = new double[5000];
        for (int i = 0; i < wide.length; i++) {
            wide[i] = random.nextDouble();
        }
        double[] narrow = new double[13];
        for (int i = 0; i < narrow.length; i++) {
            narrow[i] = i / 4.0;
        }
        AlpEncoder reused = new AlpEncoder(Double.BYTES);

        for (double[] values : new double[][] { wide, narrow, wide, narrow }) {
            assertThat(encodeDoubles(reused, values)).isEqualTo(encodeDoubles(new AlpEncoder(Double.BYTES), values));
        }
    }

    private static byte[] encodeDoubles(AlpEncoder encoder, double[] values) {
        int length = encoder.encodeDoubles(values, 0, values.length,
                AlpEncoder.presetDoubles(values, 0, values.length));
        return Arrays.copyOf(encoder.buffer(), length);
    }

    private static byte[] encodeFloats(AlpEncoder encoder, float[] values) {
        int length = encoder.encodeFloats(values, 0, values.length,
                AlpEncoder.presetFloats(values, 0, values.length));
        return Arrays.copyOf(encoder.buffer(), length);
    }

    private static void assertWithinBound(byte[] page, int count, int valueWidth) {
        assertThat((long) page.length * Byte.SIZE)
                .as("page bits against the bound for %d values", count)
                .isLessThanOrEqualTo(AlpEncoder.pageOverheadBits(valueWidth) + count * AlpEncoder.maxValueBits(valueWidth));
    }

    private static byte[] assertRoundTrip(double[] values) {
        byte[] page = encodeDoubles(new AlpEncoder(Double.BYTES), values);
        double[] decoded = (double[]) ReferencePageDecoder.decodeValues(Encoding.ALP, PhysicalType.DOUBLE, null, page,
                0, page.length, values.length, null);
        for (int i = 0; i < values.length; i++) {
            assertThat(Double.doubleToRawLongBits(decoded[i]))
                    .as("value %d (%s)", i, values[i])
                    .isEqualTo(Double.doubleToRawLongBits(values[i]));
        }
        assertWithinBound(page, values.length, Double.BYTES);
        return page;
    }

    private static byte[] assertRoundTrip(float[] values) {
        byte[] page = encodeFloats(new AlpEncoder(Float.BYTES), values);
        float[] decoded = (float[]) ReferencePageDecoder.decodeValues(Encoding.ALP, PhysicalType.FLOAT, null, page,
                0, page.length, values.length, null);
        for (int i = 0; i < values.length; i++) {
            assertThat(Float.floatToRawIntBits(decoded[i]))
                    .as("value %d (%s)", i, values[i])
                    .isEqualTo(Float.floatToRawIntBits(values[i]));
        }
        assertWithinBound(page, values.length, Float.BYTES);
        return page;
    }
}
