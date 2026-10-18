/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [AlpDecoder] on pages built byte by byte, since no writer in the test toolchain emits ALP: each
/// page is checked ([AlpDecoder#validate]) and then decoded vector by vector
/// ([AlpDecoder#decodeVector]), as an `ALP` page's reader does. The page layout and the decode
/// arithmetic follow the format's `AlpEncoding.md`; the end-to-end check against another
/// implementation's output is `AlpReadTest` in the parquet-testing runner.
class AlpDecoderTest {

    private static final long[] NO_DELTAS = {};
    private static final int[] NO_POSITIONS = {};

    /// The worked example of the specification: (exponent 4, factor 3), one NaN exception, a
    /// frame of reference of 3335 and 15-bit deltas.
    @Test
    void decodesTheSpecificationsWorkedExample() {
        long nanBits = Double.doubleToRawLongBits(Double.NaN);
        byte[] vector = doubleVector(4, 3, 3335, 15, new long[]{ 11665, 11665, 21665, 0 },
                new int[]{ 1 }, new long[]{ nanBits });
        assertThat(vector).hasSize(31);

        double[] values = decodeDoubles(page(10, 4, vector), 4);

        assertThat(values).containsExactly(1500.0, Double.NaN, 2500.0, 333.5);
        assertThat(Double.doubleToRawLongBits(values[1])).isEqualTo(nanBits);
    }

    /// The format fixes the decode as two multiplications by correctly rounded constants. 11 at
    /// (4, 1) is a value one multiplication by `1e-3` decodes to `0.011` instead.
    @Test
    void decodesDoublesWithTwoMultiplications() {
        byte[] vector = doubleVector(4, 1, 11, 0, NO_DELTAS, NO_POSITIONS, new long[0]);

        double[] values = decodeDoubles(page(10, 1, vector), 1);

        assertThat(values[0]).isEqualTo(0.011000000000000001);
    }

    /// FLOAT decodes in binary32 throughout. 1075 at (7, 6) is a value that one multiplication,
    /// a division, and the same two multiplications done in binary64 all decode to `107.5`.
    @Test
    void decodesFloatsWithTwoBinary32Multiplications() {
        byte[] vector = floatVector(7, 6, 1075, 0, NO_DELTAS, NO_POSITIONS, new int[0]);

        float[] values = decodeFloats(page(10, 1, vector), 1);

        assertThat(values[0]).isEqualTo(107.50001f);
    }

    @Test
    void decodesFloatExceptionsBitForBit() {
        int signallingNan = 0x7F800001;
        int negativeZero = Float.floatToRawIntBits(-0.0f);
        byte[] vector = floatVector(1, 0, -1000, 11, new long[]{ 1025, 0, 0, 1000 },
                new int[]{ 1, 2 }, new int[]{ signallingNan, negativeZero });

        float[] values = decodeFloats(page(10, 4, vector), 4);

        assertThat(values[0]).isEqualTo(2.5f);
        assertThat(Float.floatToRawIntBits(values[1])).isEqualTo(signallingNan);
        assertThat(Float.floatToRawIntBits(values[2])).isEqualTo(negativeZero);
        assertThat(values[3]).isEqualTo(0.0f);
    }

    @Test
    void decodesABitWidthOfZeroAsTheFrameOfReference() {
        byte[] vector = doubleVector(1, 0, 25, 0, NO_DELTAS, NO_POSITIONS, new long[0]);

        double[] values = decodeDoubles(page(10, 3, vector), 3);

        assertThat(values).containsExactly(2.5, 2.5, 2.5);
    }

    /// Deltas are unsigned, so a vector spanning more than the signed range of a long packs at 64
    /// bits and adds back with wrapping arithmetic.
    @Test
    void decodesSixtyFourBitDeltasWithWrappingAddition() {
        long min = Long.MIN_VALUE + 2048;
        long max = Long.MAX_VALUE - 2047;
        byte[] vector = doubleVector(0, 0, min, 64, new long[]{ 0, max - min }, NO_POSITIONS, new long[0]);

        double[] values = decodeDoubles(page(10, 2, vector), 2);

        assertThat(values).containsExactly((double) min, (double) max);
    }

    /// Every vector carries its own parameters, and all but the last hold `2^log_vector_size`
    /// values.
    @Test
    void decodesVectorsWithTheirOwnParametersAndAShortLastVector() {
        byte[] first = doubleVector(1, 0, 10, 4, new long[]{ 0, 1, 2, 3, 4, 5, 6, 7 }, NO_POSITIONS, new long[0]);
        byte[] last = doubleVector(2, 0, -5, 3, new long[]{ 0, 5, 7 }, new int[]{ 1 },
                new long[]{ Double.doubleToRawLongBits(Double.POSITIVE_INFINITY) });

        double[] values = decodeDoubles(page(3, 11, first, last), 11);

        // The normative arithmetic, not the nearest decimal: an encoder stores 1.2 as an
        // exception rather than as 12 at (1, 0)
        assertThat(values).containsExactly(1.0, 1.1, 1.2000000000000002, 1.3, 1.4000000000000001, 1.5,
                1.6, 1.7000000000000002, -0.05, Double.POSITIVE_INFINITY, 0.02);
    }

    /// A vector decodes on its own, in any order: the last one, shorter than the others, first.
    @Test
    void decodesTheLastPartialVectorOnItsOwn() {
        byte[] first = doubleVector(1, 0, 10, 4, new long[]{ 0, 1, 2, 3, 4, 5, 6, 7 }, NO_POSITIONS, new long[0]);
        byte[] last = doubleVector(2, 0, -5, 3, new long[]{ 0, 5, 7 }, new int[]{ 1 },
                new long[]{ Double.doubleToRawLongBits(Double.POSITIVE_INFINITY) });
        byte[] page = page(3, 11, first, last);
        AlpDecoder decoder = new AlpDecoder(page, 0, page.length, 11, PhysicalType.DOUBLE);
        decoder.validate();
        double[] vector = new double[decoder.vectorSize()];

        int count = decoder.decodeVector(1, null, vector);

        assertThat(count).isEqualTo(3);
        assertThat(Arrays.copyOf(vector, count)).containsExactly(-0.05, Double.POSITIVE_INFINITY, 0.02);
        assertThat(decoder.decodeVector(0, null, vector)).isEqualTo(8);
        assertThat(vector[7]).isEqualTo(1.7000000000000002);
    }

    @Test
    void decodesAnEmptyPage() {
        double[] values = decodeDoubles(page(10, 0), 0);

        assertThat(values).isEmpty();
    }

    /// The page is decoded from within a larger buffer, which runs on past `limit` with bytes of
    /// an earlier page.
    @Test
    void decodesBetweenOffsetAndLimit() {
        byte[] vector = doubleVector(1, 0, 10, 4, new long[]{ 0, 1, 2 }, NO_POSITIONS, new long[0]);
        byte[] page = page(10, 3, vector);
        byte[] buffer = new byte[page.length + 40];
        new Random(7).nextBytes(buffer);
        System.arraycopy(page, 0, buffer, 5, page.length);
        double[] values = decodeDoubles(buffer, 5, 5 + page.length, 3);

        assertThat(values).containsExactly(1.0, 1.1, 1.2000000000000002);
    }

    /// Every DOUBLE bit width, with the last vector ending the page so its tail reads go through
    /// the padded copy. At (0, 0) a value is its encoded integer.
    @ParameterizedTest
    @ValueSource(ints = { 1, 7, 8, 9, 13, 31, 32, 33, 47, 56, 57, 63, 64 })
    void decodesDoublesAtBitWidth(int bitWidth) {
        Random random = new Random(bitWidth);
        long frame = -123_456_789L;
        long[] deltas = randomDeltas(random, 37, bitWidth);
        byte[] vector = doubleVector(0, 0, frame, bitWidth, deltas, NO_POSITIONS, new long[0]);

        double[] values = decodeDoubles(page(10, deltas.length, vector), deltas.length);

        for (int i = 0; i < deltas.length; i++) {
            assertThat(values[i]).as("value %d", i).isEqualTo((double) (frame + deltas[i]));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 7, 8, 9, 13, 24, 25, 31, 32 })
    void decodesFloatsAtBitWidth(int bitWidth) {
        Random random = new Random(bitWidth);
        int frame = -1_000_000;
        long[] deltas = randomDeltas(random, 37, bitWidth);
        byte[] vector = floatVector(0, 0, frame, bitWidth, deltas, NO_POSITIONS, new int[0]);

        float[] values = decodeFloats(page(10, deltas.length, vector), deltas.length);

        for (int i = 0; i < deltas.length; i++) {
            assertThat(values[i]).as("value %d", i).isEqualTo((float) (frame + (int) deltas[i]));
        }
    }

    @Test
    void refusesAPageShorterThanItsHeader() {
        assertThatThrownBy(() -> decodeDoubles(new byte[]{ 0, 0, 10, 1 }, 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading the ALP page header: expected 7 bytes, got 4");
    }

    @Test
    void refusesAnUnknownCompressionMode() {
        byte[] page = page(10, 0);
        page[0] = 1;

        assertThatThrownBy(() -> decodeDoubles(page, 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("ALP compression mode not yet supported: 1");
    }

    @Test
    void refusesAnUnknownIntegerEncoding() {
        byte[] page = page(10, 0);
        page[1] = 2;

        assertThatThrownBy(() -> decodeDoubles(page, 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("ALP integer encoding not yet supported: 2");
    }

    @ParameterizedTest
    @ValueSource(ints = { 2, 16, 255 })
    void refusesALogVectorSizeOutOfRange(int logVectorSize) {
        assertThatThrownBy(() -> decodeDoubles(page(logVectorSize, 0), 0))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid ALP log vector size: " + logVectorSize + ". Must be between 3 and 15");
    }

    @Test
    void refusesAnElementCountTheDefinitionLevelsDisagreeWith() {
        byte[] vector = doubleVector(0, 0, 1, 0, NO_DELTAS, NO_POSITIONS, new long[0]);

        assertThatThrownBy(() -> decodeDoubles(page(10, 2, vector), 3))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP page header counts 2 values, but the page has 3 non-null values");
    }

    @Test
    void refusesATruncatedOffsetArray() {
        byte[] page = page(3, 0);
        // 17 values in vectors of 8 make three vectors and a 12-byte offset array
        ByteBuffer.wrap(page).order(ByteOrder.LITTLE_ENDIAN).putInt(3, 17);

        assertThatThrownBy(() -> decodeDoubles(page, 17))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading the ALP offset array: expected 12 bytes, got 0");
    }

    @Test
    void refusesAVectorThatDoesNotStartWhereThePreviousEnded() {
        byte[] first = doubleVector(0, 0, 1, 0, NO_DELTAS, NO_POSITIONS, new long[0]);
        byte[] last = doubleVector(0, 0, 2, 0, NO_DELTAS, NO_POSITIONS, new long[0]);
        byte[] page = page(3, 9, first, last);
        // The second offset, which should be 8 + 13 = 21
        ByteBuffer.wrap(page).order(ByteOrder.LITTLE_ENDIAN).putInt(7 + 4, 22);

        assertThatThrownBy(() -> decodeDoubles(page, 9))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP vector 1 starts at offset 22, expected 21");
    }

    @Test
    void refusesATruncatedVectorHeader() {
        byte[] vector = doubleVector(0, 0, 1, 0, NO_DELTAS, NO_POSITIONS, new long[0]);
        byte[] page = page(10, 1, vector);
        byte[] truncated = new byte[page.length - 5];
        System.arraycopy(page, 0, truncated, 0, truncated.length);

        assertThatThrownBy(() -> decodeDoubles(truncated, 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading the header of ALP vector 0: expected 13 bytes, got 8");
    }

    @Test
    void refusesAFloatExponentAboveTen() {
        byte[] vector = floatVector(11, 0, 1, 0, NO_DELTAS, NO_POSITIONS, new int[0]);

        assertThatThrownBy(() -> decodeFloats(page(10, 1, vector), 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid ALP exponent in vector 0: 11. Must be between 0 and 10");
    }

    @Test
    void refusesADoubleExponentAboveEighteen() {
        byte[] vector = doubleVector(19, 0, 1, 0, NO_DELTAS, NO_POSITIONS, new long[0]);

        assertThatThrownBy(() -> decodeDoubles(page(10, 1, vector), 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid ALP exponent in vector 0: 19. Must be between 0 and 18");
    }

    @Test
    void refusesAFactorAboveTheExponent() {
        byte[] vector = doubleVector(4, 5, 1, 0, NO_DELTAS, NO_POSITIONS, new long[0]);

        assertThatThrownBy(() -> decodeDoubles(page(10, 1, vector), 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid ALP factor in vector 0: 5. Must be between 0 and the exponent 4");
    }

    @Test
    void refusesAFloatBitWidthAboveThirtyTwo() {
        byte[] vector = floatVector(0, 0, 1, 33, new long[]{ 0 }, NO_POSITIONS, new int[0]);

        assertThatThrownBy(() -> decodeFloats(page(10, 1, vector), 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid ALP bit width in vector 0: 33. Must be between 0 and 32");
    }

    @Test
    void refusesMoreExceptionsThanValues() {
        long one = Double.doubleToRawLongBits(1.0);
        byte[] vector = doubleVector(0, 0, 1, 0, NO_DELTAS, new int[]{ 0, 0 }, new long[]{ one, one });

        assertThatThrownBy(() -> decodeDoubles(page(10, 1, vector), 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP vector 0 has 2 exceptions, more than its 1 values");
    }

    @Test
    void refusesATruncatedVector() {
        byte[] vector = doubleVector(0, 0, 1, 8, new long[]{ 1, 2, 3 }, NO_POSITIONS, new long[0]);
        byte[] page = page(10, 3, vector);
        byte[] truncated = new byte[page.length - 1];
        System.arraycopy(page, 0, truncated, 0, truncated.length);

        assertThatThrownBy(() -> decodeDoubles(truncated, 3))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading ALP vector 0: expected 16 bytes, got 15");
    }

    @Test
    void refusesAnExceptionPositionOutsideTheVector() {
        byte[] vector = doubleVector(0, 0, 1, 0, NO_DELTAS, new int[]{ 3 },
                new long[]{ Double.doubleToRawLongBits(1.0) });

        assertThatThrownBy(() -> decodeDoubles(page(10, 3, vector), 3))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP exception position 3 in vector 0 is outside its 3 values");
    }

    @Test
    void refusesBytesLeftOverAfterTheLastVector() {
        byte[] vector = doubleVector(0, 0, 1, 0, NO_DELTAS, NO_POSITIONS, new long[0]);
        byte[] page = page(10, 1, vector);
        byte[] padded = new byte[page.length + 2];
        System.arraycopy(page, 0, padded, 0, page.length);

        assertThatThrownBy(() -> decodeDoubles(padded, 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP value section does not end with its 1 vectors; leftover bytes: 2");
    }

    private static double[] decodeDoubles(byte[] page, int numValues) {
        return decodeDoubles(page, 0, page.length, numValues);
    }

    /// Checks the page, then decodes it vector by vector, as an `ALP` page's reader does.
    private static double[] decodeDoubles(byte[] data, int offset, int limit, int numValues) {
        AlpDecoder decoder = new AlpDecoder(data, offset, limit, numValues, PhysicalType.DOUBLE);
        decoder.validate();
        double[] values = new double[numValues];
        double[] vector = new double[decoder.vectorSize()];
        for (int v = 0; v * decoder.vectorSize() < numValues; v++) {
            int count = decoder.decodeVector(v, null, vector);
            System.arraycopy(vector, 0, values, v * decoder.vectorSize(), count);
        }
        return values;
    }

    private static float[] decodeFloats(byte[] page, int numValues) {
        AlpDecoder decoder = new AlpDecoder(page, 0, page.length, numValues, PhysicalType.FLOAT);
        decoder.validate();
        float[] values = new float[numValues];
        float[] vector = new float[decoder.vectorSize()];
        for (int v = 0; v * decoder.vectorSize() < numValues; v++) {
            int count = decoder.decodeVector(v, vector, null);
            System.arraycopy(vector, 0, values, v * decoder.vectorSize(), count);
        }
        return values;
    }

    private static long[] randomDeltas(Random random, int count, int bitWidth) {
        long mask = bitWidth == Long.SIZE ? -1L : (1L << bitWidth) - 1;
        long[] deltas = new long[count];
        for (int i = 0; i < count; i++) {
            deltas[i] = random.nextLong() & mask;
        }
        // The widest delta, so the packing really needs every bit
        deltas[count / 2] = mask;
        return deltas;
    }

    /// Page header, offset array and vectors, the offsets derived from the vector sizes.
    private static byte[] page(int logVectorSize, int numElements, byte[]... vectors) {
        int offsetsSize = vectors.length * Integer.BYTES;
        int bodySize = offsetsSize;
        for (byte[] vector : vectors) {
            bodySize += vector.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(7 + bodySize).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0).put((byte) 0).put((byte) logVectorSize).putInt(numElements);
        int offset = offsetsSize;
        for (byte[] vector : vectors) {
            buffer.putInt(offset);
            offset += vector.length;
        }
        for (byte[] vector : vectors) {
            buffer.put(vector);
        }
        return buffer.array();
    }

    private static byte[] doubleVector(int exponent, int factor, long frame, int bitWidth, long[] deltas,
            int[] exceptionPositions, long[] exceptionBits) {
        ByteBuffer forInfo = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN).putLong(frame).put((byte) bitWidth);
        ByteBuffer values = ByteBuffer.allocate(exceptionBits.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (long bits : exceptionBits) {
            values.putLong(bits);
        }
        return vector(exponent, factor, forInfo.array(), bitWidth, deltas, exceptionPositions, values.array());
    }

    private static byte[] floatVector(int exponent, int factor, int frame, int bitWidth, long[] deltas,
            int[] exceptionPositions, int[] exceptionBits) {
        ByteBuffer forInfo = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN).putInt(frame).put((byte) bitWidth);
        ByteBuffer values = ByteBuffer.allocate(exceptionBits.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int bits : exceptionBits) {
            values.putInt(bits);
        }
        return vector(exponent, factor, forInfo.array(), bitWidth, deltas, exceptionPositions, values.array());
    }

    private static byte[] vector(int exponent, int factor, byte[] forInfo, int bitWidth, long[] deltas,
            int[] exceptionPositions, byte[] exceptionValues) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer alpInfo = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) exponent).put((byte) factor).putShort((short) exceptionPositions.length);
        out.writeBytes(alpInfo.array());
        out.writeBytes(forInfo);
        out.writeBytes(pack(deltas, bitWidth));
        ByteBuffer positions = ByteBuffer.allocate(exceptionPositions.length * Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int position : exceptionPositions) {
            positions.putShort((short) position);
        }
        out.writeBytes(positions.array());
        out.writeBytes(exceptionValues);
        return out.toByteArray();
    }

    /// LSB-first bit packing, one bit at a time.
    private static byte[] pack(long[] deltas, int bitWidth) {
        byte[] packed = new byte[(deltas.length * bitWidth + 7) / 8];
        int bitPos = 0;
        for (long delta : deltas) {
            for (int bit = 0; bit < bitWidth; bit++) {
                if (((delta >>> bit) & 1) != 0) {
                    packed[bitPos >>> 3] |= (byte) (1 << (bitPos & 7));
                }
                bitPos++;
            }
        }
        return packed;
    }
}
