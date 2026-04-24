/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.function.ToIntFunction;

import org.junit.jupiter.api.Test;

import dev.hardwood.row.VariantType;

import static org.assertj.core.api.Assertions.assertThat;

/// Round-trip coverage for [VariantValueEncoder] — writes each primitive /
/// short-string / object / array kind the encoder emits, then verifies that
/// [VariantValueDecoder] reads the same bytes back to the expected type and
/// value. Catches encoder/decoder disagreements independently of any Parquet
/// fixture.
class VariantValueEncoderTest {

    @Test
    void nullRoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeNull(buf, 0));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.NULL);
    }

    @Test
    void booleanTrueRoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeBoolean(buf, 0, true));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.BOOLEAN_TRUE);
        assertThat(VariantValueDecoder.asBoolean(bytes, 0)).isTrue();
    }

    @Test
    void booleanFalseRoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeBoolean(buf, 0, false));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.BOOLEAN_FALSE);
        assertThat(VariantValueDecoder.asBoolean(bytes, 0)).isFalse();
    }

    @Test
    void int8RoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeInt8(buf, 0, -17));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.INT8);
        assertThat(VariantValueDecoder.asInt(bytes, 0)).isEqualTo(-17);
    }

    @Test
    void int16RoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeInt16(buf, 0, -12345));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.INT16);
        assertThat(VariantValueDecoder.asInt(bytes, 0)).isEqualTo(-12345);
    }

    @Test
    void int32RoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeInt32(buf, 0, Integer.MIN_VALUE));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.INT32);
        assertThat(VariantValueDecoder.asInt(bytes, 0)).isEqualTo(Integer.MIN_VALUE);
    }

    @Test
    void int64RoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeInt64(buf, 0, Long.MAX_VALUE));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.INT64);
        assertThat(VariantValueDecoder.asLong(bytes, 0)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void floatRoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeFloat(buf, 0, 3.14159f));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.FLOAT);
        assertThat(VariantValueDecoder.asFloat(bytes, 0)).isEqualTo(3.14159f);
    }

    @Test
    void doubleRoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeDouble(buf, 0, -2.718281828));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.DOUBLE);
        assertThat(VariantValueDecoder.asDouble(bytes, 0)).isEqualTo(-2.718281828);
    }

    @Test
    void dateRoundTrips() {
        LocalDate date = LocalDate.of(2025, 4, 16);
        byte[] bytes = write(buf -> VariantValueEncoder.writeDate(buf, 0, (int) date.toEpochDay()));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.DATE);
        assertThat(VariantValueDecoder.asDate(bytes, 0)).isEqualTo(date);
    }

    @Test
    void timestampMicrosUtcRoundTrips() {
        Instant ts = Instant.parse("2025-04-16T12:34:56.780Z");
        long micros = ts.getEpochSecond() * 1_000_000L + ts.getNano() / 1_000L;
        byte[] bytes = write(buf -> VariantValueEncoder.writeTimestampMicros(buf, 0, micros, true));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.TIMESTAMP);
        assertThat(VariantValueDecoder.asTimestamp(bytes, 0)).isEqualTo(ts);
    }

    @Test
    void timestampMicrosNtzRoundTrips() {
        long micros = 1_700_000_000_000_000L;
        byte[] bytes = write(buf -> VariantValueEncoder.writeTimestampMicros(buf, 0, micros, false));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.TIMESTAMP_NTZ);
    }

    @Test
    void timestampNanosRoundTrips() {
        long nanos = 1_700_000_000_123_456_789L;
        byte[] bytes = write(buf -> VariantValueEncoder.writeTimestampNanos(buf, 0, nanos, true));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.TIMESTAMP_NANOS);
        assertThat(VariantValueDecoder.asTimestamp(bytes, 0))
                .isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 123_456_789L));
    }

    @Test
    void timeNtzRoundTrips() {
        LocalTime t = LocalTime.of(12, 33, 54, 123_456_000);
        byte[] bytes = write(buf -> VariantValueEncoder.writeTimeMicros(buf, 0, t.toNanoOfDay() / 1_000L));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.TIME_NTZ);
        assertThat(VariantValueDecoder.asTime(bytes, 0)).isEqualTo(t);
    }

    @Test
    void uuidRoundTrips() {
        UUID uuid = UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56");
        byte[] bytes = write(buf -> VariantValueEncoder.writeUuid(buf, 0, uuid));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.UUID);
        assertThat(VariantValueDecoder.asUuid(bytes, 0)).isEqualTo(uuid);
    }

    @Test
    void decimal4RoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeDecimal4(buf, 0, 1234, 2));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.DECIMAL4);
        assertThat(VariantValueDecoder.asDecimal(bytes, 0)).isEqualByComparingTo("12.34");
    }

    @Test
    void decimal8RoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeDecimal8(buf, 0, 123456789L, 1));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.DECIMAL8);
        assertThat(VariantValueDecoder.asDecimal(bytes, 0)).isEqualByComparingTo("12345678.9");
    }

    @Test
    void decimal16RoundTrips() {
        BigInteger unscaled = new BigInteger("123456789012345678901234567890");
        byte[] bytes = write(buf -> VariantValueEncoder.writeDecimal16(buf, 0, unscaled, 10));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.DECIMAL16);
        assertThat(VariantValueDecoder.asDecimal(bytes, 0).unscaledValue()).isEqualTo(unscaled);
    }

    @Test
    void shortStringRoundTrips() {
        byte[] bytes = write(buf -> VariantValueEncoder.writeString(buf, 0, "hi"));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.STRING);
        assertThat(VariantValueDecoder.asString(bytes, 0)).isEqualTo("hi");
        assertThat(bytes.length).isEqualTo(3); // 1 byte header + 2 bytes payload — short-string path
    }

    @Test
    void longStringRoundTrips() {
        // 64+ bytes triggers the long-string primitive encoding (length prefix).
        String s = "a".repeat(100);
        byte[] bytes = write(buf -> VariantValueEncoder.writeString(buf, 0, s));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.STRING);
        assertThat(VariantValueDecoder.asString(bytes, 0)).isEqualTo(s);
    }

    @Test
    void stringAtShortLongBoundary() {
        // Spec: short-string header encodes length in 6 bits, so length 0..63
        // uses the short-string form (1-byte header); length 64+ flips to the
        // long-string primitive (5-byte header: 1 tag + 4 LE length).
        String sixtyThree = "a".repeat(63);
        byte[] shortBytes = write(buf -> VariantValueEncoder.writeString(buf, 0, sixtyThree));
        assertThat(VariantValueDecoder.type(shortBytes, 0)).isEqualTo(VariantType.STRING);
        assertThat(VariantValueDecoder.asString(shortBytes, 0)).isEqualTo(sixtyThree);
        assertThat(shortBytes.length).isEqualTo(64); // 1 header + 63 payload

        String sixtyFour = "a".repeat(64);
        byte[] longBytes = write(buf -> VariantValueEncoder.writeString(buf, 0, sixtyFour));
        assertThat(VariantValueDecoder.type(longBytes, 0)).isEqualTo(VariantType.STRING);
        assertThat(VariantValueDecoder.asString(longBytes, 0)).isEqualTo(sixtyFour);
        assertThat(longBytes.length).isEqualTo(69); // 1 tag + 4 length + 64 payload
    }

    @Test
    void binaryRoundTrips() {
        byte[] payload = { 0x01, 0x02, 0x03, 0x04 };
        byte[] bytes = write(buf -> VariantValueEncoder.writeBinary(buf, 0, payload));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.BINARY);
        assertThat(VariantValueDecoder.asBinary(bytes, 0)).containsExactly(payload);
    }

    @Test
    void arrayRoundTrips() {
        // Build an array of two INT8 values: [42, -3]
        byte[] elem0 = new byte[] { (byte) 0x0C, 42 };       // INT8 42
        byte[] elem1 = new byte[] { (byte) 0x0C, (byte) 0xFD }; // INT8 -3
        byte[] bytes = write(buf -> VariantValueEncoder.writeArray(buf, 0, new byte[][] { elem0, elem1 }));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.ARRAY);

        VariantValueDecoder.ArrayLayout layout = VariantValueDecoder.parseArray(bytes, 0);
        assertThat(layout.numElements()).isEqualTo(2);
        int off0 = VariantValueDecoder.arrayElementOffset(bytes, layout, 0);
        int off1 = VariantValueDecoder.arrayElementOffset(bytes, layout, 1);
        assertThat(VariantValueDecoder.asInt(bytes, off0)).isEqualTo(42);
        assertThat(VariantValueDecoder.asInt(bytes, off1)).isEqualTo(-3);
    }

    @Test
    void objectRoundTrips() {
        // Object with two fields, ids 0 and 1, each carrying an INT8 payload.
        byte[] val0 = { (byte) 0x0C, 7 };       // INT8 7
        byte[] val1 = { (byte) 0x0C, (byte) 0xFF }; // INT8 -1
        int[] ids = { 0, 1 };
        byte[][] values = { val0, val1 };
        byte[] bytes = write(buf -> VariantValueEncoder.writeObject(buf, 0, ids, values, 1));
        assertThat(VariantValueDecoder.type(bytes, 0)).isEqualTo(VariantType.OBJECT);

        VariantValueDecoder.ObjectLayout layout = VariantValueDecoder.parseObject(bytes, 0);
        assertThat(layout.numElements()).isEqualTo(2);
        assertThat(VariantValueDecoder.objectFieldId(bytes, layout, 0)).isEqualTo(0);
        assertThat(VariantValueDecoder.objectFieldId(bytes, layout, 1)).isEqualTo(1);
    }

    /// Emit via `writer` into a fresh 256-byte scratch, trim to the actual
    /// length via the decoder's valueLength helper, and return the exact bytes.
    private static byte[] write(ToIntFunction<byte[]> writer) {
        byte[] scratch = new byte[256];
        int end = writer.applyAsInt(scratch);
        int length = VariantValueDecoder.valueLength(scratch, 0);
        assertThat(length).isEqualTo(end);
        byte[] out = new byte[length];
        System.arraycopy(scratch, 0, out, 0, length);
        return out;
    }
}
