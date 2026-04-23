/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.VariantType;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies the Variant binary decoder against the canonical primitive/object/array
/// fixtures from `apache/parquet-testing`. Each `<name>.metadata` + `<name>.value`
/// pair encodes a single Variant value whose expected Java representation is
/// taken from `data_dictionary.json`. Fixtures are checked into
/// `core/src/test/resources/variant/` so this test runs in any clean build.
class VariantValueDecoderTest {

    private static PqVariant load(String caseName) throws IOException {
        byte[] metadata = readResource("/variant/" + caseName + ".metadata");
        byte[] value = readResource("/variant/" + caseName + ".value");
        return new PqVariantImpl(metadata, value);
    }

    private static byte[] readResource(String name) throws IOException {
        try (InputStream in = VariantValueDecoderTest.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("Missing test resource: " + name);
            }
            return in.readAllBytes();
        }
    }

    // ==================== Primitives ====================

    @Test
    void primitiveNull() throws IOException {
        PqVariant v = load("primitive_null");
        assertThat(v.type()).isEqualTo(VariantType.NULL);
        assertThat(v.isNull()).isTrue();
    }

    @Test
    void primitiveBooleanTrue() throws IOException {
        PqVariant v = load("primitive_boolean_true");
        assertThat(v.type()).isEqualTo(VariantType.BOOLEAN_TRUE);
        assertThat(v.asBoolean()).isTrue();
    }

    @Test
    void primitiveBooleanFalse() throws IOException {
        PqVariant v = load("primitive_boolean_false");
        assertThat(v.type()).isEqualTo(VariantType.BOOLEAN_FALSE);
        assertThat(v.asBoolean()).isFalse();
    }

    @Test
    void primitiveInt8() throws IOException {
        PqVariant v = load("primitive_int8");
        assertThat(v.type()).isEqualTo(VariantType.INT8);
        assertThat(v.asInt()).isEqualTo(42);
    }

    @Test
    void primitiveInt16() throws IOException {
        PqVariant v = load("primitive_int16");
        assertThat(v.type()).isEqualTo(VariantType.INT16);
        assertThat(v.asInt()).isEqualTo(1234);
    }

    @Test
    void primitiveInt32() throws IOException {
        PqVariant v = load("primitive_int32");
        assertThat(v.type()).isEqualTo(VariantType.INT32);
        assertThat(v.asInt()).isEqualTo(123_456);
    }

    @Test
    void primitiveInt64() throws IOException {
        PqVariant v = load("primitive_int64");
        assertThat(v.type()).isEqualTo(VariantType.INT64);
        assertThat(v.asLong()).isEqualTo(1_234_567_890_123_456_789L);
    }

    @Test
    void primitiveDouble() throws IOException {
        PqVariant v = load("primitive_double");
        assertThat(v.type()).isEqualTo(VariantType.DOUBLE);
        assertThat(v.asDouble()).isEqualTo(1234567890.1234);
    }

    @Test
    void primitiveFloat() throws IOException {
        PqVariant v = load("primitive_float");
        assertThat(v.type()).isEqualTo(VariantType.FLOAT);
        assertThat(v.asFloat()).isEqualTo(1234567940.0f);
    }

    @Test
    void primitiveString() throws IOException {
        PqVariant v = load("primitive_string");
        assertThat(v.type()).isEqualTo(VariantType.STRING);
        assertThat(v.asString()).startsWith("This string is longer than 64 bytes");
    }

    @Test
    void shortString() throws IOException {
        PqVariant v = load("short_string");
        assertThat(v.type()).isEqualTo(VariantType.STRING);
        assertThat(v.asString()).isEqualTo("Less than 64 bytes (❤️ with utf8)");
    }

    @Test
    void primitiveDate() throws IOException {
        PqVariant v = load("primitive_date");
        assertThat(v.type()).isEqualTo(VariantType.DATE);
        assertThat(v.asDate()).isEqualTo(LocalDate.of(2025, 4, 16));
    }

    @Test
    void primitiveBinary() throws IOException {
        PqVariant v = load("primitive_binary");
        assertThat(v.type()).isEqualTo(VariantType.BINARY);
        byte[] expected = Base64.getDecoder().decode("AxM33q2+78r+");
        assertThat(v.asBinary()).isEqualTo(expected);
    }

    @Test
    void primitiveDecimal4() throws IOException {
        PqVariant v = load("primitive_decimal4");
        assertThat(v.type()).isEqualTo(VariantType.DECIMAL4);
        assertThat(v.asDecimal()).isEqualTo(new BigDecimal("12.34"));
    }

    @Test
    void primitiveDecimal8() throws IOException {
        PqVariant v = load("primitive_decimal8");
        assertThat(v.type()).isEqualTo(VariantType.DECIMAL8);
        // BigDecimal.equals is scale-sensitive; compareTo matches on numeric value.
        assertThat(v.asDecimal()).isEqualByComparingTo(new BigDecimal("12345678.9"));
    }

    @Test
    void primitiveDecimal16() throws IOException {
        PqVariant v = load("primitive_decimal16");
        assertThat(v.type()).isEqualTo(VariantType.DECIMAL16);
        // data_dictionary.json renders the value as 1.2345678912345678e+16; the
        // Variant stores exact unscaled + scale, so compare BigDecimals.
        assertThat(v.asDecimal()).isNotNull();
        assertThat(v.asDecimal().doubleValue()).isEqualTo(1.2345678912345678e+16);
    }

    @Test
    void primitiveUuid() throws IOException {
        PqVariant v = load("primitive_uuid");
        assertThat(v.type()).isEqualTo(VariantType.UUID);
        assertThat(v.asUuid()).isEqualTo(UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"));
    }

    @Test
    void primitiveTimestamp() throws IOException {
        PqVariant v = load("primitive_timestamp");
        assertThat(v.type()).isEqualTo(VariantType.TIMESTAMP);
        // "2025-04-16 12:34:56.78-04:00" => 2025-04-16T16:34:56.780Z
        assertThat(v.asTimestamp()).isEqualTo(Instant.parse("2025-04-16T16:34:56.780Z"));
    }

    @Test
    void primitiveTimestampNtz() throws IOException {
        PqVariant v = load("primitive_timestampntz");
        assertThat(v.type()).isEqualTo(VariantType.TIMESTAMP_NTZ);
        // "2025-04-16 12:34:56.78" — no timezone; decoded as UTC micros.
        assertThat(v.asTimestamp()).isEqualTo(Instant.parse("2025-04-16T12:34:56.780Z"));
    }

    @Test
    void primitiveTime() throws IOException {
        PqVariant v = load("primitive_time");
        assertThat(v.type()).isEqualTo(VariantType.TIME_NTZ);
        // "12:33:54:123456" (micros). LocalTime comparison avoids nanosecond fuzz.
        LocalTime expected = LocalTime.of(12, 33, 54, 123_456_000);
        assertThat(v.asTime()).isEqualTo(expected);
    }

    // ==================== Objects ====================

    @Test
    void objectPrimitive() throws IOException {
        PqVariant v = load("object_primitive");
        assertThat(v.type()).isEqualTo(VariantType.OBJECT);
        PqVariantObject obj = v.asObject();
        assertThat(obj.getBoolean("boolean_false_field")).isFalse();
        assertThat(obj.getBoolean("boolean_true_field")).isTrue();
        // The writer chose DECIMAL4 for this value since it fits; accept either
        // tag to keep the test tolerant of encoding choices.
        PqVariant doubleVar = obj.getVariant("double_field");
        double doubleVal = doubleVar.type() == VariantType.DOUBLE
                ? doubleVar.asDouble()
                : doubleVar.asDecimal().doubleValue();
        assertThat(doubleVal).isEqualTo(1.23456789);
        assertThat(obj.getInt("int_field")).isEqualTo(1);
        assertThat(obj.getString("string_field")).isEqualTo("Apache Parquet");
        assertThat(obj.isNull("null_field")).isTrue();
    }

    @Test
    void objectEmpty() throws IOException {
        PqVariant v = load("object_empty");
        assertThat(v.type()).isEqualTo(VariantType.OBJECT);
        assertThat(v.asObject().getFieldCount()).isZero();
    }

    @Test
    void objectNested() throws IOException {
        PqVariant v = load("object_nested");
        PqVariantObject obj = v.asObject();
        assertThat(obj.getInt("id")).isEqualTo(1);
        PqVariantObject observation = obj.getObject("observation");
        assertThat(observation.getString("location")).isEqualTo("In the Volcano");
        PqVariantObject value = observation.getObject("value");
        assertThat(value.getInt("humidity")).isEqualTo(456);
        assertThat(value.getInt("temperature")).isEqualTo(123);
    }

    // ==================== Arrays ====================

    @Test
    void arrayEmpty() throws IOException {
        PqVariant v = load("array_empty");
        assertThat(v.type()).isEqualTo(VariantType.ARRAY);
        assertThat(v.asArray().size()).isZero();
    }

    @Test
    void arrayPrimitive() throws IOException {
        PqVariant v = load("array_primitive");
        PqVariantArray arr = v.asArray();
        assertThat(arr.size()).isEqualTo(4);
        assertThat(arr.get(0).asInt()).isEqualTo(2);
        assertThat(arr.get(1).asInt()).isEqualTo(1);
        assertThat(arr.get(2).asInt()).isEqualTo(5);
        assertThat(arr.get(3).asInt()).isEqualTo(9);
    }

    @Test
    void arrayNested() throws IOException {
        PqVariant v = load("array_nested");
        PqVariantArray arr = v.asArray();
        assertThat(arr.size()).isEqualTo(3);
        // Element 0: object with id=1, thing.names=[...]
        PqVariantObject obj0 = arr.get(0).asObject();
        assertThat(obj0.getInt("id")).isEqualTo(1);
        PqVariantArray thingNames = obj0.getObject("thing").getArray("names");
        assertThat(thingNames.size()).isEqualTo(2);
        assertThat(thingNames.get(0).asString()).isEqualTo("Contrarian");
        assertThat(thingNames.get(1).asString()).isEqualTo("Spider");
        // Element 1 is SQL null.
        assertThat(arr.get(1).isNull()).isTrue();
        // Element 2: object with id=2, names=[...], type="if"
        PqVariantObject obj2 = arr.get(2).asObject();
        assertThat(obj2.getInt("id")).isEqualTo(2);
        assertThat(obj2.getString("type")).isEqualTo("if");
        PqVariantArray names = obj2.getArray("names");
        assertThat(names.get(0).asString()).isEqualTo("Apple");
        assertThat(names.get(1).asString()).isEqualTo("Ray");
        assertThat(names.get(2).isNull()).isTrue();
    }
}
