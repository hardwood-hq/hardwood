/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;

import static org.assertj.core.api.Assertions.assertThat;

/// A chunk recovered at flush interns its stored values again ([ValueEncoder#reinternStored]),
/// and one the exact comparison then rejects resolves them back out
/// ([ValueEncoder#storeDictionaryValue]). Both must preserve every value and its order, and the
/// dictionary must count distinct values as interning on arrival does: by raw bit pattern for
/// floating point, by content for binary.
class ValueEncoderReinternTest {

    private static final int COUNT = 5_000;

    static Stream<Arguments> sources() {
        int[] ints = new int[COUNT];
        long[] longs = new long[COUNT];
        float[] floats = new float[COUNT];
        double[] doubles = new double[COUNT];
        byte[][] strings = new byte[COUNT][];
        byte[][] fixed = new byte[COUNT][];
        for (int i = 0; i < COUNT; i++) {
            int key = i % 97;
            ints[i] = key * 3 - 100;
            longs[i] = key * 1_000_003L;
            // 0.0 and -0.0, and two NaN payloads, are distinct entries.
            floats[i] = switch (key) {
                case 0 -> 0.0f;
                case 1 -> -0.0f;
                case 2 -> Float.NaN;
                case 3 -> Float.intBitsToFloat(0x7fc00001);
                default -> key * 0.25f;
            };
            doubles[i] = switch (key) {
                case 0 -> 0.0;
                case 1 -> -0.0;
                case 2 -> Double.NaN;
                case 3 -> Double.longBitsToDouble(0x7ff8000000000001L);
                default -> key * 0.5;
            };
            strings[i] = ("value-" + key).getBytes(StandardCharsets.UTF_8);
            fixed[i] = new byte[] { 1, 2, (byte) key, (byte) (key >> 8) };
        }
        return Stream.of(
                Arguments.of(PhysicalType.INT32, new IntArrayColumnSource(ints)),
                Arguments.of(PhysicalType.INT64, new LongArrayColumnSource(longs)),
                Arguments.of(PhysicalType.FLOAT, new FloatArrayColumnSource(floats)),
                Arguments.of(PhysicalType.DOUBLE, new DoubleArrayColumnSource(doubles)),
                Arguments.of(PhysicalType.BYTE_ARRAY, new BinaryArrayColumnSource(strings)),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, new BinaryArrayColumnSource(fixed)));
    }

    @ParameterizedTest
    @MethodSource("sources")
    void reinternThenResolveKeepsEveryValueInOrder(PhysicalType type, ColumnSource source) {
        ValueEncoder values = encoder(type);
        values.reset(source);
        values.storeRun(0, COUNT);
        byte[] plain = plain(values);

        int[] indices = new int[COUNT];
        values.reinternStored(indices);
        assertThat(values.dictionarySize()).isEqualTo(97);

        // Rejected at flush: resolved back into the store, as giveUpDictionary does. Every value
        // comes back where it was only if each index names the entry its value was interned as.
        for (int i = 0; i < COUNT; i++) {
            values.storeDictionaryValue(indices[i]);
        }
        values.dropDictionary();
        assertThat(plain(values)).isEqualTo(plain);
    }

    private static byte[] plain(ValueEncoder values) {
        ByteArrayBuilder out = new ByteArrayBuilder();
        values.encodeInto(out, ColumnEncoding.PLAIN, 0, COUNT);
        return Arrays.copyOf(out.array(), out.length());
    }

    private static ValueEncoder encoder(PhysicalType type) {
        FileSchema.Builder schema = FileSchema.builder("m");
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            schema.addColumn("v", type, RepetitionType.REQUIRED, 4);
        }
        else {
            schema.addColumn("v", type, RepetitionType.REQUIRED);
        }
        ColumnSchema column = schema.build().getColumn(0);
        return ValueEncoder.forColumn(column, ColumnEncoding.AUTO, 64, 1024);
    }
}
