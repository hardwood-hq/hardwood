/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.TestDecodeBuffers;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.encoding.AlpEncoder;
import dev.hardwood.internal.encoding.ByteStreamSplitEncoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedEncoder;
import dev.hardwood.internal.encoding.DeltaByteArrayEncoder;
import dev.hardwood.internal.encoding.DeltaLengthByteArrayEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The in-place readers against [ReferencePageDecoder], over every encoding each physical type
/// takes, three column shapes (required, optional with nulls, a list's elements with null and
/// empty lists and null elements) and four page forms: `V1` uncompressed and Snappy-compressed,
/// `V2` with its values compressed and `V2` with its values stored under a codec. The uncompressed
/// `V1` page and the stored `V2` page are also read from a direct buffer at an odd address, as a
/// page whose values are read where they lie in a file's mapping, as is a `V2` page flagged
/// compressed in a chunk with codec `UNCOMPRESSED`, as parquet-java writes one. Each page is read from its first entry and through views starting at
/// every entry, as a fill that starts mid-page reads it, and a flat column also through the
/// nested entry point, as a struct leaf is read.
class InPlaceReaderOracleTest {

    private static final int FLBA_WIDTH = 3;

    enum Shape {
        REQUIRED, OPTIONAL, REPEATED
    }

    enum Form {
        V1, V1_DIRECT, V1_SNAPPY, V2_SNAPPY, V2_STORED, V2_STORED_DIRECT, V2_UNCOMPRESSED_DIRECT
    }

    static Stream<Arguments> cases() {
        List<Arguments> cases = new ArrayList<>();
        Object[][] encodings = {
                { PhysicalType.BOOLEAN, Encoding.PLAIN },
                { PhysicalType.BOOLEAN, Encoding.RLE },
                { PhysicalType.INT32, Encoding.PLAIN },
                { PhysicalType.INT32, Encoding.DELTA_BINARY_PACKED },
                { PhysicalType.INT32, Encoding.BYTE_STREAM_SPLIT },
                { PhysicalType.INT64, Encoding.DELTA_BINARY_PACKED },
                { PhysicalType.INT64, Encoding.RLE_DICTIONARY },
                { PhysicalType.FLOAT, Encoding.ALP },
                { PhysicalType.FLOAT, Encoding.BYTE_STREAM_SPLIT },
                { PhysicalType.DOUBLE, Encoding.ALP },
                { PhysicalType.DOUBLE, Encoding.BYTE_STREAM_SPLIT },
                { PhysicalType.BYTE_ARRAY, Encoding.PLAIN },
                { PhysicalType.BYTE_ARRAY, Encoding.DELTA_LENGTH_BYTE_ARRAY },
                { PhysicalType.BYTE_ARRAY, Encoding.DELTA_BYTE_ARRAY },
                { PhysicalType.BYTE_ARRAY, Encoding.RLE_DICTIONARY },
                { PhysicalType.FIXED_LEN_BYTE_ARRAY, Encoding.PLAIN },
                { PhysicalType.FIXED_LEN_BYTE_ARRAY, Encoding.BYTE_STREAM_SPLIT },
                { PhysicalType.FIXED_LEN_BYTE_ARRAY, Encoding.DELTA_BYTE_ARRAY },
                { PhysicalType.FIXED_LEN_BYTE_ARRAY, Encoding.RLE_DICTIONARY },
                { PhysicalType.INT96, Encoding.PLAIN },
                { PhysicalType.INT96, Encoding.RLE_DICTIONARY },
        };
        for (Object[] encoding : encodings) {
            for (Shape shape : Shape.values()) {
                for (Form form : Form.values()) {
                    cases.add(Arguments.of(encoding[0], encoding[1], shape, form));
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} {1} {2} {3}")
    @MethodSource("cases")
    void readsAsTheReferenceDecodes(PhysicalType type, Encoding encoding, Shape shape, Form form) {
        ColumnSchema column = column(type, shape);
        Levels levels = levels(shape);
        int nonNull = levels.nonNull(column.maxDefinitionLevel());
        Dictionary dictionary = encoding == Encoding.RLE_DICTIONARY ? dictionary(type) : null;
        byte[] values = encode(type, encoding, nonNull);
        CompressionCodec codec = form == Form.V1 || form == Form.V1_DIRECT || form == Form.V2_UNCOMPRESSED_DIRECT
                ? CompressionCodec.UNCOMPRESSED
                : CompressionCodec.SNAPPY;
        ByteBuffer page = page(form, encoding, levels, column, values, codec);

        Object expected = new ReferencePageDecoder(column, codec, new DecompressorFactory(null))
                .decode(page.duplicate(), dictionary).values();
        PageDecoder decoder = DataPages.decoder(column, codec);

        assertThat(DataPages.read(decoder, column, page.duplicate(), dictionary)).isEqualTo(expected);
        PageDecoder.InPlaceNestedPage nested = decoder.decodeNestedInPlace(page.duplicate(), dictionary,
                new PageDecoder.LevelScratch(), TestDecodeBuffers.create());
        int size = levels.size();
        for (int from = 0; from < size; from++) {
            Object view = nested.flat() != null
                    ? DataPages.values(nested.flat(), type, from)
                    : DataPages.values(nested.binary(), column.maxDefinitionLevel(), from);
            assertThat(view).as("view from entry %d", from).isEqualTo(tail(expected, from));
        }
    }

    private static Object tail(Object values, int from) {
        int length = Array.getLength(values) - from;
        Object tail = Array.newInstance(values.getClass().getComponentType(), length);
        System.arraycopy(values, from, tail, 0, length);
        return tail;
    }

    private static ColumnSchema column(PhysicalType type, Shape shape) {
        Integer typeLength = type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? FLBA_WIDTH : null;
        return switch (shape) {
            case REQUIRED -> DataPages.required(type, typeLength);
            case OPTIONAL -> DataPages.optional(type, typeLength);
            case REPEATED -> DataPages.repeated(type, typeLength);
        };
    }

    /// A page's levels: `null` arrays for a column without them.
    private record Levels(int[] repetition, int[] definition) {

        int size() {
            return definition != null ? definition.length : 70;
        }

        int nonNull(int maxDefinitionLevel) {
            if (definition == null) {
                return size();
            }
            int count = 0;
            for (int level : definition) {
                count += level == maxDefinitionLevel ? 1 : 0;
            }
            return count;
        }
    }

    /// For a list's elements: records alternating between a null list, an empty list and lists of
    /// one to four elements, every fourth element null.
    private static Levels levels(Shape shape) {
        return switch (shape) {
            case REQUIRED -> new Levels(null, null);
            case OPTIONAL -> {
                int[] definition = new int[70];
                for (int i = 0; i < definition.length; i++) {
                    definition[i] = i % 4 == 1 || i % 9 == 0 ? 0 : 1;
                }
                yield new Levels(null, definition);
            }
            case REPEATED -> {
                List<int[]> entries = new ArrayList<>();
                int element = 0;
                for (int r = 0; r < 30; r++) {
                    switch (r % 6) {
                        case 1 -> entries.add(new int[]{ 0, 0 });
                        case 4 -> entries.add(new int[]{ 0, 1 });
                        default -> {
                            for (int k = 0; k <= r % 4; k++) {
                                entries.add(new int[]{ k == 0 ? 0 : 1, element++ % 4 == 3 ? 2 : 3 });
                            }
                        }
                    }
                }
                int[] repetition = new int[entries.size()];
                int[] definition = new int[entries.size()];
                for (int i = 0; i < entries.size(); i++) {
                    repetition[i] = entries.get(i)[0];
                    definition[i] = entries.get(i)[1];
                }
                yield new Levels(repetition, definition);
            }
        };
    }

    /// The `nonNull` values of `type` in `encoding`.
    private static byte[] encode(PhysicalType type, Encoding encoding, int nonNull) {
        return switch (encoding) {
            case PLAIN -> plain(type, nonNull);
            case RLE -> {
                RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(1);
                for (int i = 0; i < nonNull; i++) {
                    encoder.writeInt(booleanAt(i) ? 1 : 0);
                }
                byte[] bits = encoder.toByteArray();
                yield DataPages.concat(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(bits.length).array(),
                        bits);
            }
            case DELTA_BINARY_PACKED -> type == PhysicalType.INT32
                    ? DeltaBinaryPackedEncoder.encodeInts(ints(nonNull), 0, nonNull)
                    : DeltaBinaryPackedEncoder.encodeLongs(longs(nonNull), 0, nonNull);
            case BYTE_STREAM_SPLIT -> ByteStreamSplitEncoder.encode(plain(type, nonNull), 0, nonNull, width(type));
            case ALP -> {
                AlpEncoder encoder = new AlpEncoder(type == PhysicalType.DOUBLE ? Double.BYTES : Float.BYTES);
                int length;
                if (type == PhysicalType.DOUBLE) {
                    double[] values = doubles(nonNull);
                    length = encoder.encodeDoubles(values, 0, nonNull, AlpEncoder.presetDoubles(values, 0, nonNull));
                }
                else {
                    float[] values = floats(nonNull);
                    length = encoder.encodeFloats(values, 0, nonNull, AlpEncoder.presetFloats(values, 0, nonNull));
                }
                yield Arrays.copyOf(encoder.buffer(), length);
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                Packed packed = packed(type, nonNull);
                yield DeltaLengthByteArrayEncoder.encode(packed.bytes, packed.offsets, 0, nonNull);
            }
            case DELTA_BYTE_ARRAY -> {
                Packed packed = packed(type, nonNull);
                yield DeltaByteArrayEncoder.encode(packed.bytes, packed.offsets, 0, nonNull);
            }
            case RLE_DICTIONARY -> {
                RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(3);
                for (int i = 0; i < nonNull; i++) {
                    encoder.writeInt(i * 5 % 7);
                }
                yield DataPages.concat(new byte[]{ 3 }, encoder.toByteArray());
            }
            default -> throw new IllegalArgumentException("Not encoded here: " + encoding);
        };
    }

    private static Dictionary dictionary(PhysicalType type) {
        byte[] plain = plain(type, 7);
        return Dictionary.parse(plain, plain.length, 7, type,
                type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? FLBA_WIDTH : null);
    }

    private static byte[] plain(PhysicalType type, int count) {
        return switch (type) {
            case BOOLEAN -> {
                boolean[] values = new boolean[count];
                for (int i = 0; i < count; i++) {
                    values[i] = booleanAt(i);
                }
                yield PlainEncoder.encodeBooleans(values, 0, count);
            }
            case INT32 -> PlainEncoder.encodeInts(ints(count), 0, count);
            case INT64 -> PlainEncoder.encodeLongs(longs(count), 0, count);
            case FLOAT -> PlainEncoder.encodeFloats(floats(count), 0, count);
            case DOUBLE -> PlainEncoder.encodeDoubles(doubles(count), 0, count);
            case BYTE_ARRAY -> {
                Packed packed = packed(type, count);
                yield PlainEncoder.encodeByteArrays(packed.bytes, packed.offsets, 0, count);
            }
            case FIXED_LEN_BYTE_ARRAY, INT96 -> packed(type, count).bytes;
        };
    }

    private static int width(PhysicalType type) {
        return switch (type) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case FIXED_LEN_BYTE_ARRAY -> FLBA_WIDTH;
            default -> throw new IllegalArgumentException("Not fixed-width: " + type);
        };
    }

    private static boolean booleanAt(int i) {
        return i % 3 != 1;
    }

    private static int[] ints(int count) {
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = i * 41 - 900 + (i % 4) * 13;
        }
        return values;
    }

    private static long[] longs(int count) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = i * 3_000_017L - (i % 6) * 5L;
        }
        return values;
    }

    private static float[] floats(int count) {
        float[] values = new float[count];
        for (int i = 0; i < count; i++) {
            values[i] = i % 13 == 0 ? Float.NaN : (i % 40) * 0.5f;
        }
        return values;
    }

    private static double[] doubles(int count) {
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            values[i] = i % 17 == 0 ? 1.0 / 3 : (i % 60) * 0.25 + i;
        }
        return values;
    }

    /// Values end to end with their offsets: strings sharing prefixes for `BYTE_ARRAY`, the
    /// column's width otherwise.
    private record Packed(byte[] bytes, int[] offsets) {
    }

    private static Packed packed(PhysicalType type, int count) {
        int[] offsets = new int[count + 1];
        byte[][] values = new byte[count][];
        for (int i = 0; i < count; i++) {
            values[i] = switch (type) {
                case BYTE_ARRAY -> ("row-" + i / 4 + "-" + "x".repeat(i % 3)).getBytes(StandardCharsets.UTF_8);
                case FIXED_LEN_BYTE_ARRAY -> new byte[]{ (byte) (i / 4), (byte) i, 9 };
                default -> {
                    byte[] value = new byte[12];
                    value[0] = (byte) i;
                    value[11] = (byte) (i / 3);
                    yield value;
                }
            };
            offsets[i + 1] = offsets[i] + values[i].length;
        }
        byte[] bytes = new byte[offsets[count]];
        for (int i = 0; i < count; i++) {
            System.arraycopy(values[i], 0, bytes, offsets[i], values[i].length);
        }
        return new Packed(bytes, offsets);
    }

    private static ByteBuffer page(Form form, Encoding encoding, Levels levels, ColumnSchema column, byte[] values,
                                   CompressionCodec codec) {
        int size = levels.size();
        byte[] repetition = levels.repetition() != null
                ? DataPages.levels(column.maxRepetitionLevel(), levels.repetition())
                : new byte[0];
        byte[] definition = levels.definition() != null
                ? DataPages.levels(column.maxDefinitionLevel(), levels.definition())
                : new byte[0];
        return switch (form) {
            case V1, V1_DIRECT, V1_SNAPPY -> {
                byte[] body = DataPages.concat(
                        levels.repetition() != null ? DataPages.levelSection(column.maxRepetitionLevel(), levels.repetition()) : new byte[0],
                        levels.definition() != null ? DataPages.levelSection(column.maxDefinitionLevel(), levels.definition()) : new byte[0],
                        values);
                byte[] stored = form != Form.V1_SNAPPY ? body
                        : new CompressorFactory().getCompressor(codec).compress(body, 0, body.length);
                CRC32 crc = new CRC32();
                crc.update(stored);
                ThriftCompactWriter header = new ThriftCompactWriter();
                PageHeaderWriter.writeDataPageV1(header, size, body.length, stored.length, (int) crc.getValue(),
                        encoding);
                byte[] headerBytes = header.toByteArray();
                ByteBuffer heap = ByteBuffer.allocate(headerBytes.length + stored.length).put(headerBytes).put(stored)
                        .flip();
                yield form == Form.V1_DIRECT ? oddDirect(heap) : heap;
            }
            case V2_SNAPPY -> DataPages.pageV2(encoding, size, records(levels), repetition, definition, values, codec,
                    true);
            case V2_STORED -> DataPages.pageV2(encoding, size, records(levels), repetition, definition, values, codec,
                    false);
            case V2_STORED_DIRECT -> oddDirect(DataPages.pageV2(encoding, size, records(levels), repetition,
                    definition, values, codec, false));
            case V2_UNCOMPRESSED_DIRECT -> oddDirect(DataPages.pageV2(encoding, size, records(levels), repetition,
                    definition, values, codec, true));
        };
    }

    /// A copy of `heap` in a direct buffer that starts one byte past an aligned address, so that no
    /// value of the page is aligned to its width.
    private static ByteBuffer oddDirect(ByteBuffer heap) {
        ByteBuffer direct = ByteBuffer.allocateDirect(heap.remaining() + 1).position(1).slice();
        return direct.put(heap).flip();
    }

    private static int records(Levels levels) {
        if (levels.repetition() == null) {
            return levels.size();
        }
        int records = 0;
        for (int level : levels.repetition()) {
            records += level == 0 ? 1 : 0;
        }
        return records;
    }
}
