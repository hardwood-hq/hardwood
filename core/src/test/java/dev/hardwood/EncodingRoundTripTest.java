/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// Every encoding a reader reads in place, over a flat, a repeated and a struct leaf with nulls,
/// written by this project's writer in pages of five records and read back whole and through a
/// filter that masks pages. The build also runs this class at batch sizes of 7 and 1, so batches
/// start inside pages and the readers' views skip into them.
class EncodingRoundTripTest {

    private static final int RECORDS = 300;
    private static final int FLBA_WIDTH = 3;

    static Stream<Arguments> encodings() {
        return Stream.of(
                Arguments.of(PhysicalType.INT32, ColumnEncoding.DELTA_BINARY_PACKED),
                Arguments.of(PhysicalType.INT64, ColumnEncoding.DELTA_BINARY_PACKED),
                Arguments.of(PhysicalType.INT32, ColumnEncoding.BYTE_STREAM_SPLIT),
                Arguments.of(PhysicalType.INT64, ColumnEncoding.BYTE_STREAM_SPLIT),
                Arguments.of(PhysicalType.FLOAT, ColumnEncoding.BYTE_STREAM_SPLIT),
                Arguments.of(PhysicalType.DOUBLE, ColumnEncoding.BYTE_STREAM_SPLIT),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, ColumnEncoding.BYTE_STREAM_SPLIT),
                Arguments.of(PhysicalType.FLOAT, ColumnEncoding.ALP),
                Arguments.of(PhysicalType.DOUBLE, ColumnEncoding.ALP),
                Arguments.of(PhysicalType.BYTE_ARRAY, ColumnEncoding.DELTA_LENGTH_BYTE_ARRAY),
                Arguments.of(PhysicalType.BYTE_ARRAY, ColumnEncoding.DELTA_BYTE_ARRAY),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, ColumnEncoding.DELTA_BYTE_ARRAY),
                Arguments.of(PhysicalType.BOOLEAN, ColumnEncoding.PLAIN),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, ColumnEncoding.PLAIN));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("encodings")
    void readsAnOptionalFlatColumn(PhysicalType type, ColumnEncoding encoding) throws Exception {
        List<Object> expected = new ArrayList<>();
        boolean[] nulls = new boolean[RECORDS];
        for (int r = 0; r < RECORDS; r++) {
            nulls[r] = r % 4 == 1;
            expected.add(nulls[r] ? null : value(type, r));
        }
        FileSchema schema = schema(builder -> addColumn(builder, "v", type));
        byte[] file = write(schema, encoding, batch -> values(batch, "v", type, RECORDS, nulls));

        assertRows(file, expected, rows -> rows.isNull("v") ? null : rows.getRawValue("v"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("encodings")
    void readsARepeatedColumn(PhysicalType type, ColumnEncoding encoding) throws Exception {
        List<Object> expected = new ArrayList<>();
        int[] offsets = new int[RECORDS + 1];
        boolean[] listNulls = new boolean[RECORDS];
        List<Boolean> elementNulls = new ArrayList<>();
        int element = 0;
        for (int r = 0; r < RECORDS; r++) {
            listNulls[r] = r % 7 == 3;
            if (listNulls[r]) {
                expected.add(null);
            }
            else {
                List<Object> list = new ArrayList<>();
                for (int k = 0; k < r % 4; k++) {
                    boolean isNull = element % 5 == 2;
                    elementNulls.add(isNull);
                    list.add(isNull ? null : value(type, element));
                    element++;
                }
                expected.add(list);
            }
            offsets[r + 1] = element;
        }
        boolean[] nulls = new boolean[element];
        for (int i = 0; i < element; i++) {
            nulls[i] = elementNulls.get(i);
        }
        int elements = element;
        FileSchema schema = schema(builder -> builder.list("v", RepetitionType.OPTIONAL,
                el -> primitive(el, type)));
        byte[] file = write(schema, encoding, batch -> values(batch.list("v", offsets, Validity.ofNulls(listNulls)),
                "v.list.element", type, elements, nulls));

        assertRows(file, expected, rows -> {
            PqList list = rows.getList("v");
            return list == null ? null : list.rawValues();
        });
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("encodings")
    void readsAStructLeaf(PhysicalType type, ColumnEncoding encoding) throws Exception {
        List<Object> expected = new ArrayList<>();
        boolean[] structNulls = new boolean[RECORDS];
        boolean[] nulls = new boolean[RECORDS];
        for (int r = 0; r < RECORDS; r++) {
            structNulls[r] = r % 6 == 5;
            nulls[r] = r % 4 == 2;
            expected.add(structNulls[r] ? null : List.of(nulls[r] ? "null" : value(type, r)));
        }
        FileSchema schema = schema(builder -> builder.struct("s", RepetitionType.OPTIONAL,
                struct -> addColumn(struct, "x", type)));
        byte[] file = write(schema, encoding, batch -> values(batch.struct("s", Validity.ofNulls(structNulls)),
                "s.x", type, RECORDS, nulls));

        assertRows(file, expected, rows -> {
            PqStruct struct = rows.getStruct("s");
            return struct == null ? null : List.of(struct.isNull("x") ? "null" : struct.getRawValue("x"));
        });
    }

    /// Reads the file whole, then the rows `[37, 211)` through a filter on `id`, which drops the
    /// pages before and after them and masks the pages at either edge.
    private static void assertRows(byte[] file, List<Object> expected, ValueReader read) throws Exception {
        assertThat(readRows(file, null, read)).isEqualTo(normalize(expected));
        FilterPredicate filter = FilterPredicate.and(FilterPredicate.gtEq("id", 37), FilterPredicate.lt("id", 211));
        assertThat(readRows(file, filter, read)).isEqualTo(normalize(expected.subList(37, 211)));
    }

    private static List<Object> readRows(byte[] file, FilterPredicate filter, ValueReader read) throws Exception {
        List<Object> values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             RowReader rows = filter == null ? reader.rowReader() : reader.buildRowReader().filter(filter).build()) {
            int expectedId = filter == null ? 0 : 37;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getInt("id")).isEqualTo(expectedId++);
                values.add(read.read(rows));
            }
        }
        return normalize(values);
    }

    @FunctionalInterface
    private interface ValueReader {
        Object read(RowReader rows);
    }

    /// Byte arrays as hex, so that values compare by content.
    private static List<Object> normalize(List<?> values) {
        List<Object> result = new ArrayList<>();
        for (Object value : values) {
            result.add(value instanceof byte[] bytes ? HexFormat.of().formatHex(bytes)
                    : value instanceof List<?> list ? normalize(list) : value);
        }
        return result;
    }

    private static FileSchema schema(Consumer<FileSchema.Builder> leaf) {
        FileSchema.Builder builder = FileSchema.builder("schema").addColumn("id", PhysicalType.INT32,
                RepetitionType.REQUIRED);
        leaf.accept(builder);
        return builder.build();
    }

    private static void addColumn(FileSchema.Builder builder, String name, PhysicalType type) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            builder.addColumn(name, type, RepetitionType.OPTIONAL, FLBA_WIDTH);
        }
        else {
            builder.addColumn(name, type, RepetitionType.OPTIONAL);
        }
    }

    private static void addColumn(FileSchema.StructBuilder builder, String name, PhysicalType type) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            builder.addColumn(name, type, RepetitionType.OPTIONAL, FLBA_WIDTH);
        }
        else {
            builder.addColumn(name, type, RepetitionType.OPTIONAL);
        }
    }

    private static void primitive(FileSchema.ElementBuilder element, PhysicalType type) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            element.primitive(type, RepetitionType.OPTIONAL, FLBA_WIDTH);
        }
        else {
            element.primitive(type, RepetitionType.OPTIONAL);
        }
    }

    /// One row group per 100 records, pages of five, uncompressed, `id` `PLAIN` and the leaf in
    /// `encoding`.
    private static byte[] write(FileSchema schema, ColumnEncoding encoding, Consumer<ColumnBatch> leaf)
            throws Exception {
        int[] ids = new int[RECORDS];
        for (int r = 0; r < RECORDS; r++) {
            ids[r] = r;
        }
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding(encoding)
                .encoding("id", ColumnEncoding.PLAIN)
                .pageTargetRows(5)
                .rowGroupTargetRows(100)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> leaf.accept(batch.ints("id", ids)));
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// The `count` values of a leaf of `type`, value `i` [#value] unless `nulls[i]`.
    private static void values(ColumnBatch batch, String column, PhysicalType type, int count, boolean[] nulls) {
        switch (type) {
            case INT32 -> {
                int[] values = new int[count];
                for (int i = 0; i < count; i++) {
                    values[i] = (Integer) value(type, i);
                }
                batch.ints(column, values, nulls);
            }
            case INT64 -> {
                long[] values = new long[count];
                for (int i = 0; i < count; i++) {
                    values[i] = (Long) value(type, i);
                }
                batch.longs(column, values, nulls);
            }
            case FLOAT -> {
                float[] values = new float[count];
                for (int i = 0; i < count; i++) {
                    values[i] = (Float) value(type, i);
                }
                batch.floats(column, values, nulls);
            }
            case DOUBLE -> {
                double[] values = new double[count];
                for (int i = 0; i < count; i++) {
                    values[i] = (Double) value(type, i);
                }
                batch.doubles(column, values, nulls);
            }
            case BOOLEAN -> {
                boolean[] values = new boolean[count];
                for (int i = 0; i < count; i++) {
                    values[i] = (Boolean) value(type, i);
                }
                batch.booleans(column, values, nulls);
            }
            default -> {
                byte[][] values = new byte[count][];
                for (int i = 0; i < count; i++) {
                    values[i] = (byte[]) value(type, i);
                }
                if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    batch.fixed(column, values, nulls);
                }
                else {
                    batch.bytes(column, values, nulls);
                }
            }
        }
    }

    /// Value `i` of a leaf of `type`: integers with varying deltas, decimals with a few digits and
    /// an occasional `NaN` an `ALP` vector keeps as an exception, strings sharing prefixes.
    private static Object value(PhysicalType type, int i) {
        return switch (type) {
            case INT32 -> i * 37 - 1_000 + (i % 3) * 11;
            case INT64 -> i * 1_000_003L - (i % 5) * 7L;
            case FLOAT -> i % 29 == 0 ? Float.NaN : (i % 50) * 0.25f;
            case DOUBLE -> i % 31 == 0 ? Double.NaN : (i % 80) * 0.125 + i;
            case BOOLEAN -> i % 3 == 0;
            case BYTE_ARRAY -> ("key-" + i / 3 + "-" + i % 3).getBytes(StandardCharsets.UTF_8);
            case FIXED_LEN_BYTE_ARRAY -> new byte[]{ (byte) i, (byte) (i >> 8), 7 };
            case INT96 -> throw new IllegalArgumentException("No INT96 values here");
        };
    }
}
