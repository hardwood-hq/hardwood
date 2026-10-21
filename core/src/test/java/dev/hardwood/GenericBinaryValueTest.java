/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.management.ThreadMXBean;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.FieldAccessor;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.StructBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/// The generic accessors on binary logical types: `getValue` returns what the typed accessor
/// returns, on the flat and the nested reader, over plain and dictionary-encoded pages, and decodes
/// from the batch's bytes without first copying them out; and a map lookup by key, which compares
/// the keys where they lie.
class GenericBinaryValueTest {

    private static final int ROWS = 1_000;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void flatGetValueMatchesTheTypedAccessor(boolean dictionary) throws IOException {
        FileSchema schema = leaves(FileSchema.builder("schema")).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < ROWS; i++) {
                int row = i;
                writer.rowWriter().writeRow(r -> setLeaves(r, row, dictionary));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            for (int row = 0; rows.hasNext(); row++) {
                rows.next();
                assertLeaves(rows, row, dictionary);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void nestedGetValueMatchesTheTypedAccessor(boolean dictionary) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.REQUIRED, GenericBinaryValueTest::leaves)
                .list("decimals", RepetitionType.REQUIRED, element -> element.primitive(PhysicalType.BYTE_ARRAY,
                        RepetitionType.REQUIRED, LogicalType.decimal(30, 4)))
                .map("ids", RepetitionType.REQUIRED, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.primitive(PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                                LogicalType.uuid()))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < ROWS; i++) {
                int row = i;
                writer.rowWriter().writeRow(r -> r
                        .setStruct("s", s -> setLeaves(s, row, dictionary))
                        .setList("decimals", l -> l.addDecimal(decimal(row, dictionary)))
                        .setMap("ids", m -> m.addEntry(e -> e.setString("key", "id").setUuid("value", uuid(row, dictionary)))));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            for (int row = 0; rows.hasNext(); row++) {
                rows.next();
                assertLeaves(rows.getStruct("s"), row, dictionary);
                PqList decimals = rows.getList("decimals");
                assertThat(decimals.get(0)).isEqualTo(decimal(row, dictionary));
                assertThat(decimals.values()).containsExactly(decimal(row, dictionary));
                PqMap ids = rows.getMap("ids");
                assertThat(ids.getValue("id")).isEqualTo(uuid(row, dictionary));
                assertThat(ids.getEntries().get(0).getValue()).isEqualTo(uuid(row, dictionary));
            }
        }
    }

    /// Each call of `getValue` on a `DECIMAL` allocates what `getDecimal` allocates, and no copy of
    /// the value's bytes besides, on the flat and the nested reader.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void getValueOnADecimalCopiesNoBytes(boolean nested) throws IOException {
        LogicalType decimal = LogicalType.decimal(2500, 2);
        FileSchema schema = nested
                ? FileSchema.builder("schema").struct("s", RepetitionType.REQUIRED,
                        s -> s.addColumn("d", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, decimal)).build()
                : FileSchema.builder("schema").addColumn("d", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, decimal)
                        .build();
        // 1 KB of unscaled value: a copy of it outweighs whatever escape analysis saves either call
        BigDecimal wide = new BigDecimal(BigInteger.ONE.shiftLeft(1024 * 8 - 2), 2);
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(r -> {
                if (nested) {
                    r.setStruct("s", s -> s.setDecimal("d", wide));
                }
                else {
                    r.setDecimal("d", wide);
                }
            });
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            FieldAccessor fields = nested ? rows.getStruct("s") : rows;
            assertThat(fields.getValue("d")).isEqualTo(wide);

            long typed = bytesPerCall(() -> fields.getDecimal("d"));
            assertThat(bytesPerCall(() -> fields.getValue("d"))).isLessThan(typed + 512);
        }
    }

    /// A lookup by key compares the map's keys where they lie in the batch: scanning 32 keys of
    /// 256 bytes each allocates no copy of them.
    @Test
    void mapLookupByKeyCopiesNoKeys() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .map("m", RepetitionType.REQUIRED, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.primitive(PhysicalType.INT64, RepetitionType.REQUIRED))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(r -> r.setMap("m", m -> {
                for (int i = 0; i < 32; i++) {
                    long value = i;
                    m.addEntry(e -> e.setString("key", longKey(value)).setLong("value", value));
                }
            }));
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            PqMap map = rows.getMap("m");
            byte[] absent = "absent".getBytes(StandardCharsets.UTF_8);
            assertThat(map.getValue(longKey(0))).isEqualTo(0L);
            assertThat(map.containsKey("absent")).isFalse();

            assertThat(bytesPerCall(() -> map.containsKey("absent"))).isLessThan(256);
            assertThat(bytesPerCall(() -> map.getRawValue(absent))).isLessThan(256);
        }
    }

    private static String longKey(long i) {
        return "k".repeat(250) + String.format("%06d", i);
    }

    /// The bytes the current thread allocates per call of `call`, once it is compiled.
    private static long bytesPerCall(Runnable call) {
        ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        int calls = 200_000;
        for (int i = 0; i < calls; i++) {
            call.run();
        }
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < calls; i++) {
            call.run();
        }
        return (threads.getCurrentThreadAllocatedBytes() - before) / calls;
    }

    private static FileSchema.Builder leaves(FileSchema.Builder schema) {
        return schema
                .addColumn("dec_bin", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.decimal(30, 4))
                .addColumn("dec_fixed", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 13,
                        LogicalType.decimal(30, 4))
                .addColumn("uuid", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 16, LogicalType.uuid())
                .addColumn("interval", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 12,
                        LogicalType.interval())
                .addColumn("f16", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 2, LogicalType.float16());
    }

    private static void leaves(FileSchema.StructBuilder struct) {
        struct.addColumn("dec_bin", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.decimal(30, 4))
                .addColumn("dec_fixed", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 13,
                        LogicalType.decimal(30, 4))
                .addColumn("uuid", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 16, LogicalType.uuid())
                .addColumn("interval", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 12,
                        LogicalType.interval())
                .addColumn("f16", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 2, LogicalType.float16());
    }

    private static void setLeaves(StructBuilder r, int row, boolean dictionary) {
        if (row % 10 == 7) {
            r.setNull("dec_bin").setNull("dec_fixed").setNull("uuid").setNull("interval").setNull("f16");
            return;
        }
        r.setDecimal("dec_bin", decimal(row, dictionary))
                .setDecimal("dec_fixed", decimal(row, dictionary).negate())
                .setUuid("uuid", uuid(row, dictionary))
                .setInterval("interval", interval(row, dictionary))
                .setBinary("f16", float16Bytes(float16(row, dictionary)));
    }

    private static void assertLeaves(FieldAccessor fields, int row, boolean dictionary) {
        if (row % 10 == 7) {
            assertThat(fields.getValue("dec_bin")).isNull();
            assertThat(fields.getValue("dec_fixed")).isNull();
            assertThat(fields.getValue("uuid")).isNull();
            assertThat(fields.getValue("interval")).isNull();
            assertThat(fields.getValue("f16")).isNull();
            return;
        }
        assertThat(fields.getValue("dec_bin")).isEqualTo(fields.getDecimal("dec_bin")).isEqualTo(decimal(row, dictionary));
        assertThat(fields.getValue("dec_fixed")).isEqualTo(fields.getDecimal("dec_fixed"))
                .isEqualTo(decimal(row, dictionary).negate());
        assertThat(fields.getValue("uuid")).isEqualTo(fields.getUuid("uuid")).isEqualTo(uuid(row, dictionary));
        assertThat(fields.getValue("interval")).isEqualTo(fields.getInterval("interval"))
                .isEqualTo(interval(row, dictionary));
        assertThat(fields.getValue("f16")).isEqualTo(fields.getFloat("f16")).isEqualTo(float16(row, dictionary));
    }

    private static int key(int row, boolean dictionary) {
        return dictionary ? row % 5 : row;
    }

    private static BigDecimal decimal(int row, boolean dictionary) {
        return new BigDecimal(BigInteger.valueOf(key(row, dictionary)).shiftLeft(70).add(BigInteger.TEN), 4);
    }

    private static UUID uuid(int row, boolean dictionary) {
        return new UUID(key(row, dictionary), ~key(row, dictionary));
    }

    private static PqInterval interval(int row, boolean dictionary) {
        int k = key(row, dictionary);
        return new PqInterval(k, k + 1, k * 1000);
    }

    /// The little-endian `FLOAT16` encoding of `value`.
    private static byte[] float16Bytes(float value) {
        short bits = Float.floatToFloat16(value);
        return new byte[]{ (byte) bits, (byte) (bits >> 8) };
    }

    private static float float16(int row, boolean dictionary) {
        return key(row, dictionary) % 1024 + 0.5f;
    }
}
