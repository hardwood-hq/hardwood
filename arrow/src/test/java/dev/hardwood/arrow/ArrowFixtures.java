/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

/// Test fixtures written at runtime under `target/arrow-fixtures/<pid>` through Hardwood's writer
/// and verified against Arrow C++ in [ArrowDatasetOracleTest].
final class ArrowFixtures {

    static final Path DIRECTORY = Path.of("target", "arrow-fixtures", Long.toString(ProcessHandle.current().pid()));

    static final int ROWS = 7;
    static final String FIELD_ID_KEY = "PARQUET:field_id";
    private static final String ARROW_SCHEMA_KEY = "ARROW:schema";

    private ArrowFixtures() {
    }

    // ==================== The seven scalar rows ====================

    static final boolean[] FLAG_REQ = { true, false, true, true, false, false, true };
    static final boolean[] FLAG = { true, false, false, false, true, true, false };
    static final boolean[] FLAG_NULLS = { false, true, false, true, false, false, true };
    static final int[] I32_REQ = { Integer.MIN_VALUE, -1, 0, 1, 7, 100000, Integer.MAX_VALUE };
    static final int[] I32 = { 0, -5, 0, 0, 42, Integer.MAX_VALUE, Integer.MIN_VALUE };
    static final boolean[] I32_NULLS = { true, false, false, true, false, false, false };
    static final int[] I8 = { -128, 0, 0, 127, 0, 1, -1 };
    static final boolean[] I8_NULLS = { false, true, false, false, true, false, false };
    static final int[] I16 = { -32768, 32767, 0, 0, 12, 0, -7 };
    static final boolean[] I16_NULLS = { false, false, true, false, false, true, false };
    static final int[] U8 = { 0, 255, 128, 0, 1, 2, 0 };
    static final boolean[] U8_NULLS = { false, false, false, true, false, false, true };
    static final int[] U16 = { 65535, 0, 0, 1, 32768, 0, 9 };
    static final boolean[] U16_NULLS = { false, true, false, false, false, true, false };
    static final int[] U32 = { -1, 0, 0, Integer.MIN_VALUE, Integer.MAX_VALUE, 5, 0 };
    static final boolean[] U32_NULLS = { false, false, true, false, false, false, true };
    static final long[] I64 = { Long.MIN_VALUE, Long.MAX_VALUE, 0, 0, -1, 0, 3 };
    static final boolean[] I64_NULLS = { false, false, true, false, false, true, false };
    static final long[] U64 = { -1L, Long.MIN_VALUE, 0, 0, 1, 2, 0 };
    static final boolean[] U64_NULLS = { false, false, false, true, false, false, true };
    static final float[] F32 = { 1.5f, 0, Float.NaN, Float.POSITIVE_INFINITY, -0.0f, 0, -2.25f };
    static final boolean[] F32_NULLS = { false, true, false, false, false, true, false };
    static final double[] F64 = { 0, 2.5, Double.NEGATIVE_INFINITY, Double.NaN, 0.0, 1e300, 0 };
    static final boolean[] F64_NULLS = { true, false, false, false, false, false, true };
    static final LocalDate[] DAY = { LocalDate.of(1970, 1, 1), LocalDate.of(1969, 12, 31), null,
            LocalDate.of(2026, 10, 16), null, LocalDate.of(2000, 2, 29), LocalDate.of(1900, 1, 1) };
    static final LocalTime[] T_MS = { LocalTime.of(0, 0), null, LocalTime.of(23, 59, 59, 999_000_000),
            LocalTime.of(12, 0), null, LocalTime.of(7, 30, 0, 123_000_000), LocalTime.of(1, 2, 3) };
    static final LocalTime[] T_US = { null, LocalTime.of(23, 59, 59, 999_999_000), LocalTime.of(0, 0),
            LocalTime.of(8, 0, 0, 654_321_000), LocalTime.of(6, 45), null, LocalTime.of(1, 2, 3, 4_000) };
    static final long[] T_NS = { 27000123456789L, 0, 0, 86399999999999L, 0, 1, 28800654321987L };
    static final boolean[] T_NS_NULLS = { false, true, false, false, true, false, false };
    static final Instant[] TS_MS = { Instant.parse("2025-01-01T10:30:00.123Z"), null,
            Instant.parse("1969-12-31T23:59:59.999Z"), Instant.EPOCH, Instant.parse("2262-04-11T23:47:16.854Z"), null,
            Instant.parse("2000-02-29T12:00:00Z") };
    static final Instant[] TS_US = { null, Instant.parse("2025-01-02T14:45:30.654321Z"),
            Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH, null,
            Instant.parse("2262-04-11T23:47:16.854775Z"), Instant.parse("1900-01-01T00:00:00Z") };
    static final long[] TS_NS = { 1735727400123456789L, 1735829130654321987L, 0, 0, -1, 0, Long.MAX_VALUE };
    static final boolean[] TS_NS_NULLS = { false, false, true, false, false, true, false };
    static final LocalDateTime[] TS_LOCAL = { LocalDateTime.of(2025, 1, 3, 9, 15, 45, 111_222_000), null, null,
            LocalDateTime.of(1970, 1, 1, 0, 0), LocalDateTime.of(1960, 6, 15, 8, 0),
            LocalDateTime.of(2030, 12, 31, 23, 59, 59, 999_999_000), LocalDateTime.of(2000, 1, 1, 0, 0) };
    static final int[] DEC_I32 = { 123456, -1, 0, 0, 999999999, -999999999, 0 };
    static final boolean[] DEC_I32_NULLS = { false, false, true, false, false, false, true };
    static final long[] DEC_I64 = { 0, 1234567890123L, -1, 0, 999999999999999999L, 0, -999999999999999999L };
    static final boolean[] DEC_I64_NULLS = { true, false, false, false, false, true, false };
    static final BigDecimal[] DEC128 = { new BigDecimal("12345678901234567.890"), null, new BigDecimal("-0.001"),
            new BigDecimal("0.000"), new BigDecimal("99999999999999999.999"), new BigDecimal("-99999999999999999.999"),
            null };
    static final BigDecimal[] DEC256 = { null,
            new BigDecimal("123456789012345678901234567890123456789012345.67890"), new BigDecimal("-0.00001"),
            new BigDecimal("0.00000"), null, new BigDecimal("-99999999999999999999999999999999999999999999.99999"),
            new BigDecimal("1.00000") };
    static final String[] TEXT_REQ = { "alpha", "", "gamma", "δέλτα", "epsilon", "ζ", "eta" };
    static final String[] TEXT = { null, "beta", "", null, "ε", "zeta", null };
    static final byte[][] BLOB = { hex("000102"), null, hex(""), hex("fffe"), null, hex("80"), hex("78") };
    static final byte[][] FIXED4 = { ascii("abcd"), hex("00000000"), null, hex("ffffffff"), ascii("1234"), null,
            ascii("wxyz") };
    static final UUID[] ID = { UUID.fromString("12345678-1234-5678-1234-567812345678"), null,
            new UUID(0, 0), new UUID(-1L, -1L), null, UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            UUID.fromString("87654321-4321-8765-4321-876543218765") };
    static final String[] DOC = { "{\"role\":\"admin\"}", "[]", null, "{\"nested\":{\"k\":1,\"v\":[1,2,3]}}", "null",
            null, "\"text\"" };
    static final String[] LABEL = { "RED", "GREEN", null, "BLUE", "RED", null, "GREEN" };
    static final byte[][] BSON = { hex("0500000000"), null, hex("12000000026b0006000000686921800000"),
            hex("0f00000005780003000000ff00fe0000"), hex("0500000000"), null, hex("0500000000") };
    static final float[] HALF = { 1.5f, 0, -2.25f, 65504f, 0, 6.103515625e-05f, 0 };
    static final boolean[] HALF_NULLS = { false, true, false, false, true, false, true };
    static final byte[][] SPAN = { hex("010000000200000003000000"), null, hex("000000000000000000000000"),
            hex("ffffffffffffffffffffffff"), null, hex("0c0000001f000000ff000000"), null };

    // ==================== Writing ====================

    private static boolean written;

    static synchronized void ensureWritten() throws IOException {
        if (written) {
            return;
        }
        Files.createDirectories(DIRECTORY);
        writeScalars("scalars", WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build(), null);
        writeScalars("scalars_dict", WriterConfig.builder().rowGroupTargetRows(3).build(), null);
        writeScalars("scalars_stored_schema", WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build(),
                foreignArrowSchema());
        writeEmpty();
        writeFieldIds();
        writeNested();
        written = true;
    }

    static Path file(String name) {
        return DIRECTORY.resolve(name + ".parquet");
    }

    private static FileSchema scalarsSchema() {
        return FileSchema.builder("scalars")
                .primitive("flag_req", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .primitive("flag", PhysicalType.BOOLEAN, RepetitionType.OPTIONAL)
                .primitive("i32_req", PhysicalType.INT32, RepetitionType.REQUIRED)
                .primitive("i32", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .primitive("i8", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.intType(8, true)))
                .primitive("i16", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.intType(16, true)))
                .primitive("u8", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.intType(8, false)))
                .primitive("u16", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.intType(16, false)))
                .primitive("u32", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.intType(32, false)))
                .primitive("i64", PhysicalType.INT64, RepetitionType.OPTIONAL)
                .primitive("u64", PhysicalType.INT64, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.intType(64, false)))
                .primitive("f32", PhysicalType.FLOAT, RepetitionType.OPTIONAL)
                .primitive("f64", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .primitive("day", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.date()))
                .primitive("t_ms", PhysicalType.INT32, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.time(false, LogicalType.TimeUnit.MILLIS)))
                .primitive("t_us", PhysicalType.INT64, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.time(false, LogicalType.TimeUnit.MICROS)))
                .primitive("t_ns", PhysicalType.INT64, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.time(false, LogicalType.TimeUnit.NANOS)))
                .primitive("ts_ms", PhysicalType.INT64, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS)))
                .primitive("ts_us", PhysicalType.INT64, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS)))
                .primitive("ts_ns", PhysicalType.INT64, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS)))
                .primitive("ts_local", PhysicalType.INT64, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.timestamp(false, LogicalType.TimeUnit.MICROS)))
                .primitive("dec_i32", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.decimal(9, 2)))
                .primitive("dec_i64", PhysicalType.INT64, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.decimal(18, 4)))
                .primitive("dec128", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(9).logicalType(LogicalType.decimal(20, 3)))
                .primitive("dec256", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(21).logicalType(LogicalType.decimal(50, 5)))
                .primitive("text_req", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, c -> c.logicalType(LogicalType.string()))
                .primitive("text", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.string()))
                .primitive("blob", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                .primitive("fixed4", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, c -> c.typeLength(4))
                .primitive("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(16).logicalType(LogicalType.uuid()))
                .primitive("doc", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.json()))
                .primitive("label", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.enumType()))
                .primitive("bson", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.bson()))
                .primitive("half", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(2).logicalType(LogicalType.float16()))
                .primitive("span", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(12).logicalType(LogicalType.interval()))
                .primitive("nothing", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.unknown()))
                .build();
    }

    private static void writeScalars(String name, WriterConfig config, String arrowSchema) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file(name)), scalarsSchema(), config)) {
            if (arrowSchema != null) {
                writer.keyValueMetadata(ARROW_SCHEMA_KEY, arrowSchema);
            }
            ColumnWriter columns = writer.columnWriter();
            columns.writeBatch(batch -> batch
                    .booleans("flag_req", FLAG_REQ)
                    .booleans("flag", FLAG, FLAG_NULLS)
                    .ints("i32_req", I32_REQ)
                    .ints("i32", I32, I32_NULLS)
                    .ints("i8", I8, I8_NULLS)
                    .ints("i16", I16, I16_NULLS)
                    .ints("u8", U8, U8_NULLS)
                    .ints("u16", U16, U16_NULLS)
                    .ints("u32", U32, U32_NULLS)
                    .longs("i64", I64, I64_NULLS)
                    .longs("u64", U64, U64_NULLS)
                    .floats("f32", F32, F32_NULLS)
                    .doubles("f64", F64, F64_NULLS)
                    .ints("day", epochDays(DAY), nulls(DAY))
                    .ints("t_ms", millisOfDay(T_MS), nulls(T_MS))
                    .longs("t_us", microsOfDay(T_US), nulls(T_US))
                    .longs("t_ns", T_NS, T_NS_NULLS)
                    .longs("ts_ms", epochMillis(TS_MS), nulls(TS_MS))
                    .longs("ts_us", epochMicros(TS_US), nulls(TS_US))
                    .longs("ts_ns", TS_NS, TS_NS_NULLS)
                    .longs("ts_local", localMicros(TS_LOCAL), nulls(TS_LOCAL))
                    .ints("dec_i32", DEC_I32, DEC_I32_NULLS)
                    .longs("dec_i64", DEC_I64, DEC_I64_NULLS)
                    .fixed("dec128", unscaled(DEC128, 9), nulls(DEC128))
                    .fixed("dec256", unscaled(DEC256, 21), nulls(DEC256))
                    .bytes("text_req", utf8(TEXT_REQ))
                    .bytes("text", utf8(TEXT), nulls(TEXT))
                    .bytes("blob", present(BLOB, 0), nulls(BLOB))
                    .fixed("fixed4", present(FIXED4, 4), nulls(FIXED4))
                    .fixed("id", uuidBytes(ID), nulls(ID))
                    .bytes("doc", utf8(DOC), nulls(DOC))
                    .bytes("label", utf8(LABEL), nulls(LABEL))
                    .bytes("bson", present(BSON, 0), nulls(BSON))
                    .fixed("half", halfBytes(HALF), HALF_NULLS)
                    .fixed("span", present(SPAN, 12), nulls(SPAN))
                    .ints("nothing", new int[ROWS], Validity.ofNulls(allNull())));
        }
    }

    private static String foreignArrowSchema() throws IOException {
        Schema schema = new Schema(List.of(
                Field.nullable("text", ArrowType.LargeUtf8.INSTANCE),
                Field.nullable("ts_local", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Europe/Ljubljana"))));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(bytes)), schema);
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    private static void writeEmpty() throws IOException {
        FileSchema schema = FileSchema.builder("empty")
                .primitive("n", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .primitive("text", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.string()))
                .primitive("flag", PhysicalType.BOOLEAN, RepetitionType.OPTIONAL)
                .primitive("dec", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(5).logicalType(LogicalType.decimal(10, 2)))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file("empty")), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("n", new int[0])
                    .bytes("text", new byte[0][])
                    .booleans("flag", new boolean[0])
                    .fixed("dec", new byte[0][]));
        }
    }

    private static void writeNested() throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .primitive("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, s -> s
                        .primitive("street", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                c -> c.logicalType(LogicalType.string()))
                        .primitive("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .struct("home", RepetitionType.REQUIRED, s -> s
                        .primitive("zip", PhysicalType.INT32, RepetitionType.REQUIRED))
                .list("phones", RepetitionType.OPTIONAL, e -> e.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .list("tags", RepetitionType.REQUIRED, e -> e.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.logicalType(LogicalType.string())))
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        k -> k.logicalType(LogicalType.string()),
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .list("matrix", RepetitionType.OPTIONAL, e -> e.list(RepetitionType.OPTIONAL,
                        inner -> inner.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .list("contacts", RepetitionType.OPTIONAL, e -> e.struct(RepetitionType.OPTIONAL, s -> s
                        .primitive("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                c -> c.logicalType(LogicalType.string()))
                        .list("nums", RepetitionType.OPTIONAL, n -> n.primitive(PhysicalType.INT64,
                                RepetitionType.OPTIONAL))))
                .struct("owner", RepetitionType.OPTIONAL, s -> s
                        .struct("place", RepetitionType.OPTIONAL, p -> p
                                .primitive("city", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                        c -> c.logicalType(LogicalType.string()))))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file("nested")), schema,
                WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("id", new int[]{ 1, 2, 3, 4, 5 })
                    .struct("address", Validity.ofNulls(new boolean[]{ false, true, false, false, true }))
                    .bytes("address.street", utf8(new String[]{ "Main", "", "Elm", "", "" }))
                    .ints("address.zip", new int[]{ 10001, 0, 0, 3, 0 }, new boolean[]{ false, true, true, false, true })
                    .ints("home.zip", new int[]{ 1, 2, 3, 4, 5 })
                    .list("phones", new int[]{ 0, 2, 2, 2, 5, 5 },
                            Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .ints("phones.list.element", new int[]{ 1, 2, 3, 0, 5 },
                            new boolean[]{ false, false, false, true, false })
                    .list("tags", new int[]{ 0, 1, 1, 3, 3, 4 })
                    .bytes("tags.list.element", utf8(new String[]{ "a", "b", "c", "d" }))
                    .map("props", new int[]{ 0, 2, 2, 2, 3, 5 },
                            Validity.ofNulls(new boolean[]{ false, true, false, false, false }))
                    .bytes("props.key_value.key", utf8(new String[]{ "k1", "k2", "k3", "k4", "k5" }))
                    .ints("props.key_value.value", new int[]{ 1, 0, 3, 4, 5 },
                            new boolean[]{ false, true, false, false, false })
                    .list("matrix", new int[]{ 0, 2, 2, 4, 4, 5 },
                            Validity.ofNulls(new boolean[]{ false, true, false, false, false }))
                    .list("matrix.list.element", new int[]{ 0, 2, 2, 2, 3, 4 },
                            Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .ints("matrix.list.element.list.element", new int[]{ 1, 2, 3, 0 },
                            new boolean[]{ false, false, false, true })
                    .list("contacts", new int[]{ 0, 2, 2, 2, 3, 4 },
                            Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .struct("contacts.list.element", Validity.ofNulls(new boolean[]{ false, true, false, false }))
                    .bytes("contacts.list.element.name", utf8(new String[]{ "A", "", "B", "C" }))
                    .list("contacts.list.element.nums", new int[]{ 0, 2, 2, 2, 2 },
                            Validity.ofNulls(new boolean[]{ false, true, true, false }))
                    .longs("contacts.list.element.nums.list.element", new long[]{ 1, 2 })
                    .struct("owner", Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .struct("owner.place", Validity.ofNulls(new boolean[]{ false, true, true, false, false }))
                    .bytes("owner.place.city", utf8(new String[]{ "X", "", "", "", "Y" }),
                            new boolean[]{ false, true, true, true, false }));
        }
    }

    private static void writeFieldIds() throws IOException {
        FileSchema schema = FileSchema.builder("field_ids")
                .primitive("a", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.fieldId(7))
                .primitive("b", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.string()))
                .primitive("c", PhysicalType.INT64, RepetitionType.OPTIONAL, c -> c.fieldId(42))
                .primitive("zero", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.fieldId(0))
                .map("m", RepetitionType.OPTIONAL, g -> g.fieldId(50),
                        PhysicalType.INT32, k -> k.fieldId(52),
                        v -> v.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL, c -> c.fieldId(53)))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file("field_ids")), schema,
                WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("a", new int[]{ 0, 1, 2 })
                    .bytes("b", utf8(new String[]{ "row-0", "row-1", "row-2" }))
                    .longs("c", new long[]{ 0L, 10L, 20L })
                    .ints("zero", new int[]{ 0, -1, -2 })
                    .map("m", new int[]{ 0, 1, 3, 6 })
                    .ints("m.key_value.key", new int[]{ 0, 1, 2, 3, 4, 5 })
                    .longs("m.key_value.value", new long[]{ 0L, 10L, 20L, 30L, 40L, 50L }));
        }
    }

    // ==================== Value helpers ====================

    static byte[] hex(String hex) {
        return HexFormat.of().parseHex(hex);
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[][] utf8(String[] texts) {
        byte[][] bytes = new byte[texts.length][];
        for (int i = 0; i < texts.length; i++) {
            bytes[i] = texts[i] == null ? new byte[0] : utf8(texts[i]);
        }
        return bytes;
    }

    private static boolean[] nulls(Object[] values) {
        boolean[] nulls = new boolean[values.length];
        for (int i = 0; i < values.length; i++) {
            nulls[i] = values[i] == null;
        }
        return nulls;
    }

    private static boolean[] allNull() {
        boolean[] nulls = new boolean[ROWS];
        Arrays.fill(nulls, true);
        return nulls;
    }

    private static byte[][] present(byte[][] values, int width) {
        byte[][] result = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            result[i] = values[i] == null ? new byte[width] : values[i];
        }
        return result;
    }

    private static int[] epochDays(LocalDate[] days) {
        int[] result = new int[days.length];
        for (int i = 0; i < days.length; i++) {
            result[i] = days[i] == null ? 0 : Math.toIntExact(days[i].toEpochDay());
        }
        return result;
    }

    private static int[] millisOfDay(LocalTime[] times) {
        int[] result = new int[times.length];
        for (int i = 0; i < times.length; i++) {
            result[i] = times[i] == null ? 0 : Math.toIntExact(times[i].toNanoOfDay() / 1_000_000L);
        }
        return result;
    }

    private static long[] microsOfDay(LocalTime[] times) {
        long[] result = new long[times.length];
        for (int i = 0; i < times.length; i++) {
            result[i] = times[i] == null ? 0 : times[i].toNanoOfDay() / 1_000L;
        }
        return result;
    }

    private static long[] epochMillis(Instant[] instants) {
        long[] result = new long[instants.length];
        for (int i = 0; i < instants.length; i++) {
            result[i] = instants[i] == null ? 0 : instants[i].toEpochMilli();
        }
        return result;
    }

    private static long[] epochMicros(Instant[] instants) {
        long[] result = new long[instants.length];
        for (int i = 0; i < instants.length; i++) {
            result[i] = instants[i] == null ? 0 : ChronoUnit.MICROS.between(Instant.EPOCH, instants[i]);
        }
        return result;
    }

    private static long[] localMicros(LocalDateTime[] times) {
        long[] result = new long[times.length];
        for (int i = 0; i < times.length; i++) {
            result[i] = times[i] == null ? 0 : ChronoUnit.MICROS.between(
                    LocalDateTime.of(1970, 1, 1, 0, 0), times[i]);
        }
        return result;
    }

    private static byte[][] unscaled(BigDecimal[] decimals, int width) {
        byte[][] result = new byte[decimals.length][];
        for (int i = 0; i < decimals.length; i++) {
            result[i] = new byte[width];
            if (decimals[i] != null) {
                byte[] minimal = decimals[i].unscaledValue().toByteArray();
                byte sign = decimals[i].signum() < 0 ? (byte) 0xFF : 0;
                Arrays.fill(result[i], 0, width - minimal.length, sign);
                System.arraycopy(minimal, 0, result[i], width - minimal.length, minimal.length);
            }
        }
        return result;
    }

    private static byte[][] uuidBytes(UUID[] ids) {
        byte[][] result = new byte[ids.length][];
        for (int i = 0; i < ids.length; i++) {
            result[i] = ids[i] == null ? new byte[16] : uuidBytes(ids[i]);
        }
        return result;
    }

    private static byte[] uuidBytes(UUID id) {
        byte[] bytes = new byte[16];
        long most = id.getMostSignificantBits();
        long least = id.getLeastSignificantBits();
        for (int k = 0; k < 8; k++) {
            bytes[k] = (byte) (most >>> (56 - 8 * k));
            bytes[8 + k] = (byte) (least >>> (56 - 8 * k));
        }
        return bytes;
    }

    private static byte[][] halfBytes(float[] values) {
        byte[][] result = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            short half = Float.floatToFloat16(values[i]);
            result[i] = new byte[]{ (byte) half, (byte) (half >>> 8) };
        }
        return result;
    }
}
