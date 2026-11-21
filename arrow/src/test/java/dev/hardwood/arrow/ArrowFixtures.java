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
import java.util.function.IntConsumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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

/// The module's fixtures, written when the tests run rather than checked in, each with the
/// Arrow table the reader must produce from it.
///
/// The files are written by Hardwood's own writer from one declaration of seven rows; the
/// expected table is built from the same declaration through Arrow's typed setters, so every
/// value is encoded twice by independent code and the Arrow schema is stated by hand. Shapes
/// Hardwood's writer does not produce (legacy `INT96`, a dictionary that falls back to plain
/// pages mid-chunk) come from core's checked-in fixtures, read against Arrow C++ in
/// [ArrowDatasetOracleTest].
final class ArrowFixtures {

    /// One directory per test JVM: Surefire forks several, and they would otherwise race on
    /// the same files.
    static final Path DIRECTORY = Path.of("target", "arrow-fixtures", Long.toString(ProcessHandle.current().pid()));

    static final int ROWS = 7;
    static final String FIELD_ID_KEY = "PARQUET:field_id";
    private static final String ARROW_SCHEMA_KEY = "ARROW:schema";

    private ArrowFixtures() {
    }

    // ==================== The seven scalar rows ====================
    //
    // Every optional column has a different null pattern, so no two validity bitmaps are the
    // same, and nulls fall in the first and last row of some three-row row group.

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
    /// Stored bit patterns: 4294967295 and 2147483648 are negative as Java ints.
    static final int[] U32 = { -1, 0, 0, Integer.MIN_VALUE, Integer.MAX_VALUE, 5, 0 };
    static final boolean[] U32_NULLS = { false, false, true, false, false, false, true };
    static final long[] I64 = { Long.MIN_VALUE, Long.MAX_VALUE, 0, 0, -1, 0, 3 };
    static final boolean[] I64_NULLS = { false, false, true, false, false, true, false };
    /// Stored bit patterns: 18446744073709551615 and 9223372036854775808 are negative as longs.
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

    /// Writes every fixture under [#DIRECTORY] once per JVM, replacing what an earlier run left.
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
        writeCounters();
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

    /// An `ARROW:schema` entry whose types differ from the Parquet mapping (large strings,
    /// a zoned local timestamp), so a reader that honoured it would produce another schema.
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

    static final int COUNTER_ROWS = 34;

    /// Five rows of every nesting shape Hardwood's writer produces, with a null or an empty
    /// container at every level somewhere: an optional and a required struct, optional and
    /// required lists, a map, a list of lists, a list of structs holding lists, and a struct
    /// in a struct.
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
                    // {Main, 10001}, null, {Elm, null}, {"", 3}, null
                    .struct("address", Validity.ofNulls(new boolean[]{ false, true, false, false, true }))
                    .bytes("address.street", utf8(new String[]{ "Main", "", "Elm", "", "" }))
                    .ints("address.zip", new int[]{ 10001, 0, 0, 3, 0 }, new boolean[]{ false, true, true, false, true })
                    .ints("home.zip", new int[]{ 1, 2, 3, 4, 5 })
                    // [1, 2], [], null, [3, null, 5], []
                    .list("phones", new int[]{ 0, 2, 2, 2, 5, 5 },
                            Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .ints("phones.list.element", new int[]{ 1, 2, 3, 0, 5 },
                            new boolean[]{ false, false, false, true, false })
                    // [a], [], [b, c], [], [d]
                    .list("tags", new int[]{ 0, 1, 1, 3, 3, 4 })
                    .bytes("tags.list.element", utf8(new String[]{ "a", "b", "c", "d" }))
                    // {k1: 1, k2: null}, null, {}, {k3: 3}, {k4: 4, k5: 5}
                    .map("props", new int[]{ 0, 2, 2, 2, 3, 5 },
                            Validity.ofNulls(new boolean[]{ false, true, false, false, false }))
                    .bytes("props.key_value.key", utf8(new String[]{ "k1", "k2", "k3", "k4", "k5" }))
                    .ints("props.key_value.value", new int[]{ 1, 0, 3, 4, 5 },
                            new boolean[]{ false, true, false, false, false })
                    // [[1, 2], []], null, [null, [3]], [], [[null]]
                    .list("matrix", new int[]{ 0, 2, 2, 4, 4, 5 },
                            Validity.ofNulls(new boolean[]{ false, true, false, false, false }))
                    .list("matrix.list.element", new int[]{ 0, 2, 2, 2, 3, 4 },
                            Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .ints("matrix.list.element.list.element", new int[]{ 1, 2, 3, 0 },
                            new boolean[]{ false, false, false, true })
                    // [{A, [1, 2]}, null], [], null, [{B, null}], [{C, []}]
                    .list("contacts", new int[]{ 0, 2, 2, 2, 3, 4 },
                            Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .struct("contacts.list.element", Validity.ofNulls(new boolean[]{ false, true, false, false }))
                    .bytes("contacts.list.element.name", utf8(new String[]{ "A", "", "B", "C" }))
                    .list("contacts.list.element.nums", new int[]{ 0, 2, 2, 2, 2 },
                            Validity.ofNulls(new boolean[]{ false, true, true, false }))
                    .longs("contacts.list.element.nums.list.element", new long[]{ 1, 2 })
                    // {place: {X}}, {place: null}, null, {place: {null}}, {place: {Y}}
                    .struct("owner", Validity.ofNulls(new boolean[]{ false, false, true, false, false }))
                    .struct("owner.place", Validity.ofNulls(new boolean[]{ false, true, true, false, false }))
                    .bytes("owner.place.city", utf8(new String[]{ "X", "", "", "", "Y" }),
                            new boolean[]{ false, true, true, true, false }));
        }
    }

    /// Two `REQUIRED` `INT64` columns over 34 rows, `n` counting up and `m` counting down: with a
    /// batch size of 17 and a filter keeping `n >= 16`, the first batch holds one row and the
    /// second seventeen, which is the shape that once failed an allocator while replacing a
    /// loaded batch.
    private static void writeCounters() throws IOException {
        FileSchema schema = FileSchema.builder("counters")
                .primitive("n", PhysicalType.INT64, RepetitionType.REQUIRED)
                .primitive("m", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        long[] n = new long[COUNTER_ROWS];
        long[] m = new long[COUNTER_ROWS];
        for (int i = 0; i < COUNTER_ROWS; i++) {
            n[i] = i;
            m[i] = COUNTER_ROWS - i;
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file("counters")), schema,
                WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("n", n).longs("m", m));
        }
    }

    /// Four columns of which three carry a Parquet field id, one of them zero, and a map with
    /// an id on the map, its key and its value.
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
                    // {0: 0}, {1: 10, 2: 20}, {3: 30, 4: 40, 5: 50}
                    .map("m", new int[]{ 0, 1, 3, 6 })
                    .ints("m.key_value.key", new int[]{ 0, 1, 2, 3, 4, 5 })
                    .longs("m.key_value.value", new long[]{ 0L, 10L, 20L, 30L, 40L, 50L }));
        }
    }

    // ==================== Expected Arrow tables ====================

    /// The Arrow schema of the scalar files, stated by hand.
    static Schema scalarsArrowSchema() {
        return new Schema(List.of(
                field("flag_req", false, ArrowType.Bool.INSTANCE),
                field("flag", true, ArrowType.Bool.INSTANCE),
                field("i32_req", false, new ArrowType.Int(32, true)),
                field("i32", true, new ArrowType.Int(32, true)),
                field("i8", true, new ArrowType.Int(8, true)),
                field("i16", true, new ArrowType.Int(16, true)),
                field("u8", true, new ArrowType.Int(8, false)),
                field("u16", true, new ArrowType.Int(16, false)),
                field("u32", true, new ArrowType.Int(32, false)),
                field("i64", true, new ArrowType.Int(64, true)),
                field("u64", true, new ArrowType.Int(64, false)),
                field("f32", true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                field("f64", true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                field("day", true, new ArrowType.Date(DateUnit.DAY)),
                field("t_ms", true, new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
                field("t_us", true, new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                field("t_ns", true, new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
                field("ts_ms", true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                field("ts_us", true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                field("ts_ns", true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
                field("ts_local", true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                field("dec_i32", true, new ArrowType.Decimal(9, 2, 128)),
                field("dec_i64", true, new ArrowType.Decimal(18, 4, 128)),
                field("dec128", true, new ArrowType.Decimal(20, 3, 128)),
                field("dec256", true, new ArrowType.Decimal(50, 5, 256)),
                field("text_req", false, ArrowType.Utf8.INSTANCE),
                field("text", true, ArrowType.Utf8.INSTANCE),
                field("blob", true, ArrowType.Binary.INSTANCE),
                field("fixed4", true, new ArrowType.FixedSizeBinary(4)),
                field("id", true, new ArrowType.FixedSizeBinary(16)),
                field("doc", true, ArrowType.Utf8.INSTANCE),
                field("label", true, ArrowType.Binary.INSTANCE),
                field("bson", true, ArrowType.Binary.INSTANCE),
                field("half", true, new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
                field("span", true, new ArrowType.FixedSizeBinary(12)),
                field("nothing", true, ArrowType.Null.INSTANCE)));
    }

    /// The table the reader must produce from `scalars`, `scalars_dict` and
    /// `scalars_stored_schema`. The caller closes it.
    static VectorSchemaRoot expectedScalars(BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(scalarsArrowSchema(), allocator);
        root.allocateNew();
        BitVector flagReq = (BitVector) root.getVector("flag_req");
        fill(flagReq, ROWS, null, i -> flagReq.setSafe(i, FLAG_REQ[i] ? 1 : 0));
        BitVector flag = (BitVector) root.getVector("flag");
        fill(flag, ROWS, FLAG_NULLS, i -> flag.setSafe(i, FLAG[i] ? 1 : 0));
        IntVector i32Req = (IntVector) root.getVector("i32_req");
        fill(i32Req, ROWS, null, i -> i32Req.setSafe(i, I32_REQ[i]));
        IntVector i32 = (IntVector) root.getVector("i32");
        fill(i32, ROWS, I32_NULLS, i -> i32.setSafe(i, I32[i]));
        TinyIntVector i8 = (TinyIntVector) root.getVector("i8");
        fill(i8, ROWS, I8_NULLS, i -> i8.setSafe(i, (byte) I8[i]));
        SmallIntVector i16 = (SmallIntVector) root.getVector("i16");
        fill(i16, ROWS, I16_NULLS, i -> i16.setSafe(i, (short) I16[i]));
        UInt1Vector u8 = (UInt1Vector) root.getVector("u8");
        fill(u8, ROWS, U8_NULLS, i -> u8.setSafe(i, U8[i]));
        UInt2Vector u16 = (UInt2Vector) root.getVector("u16");
        fill(u16, ROWS, U16_NULLS, i -> u16.setSafe(i, U16[i]));
        UInt4Vector u32 = (UInt4Vector) root.getVector("u32");
        fill(u32, ROWS, U32_NULLS, i -> u32.setSafe(i, U32[i]));
        BigIntVector i64 = (BigIntVector) root.getVector("i64");
        fill(i64, ROWS, I64_NULLS, i -> i64.setSafe(i, I64[i]));
        UInt8Vector u64 = (UInt8Vector) root.getVector("u64");
        fill(u64, ROWS, U64_NULLS, i -> u64.setSafe(i, U64[i]));
        Float4Vector f32 = (Float4Vector) root.getVector("f32");
        fill(f32, ROWS, F32_NULLS, i -> f32.setSafe(i, F32[i]));
        Float8Vector f64 = (Float8Vector) root.getVector("f64");
        fill(f64, ROWS, F64_NULLS, i -> f64.setSafe(i, F64[i]));
        DateDayVector day = (DateDayVector) root.getVector("day");
        fill(day, ROWS, nulls(DAY), i -> day.setSafe(i, Math.toIntExact(DAY[i].toEpochDay())));
        TimeMilliVector tMs = (TimeMilliVector) root.getVector("t_ms");
        fill(tMs, ROWS, nulls(T_MS), i -> tMs.setSafe(i, Math.toIntExact(T_MS[i].toNanoOfDay() / 1_000_000L)));
        TimeMicroVector tUs = (TimeMicroVector) root.getVector("t_us");
        fill(tUs, ROWS, nulls(T_US), i -> tUs.setSafe(i, T_US[i].toNanoOfDay() / 1_000L));
        TimeNanoVector tNs = (TimeNanoVector) root.getVector("t_ns");
        fill(tNs, ROWS, T_NS_NULLS, i -> tNs.setSafe(i, T_NS[i]));
        TimeStampMilliTZVector tsMs = (TimeStampMilliTZVector) root.getVector("ts_ms");
        fill(tsMs, ROWS, nulls(TS_MS), i -> tsMs.setSafe(i, TS_MS[i].toEpochMilli()));
        TimeStampMicroTZVector tsUs = (TimeStampMicroTZVector) root.getVector("ts_us");
        fill(tsUs, ROWS, nulls(TS_US), i -> tsUs.setSafe(i, ChronoUnit.MICROS.between(Instant.EPOCH, TS_US[i])));
        TimeStampNanoTZVector tsNs = (TimeStampNanoTZVector) root.getVector("ts_ns");
        fill(tsNs, ROWS, TS_NS_NULLS, i -> tsNs.setSafe(i, TS_NS[i]));
        TimeStampMicroVector tsLocal = (TimeStampMicroVector) root.getVector("ts_local");
        fill(tsLocal, ROWS, nulls(TS_LOCAL),
                i -> tsLocal.setSafe(i, ChronoUnit.MICROS.between(LocalDateTime.of(1970, 1, 1, 0, 0), TS_LOCAL[i])));
        DecimalVector decI32 = (DecimalVector) root.getVector("dec_i32");
        fill(decI32, ROWS, DEC_I32_NULLS, i -> decI32.setSafe(i, BigDecimal.valueOf(DEC_I32[i], 2)));
        DecimalVector decI64 = (DecimalVector) root.getVector("dec_i64");
        fill(decI64, ROWS, DEC_I64_NULLS, i -> decI64.setSafe(i, BigDecimal.valueOf(DEC_I64[i], 4)));
        DecimalVector dec128 = (DecimalVector) root.getVector("dec128");
        fill(dec128, ROWS, nulls(DEC128), i -> dec128.setSafe(i, DEC128[i]));
        Decimal256Vector dec256 = (Decimal256Vector) root.getVector("dec256");
        fill(dec256, ROWS, nulls(DEC256), i -> dec256.setSafe(i, DEC256[i]));
        VarCharVector textReq = (VarCharVector) root.getVector("text_req");
        fill(textReq, ROWS, null, i -> textReq.setSafe(i, utf8(TEXT_REQ[i])));
        VarCharVector text = (VarCharVector) root.getVector("text");
        fill(text, ROWS, nulls(TEXT), i -> text.setSafe(i, utf8(TEXT[i])));
        VarBinaryVector blob = (VarBinaryVector) root.getVector("blob");
        fill(blob, ROWS, nulls(BLOB), i -> blob.setSafe(i, BLOB[i]));
        FixedSizeBinaryVector fixed4 = (FixedSizeBinaryVector) root.getVector("fixed4");
        fill(fixed4, ROWS, nulls(FIXED4), i -> fixed4.setSafe(i, FIXED4[i]));
        FixedSizeBinaryVector id = (FixedSizeBinaryVector) root.getVector("id");
        fill(id, ROWS, nulls(ID), i -> id.setSafe(i, uuidBytes(ID[i])));
        VarCharVector doc = (VarCharVector) root.getVector("doc");
        fill(doc, ROWS, nulls(DOC), i -> doc.setSafe(i, utf8(DOC[i])));
        VarBinaryVector label = (VarBinaryVector) root.getVector("label");
        fill(label, ROWS, nulls(LABEL), i -> label.setSafe(i, utf8(LABEL[i])));
        VarBinaryVector bson = (VarBinaryVector) root.getVector("bson");
        fill(bson, ROWS, nulls(BSON), i -> bson.setSafe(i, BSON[i]));
        Float2Vector half = (Float2Vector) root.getVector("half");
        fill(half, ROWS, HALF_NULLS, i -> half.setSafeWithPossibleTruncate(i, HALF[i]));
        FixedSizeBinaryVector span = (FixedSizeBinaryVector) root.getVector("span");
        fill(span, ROWS, nulls(SPAN), i -> span.setSafe(i, SPAN[i]));
        root.setRowCount(ROWS);
        return root;
    }

    /// Sets rows `0..rows` of `vector`: null where `nulls` says so, `setter` otherwise.
    private static void fill(ValueVector vector, int rows, boolean[] nulls, IntConsumer setter) {
        for (int i = 0; i < rows; i++) {
            if (nulls != null && nulls[i]) {
                setNull(vector, i);
            }
            else {
                setter.accept(i);
            }
        }
    }

    private static void setNull(ValueVector vector, int i) {
        if (vector instanceof BaseFixedWidthVector fixed) {
            fixed.setNull(i);
        }
        else {
            ((BaseVariableWidthVector) vector).setNull(i);
        }
    }

    static Schema emptyArrowSchema() {
        return new Schema(List.of(
                field("n", true, new ArrowType.Int(32, true)),
                field("text", true, ArrowType.Utf8.INSTANCE),
                field("flag", true, ArrowType.Bool.INSTANCE),
                field("dec", true, new ArrowType.Decimal(10, 2, 128))));
    }

    static VectorSchemaRoot expectedEmpty(BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(emptyArrowSchema(), allocator);
        root.setRowCount(0);
        return root;
    }

    // ==================== Value helpers ====================

    private static Field field(String name, boolean nullable, ArrowType type) {
        return new Field(name, new FieldType(nullable, type, null, null), null);
    }

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

    /// The payloads with a placeholder of `width` bytes where the value is null, which the
    /// writer ignores but will not accept as a Java `null`.
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

    /// Big-endian two's-complement unscaled values, sign-extended to `width` bytes, which is
    /// the `FIXED_LEN_BYTE_ARRAY` layout of a decimal.
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

    /// Little-endian IEEE half-precision payloads, the `FLOAT16` layout.
    private static byte[][] halfBytes(float[] values) {
        byte[][] result = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            short half = Float.floatToFloat16(values[i]);
            result[i] = new byte[]{ (byte) half, (byte) (half >>> 8) };
        }
        return result;
    }
}
