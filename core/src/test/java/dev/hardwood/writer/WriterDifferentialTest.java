/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential test for the writer, the inverse direction of [dev.hardwood.DifferentialReadTest]:
/// hardwood writes a Parquet file and DuckDB reads it back through `read_parquet`.
/// DuckDB is an independent engine, so agreement proves the produced bytes are
/// spec-correct, not merely consistent with hardwood's own reader.
///
/// Each row carries a synthetic `r` index column so the comparison is robust to
/// scan order via `ORDER BY r`.
class WriterDifferentialTest {

    @Test
    void duckDbReadsWrittenInts(@TempDir Path dir) throws Exception {
        // Boundary values exercise the signed little-endian PLAIN INT32 encoding.
        int[] v = { 0, 1, -1, 42, -100_000, 123_456, Integer.MAX_VALUE, Integer.MIN_VALUE };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();

        Path file = dir.resolve("written.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).ints(1, v));
        }

        List<Integer> actual = new ArrayList<>();
        long rowCount;
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            String from = "read_parquet('" + file.toAbsolutePath() + "')";
            try (ResultSet rs = stmt.executeQuery("SELECT v FROM " + from + " ORDER BY r")) {
                while (rs.next()) {
                    actual.add(rs.getInt("v"));
                }
            }
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) AS n FROM " + from)) {
                rs.next();
                rowCount = rs.getLong("n");
            }
        }

        List<Integer> expected = new ArrayList<>(v.length);
        for (int value : v) {
            expected.add(value);
        }
        assertThat(rowCount).isEqualTo(v.length);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsMultiPageColumn(@TempDir Path dir) throws Exception {
        // More than one target page, so the column is written across several pages.
        int n = 600_000;
        int[] v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("multipage.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, v));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, sum(v) AS s, max(v) AS mx FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            assertThat(rs.getLong("n")).isEqualTo(n);
            assertThat(rs.getLong("s")).isEqualTo((long) n * (n - 1) / 2);
            assertThat(rs.getInt("mx")).isEqualTo(n - 1);
        }
    }

    @Test
    void duckDbReadsNullableInts(@TempDir Path dir) throws Exception {
        // Interior, leading and trailing nulls, with signed extremes at present rows.
        int[] v = { 0, 0, -1, 0, Integer.MAX_VALUE, Integer.MIN_VALUE, 0 };
        boolean[] nulls = { true, false, false, true, false, false, true };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();

        Path file = dir.resolve("nullable.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).ints(1, v, nulls));
        }

        List<Integer> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                int value = rs.getInt("v");
                actual.add(rs.wasNull() ? null : value);
            }
        }

        List<Integer> expected = new ArrayList<>(v.length);
        for (int i = 0; i < v.length; i++) {
            expected.add(nulls[i] ? null : v[i]);
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsAllNullColumn(@TempDir Path dir) throws Exception {
        int n = 1_000;
        int[] r = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            r[i] = i;
            nulls[i] = true;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();

        Path file = dir.resolve("allnull.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).ints(1, new int[n], nulls));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, count(v) AS present FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            assertThat(rs.getLong("n")).isEqualTo(n);
            assertThat(rs.getLong("present")).isZero();
        }
    }

    @Test
    void duckDbReadsStruct(@TempDir Path dir) throws Exception {
        // required int32 r; optional group address { required int32 street; optional int32 zip }.
        // DuckDB resolves address.street / address.zip and must see the field absent wherever
        // the struct is null, and zip absent where zip itself is null.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, s -> s
                        .addColumn("street", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] r = { 0, 1, 2, 3 };
        Validity addressNulls = Validity.ofNulls(new boolean[] { true, false, false, false });
        int[] street = { 0, 10, 20, 30 };
        int[] zip = { 0, 0, 200, 300 };
        boolean[] zipNulls = { false, true, false, false };

        Path file = dir.resolve("struct.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("r", r)
                    .struct("address", addressNulls)
                    .ints("address.street", street)
                    .ints("address.zip", zip, zipNulls));
        }

        List<Integer> streets = new ArrayList<>();
        List<Integer> zips = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT address.street AS street, address.zip AS zip FROM read_parquet('"
                                + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                int st = rs.getInt("street");
                streets.add(rs.wasNull() ? null : st);
                int zp = rs.getInt("zip");
                zips.add(rs.wasNull() ? null : zp);
            }
        }

        assertThat(streets).containsExactly(null, 10, 20, 30);
        assertThat(zips).containsExactly(null, null, 200, 300);
    }

    @Test
    void duckDbReadsListOfInts(@TempDir Path dir) throws Exception {
        // required int32 r; optional list<optional int32> phones. Absent list, empty list,
        // and a null element must all survive to DuckDB distinctly.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("phones", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] r = { 0, 1, 2, 3 };
        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 1, 2, 3, 0, 5 };
        boolean[] elementNulls = { false, false, false, true, false };

        Path file = dir.resolve("listofints.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("r", r)
                    .list("phones", offsets, listNulls)
                    .ints("phones.list.element", elements, elementNulls));
        }

        List<List<Integer>> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT phones FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                Array array = rs.getArray("phones");
                if (rs.wasNull()) {
                    actual.add(null);
                    continue;
                }
                List<Integer> list = new ArrayList<>();
                for (Object element : (Object[]) array.getArray()) {
                    list.add(element == null ? null : ((Number) element).intValue());
                }
                actual.add(list);
            }
        }

        assertThat(actual).containsExactly(List.of(1, 2), List.of(), null, Arrays.asList(3, null, 5));
    }

    @Test
    void duckDbReadsDictionaryEncodedInts(@TempDir Path dir) throws Exception {
        // Low cardinality: a small dictionary and narrow RLE_DICTIONARY indices that DuckDB
        // must resolve through the dictionary page.
        int n = 500;
        int[] palette = { 11, -7, 11, 100_000, -7 };
        int[] v = new int[n];
        int[] r = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = palette[i % palette.length];
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();

        Path file = dir.resolve("dict.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).ints(1, v));
        }

        List<Integer> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getInt("v"));
            }
        }

        List<Integer> expected = new ArrayList<>(n);
        for (int value : v) {
            expected.add(value);
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsPlainChunkOfHighCardinalityColumn(@TempDir Path dir) throws Exception {
        // Every value distinct, so the chunk is written PLAIN with no dictionary page. DuckDB
        // must read a column chunk that carries no dictionary at all.
        int n = 5_000;
        int[] v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = i * 7 - 3;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.defaults();
        Path file = dir.resolve("fallback.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, v));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            assertThat(rs.getLong("n")).isEqualTo(n);
            long sum = 0;
            for (int value : v) {
                sum += value;
            }
            assertThat(rs.getLong("s")).isEqualTo(sum);
            assertThat(rs.getInt("mn")).isEqualTo(-3);
            assertThat(rs.getInt("mx")).isEqualTo((n - 1) * 7 - 3);
        }
    }

    @Test
    void duckDbReadsDictionaryEncodedList(@TempDir Path dir) throws Exception {
        // A dictionary-encoded LIST<INT32> element column: the index section behind the rep/def
        // levels must resolve through the dictionary for DuckDB to reassemble the lists.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] r = { 0, 1, 2, 3 };
        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 4, 4, 4, 0, 9 };
        boolean[] elementNulls = { false, false, false, true, false };

        Path file = dir.resolve("dictlist.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("r", r)
                    .list("v", offsets, listNulls)
                    .ints("v.list.element", elements, elementNulls));
        }

        List<List<Integer>> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                Array array = rs.getArray("v");
                if (rs.wasNull()) {
                    actual.add(null);
                    continue;
                }
                List<Integer> list = new ArrayList<>();
                for (Object element : (Object[]) array.getArray()) {
                    list.add(element == null ? null : ((Number) element).intValue());
                }
                actual.add(list);
            }
        }

        assertThat(actual).containsExactly(List.of(4, 4), List.of(), null, Arrays.asList(4, null, 9));
    }

    @Test
    void duckDbReadsMapOfIntToInt(@TempDir Path dir) throws Exception {
        // required int32 r; optional map<int32, optional int32> props. An absent map, an
        // empty map, and a null value must all survive to DuckDB distinctly.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] r = { 0, 1, 2, 3 };
        int[] offsets = { 0, 2, 2, 2, 3 };
        Validity mapNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] keys = { 1, 2, 3 };
        int[] values = { 10, 0, 30 };
        boolean[] valueNulls = { false, true, false };

        Path file = dir.resolve("mapofints.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("r", r)
                    .map("props", offsets, mapNulls)
                    .ints("props.key_value.key", keys)
                    .ints("props.key_value.value", values, valueNulls));
        }

        List<Boolean> nullFlags = new ArrayList<>();
        List<List<Integer>> keyLists = new ArrayList<>();
        List<List<Integer>> valueLists = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT props IS NULL AS is_null, map_keys(props) AS ks, map_values(props) AS vs "
                                + "FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                nullFlags.add(rs.getBoolean("is_null"));
                keyLists.add(toIntList(rs.getArray("ks")));
                valueLists.add(toIntList(rs.getArray("vs")));
            }
        }

        assertThat(nullFlags).containsExactly(false, false, true, false);
        assertThat(keyLists).containsExactly(List.of(1, 2), List.of(), null, List.of(3));
        assertThat(valueLists).containsExactly(Arrays.asList(10, null), List.of(), null, List.of(30));
    }

    /// A map key is annotated like any other primitive. Without the annotation DuckDB infers
    /// `MAP(BLOB, INTEGER)` and no string lookup works; with it the map is the
    /// `MAP(VARCHAR, INTEGER)` callers expect.
    @Test
    void duckDbReadsWrittenMapWithAnAnnotatedKey(@TempDir Path dir) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        byte[][] keys = { "alpha".getBytes(StandardCharsets.UTF_8), "beta".getBytes(StandardCharsets.UTF_8) };

        Path file = dir.resolve("mapwithstringkey.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("r", new int[] { 0 })
                    .map("props", new int[] { 0, 2 }, Validity.ofNulls(new boolean[] { false }))
                    .bytes("props.key_value.key", keys)
                    .ints("props.key_value.value", new int[] { 10, 20 }));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT typeof(props) AS t, map_extract(props, 'beta')[1] AS beta "
                                + "FROM read_parquet('" + file.toAbsolutePath() + "')")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("t")).isEqualTo("MAP(VARCHAR, INTEGER)");
            assertThat(rs.getInt("beta")).isEqualTo(20);
        }
    }

    private static List<Integer> toIntList(Array array) throws Exception {
        if (array == null) {
            return null;
        }
        List<Integer> list = new ArrayList<>();
        for (Object element : (Object[]) array.getArray()) {
            list.add(element == null ? null : ((Number) element).intValue());
        }
        return list;
    }

    @Test
    void duckDbReadsDeltaBinaryPackedLongs(@TempDir Path dir) throws Exception {
        // A second reader for the encodings stage 19b taught the writer. DuckDB shares no code
        // with either side of hardwood, so agreement here is about the bytes rather than about
        // hardwood's encoder and decoder agreeing with each other.
        ColumnEncoding encoding = ColumnEncoding.DELTA_BINARY_PACKED;
        int n = 20_000;
        int[] r = new int[n];
        long[] v = new long[n];
        long timestamp = 1_700_000_000_000L;
        for (int i = 0; i < n; i++) {
            r[i] = i;
            // Values that move in small steps at a large magnitude: what delta encoding is for,
            // and the shape whose deltas need far fewer bits than the values do.
            timestamp += 1 + (i % 7);
            v[i] = timestamp;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .encoding("v", encoding)
                .pageTargetBytes(4096)
                .build();
        Path file = dir.resolve("encoded.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).longs(1, v));
        }

        List<Long> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getLong("v"));
            }
        }

        List<Long> expected = new ArrayList<>(n);
        for (long value : v) {
            expected.add(value);
        }
        assertThat(actual).as("%s values", encoding).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsByteStreamSplitDoubles(@TempDir Path dir) throws Exception {
        // Floating point is what byte-stream-split is for, and it is also all DuckDB 1.4.4
        // accepts: it rejects the encoding outright for INT32, INT64 and FIXED_LEN_BYTE_ARRAY,
        // which the format has allowed since parquet-format 2.10 ("BYTE_STREAM_SPLIT encoding is
        // only supported for FLOAT or DOUBLE data"). Those three are not left unchecked — the
        // interop gate reads all of them back through parquet-java — so what is missing here is
        // DuckDB's second opinion on them, not coverage.
        int n = 20_000;
        int[] r = new int[n];
        double[] v = new double[n];
        for (int i = 0; i < n; i++) {
            r[i] = i;
            v[i] = 1000.0 + i * 0.015625;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .encoding("v", ColumnEncoding.BYTE_STREAM_SPLIT)
                .pageTargetBytes(4096)
                .build();
        Path file = dir.resolve("split.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).doubles(1, v));
        }

        List<Double> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getDouble("v"));
            }
        }

        List<Double> expected = new ArrayList<>(n);
        for (double value : v) {
            expected.add(value);
        }
        assertThat(actual).as("BYTE_STREAM_SPLIT values").containsExactlyElementsOf(expected);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = ColumnEncoding.class, names = { "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY" })
    void duckDbReadsOptionalByteArrayEncodings(ColumnEncoding encoding, @TempDir Path dir) throws Exception {
        // Sorted paths sharing long prefixes — what DELTA_BYTE_ARRAY targets — including the
        // empty value, which is the edge where a prefix and a suffix are both nothing.
        int n = 5_000;
        int[] r = new int[n];
        byte[][] v = new byte[n][];
        for (int i = 0; i < n; i++) {
            r[i] = i;
            v[i] = i == 0 ? new byte[0]
                    : ("/var/log/service/part-" + String.format("%06d", i)).getBytes(StandardCharsets.UTF_8);
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        LogicalType.string())
                .build();
        WriterConfig config = WriterConfig.builder()
                .encoding("v", encoding)
                .pageTargetBytes(4096)
                .build();
        Path file = dir.resolve("encoded.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).bytes(1, v));
        }

        List<String> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getString("v"));
            }
        }

        List<String> expected = new ArrayList<>(n);
        for (byte[] value : v) {
            expected.add(new String(value, StandardCharsets.UTF_8));
        }
        assertThat(actual).as("%s values", encoding).containsExactlyElementsOf(expected);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = CompressionCodec.class,
            names = { "UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4_RAW", "BROTLI" })
    void duckDbReadsEveryWritableCodec(CompressionCodec codec, @TempDir Path dir) throws Exception {
        // DuckDB shares no code with hardwood's compressors, so its agreement proves the
        // compressed bytes are the codec's own form rather than merely one hardwood's reader
        // accepts — the framing question every one of these codecs poses. Dictionary encoding is
        // disabled to force compressible PLAIN page bodies.
        int n = 20_000;
        int[] v = new int[n];
        int[] r = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = i % 100;
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder().encoding(ColumnEncoding.PLAIN).codec(codec).build();
        Path file = dir.resolve("compressed.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).ints(1, v));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, sum(v) AS s, max(v) AS mx FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            long sum = 0;
            long max = 0;
            for (int value : v) {
                sum += value;
                max = Math.max(max, value);
            }
            assertThat(rs.getLong("n")).as("%s row count", codec).isEqualTo(n);
            assertThat(rs.getLong("s")).as("%s value sum", codec).isEqualTo(sum);
            assertThat(rs.getLong("mx")).as("%s value max", codec).isEqualTo(max);
        }
    }

    @Test
    void duckDbReadsMultipleRowGroups(@TempDir Path dir) throws Exception {
        int n = 5_000;
        int[] v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        // A tiny row-group target forces the single batch to be split across many row
        // groups; DuckDB must read across all of them transparently.
        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(4096).build();
        Path file = dir.resolve("multirowgroup.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, v));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) AS n, sum(v) AS s, max(v) AS mx FROM read_parquet('"
                                + file.toAbsolutePath() + "')")) {
            rs.next();
            assertThat(rs.getLong("n")).isEqualTo(n);
            assertThat(rs.getLong("s")).isEqualTo((long) n * (n - 1) / 2);
            assertThat(rs.getInt("mx")).isEqualTo(n - 1);
        }
    }

    @Test
    void duckDbMetadataReportsWrittenStatistics(@TempDir Path dir) throws Exception {
        // DuckDB's parquet_metadata() decodes the statistics we wrote — the true inverse of the
        // differential for the metadata: an independent implementation reads our bytes back to
        // the expected values. The preferred min_value/max_value carry the signed bounds; the
        // deprecated min/max are absent (we never write them); the null count is exact.
        int[] v = { 42, -100_000, 7, Integer.MAX_VALUE, Integer.MIN_VALUE, 0 };

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("meta.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, v));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT stats_min, stats_max, stats_min_value, stats_max_value,"
                                + " stats_null_count, min_is_exact, max_is_exact"
                                + " FROM parquet_metadata('" + file.toAbsolutePath() + "') WHERE path_in_schema = 'v'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("stats_min_value")).isEqualTo(Integer.toString(Integer.MIN_VALUE));
            assertThat(rs.getString("stats_max_value")).isEqualTo(Integer.toString(Integer.MAX_VALUE));
            assertThat(rs.getString("stats_null_count")).isEqualTo("0");
            // The bounds are flagged exact, so a reader may treat them as actual values.
            assertThat(rs.getBoolean("min_is_exact")).isTrue();
            assertThat(rs.getBoolean("max_is_exact")).isTrue();
            // Deprecated min/max fields are never written; DuckDB reports them absent.
            assertThat(rs.getString("stats_min")).isNull();
            assertThat(rs.getString("stats_max")).isNull();
        }
    }

    @Test
    void duckDbMetadataReportsNestedListNullCount(@TempDir Path dir) throws Exception {
        // The trickiest statistic in stage 11: a LIST leaf's null_count counts every not-present
        // slot — a null list, an empty list, and a null element. DuckDB's parquet_metadata()
        // decodes it back to 3, independently confirming this matches the ecosystem convention
        // rather than only Hardwood's own reader; the bounds still cover only the present 1..5.
        // record 0: [1,2]; 1: [] (empty); 2: null list; 3: [3, null, 5].
        FileSchema schema = FileSchema.builder("schema")
                .list("phones", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 1, 2, 3, 0, 5 };
        boolean[] elementNulls = { false, false, false, true, false };

        Path file = dir.resolve("nestedstats.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("phones", offsets, listNulls)
                    .ints("phones.list.element", elements, elementNulls));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT stats_min_value, stats_max_value, stats_null_count"
                                + " FROM parquet_metadata('" + file.toAbsolutePath() + "')"
                                // DuckDB renders a leaf's path as its comma-joined schema components.
                                + " WHERE path_in_schema = 'phones, list, element'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("stats_null_count")).isEqualTo("3");
            assertThat(rs.getString("stats_min_value")).isEqualTo("1");
            assertThat(rs.getString("stats_max_value")).isEqualTo("5");
        }
    }

    @Test
    void duckDbFiltersOnWrittenStatisticsWithoutDroppingMatches(@TempDir Path dir) throws Exception {
        // Ascending values banded into small row groups, so the written per-chunk min/max let
        // DuckDB skip groups outside a range predicate. Correct statistics must not drop any
        // matching row: a range straddling several groups returns exactly the matching values.
        int n = 5_000;
        int[] v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(4096).build();
        Path file = dir.resolve("stats.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, v));
        }

        List<Integer> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath()
                                + "') WHERE v BETWEEN 2000 AND 3000 ORDER BY v")) {
            while (rs.next()) {
                actual.add(rs.getInt("v"));
            }
        }

        List<Integer> expected = new ArrayList<>();
        for (int i = 2000; i <= 3000; i++) {
            expected.add(i);
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsWrittenLongs(@TempDir Path dir) throws Exception {
        long[] v = { 0L, 1L, -1L, 1_000_000_000_000L, -5L, Long.MAX_VALUE, Long.MIN_VALUE };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("longs.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).longs(1, v));
        }

        List<Long> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getLong("v"));
            }
        }

        List<Long> expected = new ArrayList<>(v.length);
        for (long value : v) {
            expected.add(value);
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsWrittenDoublesIncludingNaNAndSignedZeros(@TempDir Path dir) throws Exception {
        double[] v = { 0.0, -0.0, 1.5, -2.5, Double.NaN, Double.MAX_VALUE, -Double.MAX_VALUE };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("doubles.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).doubles(1, v));
        }

        List<Double> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getDouble("v"));
            }
        }

        assertThat(actual).hasSize(v.length);
        for (int i = 0; i < v.length; i++) {
            if (Double.isNaN(v[i])) {
                assertThat(actual.get(i)).isNaN();
            }
            else {
                // Compare raw bits so a sign flip on zero would be caught.
                assertThat(Double.doubleToRawLongBits(actual.get(i)))
                        .isEqualTo(Double.doubleToRawLongBits(v[i]));
            }
        }
    }

    @Test
    void duckDbReadsWrittenFloats(@TempDir Path dir) throws Exception {
        float[] v = { 0.0f, -0.0f, 1.5f, -2.5f, Float.NaN, Float.MAX_VALUE, -Float.MAX_VALUE };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("floats.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).floats(1, v));
        }

        List<Float> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getFloat("v"));
            }
        }

        assertThat(actual).hasSize(v.length);
        for (int i = 0; i < v.length; i++) {
            if (Float.isNaN(v[i])) {
                assertThat(actual.get(i)).isNaN();
            }
            else {
                // Compare raw bits so a sign flip on zero would be caught.
                assertThat(Float.floatToRawIntBits(actual.get(i)))
                        .isEqualTo(Float.floatToRawIntBits(v[i]));
            }
        }
    }

    @Test
    void duckDbReadsWrittenBooleans(@TempDir Path dir) throws Exception {
        boolean[] v = { true, false, false, true, true, false, true, true, false };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("booleans.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).booleans(1, v));
        }

        List<Boolean> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT v FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getBoolean("v"));
            }
        }

        List<Boolean> expected = new ArrayList<>(v.length);
        for (boolean value : v) {
            expected.add(value);
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void duckDbReadsWrittenByteArrays(@TempDir Path dir) throws Exception {
        // Unannotated BYTE_ARRAY reads back as BLOB in DuckDB; compare the raw bytes. Includes an
        // empty value and a high-bit byte.
        byte[][] v = { "hello".getBytes(), new byte[0], "a longer value".getBytes(), { (byte) 0x80, 0x00 } };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("byte_arrays.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).bytes(1, v));
        }

        List<String> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT hex(v) AS h FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getString("h"));
            }
        }

        assertThat(actual).hasSize(v.length);
        for (int i = 0; i < v.length; i++) {
            assertThat(actual.get(i)).isEqualToIgnoringCase(HexFormat.of().formatHex(v[i]));
        }
    }

    @Test
    void duckDbReadsWrittenFixedLenByteArrays(@TempDir Path dir) throws Exception {
        byte[][] v = { "abcd".getBytes(), "wxyz".getBytes(), { (byte) 0xFF, (byte) 0xFF, 0x00, 0x01 } };
        int[] r = new int[v.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = i;
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4)
                .build();
        Path file = dir.resolve("fixed.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, r).fixed(1, v));
        }

        List<String> actual = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT hex(v) AS h FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY r")) {
            while (rs.next()) {
                actual.add(rs.getString("h"));
            }
        }

        assertThat(actual).hasSize(v.length);
        for (int i = 0; i < v.length; i++) {
            assertThat(actual.get(i)).isEqualToIgnoringCase(HexFormat.of().formatHex(v[i]));
        }
    }

    /// Logical type annotations are what make a column's bytes mean something to another
    /// engine: without them DuckDB sees a `BYTE_ARRAY` as `BLOB`, a `DATE` as `INTEGER`, and a
    /// `DECIMAL` as `BIGINT`. Asserting the column types DuckDB infers, and the values it
    /// decodes through them, proves the annotations landed on the wire.
    @Test
    void duckDbReadsWrittenLogicalTypes(@TempDir Path dir) throws Exception {
        int[] r = { 0, 1 };
        byte[][] names = { "alpha".getBytes(StandardCharsets.UTF_8), "beta".getBytes(StandardCharsets.UTF_8) };
        int[] days = { 0, 19_000 };
        long[] micros = { 0L, 1_700_000_000_000_000L };
        long[] unscaled = { 12_345L, -6_700L };
        byte[][] uuids = {
                HexFormat.of().parseHex("0123456789abcdef0123456789abcdef"),
                HexFormat.of().parseHex("ffffffffffffffffffffffffffffffff") };

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
                .addColumn("d", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.date())
                .addColumn("ts", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.timestamp(false, TimeUnit.MICROS))
                .addColumn("amount", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.decimal(18, 4))
                .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                        LogicalType.uuid())
                .build();

        Path file = dir.resolve("logical.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints(0, r)
                    .bytes(1, names)
                    .ints(2, days)
                    .longs(3, micros)
                    .longs(4, unscaled)
                    .fixed(5, uuids));
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT name, d, ts, amount, id FROM read_parquet('"
                        + file.toAbsolutePath() + "') ORDER BY r")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertThat(meta.getColumnTypeName(1)).isEqualTo("VARCHAR");
            assertThat(meta.getColumnTypeName(2)).isEqualTo("DATE");
            assertThat(meta.getColumnTypeName(3)).startsWith("TIMESTAMP");
            assertThat(meta.getColumnTypeName(4)).startsWith("DECIMAL");
            assertThat(meta.getColumnTypeName(5)).isEqualTo("UUID");

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("alpha");
            assertThat(rs.getDate("d").toLocalDate()).isEqualTo(LocalDate.of(1970, 1, 1));
            assertThat(rs.getBigDecimal("amount")).isEqualByComparingTo(new BigDecimal("1.2345"));
            assertThat(rs.getString("id")).isEqualTo("01234567-89ab-cdef-0123-456789abcdef");

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("beta");
            assertThat(rs.getDate("d").toLocalDate()).isEqualTo(LocalDate.of(1970, 1, 1).plusDays(19_000));
            // `ts` is annotated isAdjustedToUTC = false, so it is a wall clock with no zone
            // rather than an instant, and DuckDB surfaces it as TIMESTAMP. Reading it as an
            // Instant reinterprets that wall clock in the JVM's default zone, which holds only
            // where that zone happens to be UTC — see LocalTimestampTest for the same rule on
            // the read path. Comparing the wall clock is zone-independent.
            assertThat(rs.getTimestamp("ts").toLocalDateTime())
                    .isEqualTo(LocalDateTime.ofEpochSecond(1_700_000_000L, 0, ZoneOffset.UTC));
            assertThat(rs.getBigDecimal("amount")).isEqualByComparingTo(new BigDecimal("-0.6700"));
            assertThat(rs.next()).isFalse();
        }
    }
}
