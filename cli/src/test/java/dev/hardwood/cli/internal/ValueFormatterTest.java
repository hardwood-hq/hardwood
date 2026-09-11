/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.VariantType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Rendering contracts for [ValueFormatter] across its sources: reader-backed
/// rows, materialised values, and raw dictionary primitives. The reader leg
/// runs against `interval_logical_type_test.parquet` (row 0: 1 month, 15 days,
/// 1 hour; row 1: 30 days; row 2: null); the dictionary and materialised legs
/// build their columns in-test.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ValueFormatterTest {

    private static final int NO_LIMIT = BinaryValues.NO_LIMIT;

    private SchemaNode durationField;
    private int durationIdx;
    private String row0Compact;
    private String row0Expanded;
    private String row1Compact;
    private String row2Compact;

    // Row 0 of dive_screenshots_fixture.parquet: a four-field `bbox` struct and
    // a two-element list-of-structs `addresses` — the smallest shapes that
    // exercise the nested walkers.
    private String bboxCapped;
    private String bboxWhole;
    private String addressesCapped;
    private String addressesWhole;
    private String addressesExpanded;
    private String bboxMaterialised;

    @BeforeAll
    void readIntervalFixture() throws IOException {
        Path file = Path.of(getClass().getResource("/interval_logical_type_test.parquet").getPath());
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            FileSchema schema = fileReader.getFileSchema();
            durationField = schema.getField("duration");
            durationIdx = schema.getColumn("duration").columnIndex();

            rowReader.next();
            row0Compact = ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.Style.PREVIEW, ValueFormatter.PREVIEW_CELL_BUDGET);
            row0Expanded = ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.Style.EXPANDED, NO_LIMIT);
            rowReader.next();
            row1Compact = ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.Style.PREVIEW, ValueFormatter.PREVIEW_CELL_BUDGET);
            rowReader.next();
            row2Compact = ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.Style.PREVIEW, ValueFormatter.PREVIEW_CELL_BUDGET);
        }
    }

    @BeforeAll
    void readDiveFixtureRow0() throws Exception {
        withDiveFixtureReader((rowReader, schema) -> {
            int bbox = rootFieldIndex(schema, "bbox");
            int addresses = rootFieldIndex(schema, "addresses");
            SchemaNode bboxField = schema.getField("bbox");
            SchemaNode addressesField = schema.getField("addresses");
            rowReader.next();
            bboxCapped = ValueFormatter.formatReader(rowReader, bbox, bboxField, true,
                    ValueFormatter.Style.PREVIEW, 100);
            bboxWhole = ValueFormatter.formatReader(rowReader, bbox, bboxField, true,
                    ValueFormatter.Style.COMPACT, NO_LIMIT);
            addressesCapped = ValueFormatter.formatReader(rowReader, addresses, addressesField, true,
                    ValueFormatter.Style.PREVIEW, 100);
            addressesWhole = ValueFormatter.formatReader(rowReader, addresses, addressesField, true,
                    ValueFormatter.Style.COMPACT, NO_LIMIT);
            addressesExpanded = ValueFormatter.formatReader(rowReader, addresses, addressesField, true,
                    ValueFormatter.Style.EXPANDED, NO_LIMIT);
            bboxMaterialised = display(rowReader.getValue(bbox), bboxField);
        });
    }

    /// Opens the interval fixture for a test that formats live rows — e.g. to
    /// assert a contract thrown before any row is touched.
    private interface IntervalReaderCase {
        void run(RowReader rowReader) throws IOException;
    }

    private void withIntervalReader(IntervalReaderCase testCase) throws IOException {
        Path file = Path.of(getClass().getResource("/interval_logical_type_test.parquet").getPath());
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            testCase.run(rowReader);
        }
    }

    private interface DiveReaderCase {
        void run(RowReader rowReader, FileSchema schema) throws Exception;
    }

    private void withDiveFixtureReader(DiveReaderCase testCase) throws Exception {
        Path file = Path.of(getClass().getResource("/dive_screenshots_fixture.parquet").getPath());
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            testCase.run(rowReader, fileReader.getFileSchema());
        }
    }

    private static int rootFieldIndex(FileSchema schema, String name) {
        return schema.getRootNode().children().indexOf(schema.getField(name));
    }


    // ==================== dictionary source ====================

    @Test
    void timestampMicrosUtc() {
        ColumnSchema col = column(PhysicalType.INT64,
                LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS));

        // 2025-01-01T00:00:00.000000Z
        long micros = 1735689600_000_000L;

        assertThat(ValueFormatter.formatDictionary(micros, col, true, NO_LIMIT))
                .isEqualTo("2025-01-01T00:00:00Z");
    }

    @Test
    void timestampMicrosNotUtcRendersAsLocalDateTime() {
        ColumnSchema col = column(PhysicalType.INT64,
                LogicalType.timestamp(false, LogicalType.TimeUnit.MICROS));
        long micros = 1735689600_000_000L;

        // Local-wall-clock timestamp: no trailing 'Z', and LocalDateTime.toString
        // omits the seconds field when it is zero.
        assertThat(ValueFormatter.formatDictionary(micros, col, true, NO_LIMIT))
                .isEqualTo("2025-01-01T00:00");
    }

    @Test
    void dateRendersAsLocalDate() {
        ColumnSchema col = column(PhysicalType.INT32, LogicalType.date());
        // 2025-04-24 = epoch day 20202
        assertThat(ValueFormatter.formatDictionary(20202, col, true, NO_LIMIT))
                .isEqualTo("2025-04-24");
    }

    @Test
    void timeMicrosRendersAsLocalTime() {
        ColumnSchema col = column(PhysicalType.INT64,
                LogicalType.time(false, LogicalType.TimeUnit.MICROS));
        long micros = (12L * 3600 + 34 * 60 + 56) * 1_000_000L;
        assertThat(ValueFormatter.formatDictionary(micros, col, true, NO_LIMIT))
                .isEqualTo("12:34:56");
    }

    @Test
    void stringBytesDecodedAsUtf8() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, LogicalType.string());
        byte[] bytes = "héllo".getBytes(StandardCharsets.UTF_8);
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, NO_LIMIT)).isEqualTo("héllo");
    }

    @Test
    void float16BytesDecodeToFloat() {
        // Half-precision 1.5 = sign 0 | exponent 01111 (15) | fraction 1000000000
        // = 0x3E00, little-endian → 0x00, 0x3E.
        ColumnSchema col = column(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.float16());
        byte[] fp16 = { 0x00, 0x3E };
        assertThat(ValueFormatter.formatDictionary(fp16, col, true, NO_LIMIT)).isEqualTo("1.5");
    }

    @Test
    void rawLongFallbackWithoutLogicalType() {
        ColumnSchema col = column(PhysicalType.INT64, null);
        assertThat(ValueFormatter.formatDictionary(42L, col, true, NO_LIMIT)).isEqualTo("42");
    }

    @Test
    void unsignedInt32() {
        ColumnSchema col = column(PhysicalType.INT32, LogicalType.intType(32, false));
        assertThat(ValueFormatter.formatDictionary(-1, col, true, NO_LIMIT))
                .isEqualTo("4294967295");
    }

    @Test
    void rawBinaryWithoutLogicalTypeRendersAsHex() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, null);
        byte[] bytes = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, NO_LIMIT))
                .isEqualTo("0xdeadbeef");
    }

    @Test
    void printableBinaryWithoutLogicalTypeRendersAsString() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, null);
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, NO_LIMIT))
                .isEqualTo("hello");
    }

    @Test
    void intervalDictionaryBytesRenderAsComponents() {
        ColumnSchema col = column(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.interval());
        // 1 month, 15 days, 3_600_000 ms — little-endian unsigned 32-bit
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(1);
        bb.putInt(15);
        bb.putInt(3_600_000);
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, NO_LIMIT))
                .isEqualTo("1mo 15d 3600000ms");
    }

    @Test
    void dictionaryNullRendersAsNull() {
        ColumnSchema col = column(PhysicalType.INT64, null);
        assertThat(ValueFormatter.formatDictionary(null, col, true, NO_LIMIT)).isEqualTo("null");
    }

    @Test
    void emptyDictionaryBytesRenderEmptyWithAndWithoutAnnotation() {
        ColumnSchema annotated = column(PhysicalType.BYTE_ARRAY, LogicalType.string());
        ColumnSchema unannotated = column(PhysicalType.BYTE_ARRAY, null);
        assertThat(ValueFormatter.formatDictionary(new byte[0], annotated, true, NO_LIMIT)).isEmpty();
        assertThat(ValueFormatter.formatDictionary(new byte[0], unannotated, true, NO_LIMIT)).isEmpty();
    }

    @Test
    void dictionaryPrimitiveMismatchFailsFast() {
        // A Long dictionary value can never back DATE (an INT32 logical type).
        ColumnSchema col = column(PhysicalType.INT64, LogicalType.date());
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(42L, col, true, NO_LIMIT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Column 'value' has logical type DATE, which INT64 values cannot carry");
    }

    @Test
    void unknownDictionaryPrimitiveFailsFast() {
        ColumnSchema col = column(PhysicalType.INT64, null);
        assertThatThrownBy(() -> ValueFormatter.formatDictionary("not-a-primitive", col, true, NO_LIMIT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Dictionary records carry Integer, Long, Float, Double or byte[] values, got "
                        + "java.lang.String");
    }

    // ==================== control-character sanitisation ====================

    @Test
    void dictionaryStringWithEmbeddedControlRendersMiddleDot() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, LogicalType.string());
        assertThat(ValueFormatter.formatDictionary("A\u0001B".getBytes(StandardCharsets.UTF_8), col, true, NO_LIMIT))
                .isEqualTo("A·B");
    }

    @Test
    void dictionaryStringWithTabAndNewlineRendersMiddleDots() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, LogicalType.string());
        assertThat(ValueFormatter.formatDictionary("tab\tsep".getBytes(StandardCharsets.UTF_8), col, true, NO_LIMIT))
                .isEqualTo("tab·sep");
        assertThat(ValueFormatter.formatDictionary("line\nbreak".getBytes(StandardCharsets.UTF_8), col, true, NO_LIMIT))
                .isEqualTo("line·break");
    }

    @Test
    void allControlDictionaryStringRendersAsUtf8Hex() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, LogicalType.string());
        assertThat(ValueFormatter.formatDictionary(new byte[19], col, true, NO_LIMIT))
                .isEqualTo("0x" + "0".repeat(38));
    }

    @Test
    void mixedControlsWithPrintableTextStayText() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, LogicalType.string());
        // Exactly one printable character after the controls: still text.
        assertThat(ValueFormatter.formatDictionary("\u0001\u0002A\u0003".getBytes(StandardCharsets.UTF_8), col, true,
                NO_LIMIT))
                .isEqualTo("··A·");
    }

    @Test
    void materialisedStringWithControlRendersMiddleDot() {
        assertThat(display("A\u0001B", primitive(PhysicalType.BYTE_ARRAY,
                LogicalType.string()))).isEqualTo("A·B");
    }

    @Test
    void materialisedAnnotatedBytesWithControlRenderMiddleDots() {
        assertThat(display("tab\tsep".getBytes(StandardCharsets.UTF_8),
                primitive(PhysicalType.BYTE_ARRAY, LogicalType.string())))
                .isEqualTo("tab·sep");
    }

    // ==================== materialised source ====================

    @Test
    void materialisedNullRendersAsNull() {
        assertThat(display(null, primitive(PhysicalType.BYTE_ARRAY, null)))
                .isEqualTo("null");
    }

    @Test
    void materialisedBareByteArrayAsStringWhenValidUtf8() {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        assertThat(display(bytes, primitive(PhysicalType.BYTE_ARRAY, null)))
                .isEqualTo("hello");
    }

    @Test
    void materialisedBareByteArrayAsHexWhenInvalidUtf8() {
        // Lone continuation byte — not a valid UTF-8 sequence.
        byte[] bytes = { (byte) 0xC3, (byte) 0x28, (byte) 0xA0, (byte) 0xA1 };
        assertThat(display(bytes, primitive(PhysicalType.BYTE_ARRAY, null)))
                .isEqualTo("0xc328a0a1");
    }

    @Test
    void materialisedUnsignedInt32() {
        SchemaNode node = primitive(PhysicalType.INT32, LogicalType.intType(32, false));
        assertThat(display(-1, node)).isEqualTo("4294967295");
    }

    @Test
    void materialisedDecimalRendersPlainString() {
        SchemaNode node = primitive(PhysicalType.INT32, LogicalType.decimal(9, 7));
        // BigDecimal.toString would give "1E-7"; the canonical form is plain.
        assertThat(display(new BigDecimal("0.0000001"), node))
                .isEqualTo("0.0000001");
    }

    @Test
    void decimalRendersIdenticallyAcrossDictionaryAndMaterialisedSources() {
        ColumnSchema col = column(PhysicalType.INT32, LogicalType.decimal(9, 7));
        SchemaNode node = primitive(PhysicalType.INT32, LogicalType.decimal(9, 7));
        // BigInteger.ONE.toByteArray() — the unscaled Int32 encoding of
        // 0.0000001 at scale 7.
        byte[] unscaledOne = BigInteger.ONE.toByteArray();

        assertThat(ValueFormatter.formatDictionary(unscaledOne, col, true, NO_LIMIT))
                .isEqualTo(display(new BigDecimal("0.0000001"), node))
                .isEqualTo("0.0000001");
    }

    @Test
    void materialisedInt96EpochBytesRenderAsInstant() {
        // INT96 is little-endian: 8 bytes nanos-of-day, 4 bytes Julian day.
        // The Unix epoch is Julian day 2440588 with zero nanos-of-day.
        byte[] epochBytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(epochBytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(0, 0L);
        bb.putInt(8, 2440588);
        assertThat(display(epochBytes, primitive(PhysicalType.INT96, null)))
                .isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void materialisedUuidWrongLengthFails() {
        assertThatThrownBy(() -> display(new byte[15],
                primitive(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.uuid())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field 'f': UUID requires exactly 16 bytes, got 15");
    }

    @Test
    void materialisedIntervalWrongLengthFails() {
        assertThatThrownBy(() -> display(new byte[11],
                primitive(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.interval())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field 'f': INTERVAL requires exactly 12 bytes, got 11");
    }

    @Test
    void materialisedInt96WrongLengthFails() {
        assertThatThrownBy(() -> display(new byte[16], primitive(PhysicalType.INT96, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field 'f': INT96 requires exactly 12 bytes, got 16");
    }

    @Test
    void emptyMaterialisedMapUsesCanonicalEmptyGrammar() {
        PqMap emptyMap = (PqMap) Proxy.newProxyInstance(
                PqMap.class.getClassLoader(),
                new Class<?>[] { PqMap.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "isEmpty" -> true;
                    case "size" -> 0;
                    case "getEntries" -> List.of();
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        assertThat(display(emptyMap, null)).isEqualTo("{}");
    }

    @Test
    void variantObjectDisplayWalkersSanitiseNamesAndUseRawLookupKeys() {
        String[] rawNames = { "bad\nname\u001bkey", "tab\tname", "nul\u0000name", "plain" };
        PqVariantObject object = variantObjectWithFields(rawNames);
        PqVariant value = (PqVariant) Proxy.newProxyInstance(
                PqVariant.class.getClassLoader(),
                new Class<?>[] { PqVariant.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "type" -> VariantType.OBJECT;
                    case "asObject" -> object;
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        assertThat(display(value, null))
                .isEqualTo("{ bad·name·key : x, tab·name : x, nul·name : x, plain : x }");
        assertThat(ValueFormatter.formatValue(value, null, ValueFormatter.Style.PREVIEW, 100))
                .isEqualTo("{ bad·name·key : x, tab·name : x, nul·name : x, …+1 }");
        assertThat(ValueFormatter.formatValue(value, null, ValueFormatter.Style.EXPORT, NO_LIMIT))
                .isEqualTo("{\"bad\\nname\\u001bkey\": \"x\", \"tab\\tname\": \"x\", \"nul\\u0000name\": \"x\", \"plain\": \"x\"}");
        assertThat(ValueFormatter.formatValue(value, null, ValueFormatter.Style.EXPANDED, NO_LIMIT))
                .isEqualTo("""
                        {
                          bad·name·key: x,
                          tab·name: x,
                          nul·name: x,
                          plain: x
                        }""");
    }

    private static PqVariantObject variantObjectWithFields(String[] rawNames) {
        return (PqVariantObject) Proxy.newProxyInstance(
                PqVariantObject.class.getClassLoader(),
                new Class<?>[] { PqVariantObject.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "getFieldCount" -> rawNames.length;
                    case "getFieldName" -> rawNames[(int) args[0]];
                    case "getVariant" -> {
                        String requestedName = (String) args[0];
                        if (!Arrays.asList(rawNames).contains(requestedName)) {
                            throw new AssertionError("Lookup used sanitized field name: " + requestedName);
                        }
                        yield stringVariant("x");
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static PqVariant stringVariant(String value) {
        return scalarVariant(VariantType.STRING, "asString", value);
    }

    /// A Variant scalar of `type` whose `accessor` returns `value`.
    private static PqVariant scalarVariant(VariantType type, String accessor, Object value) {
        return (PqVariant) Proxy.newProxyInstance(
                PqVariant.class.getClassLoader(),
                new Class<?>[] { PqVariant.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("type")) {
                        return type;
                    }
                    if (method.getName().equals(accessor)) {
                        return value;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    @Test
    void nestedValueIntoScalarSchemaFails() {
        SchemaNode group = FileSchema.builder("m")
                .struct("s", RepetitionType.OPTIONAL,
                        b -> b.addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build()
                .getField("s");
        assertThatThrownBy(() -> display(42, group))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Field 's' is a group in the schema, but the value is a java.lang.Integer");
    }

    // ==================== reader source ====================

    @Test
    void readerRendersIntervalComponents() {
        assertThat(row0Compact).isEqualTo("1mo 15d 3600000ms");
    }

    @Test
    void readerOmitsZeroComponentsAndRendersNullAsNull() {
        assertThat(row1Compact).isEqualTo("30d");
        assertThat(row2Compact).isEqualTo("null");
    }

    @Test
    void expandedMatchesCompactForPrimitiveLeaf() {
        assertThat(row0Expanded).isEqualTo(row0Compact);
    }

    /// PREVIEW caps each nested collection at three visible entries, at every
    /// depth: the struct's fourth member and the list element's fourth field
    /// each collapse into `…+N` — this is the text the dive preview cell clips.
    @Test
    void previewCapsNestedCollectionsAtThreeEntriesAndMarksTheRemainder() {
        assertThat(bboxCapped)
                .isEqualTo("{ xmin : -123.0, xmax : -122.5, ymin : 37.0, …+1 }");
        assertThat(addressesCapped)
                .isEqualTo("[{ freeform : 100 Main St, locality : New York, region : NA, …+1 }]");
    }

    /// COMPACT renders every nested entry; the caps belong to PREVIEW.
    @Test
    void compactRendersEveryNestedEntry() {
        assertThat(bboxWhole)
                .isEqualTo("{ xmin : -123.0, xmax : -122.5, ymin : 37.0, ymax : 37.4 }");
        assertThat(addressesWhole)
                .isEqualTo("[{ freeform : 100 Main St, locality : New York, region : NA,"
                        + " country : United States }]");
    }

    /// The EXPANDED style renders one nested entry per line at two-space
    /// indentation per level — the dive record modal's whole-value form.
    @Test
    void expandedRendersNestedEntriesIndentedOnePerLine() {
        assertThat(addressesExpanded).isEqualTo("""
                [
                  {
                    freeform: 100 Main St,
                    locality: New York,
                    region: NA,
                    country: United States
                  }
                ]""");
    }

    /// The reader and materialised sources render nested values through one
    /// walker, so a `print` cell spells a struct as the reader path does.
    @Test
    void materialisedWalkerRendersNestedValuesWhole() {
        assertThat(bboxMaterialised).isEqualTo(bboxWhole);
    }

    // ==================== budget contract ====================

    @Test
    void textBudgetsNeverCutTruncationIsTheCallersJob() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, LogicalType.string());
        assertThat(ValueFormatter.formatDictionary("hello".getBytes(StandardCharsets.UTF_8), col, true, 1))
                .isEqualTo("hello");
    }

    @Test
    void hexBudgetBuildsOnlyToBudgetWithOneByteOvershoot() {
        byte[] bytes = { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, null);

        // Full hex is 10 cells. A budget exactly the full length fits; a
        // shorter budget overshoots by one byte (so the caller sees there is
        // more and marks the cut) — budget 6 yields 8 cells, budget 5 yields 6.
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, 10)).isEqualTo("0xdeadbeef");
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, 6)).isEqualTo("0xdeadbe");
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, 5)).isEqualTo("0xdead");
    }

    @Test
    void readerAcceptsBudgetOfOne() throws IOException {
        withIntervalReader(rowReader -> {
            rowReader.next();
            assertThat(ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.Style.COMPACT, 1)).isEqualTo("1mo 15d 3600000ms");
        });
    }

    @Test
    void zeroBudgetRejectedOnEveryEntryPoint() throws IOException {
        assertBudgetRejectedOnEveryEntryPoint(0);
    }

    @Test
    void negativeBudgetBelowNoLimitRejectedOnEveryEntryPoint() throws IOException {
        assertBudgetRejectedOnEveryEntryPoint(-2);
    }

    private void assertBudgetRejectedOnEveryEntryPoint(int budget) throws IOException {
        String message = "budget must be BinaryValues.NO_LIMIT (-1, unlimited) or a positive number of terminal"
                + " cells, got " + budget;
        withIntervalReader(rowReader -> assertThatThrownBy(() -> ValueFormatter.formatReader(rowReader, durationIdx,
                durationField, true, ValueFormatter.Style.COMPACT, budget))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message));
        assertThatThrownBy(() -> ValueFormatter.formatValue("x", primitive(PhysicalType.BYTE_ARRAY, null),
                ValueFormatter.Style.COMPACT, budget))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(1, column(PhysicalType.INT32, null), true, budget))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[] { 1 }, stringColumn(), true, budget))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    // ==================== null dependencies ====================

    @Test
    void nullReaderRejected() {
        assertThatThrownBy(() -> ValueFormatter.formatReader(null, 0, durationField, true,
                ValueFormatter.Style.COMPACT, NO_LIMIT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("reader");
    }

    @Test
    void nullFieldRejected() throws IOException {
        withIntervalReader(rowReader -> assertThatThrownBy(() -> ValueFormatter.formatReader(rowReader, 0, null, true,
                ValueFormatter.Style.COMPACT, NO_LIMIT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("field"));
    }

    @Test
    void nullColumnRejected() {
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(1, null, true, NO_LIMIT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("col");
    }

    // ==================== raw statistics bytes ====================

    @Test
    void statsRendersPrintableString() {
        assertThat(ValueFormatter.formatBytes("hello".getBytes(StandardCharsets.UTF_8), stringColumn(), true))
                .isEqualTo("hello");
    }

    @Test
    void statsRendersNonAsciiPrintableString() {
        assertThat(ValueFormatter.formatBytes("Última".getBytes(StandardCharsets.UTF_8), stringColumn(), true))
                .isEqualTo("Última");
    }

    @Test
    void statsRendersLongStringInFull() {
        String longValue = "abcdefghijklmnopqrstuvwxyz";
        assertThat(ValueFormatter.formatBytes(longValue.getBytes(StandardCharsets.UTF_8), stringColumn(), true))
                .isEqualTo(longValue);
    }

    @Test
    void statsReplacesControlCharsWithPlaceholder() {
        byte[] mixed = { 'A', 0x01, 'B', 0x00, 'C' };
        assertThat(ValueFormatter.formatBytes(mixed, stringColumn(), true))
                .isEqualTo("A·B·C");
    }

    @Test
    void statsRendersAllControlBytesAsHex() {
        assertThat(ValueFormatter.formatBytes(new byte[19], stringColumn(), true))
                .isEqualTo("0x" + "00".repeat(19));
    }

    @Test
    void statsDistinctLongStringsRenderDistinctly() {
        String first = "the-quick-brown-fox-jumps-over-the-lazy-dog-0";
        String second = "the-quick-brown-fox-jumps-over-the-lazy-dog-1";

        assertThat(ValueFormatter.formatBytes(first.getBytes(StandardCharsets.UTF_8), stringColumn(), true))
                .isEqualTo(first);
        assertThat(ValueFormatter.formatBytes(second.getBytes(StandardCharsets.UTF_8), stringColumn(), true))
                .isEqualTo(second);
    }

    @Test
    void statsEmptyByteBackedValueRendersExplicitEmptyString() {
        // The `isByteBacked` rule is physical: empty statistics bytes on any
        // BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY column render as an explicit ""
        // (distinguishing "present but empty" from an absent statistic).
        assertThat(ValueFormatter.formatBytes(new byte[0], stringColumn(), true)).isEqualTo("\"\"");
        assertThat(ValueFormatter.formatBytes(new byte[0], bareByteArrayColumn(), true)).isEqualTo("\"\"");
    }

    @Test
    void statsAbsentBytesRenderDash() {
        assertThat(ValueFormatter.formatBytes(null, stringColumn(), true)).isEqualTo("-");
        assertThat(ValueFormatter.formatBytes(null, stringColumn(), false)).isEqualTo("-");
    }

    @Test
    void statsDecodesInt32() {
        byte[] bytes = { 0x2A, 0x00, 0x00, 0x00 };
        assertThat(ValueFormatter.formatBytes(bytes, intColumn(), true)).isEqualTo("42");
    }

    @Test
    void statsRendersTimestampMicrosLogically() {
        ColumnSchema col = timestampColumn(true, LogicalType.TimeUnit.MICROS);
        // 2025-01-01T00:00:00Z = 1735689600_000_000 micros, little-endian INT64
        byte[] bytes = littleEndian(1735689600_000_000L);
        assertThat(ValueFormatter.formatBytes(bytes, col, true)).isEqualTo("2025-01-01T00:00:00Z");
    }

    @Test
    void statsPhysicalModeRendersTimestampAsRawLong() {
        ColumnSchema col = timestampColumn(true, LogicalType.TimeUnit.MICROS);
        long micros = 1735689600_000_000L;
        assertThat(ValueFormatter.formatBytes(littleEndian(micros), col, false))
                .isEqualTo(Long.toString(micros));
    }

    @Test
    void statsRendersDateLogically() {
        // epoch day 20202 = 2025-04-24, little-endian INT32
        ColumnSchema col = column(PhysicalType.INT32, LogicalType.date());
        byte[] bytes = littleEndian(20202);
        assertThat(ValueFormatter.formatBytes(bytes, col, true)).isEqualTo("2025-04-24");
    }

    @Test
    void statsRendersIntervalLogically() {
        // 1 month, 15 days, 3_600_000 ms — little-endian unsigned 32-bit
        assertThat(ValueFormatter.formatBytes(intervalBytes(), intervalColumn(), true))
                .isEqualTo("1mo 15d 3600000ms");
    }

    @Test
    void statsIntervalPhysicalModeRendersAsHex() {
        assertThat(ValueFormatter.formatBytes(intervalBytes(), intervalColumn(), false))
                .isEqualTo("0x" + HexFormat.of().formatHex(intervalBytes()));
    }

    /// INT96 statistics bounds render as the timestamp in logical mode — the
    /// same text `print` and the dictionary surfaces show — instead of bare
    /// hex (#1021 canonicalisation).
    @Test
    void statsInt96RendersAsInstantInLogicalMode() {
        byte[] epochBytes = int96EpochBytes();
        assertThat(ValueFormatter.formatBytes(epochBytes, int96Column(), true))
                .isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void statsInt96PhysicalModeRendersAs0xPrefixedHex() {
        byte[] epochBytes = int96EpochBytes();
        assertThat(ValueFormatter.formatBytes(epochBytes, int96Column(), false))
                .isEqualTo("0x" + HexFormat.of().formatHex(epochBytes));
    }

    @Test
    void int96RendersIdenticallyAcrossMaterialisedAndStatisticsSources() {
        byte[] epochBytes = int96EpochBytes();
        assertThat(ValueFormatter.formatBytes(epochBytes, int96Column(), true))
                .isEqualTo(display(epochBytes, primitive(PhysicalType.INT96, null)))
                .isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void int96RendersCanonicalTimestampAcrossEverySource() throws IOException {
        Path file = Path.of(getClass().getResource("/int96_timestamp_test.parquet").getPath());
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            FileSchema schema = fileReader.getFileSchema();
            SchemaNode field = schema.getField("ts");
            int index = schema.getColumn("ts").columnIndex();
            rowReader.next();
            byte[] raw = (byte[]) rowReader.getRawValue(index);
            String expected = "2026-03-05T09:30:00.123456Z";

            assertThat(ValueFormatter.formatReader(rowReader, index, field, true,
                    ValueFormatter.Style.COMPACT, 1)).isEqualTo(expected);
            assertThat(ValueFormatter.formatDictionary(raw, int96Column(), true, 1)).isEqualTo(expected);
            assertThat(display(raw, field)).isEqualTo(expected);
            assertThat(ValueFormatter.formatBytes(raw, int96Column(), true, 1)).isEqualTo(expected);
        }
    }

    @Test
    void int96PhysicalModeRendersHexAcrossEverySource() throws IOException {
        Path file = Path.of(getClass().getResource("/int96_timestamp_test.parquet").getPath());
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            FileSchema schema = fileReader.getFileSchema();
            SchemaNode field = schema.getField("ts");
            int index = schema.getColumn("ts").columnIndex();
            rowReader.next();
            byte[] raw = (byte[]) rowReader.getRawValue(index);
            String expected = "0x" + HexFormat.of().formatHex(raw);

            assertThat(ValueFormatter.formatReader(rowReader, index, field, false,
                    ValueFormatter.Style.COMPACT, NO_LIMIT)).isEqualTo(expected);
            assertThat(ValueFormatter.formatDictionary(raw, int96Column(), false, NO_LIMIT)).isEqualTo(expected);
            assertThat(ValueFormatter.formatBytes(raw, int96Column(), false, NO_LIMIT)).isEqualTo(expected);
        }
    }

    @Test
    void int96DictionaryRendersMalformedWidthsAsHexInBothModes() {
        for (int length : new int[] { 0, 11, 13 }) {
            for (boolean logical : new boolean[] { true, false }) {
                assertThat(ValueFormatter.formatDictionary(new byte[length], int96Column(), logical, NO_LIMIT))
                        .isEqualTo(zeroHex(length));
            }
        }
    }

    @Test
    void int96DictionaryNullRemainsNullInBothModes() {
        assertThat(ValueFormatter.formatDictionary(null, int96Column(), true, NO_LIMIT)).isEqualTo("null");
        assertThat(ValueFormatter.formatDictionary(null, int96Column(), false, NO_LIMIT)).isEqualTo("null");
    }

    @Test
    void statsInt96MalformedWidthsRenderAsHexInBothModes() {
        for (int length : new int[] { 11, 13, 16 }) {
            for (boolean logical : new boolean[] { true, false }) {
                assertThat(ValueFormatter.formatBytes(new byte[length], int96Column(), logical, NO_LIMIT))
                        .isEqualTo(zeroHex(length));
            }
        }
    }

    /// GeoParquet 1.x predates the `GEOMETRY` logical type and stores WKB in a
    /// bare `BYTE_ARRAY`, so the writer's min/max are opaque bytes. Rendering
    /// them as lenient UTF-8 turned every bound into mojibake.
    @Test
    void statsUnannotatedBinaryBoundsDoNotRenderAsText() {
        assertThat(ValueFormatter.formatBytes(WKB_POINT, bareByteArrayColumn(), true))
                .isEqualTo("0x010100000000000000005366c0f71622f0fa1955c0");
    }

    /// A capped cell shows a marked prefix of the hex, the same treatment a
    /// long string gets — enough to tell two bounds apart.
    @Test
    void statsUnannotatedBinaryBoundsHonourAnExplicitBudget() {
        assertThat(ValueFormatter.formatBytes(WKB_POINT, bareByteArrayColumn(), true, 20))
                .isEqualTo("0x01010000000000000000");
    }

    @Test
    void statsAnnotatedStringBoundsKeepRenderingAsText() {
        byte[] bytes = { 'a', 0x00, 'b' };

        assertThat(ValueFormatter.formatBytes(bytes, stringColumn(), true)).isEqualTo("a·b");
    }

    @Test
    void statsNullColumnRejected() {
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[] { 1 }, null, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("col");
    }

    // ==================== decoded dictionary entries ====================

    @Test
    void decodedDecimalRendersPlainString() {
        // String.valueOf(BigDecimal) would give "1E-7"; the canonical form is
        // plain — the same text every other source shows for the value.
        ColumnSchema col = column(PhysicalType.INT32, LogicalType.decimal(9, 7));
        assertThat(ValueFormatter.formatDecoded(1, col)).isEqualTo("0.0000001");
    }

    @Test
    void decodedDecimalRendersIdenticallyAcrossDecodedAndMaterialisedSources() {
        ColumnSchema col = column(PhysicalType.INT32, LogicalType.decimal(9, 7));
        SchemaNode node = primitive(PhysicalType.INT32, LogicalType.decimal(9, 7));

        assertThat(ValueFormatter.formatDecoded(1, col))
                .isEqualTo(display(new BigDecimal("0.0000001"), node))
                .isEqualTo("0.0000001");
    }

    @Test
    void decodedIntAndLongRenderUnsignedAndSigned() {
        ColumnSchema unsigned32 = column(PhysicalType.INT32, LogicalType.intType(32, false));
        ColumnSchema signed64 = column(PhysicalType.INT64, null);

        assertThat(ValueFormatter.formatDecoded(-1, unsigned32)).isEqualTo("4294967295");
        assertThat(ValueFormatter.formatDecoded(-1L, signed64)).isEqualTo("-1");
    }

    @Test
    void decodedFloatAndDoubleRenderAsJavaText() {
        assertThat(ValueFormatter.formatDecoded(1.5f)).isEqualTo("1.5");
        assertThat(ValueFormatter.formatDecoded(-2.25d)).isEqualTo("-2.25");
    }

    @Test
    void decodedSchemaBearingOverloadsRejectNullColumn() {
        assertThatThrownBy(() -> ValueFormatter.formatDecoded(1, (ColumnSchema) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("col");
        assertThatThrownBy(() -> ValueFormatter.formatDecoded(1L, (ColumnSchema) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("col");
    }

    // ==================== interval helpers ====================

    @Test
    void intervalRendersAsReadableComponents() {
        // Row 0 from interval_logical_type_test.parquet: 1 month, 15 days, 1 hour (3_600_000 ms)
        assertThat(ValueFormatter.formatInterval(new PqInterval(1, 15, 3_600_000)))
                .isEqualTo("1mo 15d 3600000ms");
    }

    @Test
    void intervalWithZeroComponentsOmitsThem() {
        assertThat(ValueFormatter.formatInterval(new PqInterval(0, 30, 0)))
                .isEqualTo("30d");
    }

    @Test
    void intervalAllZeroRendersAsZeroMs() {
        assertThat(ValueFormatter.formatInterval(new PqInterval(0, 0, 0)))
                .isEqualTo("0ms");
    }

    @Test
    void intervalAtMaxUnsigned32BitRenders() {
        // 0xFFFFFFFF = 4_294_967_295 — the upper bound of the on-disk
        // unsigned-32-bit encoding, which `PqInterval` exposes verbatim as a long.
        long maxUint32 = 0xFFFFFFFFL;
        assertThat(ValueFormatter.formatInterval(new PqInterval(maxUint32, maxUint32, maxUint32)))
                .isEqualTo("4294967295mo 4294967295d 4294967295ms");
    }

    private static SchemaNode.PrimitiveNode primitive(PhysicalType type, LogicalType logical) {
        return new SchemaNode.PrimitiveNode("f", type, RepetitionType.REQUIRED, logical, 0, 0, 0);
    }

    private static ColumnSchema column(PhysicalType type, LogicalType logical) {
        return new ColumnSchema(
                FieldPath.of("value"),
                type,
                RepetitionType.REQUIRED,
                null,
                0,
                0,
                0,
                logical);
    }

    // ==================== statistics/decoded helpers ====================

    private static ColumnSchema stringColumn() {
        return new ColumnSchema(FieldPath.of("s"), PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                null, 0, 1, 0, LogicalType.string());
    }

    private static ColumnSchema intColumn() {
        return new ColumnSchema(FieldPath.of("i"), PhysicalType.INT32, RepetitionType.OPTIONAL,
                null, 0, 1, 0, null);
    }

    private static ColumnSchema bareByteArrayColumn() {
        return new ColumnSchema(FieldPath.of("geometry"), PhysicalType.BYTE_ARRAY,
                RepetitionType.OPTIONAL, null, 0, 1, 0, null);
    }

    private static ColumnSchema timestampColumn(boolean isUtc, LogicalType.TimeUnit unit) {
        return new ColumnSchema(FieldPath.of("ts"), PhysicalType.INT64, RepetitionType.OPTIONAL,
                null, 0, 1, 0, LogicalType.timestamp(isUtc, unit));
    }

    private static ColumnSchema intervalColumn() {
        return new ColumnSchema(FieldPath.of("iv"), PhysicalType.FIXED_LEN_BYTE_ARRAY,
                RepetitionType.OPTIONAL, null, 0, 1, 0, LogicalType.interval());
    }

    private static ColumnSchema int96Column() {
        return new ColumnSchema(FieldPath.of("ts96"), PhysicalType.INT96, RepetitionType.OPTIONAL,
                null, 0, 1, 0, null);
    }

    private static byte[] littleEndian(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] littleEndian(long value) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (value >> (i * 8));
        }
        return bytes;
    }

    private static byte[] intervalBytes() {
        // 1 month, 15 days, 3_600_000 ms — little-endian unsigned 32-bit fields
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(1);
        bb.putInt(15);
        bb.putInt(3_600_000);
        return bytes;
    }

    private static byte[] int96EpochBytes() {
        // INT96 is little-endian: 8 bytes nanos-of-day, 4 bytes Julian day.
        // The Unix epoch is Julian day 2440588 with zero nanos-of-day.
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(0, 0L);
        bb.putInt(8, 2440588);
        return bytes;
    }

    /// A WKB `Point` — the payload GeoParquet 1.x stores in an unannotated
    /// `BYTE_ARRAY` geometry column.
    private static final byte[] WKB_POINT =
            HexFormat.of().parseHex("010100000000000000005366c0f71622f0fa1955c0");

    @Test
    void nullColumnIsRejectedBeforeAbsentByteShortcut() {
        assertThatThrownBy(() -> ValueFormatter.formatBytes(null, null, true, NO_LIMIT))
                .isInstanceOf(NullPointerException.class).hasMessage("col");
    }

    /// A schema-less nested value walks whole, the way legacy list and map
    /// layouts without resolvable child schemas always have — the walkers pass
    /// `null` child schemas when the group or element node does not resolve.
    @Test
    void materialisedNestedValueWalksSchemaLessWithoutAResolvableSchema() throws Exception {
        withDiveFixtureReader((rowReader, schema) -> {
            int bbox = rootFieldIndex(schema, "bbox");
            rowReader.next();
            assertThat(display(rowReader.getValue(bbox), null))
                    .isEqualTo(bboxMaterialised);
        });
    }

    /// A fixed-width payload of the wrong length is not the value its type
    /// claims; the statistics and dictionary paths show its bytes.
    @Test
    void malformedFixedLengthDictionaryAndStatisticsValuesRenderAsHex() {
        ColumnSchema uuid = byteBackedColumn(LogicalType.uuid());
        ColumnSchema interval = intervalColumn();
        ColumnSchema float16 = byteBackedColumn(LogicalType.float16());

        assertThat(ValueFormatter.formatDictionary(new byte[15], uuid, true, NO_LIMIT)).isEqualTo(zeroHex(15));
        assertThat(ValueFormatter.formatBytes(new byte[15], uuid, true, NO_LIMIT)).isEqualTo(zeroHex(15));
        assertThat(ValueFormatter.formatDictionary(new byte[11], interval, true, NO_LIMIT)).isEqualTo(zeroHex(11));
        assertThat(ValueFormatter.formatBytes(new byte[11], interval, true, NO_LIMIT)).isEqualTo(zeroHex(11));
        assertThat(ValueFormatter.formatDictionary(new byte[0], interval, true, NO_LIMIT)).isEqualTo(zeroHex(0));
        assertThat(ValueFormatter.formatDictionary(new byte[3], float16, true, NO_LIMIT)).isEqualTo(zeroHex(3));
    }

    private static ColumnSchema byteBackedColumn(LogicalType logical) {
        return new ColumnSchema(FieldPath.of("value"), PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                null, 0, 1, 0, logical);
    }

    // ==================== one walker, every style ====================

    /// Core hands an unsigned leaf inside a struct or list back as its signed
    /// Java value; the walker resolves the leaf's schema, so every source and
    /// style renders it unsigned.
    @Test
    void nestedUnsignedIntegersRenderUnsignedFromEverySource(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("nested_unsigned.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.REQUIRED, struct -> struct
                        .addColumn("u32", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.intType(32, false))
                        .addColumn("u64", PhysicalType.INT64, RepetitionType.REQUIRED, LogicalType.intType(64, false)))
                .list("l", RepetitionType.REQUIRED,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.REQUIRED,
                                LogicalType.intType(32, false)))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.rowWriter().writeRow(row -> row
                    .setStruct("s", struct -> struct.setInt("u32", -1).setLong("u64", -1L))
                    .setList("l", list -> list.addInt(-1)));
        }

        String struct = "{ u32 : 4294967295, u64 : 18446744073709551615 }";
        String list = "[4294967295]";
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            FileSchema fileSchema = fileReader.getFileSchema();
            int structIndex = rootFieldIndex(fileSchema, "s");
            int listIndex = rootFieldIndex(fileSchema, "l");
            SchemaNode structField = fileSchema.getField("s");
            SchemaNode listField = fileSchema.getField("l");
            rowReader.next();
            for (ValueFormatter.Style style : List.of(ValueFormatter.Style.COMPACT, ValueFormatter.Style.PREVIEW)) {
                assertThat(ValueFormatter.formatReader(rowReader, structIndex, structField, true, style, NO_LIMIT))
                        .isEqualTo(struct);
                assertThat(ValueFormatter.formatReader(rowReader, listIndex, listField, true, style, NO_LIMIT))
                        .isEqualTo(list);
            }
            assertThat(ValueFormatter.formatReader(rowReader, structIndex, structField, true,
                    ValueFormatter.Style.EXPANDED, NO_LIMIT)).isEqualTo("""
                            {
                              u32: 4294967295,
                              u64: 18446744073709551615
                            }""");
            assertThat(display(rowReader.getValue(structIndex), structField)).isEqualTo(struct);
            assertThat(display(rowReader.getValue(listIndex), listField)).isEqualTo(list);

            String structJson = "{\"u32\": 4294967295, \"u64\": 18446744073709551615}";
            assertThat(ValueFormatter.formatReader(rowReader, structIndex, structField, true,
                    ValueFormatter.Style.EXPORT, NO_LIMIT)).isEqualTo(structJson);
            assertThat(ValueFormatter.formatValue(rowReader.getValue(structIndex), structField,
                    ValueFormatter.Style.EXPORT, NO_LIMIT)).isEqualTo(structJson);
            assertThat(ValueFormatter.formatReader(rowReader, listIndex, listField, true,
                    ValueFormatter.Style.EXPORT, NO_LIMIT)).isEqualTo(list);
        }
    }

    @Test
    void exportStyleWritesControlCharactersVerbatim() {
        SchemaNode string = primitive(PhysicalType.BYTE_ARRAY, LogicalType.string());
        assertThat(ValueFormatter.formatValue("A\u0001B", string, ValueFormatter.Style.EXPORT, NO_LIMIT))
                .isEqualTo("A\u0001B");
        assertThat(ValueFormatter.formatValue("tab\tsep".getBytes(StandardCharsets.UTF_8), string,
                ValueFormatter.Style.EXPORT, NO_LIMIT))
                .isEqualTo("tab\tsep");
    }

    // ==================== Variant scalars ====================

    /// A Variant timestamp without a time zone reads as wall-clock time — the
    /// text a `TIMESTAMP` column not adjusted to UTC shows — in the display
    /// grammar and in JSON alike.
    @Test
    void variantTimestampWithoutZoneRendersAsLocalDateTime() {
        Instant instant = Instant.parse("2026-01-01T09:30:00Z");
        PqVariant ntz = scalarVariant(VariantType.TIMESTAMP_NTZ, "asTimestamp", instant);
        PqVariant utc = scalarVariant(VariantType.TIMESTAMP, "asTimestamp", instant);

        assertThat(display(ntz, null)).isEqualTo("2026-01-01T09:30");
        assertThat(export(ntz)).isEqualTo("\"2026-01-01T09:30\"");
        assertThat(display(utc, null)).isEqualTo("2026-01-01T09:30:00Z");
        assertThat(export(utc)).isEqualTo("\"2026-01-01T09:30:00Z\"");
    }

    @Test
    void variantJsonQuotesNonFiniteNumbersAndKeepsStringsVerbatim() throws Exception {
        assertThat(export(scalarVariant(VariantType.FLOAT, "asFloat", Float.NaN)))
                .isEqualTo("\"NaN\"");
        assertThat(export(scalarVariant(VariantType.DOUBLE, "asDouble", Double.POSITIVE_INFINITY)))
                .isEqualTo("\"Infinity\"");
        assertThat(export(scalarVariant(VariantType.DOUBLE, "asDouble", Double.NEGATIVE_INFINITY)))
                .isEqualTo("\"-Infinity\"");
        assertThat(export(scalarVariant(VariantType.FLOAT, "asFloat", 1.5f))).isEqualTo("1.5");

        String json = export(scalarVariant(VariantType.STRING, "asString", "A\u0001B"));
        assertThat(json).isEqualTo("\"A\\u0001B\"");
        JSONAssert.assertEquals("[\"A\\u0001B\"]", "[" + json + "]", JSONCompareMode.STRICT);
    }

    // ==================== EXPORT writes JSON ====================

    /// EXPORT writes nested values as JSON from both sources: struct fields as
    /// object members, list elements as array items, strings quoted and
    /// numbers bare.
    @Test
    void exportWritesNestedValuesAsTypedJson() throws Exception {
        withDiveFixtureReader((rowReader, schema) -> {
            int bbox = rootFieldIndex(schema, "bbox");
            int addresses = rootFieldIndex(schema, "addresses");
            SchemaNode bboxField = schema.getField("bbox");
            SchemaNode addressesField = schema.getField("addresses");
            rowReader.next();

            String bboxJson = ValueFormatter.formatReader(rowReader, bbox, bboxField, true,
                    ValueFormatter.Style.EXPORT, NO_LIMIT);
            String addressesJson = ValueFormatter.formatReader(rowReader, addresses, addressesField, true,
                    ValueFormatter.Style.EXPORT, NO_LIMIT);

            assertThat(bboxJson).isEqualTo("{\"xmin\": -123.0, \"xmax\": -122.5, \"ymin\": 37.0, \"ymax\": 37.4}");
            assertThat(ValueFormatter.formatValue(rowReader.getValue(bbox), bboxField, ValueFormatter.Style.EXPORT,
                    NO_LIMIT)).isEqualTo(bboxJson);
            assertThat(addressesJson).isEqualTo("[{\"freeform\": \"100 Main St\", \"locality\": \"New York\","
                    + " \"region\": \"NA\", \"country\": \"United States\"}]");
            JSONAssert.assertEquals("""
                    [{"freeform": "100 Main St", "locality": "New York", "region": "NA", "country": "United States"}]""",
                    addressesJson, JSONCompareMode.STRICT);
        });
    }

    /// A map exports as an object keyed by the key's text and a list as an
    /// array; a null item is JSON null, and NaN, which JSON numbers cannot
    /// express, is a string.
    @Test
    void exportWritesMapKeysAsTextAndNonFiniteFloatsAsStrings() {
        assertThat(export(mapOf(1, Float.NaN))).isEqualTo("{\"1\": \"NaN\"}");
        assertThat(export(listOf(true, null, 2.5d))).isEqualTo("[true, null, 2.5]");
    }

    private static PqMap mapOf(Object key, Object value) {
        PqMap.Entry entry = (PqMap.Entry) Proxy.newProxyInstance(
                PqMap.Entry.class.getClassLoader(),
                new Class<?>[] { PqMap.Entry.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "getKey", "getRawKey" -> key;
                    case "getValue", "getRawValue" -> value;
                    case "isValueNull" -> value == null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (PqMap) Proxy.newProxyInstance(
                PqMap.class.getClassLoader(),
                new Class<?>[] { PqMap.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "getEntries" -> List.of(entry);
                    case "isEmpty" -> false;
                    case "size" -> 1;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static PqList listOf(Object... items) {
        return (PqList) Proxy.newProxyInstance(
                PqList.class.getClassLoader(),
                new Class<?>[] { PqList.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "size" -> items.length;
                    case "isEmpty" -> items.length == 0;
                    case "isNull" -> items[(int) args[0]] == null;
                    case "get" -> items[(int) args[0]];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    // ==================== values that do not decode ====================

    /// A statistic whose width does not match its physical type is not a value
    /// of that type; its bytes show as hex.
    @Test
    void statsOfTheWrongWidthRenderAsHex() {
        byte[] three = { 1, 2, 3 };
        assertThat(ValueFormatter.formatBytes(three, intColumn(), true)).isEqualTo("0x010203");
        assertThat(ValueFormatter.formatBytes(three, column(PhysicalType.INT32, LogicalType.date()), true))
                .isEqualTo("0x010203");
        assertThat(ValueFormatter.formatBytes(three, column(PhysicalType.INT64, null), true)).isEqualTo("0x010203");
        assertThat(ValueFormatter.formatBytes(three, column(PhysicalType.DOUBLE, null), true)).isEqualTo("0x010203");
        assertThat(ValueFormatter.formatBytes(new byte[] { 1, 0 }, column(PhysicalType.BOOLEAN, null), true))
                .isEqualTo("0x0100");
    }

    /// A TIME outside a day is no time of day: statistics and dictionary entries
    /// show the stored integer.
    @Test
    void timeOutsideADayRendersAsItsStoredInteger() {
        ColumnSchema micros = column(PhysicalType.INT64, LogicalType.time(false, LogicalType.TimeUnit.MICROS));
        ColumnSchema millis = column(PhysicalType.INT32, LogicalType.time(false, LogicalType.TimeUnit.MILLIS));

        assertThat(ValueFormatter.formatBytes(littleEndian(-1L), micros, true)).isEqualTo("-1");
        assertThat(ValueFormatter.formatDictionary(-1L, micros, true, NO_LIMIT)).isEqualTo("-1");
        assertThat(ValueFormatter.formatDecoded(-1L, micros)).isEqualTo("-1");
        assertThat(ValueFormatter.formatDictionary(86_400_000, millis, true, NO_LIMIT)).isEqualTo("86400000");
        assertThat(ValueFormatter.formatDecoded(86_400_000, millis)).isEqualTo("86400000");
        assertThat(ValueFormatter.formatDecoded(86_399_999, millis)).isEqualTo("23:59:59.999");
    }

    // ==================== physical toggle, field names ====================

    /// With the physical toggle off, list elements render their stored values,
    /// as struct fields and map entries do.
    @Test
    void physicalModeReadsListElementsRaw() {
        SchemaNode dates = FileSchema.builder("m")
                .list("dates", RepetitionType.REQUIRED,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.date()))
                .build()
                .getField("dates");
        PqList list = (PqList) Proxy.newProxyInstance(
                PqList.class.getClassLoader(),
                new Class<?>[] { PqList.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "size" -> 1;
                    case "isEmpty", "isNull" -> false;
                    case "get" -> LocalDate.of(2025, 4, 24);
                    case "getRaw" -> 20202;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        RowReader reader = (RowReader) Proxy.newProxyInstance(
                RowReader.class.getClassLoader(),
                new Class<?>[] { RowReader.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "isNull" -> false;
                    case "getValue" -> list;
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        assertThat(ValueFormatter.formatReader(reader, 0, dates, true, ValueFormatter.Style.COMPACT, NO_LIMIT))
                .isEqualTo("[2025-04-24]");
        assertThat(ValueFormatter.formatReader(reader, 0, dates, false, ValueFormatter.Style.COMPACT, NO_LIMIT))
                .isEqualTo("[20202]");
    }

    /// A struct field name is file metadata: the display styles sanitise it,
    /// and EXPORT writes it verbatim for JSON to escape.
    @Test
    void structFieldNamesAreSanitisedForDisplayAndEscapedForExport() {
        String name = "a" + (char) 10 + "b";
        PqStruct struct = (PqStruct) Proxy.newProxyInstance(
                PqStruct.class.getClassLoader(),
                new Class<?>[] { PqStruct.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "getFieldCount" -> 1;
                    case "getFieldName" -> name;
                    case "isNull" -> false;
                    case "getValue", "getRawValue" -> 1;
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        assertThat(display(struct, null)).isEqualTo("{ a·b : 1 }");
        assertThat(export(struct)).isEqualTo("{\"a\\nb\": 1}");
    }

    /// A `convert` rendering of a materialised value.
    private static String export(Object value) {
        return ValueFormatter.formatValue(value, null, ValueFormatter.Style.EXPORT, NO_LIMIT);
    }

    /// A `print` cell's rendering of a materialised value.
    private static String display(Object value, SchemaNode field) {
        return ValueFormatter.formatValue(value, field, ValueFormatter.Style.COMPACT, NO_LIMIT);
    }

    private static String zeroHex(int length) {
        return "0x" + "00".repeat(length);
    }
}
