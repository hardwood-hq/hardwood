/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HexFormat;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

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
                    ValueFormatter.NestedStyle.COMPACT, ValueFormatter.PREVIEW_CELL_BUDGET);
            row0Expanded = ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.NestedStyle.EXPANDED, NO_LIMIT);
            rowReader.next();
            row1Compact = ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.NestedStyle.COMPACT, ValueFormatter.PREVIEW_CELL_BUDGET);
            rowReader.next();
            row2Compact = ValueFormatter.formatReader(rowReader, durationIdx, durationField, true,
                    ValueFormatter.NestedStyle.COMPACT, ValueFormatter.PREVIEW_CELL_BUDGET);
        }
    }

    @BeforeAll
    void readDiveFixtureRow0() throws IOException {
        withDiveFixtureReader((rowReader, schema) -> {
            int bbox = rootFieldIndex(schema, "bbox");
            int addresses = rootFieldIndex(schema, "addresses");
            SchemaNode bboxField = schema.getField("bbox");
            SchemaNode addressesField = schema.getField("addresses");
            rowReader.next();
            bboxCapped = ValueFormatter.formatReader(rowReader, bbox, bboxField, true,
                    ValueFormatter.NestedStyle.COMPACT, 100);
            bboxWhole = ValueFormatter.formatReader(rowReader, bbox, bboxField, true,
                    ValueFormatter.NestedStyle.COMPACT, NO_LIMIT);
            addressesCapped = ValueFormatter.formatReader(rowReader, addresses, addressesField, true,
                    ValueFormatter.NestedStyle.COMPACT, 100);
            addressesWhole = ValueFormatter.formatReader(rowReader, addresses, addressesField, true,
                    ValueFormatter.NestedStyle.COMPACT, NO_LIMIT);
            addressesExpanded = ValueFormatter.formatReader(rowReader, addresses, addressesField, true,
                    ValueFormatter.NestedStyle.EXPANDED, NO_LIMIT);
            bboxMaterialised = ValueFormatter.formatValue(rowReader.getValue(bbox), bboxField, NO_LIMIT);
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
        void run(RowReader rowReader, FileSchema schema) throws IOException;
    }

    private void withDiveFixtureReader(DiveReaderCase testCase) throws IOException {
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
                new LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS));

        // 2025-01-01T00:00:00.000000Z
        long micros = 1735689600_000_000L;

        assertThat(ValueFormatter.formatDictionary(micros, col, true, NO_LIMIT))
                .isEqualTo("2025-01-01T00:00:00Z");
    }

    @Test
    void timestampMicrosNotUtcRendersAsLocalDateTime() {
        ColumnSchema col = column(PhysicalType.INT64,
                new LogicalType.TimestampType(false, LogicalType.TimeUnit.MICROS));
        long micros = 1735689600_000_000L;

        // Local-wall-clock timestamp: no trailing 'Z', and LocalDateTime.toString
        // omits the seconds field when it is zero.
        assertThat(ValueFormatter.formatDictionary(micros, col, true, NO_LIMIT))
                .isEqualTo("2025-01-01T00:00");
    }

    @Test
    void dateRendersAsLocalDate() {
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.DateType());
        // 2025-04-24 = epoch day 20202
        assertThat(ValueFormatter.formatDictionary(20202, col, true, NO_LIMIT))
                .isEqualTo("2025-04-24");
    }

    @Test
    void timeMicrosRendersAsLocalTime() {
        ColumnSchema col = column(PhysicalType.INT64,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MICROS));
        long micros = (12L * 3600 + 34 * 60 + 56) * 1_000_000L;
        assertThat(ValueFormatter.formatDictionary(micros, col, true, NO_LIMIT))
                .isEqualTo("12:34:56");
    }

    @Test
    void stringBytesDecodedAsUtf8() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        byte[] bytes = "héllo".getBytes(StandardCharsets.UTF_8);
        assertThat(ValueFormatter.formatDictionary(bytes, col, true, NO_LIMIT)).isEqualTo("héllo");
    }

    @Test
    void float16BytesDecodeToFloat() {
        // Half-precision 1.5 = sign 0 | exponent 01111 (15) | fraction 1000000000
        // = 0x3E00, little-endian → 0x00, 0x3E.
        ColumnSchema col = column(PhysicalType.FIXED_LEN_BYTE_ARRAY, new LogicalType.Float16Type());
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
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.IntType(32, false));
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
        ColumnSchema col = column(PhysicalType.FIXED_LEN_BYTE_ARRAY, new LogicalType.IntervalType());
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
        ColumnSchema annotated = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        ColumnSchema unannotated = column(PhysicalType.BYTE_ARRAY, null);
        assertThat(ValueFormatter.formatDictionary(new byte[0], annotated, true, NO_LIMIT)).isEmpty();
        assertThat(ValueFormatter.formatDictionary(new byte[0], unannotated, true, NO_LIMIT)).isEmpty();
    }

    @Test
    void dictionaryPrimitiveMismatchFailsFast() {
        // A Long dictionary value can never back DATE (an INT32 logical type).
        ColumnSchema col = column(PhysicalType.INT64, new LogicalType.DateType());
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(42L, col, true, NO_LIMIT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DATE");
    }

    @Test
    void unknownDictionaryPrimitiveFailsFast() {
        ColumnSchema col = column(PhysicalType.INT64, null);
        assertThatThrownBy(() -> ValueFormatter.formatDictionary("not-a-primitive", col, true, NO_LIMIT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("String");
    }

    // ==================== control-character sanitisation ====================

    @Test
    void dictionaryStringWithEmbeddedControlRendersMiddleDot() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        assertThat(ValueFormatter.formatDictionary("A\u0001B".getBytes(StandardCharsets.UTF_8), col, true, NO_LIMIT))
                .isEqualTo("A·B");
    }

    @Test
    void dictionaryStringWithTabAndNewlineRendersMiddleDots() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        assertThat(ValueFormatter.formatDictionary("tab\tsep".getBytes(StandardCharsets.UTF_8), col, true, NO_LIMIT))
                .isEqualTo("tab·sep");
        assertThat(ValueFormatter.formatDictionary("line\nbreak".getBytes(StandardCharsets.UTF_8), col, true, NO_LIMIT))
                .isEqualTo("line·break");
    }

    @Test
    void allControlDictionaryStringRendersAsUtf8Hex() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        assertThat(ValueFormatter.formatDictionary(new byte[19], col, true, NO_LIMIT))
                .isEqualTo("0x" + "0".repeat(38));
    }

    @Test
    void mixedControlsWithPrintableTextStayText() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        // Exactly one printable character after the controls: still text.
        assertThat(ValueFormatter.formatDictionary("\u0001\u0002A\u0003".getBytes(StandardCharsets.UTF_8), col, true,
                NO_LIMIT))
                .isEqualTo("··A·");
    }

    @Test
    void materialisedStringWithControlRendersMiddleDot() {
        assertThat(ValueFormatter.formatValue("A\u0001B", primitive(PhysicalType.BYTE_ARRAY,
                new LogicalType.StringType()), NO_LIMIT)).isEqualTo("A·B");
    }

    @Test
    void materialisedAnnotatedBytesWithControlRenderMiddleDots() {
        assertThat(ValueFormatter.formatValue("tab\tsep".getBytes(StandardCharsets.UTF_8),
                primitive(PhysicalType.BYTE_ARRAY, new LogicalType.StringType()), NO_LIMIT))
                .isEqualTo("tab·sep");
    }

    // ==================== materialised source ====================

    @Test
    void materialisedNullRendersAsNull() {
        assertThat(ValueFormatter.formatValue(null, primitive(PhysicalType.BYTE_ARRAY, null), NO_LIMIT))
                .isEqualTo("null");
    }

    @Test
    void materialisedBareByteArrayAsStringWhenValidUtf8() {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        assertThat(ValueFormatter.formatValue(bytes, primitive(PhysicalType.BYTE_ARRAY, null), NO_LIMIT))
                .isEqualTo("hello");
    }

    @Test
    void materialisedBareByteArrayAsHexWhenInvalidUtf8() {
        // Lone continuation byte — not a valid UTF-8 sequence.
        byte[] bytes = { (byte) 0xC3, (byte) 0x28, (byte) 0xA0, (byte) 0xA1 };
        assertThat(ValueFormatter.formatValue(bytes, primitive(PhysicalType.BYTE_ARRAY, null), NO_LIMIT))
                .isEqualTo("0xc328a0a1");
    }

    @Test
    void materialisedUnsignedInt32() {
        SchemaNode node = primitive(PhysicalType.INT32, new LogicalType.IntType(32, false));
        assertThat(ValueFormatter.formatValue(-1, node, NO_LIMIT)).isEqualTo("4294967295");
    }

    @Test
    void materialisedDecimalRendersPlainString() {
        SchemaNode node = primitive(PhysicalType.INT32, new LogicalType.DecimalType(7, 9));
        // BigDecimal.toString would give "1E-7"; the canonical form is plain.
        assertThat(ValueFormatter.formatValue(new BigDecimal("0.0000001"), node, NO_LIMIT))
                .isEqualTo("0.0000001");
    }

    @Test
    void decimalRendersIdenticallyAcrossDictionaryAndMaterialisedSources() {
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.DecimalType(7, 9));
        SchemaNode node = primitive(PhysicalType.INT32, new LogicalType.DecimalType(7, 9));
        // BigInteger.ONE.toByteArray() — the unscaled Int32 encoding of
        // 0.0000001 at scale 7.
        byte[] unscaledOne = BigInteger.ONE.toByteArray();

        assertThat(ValueFormatter.formatDictionary(unscaledOne, col, true, NO_LIMIT))
                .isEqualTo(ValueFormatter.formatValue(new BigDecimal("0.0000001"), node, NO_LIMIT))
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
        assertThat(ValueFormatter.formatValue(epochBytes, primitive(PhysicalType.INT96, null), NO_LIMIT))
                .isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void materialisedUuidWrongLengthFails() {
        assertThatThrownBy(() -> ValueFormatter.formatValue(new byte[15],
                primitive(PhysicalType.FIXED_LEN_BYTE_ARRAY, new LogicalType.UuidType()), NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 bytes");
    }

    @Test
    void materialisedIntervalWrongLengthFails() {
        assertThatThrownBy(() -> ValueFormatter.formatValue(new byte[11],
                primitive(PhysicalType.FIXED_LEN_BYTE_ARRAY, new LogicalType.IntervalType()), NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("12 bytes");
    }

    @Test
    void materialisedInt96WrongLengthFails() {
        assertThatThrownBy(() -> ValueFormatter.formatValue(new byte[16], primitive(PhysicalType.INT96, null),
                NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("12 bytes");
    }

    @Test
    void nestedValueIntoScalarSchemaFails() {
        SchemaNode group = FileSchema.builder("m")
                .struct("s", RepetitionType.OPTIONAL,
                        b -> b.addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build()
                .getField("s");
        assertThatThrownBy(() -> ValueFormatter.formatValue(42, group, NO_LIMIT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("s")
                .hasMessageContaining("Integer");
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

    /// A finite COMPACT budget caps each nested collection at three visible
    /// entries, at every depth: the struct's fourth member and the list
    /// element's fourth field each collapse into `…+N` — this is the text the
    /// dive preview cell clips.
    @Test
    void compactCapsNestedCollectionsAtThreeEntriesAndMarksTheRemainder() {
        assertThat(bboxCapped)
                .isEqualTo("{ xmin : -123.0, xmax : -122.5, ymin : 37.0, …+1 }");
        assertThat(addressesCapped)
                .isEqualTo("[{ freeform : 100 Main St, locality : New York, region : NA, …+1 }]");
    }

    /// `NO_LIMIT` renders every nested entry — the caps are a preview-cell
    /// device, not part of the display grammar.
    @Test
    void unlimitedBudgetRendersEveryNestedEntry() {
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

    /// The materialised walker renders nested values whole from the same
    /// display grammar, so a `print` cell and a COMPACT `NO_LIMIT` cell spell
    /// a struct identically.
    @Test
    void materialisedWalkerRendersNestedValuesWhole() {
        assertThat(bboxMaterialised).isEqualTo(bboxWhole);
    }

    // ==================== budget contract ====================

    @Test
    void textBudgetsNeverCutTruncationIsTheCallersJob() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
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
                    ValueFormatter.NestedStyle.COMPACT, 1)).isEqualTo("1mo 15d 3600000ms");
        });
    }

    @Test
    void zeroBudgetRejectedOnEveryEntryPoint() throws IOException {
        withIntervalReader(rowReader -> assertThatThrownBy(() -> ValueFormatter.formatReader(rowReader, durationIdx,
                durationField, true, ValueFormatter.NestedStyle.COMPACT, 0))
                .isInstanceOf(IllegalArgumentException.class));
        assertThatThrownBy(() -> ValueFormatter.formatValue("x", primitive(PhysicalType.BYTE_ARRAY, null), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(1, column(PhysicalType.INT32, null), true, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeBudgetBelowNoLimitRejectedOnEveryEntryPoint() throws IOException {
        withIntervalReader(rowReader -> assertThatThrownBy(() -> ValueFormatter.formatReader(rowReader, durationIdx,
                durationField, true, ValueFormatter.NestedStyle.COMPACT, -2))
                .isInstanceOf(IllegalArgumentException.class));
        assertThatThrownBy(() -> ValueFormatter.formatValue("x", primitive(PhysicalType.BYTE_ARRAY, null), -2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(1, column(PhysicalType.INT32, null), true, -2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ==================== null dependencies ====================

    @Test
    void nullReaderRejected() {
        assertThatThrownBy(() -> ValueFormatter.formatReader(null, 0, durationField, true,
                ValueFormatter.NestedStyle.COMPACT, NO_LIMIT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("reader");
    }

    @Test
    void nullFieldRejected() throws IOException {
        withIntervalReader(rowReader -> assertThatThrownBy(() -> ValueFormatter.formatReader(rowReader, 0, null, true,
                ValueFormatter.NestedStyle.COMPACT, NO_LIMIT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("field"));
        assertThatThrownBy(() -> ValueFormatter.formatValue("x", null, NO_LIMIT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("field");
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
        assertThat(ValueFormatter.formatBytes("hello".getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo("hello");
    }

    @Test
    void statsRendersNonAsciiPrintableString() {
        assertThat(ValueFormatter.formatBytes("Última".getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo("Última");
    }

    @Test
    void statsRendersLongStringInFull() {
        String longValue = "abcdefghijklmnopqrstuvwxyz";
        assertThat(ValueFormatter.formatBytes(longValue.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo(longValue);
    }

    @Test
    void statsReplacesControlCharsWithPlaceholder() {
        byte[] mixed = { 'A', 0x01, 'B', 0x00, 'C' };
        assertThat(ValueFormatter.formatBytes(mixed, stringColumn()))
                .isEqualTo("A·B·C");
    }

    @Test
    void statsRendersAllControlBytesAsHex() {
        assertThat(ValueFormatter.formatBytes(new byte[19], stringColumn()))
                .isEqualTo("0x" + "00".repeat(19));
    }

    @Test
    void statsDistinctLongStringsRenderDistinctly() {
        String first = "the-quick-brown-fox-jumps-over-the-lazy-dog-0";
        String second = "the-quick-brown-fox-jumps-over-the-lazy-dog-1";

        assertThat(ValueFormatter.formatBytes(first.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo(first);
        assertThat(ValueFormatter.formatBytes(second.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo(second);
    }

    @Test
    void statsEmptyByteBackedValueRendersExplicitEmptyString() {
        // The `isByteBacked` rule is physical: empty statistics bytes on any
        // BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY column render as an explicit ""
        // (distinguishing "present but empty" from an absent statistic).
        assertThat(ValueFormatter.formatBytes(new byte[0], stringColumn())).isEqualTo("\"\"");
        assertThat(ValueFormatter.formatBytes(new byte[0], bareByteArrayColumn())).isEqualTo("\"\"");
    }

    @Test
    void statsAbsentBytesRenderDash() {
        assertThat(ValueFormatter.formatBytes(null, stringColumn())).isEqualTo("-");
        assertThat(ValueFormatter.formatBytes(null, stringColumn(), false)).isEqualTo("-");
    }

    @Test
    void statsDecodesInt32() {
        byte[] bytes = { 0x2A, 0x00, 0x00, 0x00 };
        assertThat(ValueFormatter.formatBytes(bytes, intColumn())).isEqualTo("42");
    }

    @Test
    void statsRendersTimestampMicrosLogically() {
        ColumnSchema col = timestampColumn(true, LogicalType.TimeUnit.MICROS);
        // 2025-01-01T00:00:00Z = 1735689600_000_000 micros, little-endian INT64
        byte[] bytes = littleEndian(1735689600_000_000L);
        assertThat(ValueFormatter.formatBytes(bytes, col)).isEqualTo("2025-01-01T00:00:00Z");
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
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.DateType());
        byte[] bytes = littleEndian(20202);
        assertThat(ValueFormatter.formatBytes(bytes, col)).isEqualTo("2025-04-24");
    }

    @Test
    void statsRendersIntervalLogically() {
        // 1 month, 15 days, 3_600_000 ms — little-endian unsigned 32-bit
        assertThat(ValueFormatter.formatBytes(intervalBytes(), intervalColumn()))
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
        assertThat(ValueFormatter.formatBytes(epochBytes, int96Column()))
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
        assertThat(ValueFormatter.formatBytes(epochBytes, int96Column()))
                .isEqualTo(ValueFormatter.formatValue(epochBytes, primitive(PhysicalType.INT96, null), NO_LIMIT))
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
                    ValueFormatter.NestedStyle.COMPACT, 1)).isEqualTo(expected);
            assertThat(ValueFormatter.formatDictionary(raw, int96Column(), true, 1)).isEqualTo(expected);
            assertThat(ValueFormatter.formatValue(raw, field, NO_LIMIT)).isEqualTo(expected);
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
                    ValueFormatter.NestedStyle.COMPACT, NO_LIMIT)).isEqualTo(expected);
            assertThat(ValueFormatter.formatDictionary(raw, int96Column(), false, NO_LIMIT)).isEqualTo(expected);
            assertThat(ValueFormatter.formatBytes(raw, int96Column(), false, NO_LIMIT)).isEqualTo(expected);
        }
    }

    @Test
    void int96DictionaryRejectsMalformedWidthsInBothModes() {
        for (int length : new int[] { 0, 11, 13 }) {
            for (boolean logical : new boolean[] { true, false }) {
                assertThatThrownBy(() -> ValueFormatter.formatDictionary(
                        new byte[length], int96Column(), logical, NO_LIMIT))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("INT96 requires exactly 12 bytes, got " + length);
            }
        }
    }

    @Test
    void int96DictionaryNullRemainsNullInBothModes() {
        assertThat(ValueFormatter.formatDictionary(null, int96Column(), true, NO_LIMIT)).isEqualTo("null");
        assertThat(ValueFormatter.formatDictionary(null, int96Column(), false, NO_LIMIT)).isEqualTo("null");
    }

    @Test
    void statsInt96WrongLengthFails() {
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[16], int96Column()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("12 bytes");
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

        assertThat(ValueFormatter.formatBytes(bytes, stringColumn())).isEqualTo("a·b");
    }

    @Test
    void statsBudgetRejectionsAndNullColumn() {
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[] { 1 }, stringColumn(), true, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[] { 1 }, stringColumn(), true, -2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[] { 1 }, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("col");
    }

    // ==================== decoded dictionary entries ====================

    @Test
    void decodedDecimalRendersPlainString() {
        // String.valueOf(BigDecimal) would give "1E-7"; the canonical form is
        // plain — the same text every other source shows for the value.
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.DecimalType(7, 9));
        assertThat(ValueFormatter.formatDecoded(1, col)).isEqualTo("0.0000001");
    }

    @Test
    void decodedDecimalRendersIdenticallyAcrossDecodedAndMaterialisedSources() {
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.DecimalType(7, 9));
        SchemaNode node = primitive(PhysicalType.INT32, new LogicalType.DecimalType(7, 9));

        assertThat(ValueFormatter.formatDecoded(1, col))
                .isEqualTo(ValueFormatter.formatValue(new BigDecimal("0.0000001"), node, NO_LIMIT))
                .isEqualTo("0.0000001");
    }

    @Test
    void decodedIntAndLongRenderUnsignedAndSigned() {
        ColumnSchema unsigned32 = column(PhysicalType.INT32, new LogicalType.IntType(32, false));
        ColumnSchema signed64 = column(PhysicalType.INT64, null);

        assertThat(ValueFormatter.formatDecoded(-1, unsigned32)).isEqualTo("4294967295");
        assertThat(ValueFormatter.formatDecoded(-1L, signed64)).isEqualTo("-1");
    }

    @Test
    void decodedFloatDoubleBooleanRenderAsJavaText() {
        assertThat(ValueFormatter.formatDecoded(1.5f)).isEqualTo("1.5");
        assertThat(ValueFormatter.formatDecoded(-2.25d)).isEqualTo("-2.25");
        assertThat(ValueFormatter.formatDecoded(true)).isEqualTo("true");
    }

    @Test
    void decodedByteArrayDelegatesToTheBytesPipeline() {
        assertThat(ValueFormatter.formatDecoded(null, stringColumn())).isEqualTo("-");
        assertThat(ValueFormatter.formatDecoded("hello".getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo("hello");
    }

    @Test
    void decodedSchemaBearingOverloadsRejectNullColumn() {
        assertThatThrownBy(() -> ValueFormatter.formatDecoded(1, (ColumnSchema) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("col");
        assertThatThrownBy(() -> ValueFormatter.formatDecoded(1L, (ColumnSchema) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("col");
        assertThatThrownBy(() -> ValueFormatter.formatDecoded(new byte[1], (ColumnSchema) null))
                .isInstanceOf(NullPointerException.class);
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
                null, 0, 1, 0, new LogicalType.StringType());
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
                null, 0, 1, 0, new LogicalType.TimestampType(isUtc, unit));
    }

    private static ColumnSchema intervalColumn() {
        return new ColumnSchema(FieldPath.of("iv"), PhysicalType.FIXED_LEN_BYTE_ARRAY,
                RepetitionType.OPTIONAL, null, 0, 1, 0, new LogicalType.IntervalType());
    }

    private static ColumnSchema int96Column() {
        return new ColumnSchema(FieldPath.of("ts96"), PhysicalType.INT96, RepetitionType.OPTIONAL,
                null, 0, 1, 0, null);
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
    void nullSchemaIsRejectedBeforeNullValueAndAbsentByteShortcuts() {
        assertThatThrownBy(() -> ValueFormatter.formatValue(null, null, NO_LIMIT))
                .isInstanceOf(NullPointerException.class).hasMessage("field");
        assertThatThrownBy(() -> ValueFormatter.formatBytes(null, null, true, NO_LIMIT))
                .isInstanceOf(NullPointerException.class).hasMessage("col");
        assertThatThrownBy(() -> ValueFormatter.formatDecoded((byte[]) null, null))
                .isInstanceOf(NullPointerException.class).hasMessage("col");
    }

    @Test
    void malformedFixedLengthDictionaryAndStatisticsValuesFail() {
        ColumnSchema uuid = byteBackedColumn(new LogicalType.UuidType());
        ColumnSchema interval = intervalColumn();

        assertThatThrownBy(() -> ValueFormatter.formatDictionary(new byte[15], uuid, true, NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[15], uuid, true, NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(new byte[11], interval, true, NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatBytes(new byte[11], interval, true, NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ValueFormatter.formatDictionary(new byte[0], interval, true, NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ColumnSchema byteBackedColumn(LogicalType logical) {
        return new ColumnSchema(FieldPath.of("value"), PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                null, 0, 1, 0, logical);
    }
}
