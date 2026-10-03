/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The `ColumnBatch` input forms that mirror the reader's variable-width outputs: `strings(...)`
/// for `getStrings()`, and the packed `bytes(...)` / `fixed(...)` for `getBinaryValues()` plus
/// `getBinaryOffsets()`. Each is another spelling of values `bytes(byte[][])` already accepts,
/// so the file it produces is asserted byte-identical to the `byte[][]` one.
class WriterStringAndPackedBinaryTest {

    private static final int ROWS = 5000;

    /// A low-cardinality text column, a high-cardinality optional one, a `UUID`, a `FLOAT16` and
    /// a binary `DECIMAL`: dictionary and plain chunks, both binary physical types, and each of
    /// the statistics orders a binary column accumulates in.
    private static final FileSchema SCHEMA = FileSchema.builder("schema")
            .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
            .addColumn("u", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
            .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 16, new LogicalType.UuidType())
            .addColumn("h", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 2, new LogicalType.Float16Type())
            .addColumn("d", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.DecimalType(18, 2))
            .build();

    @Test
    void packedValuesWriteTheFileByteArraysWrite() throws Exception {
        Columns columns = Columns.generate();

        byte[] fromArrays = write(SCHEMA, batch -> batch
                .bytes(0, columns.s)
                .bytes(1, columns.u, columns.uNulls)
                .fixed(2, columns.id, columns.idNulls)
                .fixed(3, columns.h)
                .bytes(4, columns.d, columns.dNulls));
        // Packed with a leading and a trailing margin, which the reader's capacity-sized buffer
        // has at its end and a slice of a larger buffer has at both.
        Packed s = Packed.of(columns.s, 3, 5);
        Packed u = Packed.of(columns.u, 0, 7);
        Packed id = Packed.of(columns.id, 16, 0);
        Packed h = Packed.of(columns.h, 1, 1);
        Packed d = Packed.of(columns.d, 2, 2);
        byte[] fromPacked = write(SCHEMA, batch -> batch
                .bytes("s", s.values, s.offsets)
                .bytes("u", u.values, u.offsets, columns.uNulls)
                .fixed("id", id.values, id.offsets, Validity.ofNulls(columns.idNulls))
                .fixed("h", h.values, h.offsets)
                .bytes("d", d.values, d.offsets, Validity.ofNulls(columns.dNulls)));

        assertThat(fromPacked).isEqualTo(fromArrays);
    }

    @Test
    void aValueInternsAsOneDictionaryEntryWhicheverFormItArrivesIn() throws Exception {
        // One row group, so every batch interns into the same dictionary: a value hashed and
        // compared as a whole array and as a slice of a packed buffer must find one entry.
        FileSchema schema = text(RepetitionType.REQUIRED);
        byte[][] values = Columns.generate().s;
        byte[][] first = Arrays.copyOfRange(values, 0, 100);
        byte[][] second = Arrays.copyOfRange(values, 100, 200);
        byte[] whole = values[0].clone();

        byte[] fromArrays = write(schema, batch -> batch.bytes(0, first), batch -> batch.bytes(0, second),
                batch -> batch.bytes(0, new byte[][] { whole }));
        Packed packed = Packed.of(second, 4, 4);
        byte[] mixed = write(schema, batch -> batch.bytes(0, first),
                batch -> batch.bytes(0, packed.values, packed.offsets),
                batch -> batch.bytes(0, whole, new int[] { 0, whole.length }));

        assertThat(mixed).isEqualTo(fromArrays);
    }

    @Test
    void copyThroughTheReadersPackedFormIsByteIdentical() throws Exception {
        Columns columns = Columns.generate();
        byte[] original = write(SCHEMA, batch -> batch
                .bytes(0, columns.s)
                .bytes(1, columns.u, columns.uNulls)
                .fixed(2, columns.id, columns.idNulls)
                .fixed(3, columns.h)
                .bytes(4, columns.d, columns.dNulls));

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(original)));
                ColumnReaders readers = reader.columnReaders(ColumnProjection.all());
                ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            while (readers.nextBatch()) {
                writer.columnWriter().writeBatch(batch -> {
                    for (int c = 0; c < SCHEMA.getColumnCount(); c++) {
                        ColumnReader column = readers.getColumnReader(c);
                        boolean fixed = column.getColumnSchema().type() == PhysicalType.FIXED_LEN_BYTE_ARRAY;
                        if (column.getColumnSchema().repetitionType() == RepetitionType.REQUIRED) {
                            if (fixed) {
                                batch.fixed(c, column.getBinaryValues(), column.getBinaryOffsets());
                            }
                            else {
                                batch.bytes(c, column.getBinaryValues(), column.getBinaryOffsets());
                            }
                        }
                        else if (fixed) {
                            batch.fixed(c, column.getBinaryValues(), column.getBinaryOffsets(),
                                    column.getLeafValidity());
                        }
                        else {
                            batch.bytes(c, column.getBinaryValues(), column.getBinaryOffsets(),
                                    column.getLeafValidity());
                        }
                    }
                });
            }
        }

        assertThat(InMemoryFiles.toByteArray(out)).isEqualTo(original);
    }

    @Test
    void stringsWriteTheirUtf8Encoding() throws Exception {
        String[] values = { "hello", "", "grüße", "日本語", "🦆", "hello" };
        boolean[] nulls = { false, true, false, false, true, false };
        values[1] = null; // the slot at a null row may be anything, a Java null included
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .addColumn("tag", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.EnumType())
                .addColumn("raw", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                .build();
        String[] all = values.clone();
        all[1] = "";

        byte[] fromStrings = write(schema, batch -> batch
                .strings(0, all)
                .strings("tag", values, nulls)
                .strings(2, values, Validity.ofNulls(nulls)));
        byte[] fromBytes = write(schema, batch -> batch
                .bytes(0, utf8(all))
                .bytes("tag", utf8(values), nulls)
                .bytes(2, utf8(values), Validity.ofNulls(nulls)));

        assertThat(fromStrings).isEqualTo(fromBytes);
        String[] expected = { "hello", null, "grüße", "日本語", null, "hello" };
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(fromStrings)));
                ColumnReader name = reader.columnReader(0);
                ColumnReader tag = reader.columnReader(1)) {
            assertThat(name.nextBatch()).isTrue();
            assertThat(name.getStrings()).containsExactly(all);
            assertThat(tag.nextBatch()).isTrue();
            assertThat(tag.getStrings()).containsExactly(expected);
        }
    }

    @Test
    void rejectsStringsOnAColumnThatDoesNotHoldText() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.DecimalType(9, 2))
                .build();
        assertRejected(schema, batch -> batch.strings(0, new String[] { "1.00" }),
                "Column 0 (amount) is BYTE_ARRAY annotated DECIMAL(9, 2), which does not hold text;"
                        + " strings(...) takes a BYTE_ARRAY column annotated STRING, ENUM or JSON, or unannotated");
        FileSchema ints = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        assertRejected(ints, batch -> batch.strings("id", new String[] { "1" }),
                "Column 0 (id) is INT32, which does not hold text;"
                        + " strings(...) takes a BYTE_ARRAY column annotated STRING, ENUM or JSON, or unannotated");
    }

    @Test
    void rejectsANullStringAtAPresentRow() {
        assertRejected(text(RepetitionType.OPTIONAL),
                batch -> batch.strings(0, new String[] { "a", null }, new boolean[] { false, false }),
                "Column 0 (v) has a null value at present row 1");
    }

    @Test
    void rejectsMalformedOffsets() {
        FileSchema schema = text(RepetitionType.REQUIRED);
        byte[] values = new byte[10];
        assertRejected(schema, batch -> batch.bytes(0, values, null),
                "offsets must not be null for column 0 (v)");
        assertRejected(schema, batch -> batch.bytes(0, values, new int[0]),
                "Column 0 (v) has no offsets; they hold one entry more than the values they delimit");
        assertRejected(schema, batch -> batch.bytes(0, values, new int[] { -1, 4 }),
                "Column 0 (v) has offset -1 at index 0, before the start of the values");
        assertRejected(schema, batch -> batch.bytes(0, values, new int[] { 0, 6, 4, 8 }),
                "Column 0 (v) has offset 4 at index 2, below the offset 6 before it");
        assertRejected(schema, batch -> batch.bytes(0, values, new int[] { 0, 4, 11 }),
                "Column 0 (v) has offset 11 at index 2, past the end of the 10 value bytes");
        assertRejected(schema, batch -> batch.bytes(0, null, new int[] { 0 }),
                "values must not be null for column 0 (v)");
    }

    @Test
    void checksANullRowsSpanAndAMaskAgainstTheOffsets() {
        FileSchema schema = text(RepetitionType.OPTIONAL);
        byte[] values = new byte[10];
        // A null row's bytes are ignored, but its span is still part of the run the rows after
        // it are found by.
        assertRejected(schema,
                batch -> batch.bytes(0, values, new int[] { 0, 6, 4, 8 }, new boolean[] { false, true, false }),
                "Column 0 (v) has offset 4 at index 2, below the offset 6 before it");
        assertRejected(schema,
                batch -> batch.bytes(0, values, new int[] { 0, 2, 4 }, new boolean[] { false, true, false }),
                "Column 0 (v) has 2 values but 3 null flags");
    }

    @Test
    void checksPackedValuesAsItChecksArrays() {
        FileSchema uuid = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 4)
                .build();
        assertRejected(uuid, batch -> batch.fixed(0, new byte[12], new int[] { 0, 4, 7, 11 }),
                "Column 0 (id) has a value of length 3 at row 1 but the FIXED_LEN_BYTE_ARRAY type length is 4");
        // The short value sits at a null row, so it is never encoded and never checked.
        byte[] written = write(uuid, batch -> batch.fixed(0, new byte[12], new int[] { 0, 4, 7, 11 },
                new boolean[] { false, true, false }));
        assertThat(written).isNotEmpty();

        FileSchema decimal = FileSchema.builder("schema")
                .addColumn("d", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.DecimalType(2, 0))
                .build();
        byte[] thousand = BigInteger.valueOf(1000).toByteArray();
        byte[] packed = new byte[1 + thousand.length];
        packed[0] = 7;
        System.arraycopy(thousand, 0, packed, 1, thousand.length);
        assertRejected(decimal, batch -> batch.bytes(0, packed, new int[] { 0, 1, packed.length }),
                "Column 0 (d) has a value at row 1 that is not an unscaled value the column's DECIMAL(2, 0) can hold");
        assertRejected(text(RepetitionType.REQUIRED), batch -> batch.fixed(0, new byte[4], new int[] { 0, 4 }),
                "Column 0 (v) is BYTE_ARRAY, not FIXED_LEN_BYTE_ARRAY");
    }

    private static FileSchema text(RepetitionType repetition) {
        return FileSchema.builder("schema")
                .addColumn("v", PhysicalType.BYTE_ARRAY, repetition, new LogicalType.StringType())
                .build();
    }

    private static void assertRejected(FileSchema schema, Consumer<ColumnBatch> filler, String message) {
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(filler))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message);
        }
        catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @SafeVarargs
    private static byte[] write(FileSchema schema, Consumer<ColumnBatch>... fillers) {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (Consumer<ColumnBatch> filler : fillers) {
                writer.columnWriter().writeBatch(filler);
            }
        }
        catch (Exception e) {
            throw new AssertionError(e);
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static byte[][] utf8(String[] values) {
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            encoded[i] = values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8);
        }
        return encoded;
    }

    /// The five columns of [#SCHEMA] as `byte[][]`, with a value at every null row that differs
    /// from what the packed form carries there, so a null row that leaked into the file would
    /// show up as a byte difference.
    private record Columns(byte[][] s, byte[][] u, boolean[] uNulls, byte[][] id, boolean[] idNulls,
                           byte[][] h, byte[][] d, boolean[] dNulls) {

        static Columns generate() {
            String[] pool = { "red", "green", "blue", "", "a much longer colour name than the others" };
            byte[][] s = new byte[ROWS][];
            byte[][] u = new byte[ROWS][];
            boolean[] uNulls = new boolean[ROWS];
            byte[][] id = new byte[ROWS][];
            boolean[] idNulls = new boolean[ROWS];
            byte[][] h = new byte[ROWS][];
            byte[][] d = new byte[ROWS][];
            boolean[] dNulls = new boolean[ROWS];
            for (int i = 0; i < ROWS; i++) {
                s[i] = pool[i % pool.length].getBytes(StandardCharsets.UTF_8);
                uNulls[i] = i % 7 == 3;
                u[i] = uNulls[i] ? null : Long.toHexString(i * 0x9E3779B97F4A7C15L).getBytes(StandardCharsets.UTF_8);
                idNulls[i] = i % 11 == 0;
                id[i] = idNulls[i] ? new byte[3] : ByteBuffer.allocate(16).putLong(i).putLong(~i).array();
                short half = Float.floatToFloat16((i % 101) - 50.5f);
                h[i] = new byte[] { (byte) half, (byte) (half >> 8) };
                dNulls[i] = i % 5 == 1;
                d[i] = dNulls[i] ? null : BigInteger.valueOf((i - ROWS / 2) * 12_345L).toByteArray();
            }
            return new Columns(s, u, uNulls, id, idNulls, h, d, dNulls);
        }
    }

    /// `byte[][]` values packed end to end after `lead` bytes of margin and followed by `trail`
    /// more; a `null` value spans nothing.
    private record Packed(byte[] values, int[] offsets) {

        static Packed of(byte[][] source, int lead, int trail) {
            int[] offsets = new int[source.length + 1];
            offsets[0] = lead;
            for (int i = 0; i < source.length; i++) {
                offsets[i + 1] = offsets[i] + (source[i] == null ? 0 : source[i].length);
            }
            byte[] values = new byte[offsets[source.length] + trail];
            Arrays.fill(values, (byte) 0x5A);
            for (int i = 0; i < source.length; i++) {
                if (source[i] != null) {
                    System.arraycopy(source[i], 0, values, offsets[i], source[i].length);
                }
            }
            return new Packed(values, offsets);
        }
    }
}
