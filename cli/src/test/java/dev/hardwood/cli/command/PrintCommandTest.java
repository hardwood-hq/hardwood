/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.cli.dive.internal.Plurals;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.row.PqFile;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

class PrintCommandTest implements PrintCommandContract {

    private final String VARIANT_FILE = Cli.file("/variant_test.parquet");

    private final String VARIANT_SHREDDED_FILE = Cli.file("/variant_shredded_test.parquet");

    private final String VARIANT_ATTRIBUTES_FILE = Cli.file("/variant_attributes_example.parquet");

    @Override
    public String plainFile() {
        return Cli.file("/plain_uncompressed.parquet");
    }

    @Override
    public String byteArrayFile() {
        return Cli.file("/delta_byte_array_test.parquet");
    }

    @Override
    public String deepNestedFile() {
        return Cli.file("/deep_nested_struct_test.parquet");
    }

    @Override
    public String listFile() {
        return Cli.file("/list_basic_test.parquet");
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Override
    public String unsignedIntFile() {
        return Cli.file("/unsigned_int_test.parquet");
    }

    @Override
    public String multiRowGroupIntFile() {
        return Cli.file("/filter_pushdown_int.parquet");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("print", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo("Remote URIs are not implemented yet.");
    }

    @Test
    void rendersUnshreddedVariantValuesAsDecodedScalars() {
        Cli.Result result = Cli.launch("print", "-f", VARIANT_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+-------+
                | id | var   |
                +----+-------+
                | 1  | true  |
                | 2  | false |
                | 3  | 42    |
                | 4  | hi    |
                +----+-------+""");
    }

    @Test
    void rendersShreddedVariantValuesAsDecodedScalars() {
        Cli.Result result = Cli.launch("print", "-f", VARIANT_SHREDDED_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+---------------+
                | id | var           |
                +----+---------------+
                | 1  | 42            |
                | 2  | true          |
                | 3  | null          |
                | 4  | 1000000000000 |
                +----+---------------+""");
    }

    @Test
    void rendersVariantObjectInDisplayGrammar() {
        Cli.Result result = Cli.launch("print", "-f", VARIANT_ATTRIBUTES_FILE, "-w", "120");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+-------------+---------------------------------+
                | id | name        | value                           |
                +----+-------------+---------------------------------+
                | 1  | age         | 42                              |
                | 1  | email       | ada@example.com                 |
                | 1  | preferences | { opt_in : true, theme : dark } |
                +----+-------------+---------------------------------+""");
    }

    @Test
    void sanitisesControlCharactersInTruncateMode(@TempDir Path tempDir) throws Exception {
        Path file = writeControlCharactersFile(tempDir);
        Cli.Result result = Cli.launch("print", "-f", file.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+------------------------------------------+
                | id | s                                        |
                +----+------------------------------------------+
                | 1  | A·B                                      |
                | 2  | tab·sep                                  |
                | 3  | line·break                               |
                | 4  | 0x00000000000000000000000000000000000000 |
                +----+------------------------------------------+""");
    }

    @Test
    void sanitisesControlCharactersInWrapMode(@TempDir Path tempDir) throws Exception {
        Path file = writeControlCharactersFile(tempDir);
        Cli.Result result = Cli.launch("print", "-f", file.toString(), "--no-truncate", "-w", "20");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+----------------------+
                | id | s                    |
                +----+----------------------+
                | 1  | A·B                  |
                | 2  | tab·sep              |
                | 3  | line·break           |
                | 4  | 0x000000000000000000 |
                |    | 00000000000000000000 |
                +----+----------------------+""");
    }

    /// A `FILE` group prints as its stored fields.
    @Test
    void printsAFileGroupAsItsStoredFields(@TempDir Path tempDir) throws Exception {
        Path file = writeFileReferencesFile(tempDir);
        Cli.Result result = Cli.launch("print", "-f", file.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+----------------------------------------------------+
                | id | photo                                              |
                +----+----------------------------------------------------+
                | 1  | { uri : s3://b/cat.jpg, offset : 16, size : 4, co… |
                | 2  | { uri : null, offset : null, size : null, content… |
                | 3  | null                                               |
                +----+----------------------------------------------------+""");
    }

    private static Path writeFileReferencesFile(Path tempDir) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .file("photo", RepetitionType.OPTIONAL)
                .build();
        Path file = tempDir.resolve("file_references.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.rowWriter().writeRow(row -> row.setInt("id", 1)
                    .setFile("photo", PqFile.external("s3://b/cat.jpg", 16, 4).withContentType("image/jpeg")));
            writer.rowWriter().writeRow(row -> row.setInt("id", 2)
                    .setFile("photo", PqFile.inline(new byte[] {1, 2})));
            writer.rowWriter().writeRow(row -> row.setInt("id", 3));
        }
        return file;
    }

    /// One `STRING` column carrying an embedded `\u0001`, a horizontal tab, a
    /// line feed and a run of NULs — the control characters that must never
    /// reach a table cell raw (#865).
    private static Path writeControlCharactersFile(Path tempDir) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
                .build();
        Path file = tempDir.resolve("control_characters.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("id", new int[] { 1, 2, 3, 4 })
                    .bytes("s", new byte[][] {
                            "A\u0001B".getBytes(StandardCharsets.UTF_8),
                            "tab\tsep".getBytes(StandardCharsets.UTF_8),
                            "line\nbreak".getBytes(StandardCharsets.UTF_8),
                            new byte[19] }));
        }
        return file;
    }

    /// Rows printed before a read failure stay on standard output, and the error names
    /// how many there are.
    @Test
    void readFailureCountsPrintedRows(@TempDir Path tempDir) throws Exception {
        Path file = CorruptFiles.withBadLastRowGroup(tempDir);

        Cli.Result result = Cli.launch("print", "-f", file.toString());

        // Every table line but the header's is a row; the separators start with `+`.
        long rows = Math.max(0, result.output().lines().filter(line -> line.startsWith("|")).count() - 1);
        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.errorOutput()).isEqualTo(rows == 0
                ? CorruptFiles.CRC_FAILURE
                : CorruptFiles.CRC_FAILURE + "\nPrinted " + Plurals.format(rows, "row", "rows")
                        + " before the failure; the output is incomplete.");
    }

    /// A codec Hardwood does not implement is reported as a read error naming the column, like a
    /// file it cannot read for any other reason.
    @Test
    void reportsACodecItDoesNotImplementAsAReadError() {
        Cli.Result result = Cli.launch("print", "-f", Cli.file("/lzo_codec.parquet"));

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.errorOutput()).isEqualTo("Error reading file: [lzo_codec.parquet: row group 0, column 'name', page 0] "
                + "LZO compression is not supported");
    }

    /// A value its annotation does not admit (an `INT(8)` of 300, a `UINT(8)` of 4294967295, a
    /// `TIME(MILLIS)` of a full day) prints as a marker naming the stored value and the reason; the
    /// rest of the row, and the next row, print as usual.
    @Test
    void printsAMarkerForAValueItsAnnotationDoesNotAdmit() {
        Cli.Result result = Cli.launch("print", "-f", Cli.file("/undecodable_values.parquet"));

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+----------------------------+------------------------------------+---------------------------+
                | id | i8                         | u8                                 | t                         |
                +----+----------------------------+------------------------------------+---------------------------+
                | 1  | <300: outside INT_8 range> | <4294967295: outside UINT_8 range> | <86400000: outside a day> |
                | 2  | 5                          | 7                                  | 00:00:01                  |
                +----+----------------------------+------------------------------------+---------------------------+""");
    }

    /// The same markers inside a struct field, a list element, a map key and value, and a shredded
    /// Variant whose `INT(8)` `typed_value` holds 300; the rest of each value prints as usual.
    @Test
    void printsAMarkerInsideANestedValue() {
        Cli.Result result = Cli.launch("print", "-f", Cli.file("/undecodable_nested_values.parquet"));

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+-------------------------------------+---------------------------------------+----------------------------------------------------+------------------------------------+
                | id | s                                   | l                                     | m                                                  | var                                |
                +----+-------------------------------------+---------------------------------------+----------------------------------------------------+------------------------------------+
                | 1  | { i8 : <300: outside INT_8 range> } | [1, 2, 3, <300: outside INT_8 range>] | { <300: outside INT_8 range> : <86400000: outside… | <variant: 300 outside INT_8 range> |
                | 2  | { i8 : 5 }                          | [5]                                   | { 5 : 00:00:01 }                                   | 5                                  |
                +----+-------------------------------------+---------------------------------------+----------------------------------------------------+------------------------------------+""");
    }
}
