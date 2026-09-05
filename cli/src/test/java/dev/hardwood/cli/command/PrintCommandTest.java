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
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

class PrintCommandTest implements PrintCommandContract {

    private final String VARIANT_FILE = getClass().getResource("/variant_test.parquet").getPath();

    private final String VARIANT_SHREDDED_FILE = getClass().getResource("/variant_shredded_test.parquet").getPath();

    private final String VARIANT_ATTRIBUTES_FILE = getClass().getResource("/variant_attributes_example.parquet").getPath();

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String byteArrayFile() {
        return getClass().getResource("/delta_byte_array_test.parquet").getPath();
    }

    @Override
    public String deepNestedFile() {
        return getClass().getResource("/deep_nested_struct_test.parquet").getPath();
    }

    @Override
    public String listFile() {
        return getClass().getResource("/list_basic_test.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Override
    public String unsignedIntFile() {
        return getClass().getResource("/unsigned_int_test.parquet").getPath();
    }

    @Override
    public String multiRowGroupIntFile() {
        return getClass().getResource("/filter_pushdown_int.parquet").getPath();
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
        // The table display grammar is unquoted — the same text `convert` CSV
        // and `dive` show; the JSON grammar exists only in the export writer.
        String separator = "+----+-------------+" + "-".repeat(33) + "+";
        assertThat(result.output()).isEqualTo(String.join("\n",
                separator,
                row("id", "name", "value"),
                separator,
                row("1", "age", "42"),
                row("1", "email", "ada@example.com"),
                row("1", "preferences", "{ opt_in : true, theme : dark }"),
                separator));
    }

    private static String row(String id, String name, String value) {
        return "| " + Strings.padRight(id, 2) + " | " + Strings.padRight(name, 11) + " | "
                + Strings.padRight(value, 31) + " |";
    }

    @Test
    void sanitisesControlCharactersInTruncateMode(@TempDir Path tempDir) throws Exception {
        Path file = writeControlCharactersFile(tempDir);
        Cli.Result result = Cli.launch("print", "-f", file.toString());

        assertThat(result.exitCode()).isZero();
        String separator = "+----+" + "-".repeat(42) + "+";
        assertThat(result.output()).isEqualTo(String.join("\n",
                separator,
                "| " + Strings.padRight("id", 2) + " | " + Strings.padRight("s", 40) + " |",
                separator,
                "| " + Strings.padRight("1", 2) + " | " + Strings.padRight("A·B", 40) + " |",
                "| " + Strings.padRight("2", 2) + " | " + Strings.padRight("tab·sep", 40) + " |",
                "| " + Strings.padRight("3", 2) + " | " + Strings.padRight("line·break", 40) + " |",
                "| " + Strings.padRight("4", 2) + " | " + Strings.padRight(NINETEEN_NULS_AS_HEX, 40) + " |",
                separator));
    }

    @Test
    void sanitisesControlCharactersInWrapMode(@TempDir Path tempDir) throws Exception {
        Path file = writeControlCharactersFile(tempDir);
        Cli.Result result = Cli.launch("print", "-f", file.toString(), "--no-truncate", "-w", "20");

        assertThat(result.exitCode()).isZero();
        String separator = "+----+" + "-".repeat(22) + "+";
        assertThat(result.output()).isEqualTo(String.join("\n",
                separator,
                "| " + Strings.padRight("id", 2) + " | " + Strings.padRight("s", 20) + " |",
                separator,
                "| " + Strings.padRight("1", 2) + " | " + Strings.padRight("A·B", 20) + " |",
                "| " + Strings.padRight("2", 2) + " | " + Strings.padRight("tab·sep", 20) + " |",
                "| " + Strings.padRight("3", 2) + " | " + Strings.padRight("line·break", 20) + " |",
                "| " + Strings.padRight("4", 2) + " | " + "0x000000000000000000" + " |",
                "| " + Strings.padRight("", 2) + " | " + "00000000000000000000" + " |",
                separator));
    }

    /// The 19-NUL row: all-control text renders as `0x` hex of the UTF-8 bytes.
    private static final String NINETEEN_NULS_AS_HEX = "0x" + "0".repeat(38);

    /// One `STRING` column carrying an embedded `\u0001`, a horizontal tab, a
    /// line feed and a run of NULs — the control characters that must never
    /// reach a table cell raw (#865).
    private static Path writeControlCharactersFile(Path tempDir) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
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
}
