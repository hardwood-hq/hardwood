/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Shared test contract for the `info` command.
interface InfoCommandContract {

    String plainFile();

    String nonexistentFile();

    /// A file whose key-value metadata mirrors what real writers embed in practice —
    /// PyArrow's own `ARROW:schema`, Spark's `org.apache.spark.sql.parquet.row.metadata`,
    /// and `pandas` — plus the edge cases real writers don't reliably produce on demand:
    /// `short.key=1.2.3` (5 bytes, prints in full), `empty.key=` (0 bytes), `absent.key`
    /// (no `value` field at all), `at.limit.key` / `over.limit.key` straddling the
    /// truncation boundary at 60 and 61 characters, and `control.key` carrying a raw
    /// newline and escape sequence.
    String kvMetadataFile();

    @Test
    default void displaysFileInfo() {
        Cli.Result result = Cli.launch("info", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        // `startsWith`, not exact equality: `plainFile()` carries its own key-value
        // metadata (e.g. PyArrow's `ARROW:schema`), covered separately by
        // `displaysKeyValueMetadataSection()`. This test owns only the six base facts.
        assertThat(result.output()).startsWith("""
                Format Version:    2
                Created By:        parquet-cpp-arrow version 24.0.0
                Row Groups:        1
                Total Rows:        3
                Uncompressed Size: 174 B
                Compressed Size:   174 B""");
    }

    @Test
    default void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("info", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    default void displaysKeyValueMetadataSection() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile());

        assertThat(result.exitCode()).isZero();
        // ARROW:schema itself isn't asserted line-for-line — its bytes are an
        // opaque, PyArrow-internal serialization — but its presence and the
        // alignment it drives (it's the widest *size* entry) are covered by
        // asserting the other lines around it.
        assertThat(result.output()).contains(
                "Key/Value Metadata (9):",
                "  short.key                                      5 B  1.2.3",
                "  empty.key                                      0 B",
                "  pandas                                       465 B  "
                        + "{\"index_columns\":[\"__index_level_0__\"],\"column_indexes\":[{\"…",
                "  org.apache.spark.sql.parquet.row.metadata    223 B  "
                        + "{\"type\":\"struct\",\"fields\":[{\"name\":\"order_id\",\"type\":\"long\"…",
                "ARROW:schema");
    }

    /// A value of exactly `MAX_VALUE_WIDTH` cells prints in full; one cell more is
    /// cut short, with the ellipsis inside the budget rather than overhanging
    /// it. Both sides of the boundary are asserted, so moving it —
    /// or flipping the comparison — cannot pass unnoticed.
    @Test
    default void truncatesOnlyBeyondTheValueWidthLimit() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains(
                "  at.limit.key                                  60 B  " + "b".repeat(60),
                "  over.limit.key                                61 B  " + "c".repeat(59) + "…");
    }

    /// An entry whose optional `value` field is absent is a different thing from one
    /// with an empty value: `KeyValue.value` is optional in `parquet.thrift`, and the
    /// reader surfaces the difference as `null`. The size column carries the
    /// distinction — `—` against `0 B` — since both render an empty value column.
    @Test
    default void distinguishesAbsentValueFromEmptyValue() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains(
                "  empty.key                                      0 B\n",
                "  absent.key                                       —\n");
    }

    /// Control characters in a value are replaced before it is printed. A raw newline
    /// would spill the entry across two lines and break the alignment of every
    /// following one; a raw escape sequence would reprogram the reader's terminal
    /// from content the file supplies.
    @Test
    default void replacesControlCharactersInValues() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains(
                "  control.key                                   16 B  line1·line2·[31m\n");
        assertThat(result.output()).doesNotContain("\033");
    }

    /// The counterpart to [#printsSingleKeyValueInFull]: an entry with no value at all
    /// fails rather than emitting an empty line, so a consumer down the pipe never
    /// mistakes "no value" for "empty value".
    @Test
    default void failsOnKvKeyWithoutValue() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile(), "--kv-key", "absent.key");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.output()).isEmpty();
        assertThat(result.errorOutput()).contains("absent.key", "no value");
    }

    @Test
    default void printsSingleKeyValueInFull() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile(), "--kv-key",
                "org.apache.spark.sql.parquet.row.metadata");

        assertThat(result.exitCode()).isZero();
        // Full, untruncated raw value and nothing else — no summary block, no
        // ellipsis — so the output is safe to pipe straight into another tool.
        assertThat(result.output()).isEqualTo("""
                {"type":"struct","fields":[{"name":"order_id","type":"long","nullable":false,\
                "metadata":{}},{"name":"customer","type":"string","nullable":true,"metadata":{}},\
                {"name":"amount","type":"double","nullable":true,"metadata":{}}]}""");
    }

    @Test
    default void failsOnMissingKvKey() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile(), "--kv-key", "does.not.exist");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("does.not.exist");
    }

    /// `--kv-key` is the raw-value counterpart to the summary's sanitised value
    /// column: control characters print verbatim, so the output stays
    /// byte-faithful for the pipe it was built for.
    @Test
    default void printsControlCharactersVerbatimForKvKey() {
        Cli.Result result = Cli.launch("info", "-f", kvMetadataFile(), "--kv-key", "control.key");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("line1\nline2\u001B[31m");
    }
}
