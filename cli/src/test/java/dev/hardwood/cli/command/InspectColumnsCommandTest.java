/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

class InspectColumnsCommandTest implements InspectColumnsCommandContract {

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String pageIndexFile() {
        return getClass().getResource("/column_index_pushdown.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    /// The unencoded size is the column that predicts read-side memory:
    /// compressed and uncompressed both measure the encoded form, so a
    /// dictionary-encoded column looks cheap next to what it costs to
    /// materialise. The file records it for `BYTE_ARRAY`; for a fixed width it
    /// is the present-value count times the width, so every column has one.
    ///
    /// Asserted here rather than on the shared contract because covering it
    /// there would mean uploading another fixture to the S3 test bucket, and
    /// reading a footer over S3 does not change how size statistics parse.
    @Test
    void populatesUnencodedSizeForEveryColumn() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f",
                getClass().getResource("/size_statistics_test.parquet").getPath());

        assertThat(result.exitCode()).isZero();
        // `name` holds 3 present values totalling 15 bytes, recorded in the footer.
        assertThat(rankedCellOf(result.output(), "name", "Unencoded")).isEqualTo("15 B");
        // Computed: 3 present DOUBLEs and 3 present INT32s.
        assertThat(rankedCellOf(result.output(), "score", "Unencoded")).isEqualTo("24 B");
        assertThat(rankedCellOf(result.output(), "tags.list.element", "Unencoded")).isEqualTo("12 B");
    }

    /// A dictionary page header the parser rejects is a damaged file, not a
    /// column without a dictionary: both tables report it, naming the chunk,
    /// rather than dropping the cardinality from the Encoding cell.
    @Test
    void aDamagedDictionaryPageHeaderIsReported(@TempDir Path tempDir) throws IOException {
        Path damaged = damageFirstDictionaryPageHeader(tempDir);
        String expected = "Error reading file: [damaged.parquet: row group 0, column 'category'] "
                + "PageHeader field 15 — Unknown field type: 15";

        Cli.Result ranked = Cli.launch("inspect", "columns", "-f", damaged.toString());
        Cli.Result detail = Cli.launch("inspect", "columns", "-f", damaged.toString(), "--column", "category");

        assertThat(ranked.exitCode()).isNotZero();
        assertThat(ranked.errorOutput()).isEqualTo(expected);
        assertThat(detail.exitCode()).isNotZero();
        assertThat(detail.errorOutput()).isEqualTo(expected);
    }

    /// An offset index the parser rejects is a damaged file, not a column
    /// without a page index: the ranked table reports it, naming the chunk,
    /// rather than showing `—` for its page count.
    @Test
    void aDamagedOffsetIndexIsReported(@TempDir Path tempDir) throws IOException {
        Path source = Path.of(getClass().getResource("/column_index_pushdown.parquet").getPath());
        long offset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(source))) {
            offset = reader.getFileMetaData().rowGroups().get(0).columns().get(0).offsetIndexOffset();
        }
        Path damaged = DamagedFiles.damage(source, offset, tempDir);

        Cli.Result result = Cli.launch("inspect", "columns", "-f", damaged.toString());

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo(
                "Error reading file: [damaged.parquet: row group 0, column 'id'] OffsetIndex field 15 — Unknown field type: 15");
    }

    private Path damageFirstDictionaryPageHeader(Path tempDir) throws IOException {
        Path source = Path.of(getClass().getResource("/dictionary_uncompressed.parquet").getPath());
        long offset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(source))) {
            ColumnChunk chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(1);
            offset = chunk.metaData().dictionaryPageOffset();
        }
        return DamagedFiles.damage(source, offset, tempDir);
    }

    /// The ranked table is where a reader decides which column to look at more
    /// closely, so the encoding is named there and not only under `--column`.
    @Test
    void namesTheDataPageEncodingInTheRankedTable() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile());

        assertThat(result.exitCode()).isZero();
        assertThat(rankedCellOf(result.output(), "name", "Encoding")).isEqualTo("PLAIN");
    }

    /// `DICT` alone reads identically for a dictionary that pays for itself and
    /// one that holds an entry per value — a second copy of the column that no
    /// codec undoes, since the index stream is distinct by construction. The
    /// cardinality is what separates them, and it is on the ranked table
    /// because that is where a reader picks the column to open.
    @Test
    void theRankedTableCarriesTheDictionaryCardinality() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", diveFixtureFile());

        assertThat(result.exitCode()).isZero();
        // Every value distinct: the dictionary reproduces the column.
        assertThat(rankedCellOf(result.output(), "names.primary", "Encoding"))
                .isEqualTo("DICT 100%");
        // A handful of entries covering every value: what a dictionary is for.
        assertThat(rankedCellOf(result.output(), "category", "Encoding"))
                .isEqualTo("DICT 4%");
    }

    /// A column with no dictionary page has no cardinality to report, and the
    /// label must not grow a misleading `0%`.
    @Test
    void aColumnWithoutADictionaryCarriesNoCardinality() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile());

        assertThat(result.exitCode()).isZero();
        assertThat(rankedCellOf(result.output(), "name", "Encoding")).isEqualTo("PLAIN");
    }

    private String diveFixtureFile() {
        return getClass().getResource("/dive_screenshots_fixture.parquet").getPath();
    }

    /// One cell of the ranked table, found by header name rather than by
    /// position: asserting on the whole output would pass on the border dashes
    /// alone, and a fixed index breaks every time a column is inserted.
    private static String rankedCellOf(String output, String column, String header) {
        List<String> lines = output.lines().toList();
        int cell = -1;
        for (String line : lines) {
            String[] cells = line.split("\\|");
            if (cell < 0) {
                for (int i = 0; i < cells.length; i++) {
                    if (cells[i].trim().equals(header)) {
                        cell = i;
                    }
                }
                continue;
            }
            if (cells.length > cell && cells[2].trim().equals(column)) {
                return cells[cell].trim();
            }
        }
        throw new AssertionError("no ranked " + header + " cell for " + column + " in:\n" + output);
    }

    private String sizeStatisticsFile() {
        return getClass().getResource("/size_statistics_test.parquet").getPath();
    }

    /// The detail mode is the non-interactive twin of the dive facts pane:
    /// the same derived quantities, one row per row group, then the named
    /// level buckets.
    @Test
    void columnDetailPrintsDerivedCountsAndNamedLevels() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--column", "tags.list.element");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("tags.list.element", "INT32", "max def 3", "max rep 1");
        // def [1, 1, 0, 3] and rep [4, 1]: 5 values over 4 records, 3 present.
        assertThat(result.output()).contains("Definition levels (all row groups, max 3)");
        assertThat(result.output()).contains("tags null", "tags empty", "element null", "element present");
        assertThat(result.output()).contains("Repetition levels (all row groups, max 1)");
        assertThat(result.output()).contains("new record", "tags.list");
    }

    /// Level histograms combine by addition, so the file-wide block is exact
    /// rather than a sample. Narrowing to one row group says so in the title.
    @Test
    void rowGroupNarrowsTheHistogramsToOne() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--column", "tags.list.element", "--row-group", "0");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("Definition levels (RG #0, max 3)");
        assertThat(result.output()).contains("Repetition levels (RG #0, max 1)");
    }

    private String multiRowGroupFile() {
        return getClass().getResource("/dive_screenshots_fixture.parquet").getPath();
    }

    /// The file-wide block has to be the file's own histogram. This fixture
    /// carries four row groups of 300 values each, so a block that sampled
    /// the first chunk instead of summing all four would read 300 where the
    /// file holds 1,200.
    @Test
    void levelHistogramsSumAcrossRowGroupsRatherThanSamplingOne() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", multiRowGroupFile(),
                "--column", "websites.list.element");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("Definition levels (all row groups, max 3)");
        assertThat(levelCountOf(result.output(), "element present")).isEqualTo("1,200");
        assertThat(levelCountOf(result.output(), "new record")).isEqualTo("600");
        assertThat(levelCountOf(result.output(), "websites.list")).isEqualTo("600");
    }

    /// `--row-group` narrows the histograms as well as the table, so the
    /// counts drop to the one chunk's rather than staying file-wide.
    @Test
    void rowGroupNarrowsTheSummedHistogramsToOneChunk() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", multiRowGroupFile(),
                "--column", "websites.list.element", "--row-group", "2");

        assertThat(result.exitCode()).isZero();
        assertThat(levelCountOf(result.output(), "element present")).isEqualTo("300");
        assertThat(levelCountOf(result.output(), "new record")).isEqualTo("150");
        assertThat(tableRowGroups(result.output())).containsExactly("2");
    }

    /// The count column of the rendered level row named `label`.
    private static String levelCountOf(String output, String label) {
        Matcher matcher = Pattern.compile("^\\s+\\d+\\s+" + Pattern.quote(label) + "\\s+([\\d,]+)\\s",
                Pattern.MULTILINE).matcher(output);
        if (!matcher.find()) {
            throw new AssertionError("no level row labelled " + label + " in:\n" + output);
        }
        return matcher.group(1);
    }

    /// The `RG` cell of every per-row-group row in the detail table.
    private static List<String> tableRowGroups(String output) {
        List<String> rowGroups = new ArrayList<>();
        for (String line : output.lines().toList()) {
            String[] cells = line.split("\\|");
            if (cells.length > 8 && cells[1].trim().matches("\\d+")) {
                rowGroups.add(cells[1].trim());
            }
        }
        return rowGroups;
    }

    /// A column that cannot repeat stores one value per record and a required
    /// one has no nulls, so `Records`, `Present` and `Fan-out` all follow from
    /// the shape rather than from a histogram. The table prints them anyway:
    /// the column occupies its width regardless, and a blank cell makes a
    /// reader — or a parser — reconstruct a number we already know.
    ///
    /// The dive facts pane still drops these, where a redundant fact costs a
    /// line in a pane that overflows.
    @Test
    void columnDetailFillsInCountsThatFollowFromTheColumnShape() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--column", "name");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("max rep 0");
        // 4 values, 1 null: one record each, three present, one value per record.
        assertThat(cellOf(result.output(), "0", 4)).isEqualTo("4");      // Records
        assertThat(cellOf(result.output(), "0", 5)).isEqualTo("3");      // Present
        assertThat(cellOf(result.output(), "0", 6)).isEqualTo("1.00");   // Fan-out
    }

    /// Cell `index` of the detail row for row group `rg`, counting the split
    /// on `|` so index 1 is `RG`.
    private static String cellOf(String output, String rg, int index) {
        for (String line : output.lines().toList()) {
            String[] cells = line.split("\\|");
            if (cells.length > index && cells[1].trim().equals(rg)) {
                return cells[index].trim();
            }
        }
        throw new AssertionError("no detail row for RG " + rg + " in:\n" + output);
    }

    /// The detail table carries the storage numbers as well as the shape ones.
    /// Splitting them across the two surfaces by accident left `--column`
    /// with no compressed size at all, which is the first thing a reader who
    /// drilled into one column wants.
    @Test
    void columnDetailCarriesStorageAndContentTogether() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--column", "name");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains(
                "RG", "Values", "Nulls", "Records", "Present", "Fan-out", "Codec",
                "Compressed", "Compression", "Encoding", "Unencoded");
        // Page-index coverage belongs to dive, where it says whether drilling
        // in is worth a keystroke. Here there is nothing to drill into.
        assertThat(result.output()).doesNotContain("Size stats");
    }

    @Test
    void levelRowsCarryNoTrailingWhitespace() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--column", "tags.list.element");

        assertThat(result.output().lines())
                .as("a zero-share bucket draws no bar, so its row must not end in the bar separator")
                .allSatisfy(line -> assertThat(line).isEqualTo(line.stripTrailing()));
    }

    @Test
    void rowGroupWithoutColumnIsRejected() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--row-group", "0");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("--row-group requires --column");
    }

    @Test
    void unknownColumnPathIsRejected() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--column", "no.such.column");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("no.such.column");
    }

    @Test
    void outOfRangeRowGroupIsRejected() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", sizeStatisticsFile(),
                "--column", "tags.list.element", "--row-group", "7");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("7");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
}
