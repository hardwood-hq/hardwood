/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal.table;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.cli.internal.Strings;

import static org.assertj.core.api.Assertions.assertThat;

/// Grid and layout tests for [RowTable]: how a table of pre-rendered strings
/// pads, aligns and draws its borders. Value-to-text rendering lives in
/// [dev.hardwood.cli.internal.ValueFormatter] and its tests.
class RowTableTest {

    @Test
    void displayWidthCountsAsciiAsOne() {
        assertThat(Strings.width("hello")).isEqualTo(5);
        assertThat(Strings.width("")).isZero();
    }

    @Test
    void displayWidthTreatsLatinAccentsAsNarrow() {
        assertThat(Strings.width("Última")).isEqualTo(6);
        assertThat(Strings.width("Ñuble")).isEqualTo(5);
    }

    @Test
    void displayWidthCountsHangulAsWide() {
        // 5 Hangul syllables → 10 terminal cells
        assertThat(Strings.width("말도나도주")).isEqualTo(10);
    }

    @Test
    void displayWidthCountsCjkIdeographsAsWide() {
        // 3 CJK ideographs → 6 terminal cells
        assertThat(Strings.width("漢字水")).isEqualTo(6);
    }

    @Test
    void displayWidthCountsKanaAsWide() {
        assertThat(Strings.width("コキンボ")).isEqualTo(8);
    }

    @Test
    void rendersTableWithWideCharsAligned() {
        String[] headers = {"A", "B"};
        List<String[]> rows = List.of(
                new String[]{"buenos aires", "12"},
                new String[]{"말도나도주", "3"}
        );
        String out = RowTable.renderTable(headers, rows);
        String[] lines = out.split("\n");
        // Every line must have the same display width so the borders align visually.
        int expected = Strings.width(lines[0]);
        for (String line : lines) {
            assertThat(Strings.width(line))
                    .as("line width: %s", line)
                    .isEqualTo(expected);
        }
    }

    @Test
    void rendersTransposedTableWithWideCharsAligned() {
        String out = RowTable.renderTransposedTable(
                new String[]{"name", "말도"},
                List.of(
                        new String[]{"city", "漢字水"},
                        new String[]{"note", "x"}));

        assertThat(out).isEqualTo("""
                +------+--------+
                | name |   말도 |
                +------+--------+
                | city | 漢字水 |
                +------+--------+
                | note |      x |
                +------+--------+""");
        int expectedWidth = Strings.width(out.lines().findFirst().orElseThrow());
        assertThat(out.lines()).allSatisfy(line ->
                assertThat(Strings.width(line)).isEqualTo(expectedWidth));
    }
}
