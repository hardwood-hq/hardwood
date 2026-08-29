/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal.table;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Test;

import dev.hardwood.cli.internal.Strings;

import static org.assertj.core.api.Assertions.assertThat;

class StreamedTableTest {

    @Test
    void truncatesWideCharactersWithoutMisaligningTheTable() {
        String output = render(List.<String[]>of(new String[]{"말도나도주"}), 6, true);

        assertThat(output).isEqualTo("""
                +--------+
                | name   |
                +--------+
                | 말도…  |
                +--------+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void wrapsWideCharactersAtDisplayWidthBoundaries() {
        String output = render(List.<String[]>of(new String[]{"말도나"}), 4, false);

        assertThat(output).isEqualTo("""
                +------+
                | name |
                +------+
                | 말도 |
                | 나   |
                +------+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void rendersAllEmptyRowsWhenWrapping() {
        String output = render(
                List.<String[]>of(new String[]{"", ""}),
                new String[]{"left", "right"},
                10,
                false);

        assertThat(output).isEqualTo("""
                +------+-------+
                | left | right |
                +------+-------+
                |      |       |
                +------+-------+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void wrapsRowsWithEmptyAndNonEmptyCells() {
        String output = render(
                List.<String[]>of(new String[]{"", "abcdef"}),
                new String[]{"empty", "value"},
                5,
                false);

        assertThat(output).isEqualTo("""
                +-------+-------+
                | empty | value |
                +-------+-------+
                |       | abcde |
                |       | f     |
                +-------+-------+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void truncatesSurrogatePairsAtDisplayCellBoundaries() {
        String output = render(List.<String[]>of(new String[]{"😀abcd"}), 5, true);

        assertThat(output).isEqualTo("""
                +-------+
                | name  |
                +-------+
                | 😀ab… |
                +-------+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void truncatesSurrogatePairsOnCodePointBoundaries() {
        // The truncation boundary falls mid-cell where an emoji sits: it must be dropped
        // whole, never split into a lone surrogate. "😀" occupies two char units, so a
        // code-unit-based cut would land inside the pair.
        String output = render(List.<String[]>of(new String[]{"😀😀😀"}), "x", 2, true);

        assertThat(output).isEqualTo("""
                +-----+
                | x   |
                +-----+
                | 😀… |
                +-----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void floorsColumnWidthAtWideGlyphMinContentWidth() {
        // A column containing a 2-cell glyph cannot be squeezed below its widest
        // unbreakable token, so the sampled wide rows render flush with the border.
        String output = render(List.<String[]>of(new String[]{"가나"}), "x", 1, false);

        assertThat(output).isEqualTo("""
                +----+
                | x  |
                +----+
                | 가 |
                | 나 |
                +----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void floorsColumnWidthAtWideGlyphInTheHeader() {
        // The header participates in the min-content width just like the sampled cells:
        // a wide header over all-Latin data still cannot be squeezed below two cells.
        String output = render(List.<String[]>of(new String[]{"a"}), "가", 1, false);

        assertThat(output).isEqualTo("""
                +----+
                | 가 |
                +----+
                | a  |
                +----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void floorsColumnWidthAtWideGlyphWhenTruncating() {
        // The floor is a property of the column, not of the overflow strategy, so it
        // applies in truncate mode too: the glyph is rendered rather than replaced by
        // an ellipsis that would fit the requested one-cell cap.
        String output = render(List.<String[]>of(new String[]{"가"}), "x", 1, true);

        assertThat(output).isEqualTo("""
                +----+
                | x  |
                +----+
                | 가 |
                +----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void truncationKeepsAWideCharacterBesideTheEllipsis() {
        // A truncated column reserves a cell for the ellipsis on top of its widest
        // glyph, so the marker can never crowd out the character it is marking.
        String output = render(List.<String[]>of(new String[]{"말도나도주"}), "x", 1, true);

        assertThat(output).isEqualTo("""
                +-----+
                | x   |
                +-----+
                | 말… |
                +-----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void truncationKeepsANarrowCharacterBesideTheEllipsis() {
        // Same rule for one-cell glyphs: at maxWidth 1 the column widens to 2 rather
        // than rendering a bare ellipsis that carries no information at all.
        String output = render(List.<String[]>of(new String[]{"hello"}), "x", 1, true);

        assertThat(output).isEqualTo("""
                +----+
                | x  |
                +----+
                | h… |
                +----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void truncationSizesToTheFirstGlyphNotTheWidestOne() {
        // The 가 is cut away by the truncation, so the column owes it no room: sizing to
        // the widest glyph would leave a 3-cell column rendering 2 cells of content.
        String output = render(List.<String[]>of(new String[]{"a가b나c"}), "h", 1, true);

        assertThat(output).isEqualTo("""
                +----+
                | h  |
                +----+
                | a… |
                +----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void wrappingStillSizesToTheWidestGlyph() {
        // The counterpart: wrapping has to put every glyph on a line eventually, so the
        // 가 does set the floor even though it is not the first character.
        String output = render(List.<String[]>of(new String[]{"a가b나c"}), "h", 1, false);

        assertThat(output).isEqualTo("""
                +----+
                | h  |
                +----+
                | a  |
                | 가 |
                | b  |
                | 나 |
                | c  |
                +----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void doesNotReserveEllipsisRoomForUntruncatedColumns() {
        // The extra cell is only for columns that actually get truncated: a column
        // whose values all fit is sized to the content, not content plus a marker.
        String output = render(List.<String[]>of(new String[]{"ab"}), "id", 50, true);

        assertThat(output).isEqualTo("""
                +----+
                | id |
                +----+
                | ab |
                +----+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void keepsForceProgressWhenWideGlyphIsOutsideSampledRows() {
        // The min-content floor only sees sampled rows. When a wide glyph appears in
        // an unsampled row, the force-progress branch still prevents an infinite loop.
        String output = render(
                List.<String[]>of(new String[]{"x"}, new String[]{"가"}),
                "x",
                1,
                false,
                1);

        assertThat(output).isEqualTo("""
                +---+
                | x |
                +---+
                | x |
                | 가 |
                +---+""");
    }

    @Test
    void closesTableWhenRowsArePrintedWithZeroSampleSize() {
        String output = render(List.<String[]>of(new String[]{"a"}, new String[]{"b"}), "s", 10, true, 0);

        assertThat(output).isEqualTo("""
                +---+
                | s |
                +---+
                | a |
                | b |
                +---+""");
        assertEqualDisplayWidths(output);
    }

    @Test
    void doesNotAddExtraSeparatorForZeroRowsWithZeroSampleSize() {
        String output = render(List.of(), "s", 10, true, 0);

        assertThat(output).isEqualTo("""
                +---+
                | s |
                +---+""");
        assertEqualDisplayWidths(output);
    }

    private static String render(List<String[]> rows, int maxWidth, boolean truncate) {
        return render(rows, "name", maxWidth, truncate);
    }

    private static String render(List<String[]> rows, String header, int maxWidth, boolean truncate) {
        return render(rows, header, maxWidth, truncate, rows.size());
    }

    private static String render(List<String[]> rows, String[] headers, int maxWidth, boolean truncate) {
        return render(rows, headers, maxWidth, truncate, rows.size());
    }

    private static String render(List<String[]> rows, String header, int maxWidth, boolean truncate, int sampleSize) {
        return render(rows, new String[]{header}, maxWidth, truncate, sampleSize);
    }

    private static String render(List<String[]> rows, String[] headers, int maxWidth, boolean truncate, int sampleSize) {
        StringWriter output = new StringWriter();
        new StreamedTable().print(
                new PrintWriter(output),
                headers,
                rows.stream()
                        .<IntFunction<String>>map(row -> column -> row[column])
                        .iterator(),
                sampleSize,
                maxWidth,
                truncate,
                false);
        return output.toString().stripTrailing();
    }

    private static void assertEqualDisplayWidths(String output) {
        int expectedWidth = Strings.width(output.lines().findFirst().orElseThrow());
        assertThat(output.lines()).allSatisfy(line ->
                assertThat(Strings.width(line)).isEqualTo(expectedWidth));
    }
}
