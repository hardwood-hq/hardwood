/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.tamboui.text.CharWidth;

import static org.assertj.core.api.Assertions.assertThat;

class StringsTest {

    @Test
    void hardWrapChunksAsciiAtTheWidthBoundary() {
        assertThat(Strings.hardWrap("abcdefgh", 3)).containsExactly("abc", "def", "gh");
        assertThat(Strings.hardWrap("abc", 3)).containsExactly("abc");
    }

    @Test
    void hardWrapPreservesHardLineBreaks() {
        assertThat(Strings.hardWrap("ab\n\ncd", 4)).containsExactly("ab", "", "cd");
    }

    @Test
    void hardWrapCountsWideGlyphsAsTwoCells() {
        // Each CJK ideograph occupies two cells, so only two fit in five.
        List<String> lines = Strings.hardWrap("日本語テキスト", 5);
        assertThat(lines).containsExactly("日本", "語テ", "キス", "ト");
        for (String line : lines) {
            assertThat(CharWidth.of(line)).isLessThanOrEqualTo(5);
        }
    }

    @Test
    void hardWrapCountsCombiningMarksAsZeroCells() {
        // 10 chars, 5 cells: every 'e' carries a zero-width combining accent.
        String accented = "e\u0301".repeat(5);
        assertThat(accented).hasSize(10);
        assertThat(Strings.hardWrap(accented, 5))
                .as("the string already fits five cells, so it must not be split")
                .containsExactly(accented);
    }

    @Test
    void hardWrapEmitsAGlyphWiderThanTheBudgetOnItsOwnLine() {
        assertThat(Strings.hardWrap("日本", 1))
                .as("overflowing by one cell beats looping forever making no progress")
                .containsExactly("日", "本");
    }

    @Test
    void truncateRightLeavesStringsWithinTheBudgetAlone() {
        assertThat(Strings.truncateRight("abcde", 5)).isEqualTo("abcde");
        assertThat(Strings.truncateRight("abc", 5)).isEqualTo("abc");
    }

    @Test
    void truncateRightCountsTheEllipsisTowardsTheBudget() {
        assertThat(Strings.truncateRight("abcdef", 5))
                .as("four characters plus the ellipsis, not five plus one")
                .isEqualTo("abcd" + Strings.ELLIPSIS);
        assertThat(CharWidth.of(Strings.truncateRight("abcdef", 5))).isEqualTo(5);
    }

    @Test
    void truncateRightNeverCutsInsideACodePoint() {
        // The cut lands where the emoji's surrogate pair starts; taking half of it
        // would emit a lone surrogate that renders as a replacement character.
        String value = "abcd😀ef";
        assertThat(Strings.truncateRight(value, 5)).isEqualTo("abcd" + Strings.ELLIPSIS);
    }

    @Test
    void truncateRightCountsWideGlyphsAsTwoCells() {
        assertThat(Strings.truncateRight("日本語", 5))
                .as("two ideographs fill four cells, leaving exactly one for the ellipsis")
                .isEqualTo("日本" + Strings.ELLIPSIS);
    }

    @Test
    void padRightPadsToDisplayCellsNotCharCount() {
        assertThat(Strings.padRight("日本", 6))
                .as("two ideographs already occupy four cells")
                .isEqualTo("日本  ");
        assertThat(Strings.padRight("ab", 4)).isEqualTo("ab  ");
        assertThat(Strings.padRight("abcd", 2))
                .as("strings at or above the width are returned unchanged")
                .isEqualTo("abcd");
    }
    @Test
    void graphemeBoundariesStayIntactAcrossWidthOperations() {
        String family = "👨‍👩‍👧‍👦";
        String flag = "🇺🇸";
        String combining = "e\u0301";

        assertThat(Strings.firstGlyph(family)).isEqualTo(2);
        assertThat(Strings.widestGlyph(flag)).isEqualTo(2);
        assertThat(Strings.hardWrap(family, 1)).containsExactly(family);
        assertThat(Strings.truncateRight(family + "xy", 3))
                .isEqualTo(family + Strings.ELLIPSIS);
        assertThat(Strings.truncateLeft("xy" + family, 3))
                .isEqualTo(Strings.ELLIPSIS + family);
        assertThat(Strings.hardWrap(combining, 1)).containsExactly(combining);
    }

    @Test
    void combiningOnlyClustersMakeProgress() {
        String combiningOnly = "\u0301\u0308";

        assertThat(Strings.firstGlyph(combiningOnly)).isEqualTo(1);
        assertThat(Strings.widestGlyph(combiningOnly)).isEqualTo(1);
        assertThat(Strings.hardWrap(combiningOnly, 1)).containsExactly(combiningOnly);
    }
    @Test
    void leftTruncationAndWordWrapUseDisplayCells() {
        assertThat(Strings.truncateLeft("ab😀", 3))
                .isEqualTo(Strings.ELLIPSIS + "😀");
        assertThat(Strings.wordWrap("日本語", 3))
                .containsExactly("日", "本", "語");
    }
}
