/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dev.tamboui.text.CharWidth;

/// Small string helpers shared by every screen and command that draws columnar
/// content. Kept here so the truncation/padding behavior — including the
/// ellipsis character — stays consistent across all of them.
public final class Strings {

    /// The character used to mark visually-truncated content. Centralised
    /// here so changes propagate to every screen at once.
    public static final char ELLIPSIS = '…';

    private static final Pattern GRAPHEME = Pattern.compile("\\X");

    private Strings() {
    }

    /// Returns the number of terminal cells `s` occupies: wide glyphs (CJK,
    /// emoji) count double and combining marks count zero.
    public static int width(String s) {
        return CharWidth.of(s);
    }

    /// Pads `s` on the right with spaces to at least `width` cells. Strings
    /// already at or above `width` are returned unchanged (no truncation).
    public static String padRight(String s, int width) {
        int actual = width(s);
        if (actual >= width) {
            return s;
        }
        return s + " ".repeat(width - actual);
    }

    /// Replaces every ISO control character in `s` with one middle-dot cell, so
    /// a value containing raw control bytes can never break the borders of the
    /// table it renders into. Text whose characters are all controls has no
    /// readable content left to show; it renders as `0x`-prefixed hex of the
    /// UTF-8 bytes instead — the same rule byte-backed values follow.
    public static String sanitizeControls(String s) {
        int controls = 0;
        int length = s.length();
        for (int i = 0; i < length; i++) {
            if (Character.isISOControl(s.charAt(i))) {
                controls++;
            }
        }
        if (controls == 0) {
            return s;
        }
        if (controls == length) {
            return BinaryValues.toHex(s.getBytes(StandardCharsets.UTF_8));
        }
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            sb.append(Character.isISOControl(c) ? '·' : c);
        }
        return sb.toString();
    }

    /// Truncates `s` from the left so the suffix stays visible (e.g. for
    /// long column paths, where the trailing leaf name is the distinctive
    /// part). Strings within `maxWidth` are returned unchanged.
    public static String truncateLeft(String s, int maxWidth) {
        if (width(s) <= maxWidth) {
            return s;
        }
        int suffixBudget = maxWidth - width(String.valueOf(ELLIPSIS));
        if (suffixBudget <= 0) {
            return String.valueOf(ELLIPSIS);
        }
        return ELLIPSIS + suffixByWidth(s, suffixBudget);
    }

    /// Truncates `s` from the right so the prefix stays visible, marking the
    /// cut with a trailing [#ELLIPSIS]. Strings within `maxWidth` are returned
    /// unchanged; the ellipsis counts towards `maxWidth`, so the result never
    /// occupies more than `maxWidth` cells.
    ///
    /// Cutting measures display cells rather than `char`s, so the result never
    /// ends in half a surrogate pair and a wide glyph never straddles the
    /// boundary.
    public static String truncateRight(String s, int maxWidth) {
        if (width(s) <= maxWidth) {
            return s;
        }
        return prefixByWidth(s, 0, maxWidth - 1) + ELLIPSIS;
    }

    /// Word-wraps `value` so each returned line fits within `width` cells.
    /// Hard line breaks in the source are preserved. Words longer than `width`
    /// are character-chunked so they don't overflow the boundary.
    public static List<String> wordWrap(String value, int width) {
        List<String> out = new ArrayList<>();
        if (width <= 0) {
            out.add(value);
            return out;
        }
        for (String line : value.split("\n", -1)) {
            if (line.isEmpty()) {
                out.add("");
                continue;
            }
            String[] words = line.split(" ", -1);
            StringBuilder currentLine = new StringBuilder();
            for (String word : words) {
                while (Strings.width(word) > width) {
                    if (!currentLine.isEmpty()) {
                        out.add(currentLine.toString());
                        currentLine.setLength(0);
                    }
                    String chunk = prefixByWidth(word, 0, width);
                    out.add(chunk);
                    word = word.substring(chunk.length());
                }
                if (currentLine.isEmpty()) {
                    currentLine.append(word);
                } else if (Strings.width(currentLine.toString()) + 1 + Strings.width(word) <= width) {
                    currentLine.append(" ").append(word);
                } else {
                    out.add(currentLine.toString());
                    currentLine.setLength(0);
                    currentLine.append(word);
                }
            }
            if (!currentLine.isEmpty()) {
                out.add(currentLine.toString());
            }
        }
        return out;
    }

    /// Splits `value` into display lines of at most `width` cells without
    /// respecting word boundaries. Hard line breaks in the source are
    /// preserved; each segment is then chunked at `width` if it's longer.
    ///
    /// Chunking measures display cells, not `char`s, so that a line's rendered
    /// width matches what callers budgeted for: wide glyphs (CJK, emoji) count
    /// double and combining marks count zero.
    public static List<String> hardWrap(String value, int width) {
        List<String> out = new ArrayList<>();
        if (width <= 0) {
            out.add(value);
            return out;
        }
        for (String line : value.split("\n", -1)) {
            if (line.isEmpty()) {
                out.add("");
                continue;
            }
            String rest = line;
            while (!rest.isEmpty()) {
                String chunk = prefixByWidth(rest, 0, width);
                out.add(chunk);
                rest = rest.substring(chunk.length());
            }
        }
        return out;
    }

    /// Returns the first extended grapheme cluster width, with a one-cell floor.
    public static int firstGlyph(String s) {
        Matcher matcher = GRAPHEME.matcher(s);
        return matcher.find() ? Math.max(1, CharWidth.of(matcher.group())) : 1;
    }

    /// Returns the widest extended grapheme cluster width, with a one-cell floor.
    public static int widestGlyph(String s) {
        int widest = 1;
        Matcher matcher = GRAPHEME.matcher(s);
        while (matcher.find()) {
            widest = Math.max(widest, CharWidth.of(matcher.group()));
        }
        return widest;
    }

    /// Returns a whole-cluster prefix beginning at `start` within `maxWidth` cells.
    /// An over-wide first cluster is returned whole so callers always make progress.
    public static String prefixByWidth(String s, int start, int maxWidth) {
        Matcher matcher = GRAPHEME.matcher(s);
        matcher.region(start, s.length());
        int end = start;
        int used = 0;
        while (matcher.find()) {
            String cluster = matcher.group();
            int clusterWidth = CharWidth.of(cluster);
            if (end > start && used + clusterWidth > maxWidth) {
                break;
            }
            end = matcher.end();
            used += clusterWidth;
            if (used > maxWidth) {
                break;
            }
        }
        return s.substring(start, end);
    }

    /// Returns the longest whole-cluster suffix within `maxWidth` cells.
    public static String suffixByWidth(String s, int maxWidth) {
        List<Integer> starts = new ArrayList<>();
        List<Integer> ends = new ArrayList<>();
        List<Integer> widths = new ArrayList<>();
        Matcher matcher = GRAPHEME.matcher(s);
        while (matcher.find()) {
            starts.add(matcher.start());
            ends.add(matcher.end());
            widths.add(CharWidth.of(matcher.group()));
        }
        int first = starts.size();
        int used = 0;
        for (int i = widths.size() - 1; i >= 0; i--) {
            int clusterWidth = widths.get(i);
            if (used + clusterWidth > maxWidth) {
                break;
            }
            used += clusterWidth;
            first = i;
        }
        return first == starts.size() ? "" : s.substring(starts.get(first), ends.get(ends.size() - 1));
    }
}
