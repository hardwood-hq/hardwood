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

import dev.tamboui.text.CharWidth;

/// Small string helpers shared by every screen and command that draws columnar
/// content. Kept here so the truncation/padding behavior — including the
/// ellipsis character — stays consistent across all of them.
public final class Strings {

    /// The character used to mark visually-truncated content. Centralised
    /// here so changes propagate to every screen at once.
    public static final char ELLIPSIS = '…';

    /// Stands in for a value the file does not carry, on every command and
    /// every `dive` screen. Absent is not the same as empty: a `0 B` size or an
    /// empty-string bound is something the writer recorded, this marker says it
    /// recorded nothing. Centralised here so the two surfaces cannot drift back
    /// into spelling absence differently.
    public static final String ABSENT_VALUE = "—";

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
        if (s.length() <= maxWidth) {
            return s;
        }
        return ELLIPSIS + s.substring(s.length() - maxWidth + 1);
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
        return CharWidth.substringByWidth(s, maxWidth - 1) + ELLIPSIS;
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
                while (word.length() > width) {
                    if (!currentLine.isEmpty()) {
                        out.add(currentLine.toString());
                        currentLine.setLength(0);
                    }
                    out.add(word.substring(0, width));
                    word = word.substring(width);
                }
                if (currentLine.isEmpty()) {
                    currentLine.append(word);
                } else if (currentLine.length() + 1 + word.length() <= width) {
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
                String chunk = CharWidth.substringByWidth(rest, width);
                if (chunk.isEmpty()) {
                    // A single glyph wider than the whole budget: emit it on
                    // its own line and overflow by one cell rather than loop
                    // forever making no progress.
                    chunk = rest.substring(0, rest.offsetByCodePoints(0, 1));
                }
                out.add(chunk);
                rest = rest.substring(chunk.length());
            }
        }
        return out;
    }
}
