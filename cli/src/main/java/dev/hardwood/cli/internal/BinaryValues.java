/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/// Shared rendering of binary payloads the schema gives no interpretation for:
/// `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` columns carrying no logical-type
/// annotation, and byte-backed logical types whose payload length rules out
/// their own decoder.
///
/// Such a column may hold text — older writers routinely omitted the `STRING`
/// annotation — or an opaque blob such as WKB geometry, a Protobuf payload or a
/// hash. The bytes are the only evidence either way, so they are decoded
/// strictly: well-formed UTF-8 with no control characters that does not start
/// with `0x` is text, anything else renders as `0x`-prefixed lowercase hex. A
/// rendered `0x…` value therefore always means bytes.
///
/// The hex is always complete as far as the caller can display it. A surface
/// too narrow for it truncates the way it truncates any other long value, so a
/// payload is clipped and marked rather than described — the same treatment a
/// long string gets, on every screen and in every command.
public final class BinaryValues {

    /// Passed as `maxChars` by a caller that renders the whole value however
    /// long it is: an export, a modal, a wrapped `--no-truncate` table.
    public static final int NO_LIMIT = -1;

    private BinaryValues() {
    }

    /// Renders `bytes` as text when they are displayable text, and as hex
    /// otherwise, holding the hex to what a caller displaying `maxChars`
    /// characters can use. Empty input renders as the empty string; callers
    /// that need to distinguish "empty" from "absent" in a table cell add that
    /// marker themselves.
    ///
    /// A payload is hexed lazily, so a multi-megabyte blob costs a cell rather
    /// than twice its own size to render into one. The text branch has no such
    /// bound: deciding text from binary means validating every byte, and a
    /// prefix could be well-formed where the whole is not.
    public static String render(byte[] bytes, int maxChars) {
        if (bytes.length == 0) {
            return "";
        }
        String text = asText(bytes);
        return text != null ? text : toHex(bytes, maxChars);
    }

    /// Renders `bytes` whole. Equivalent to `render(bytes, NO_LIMIT)`.
    public static String render(byte[] bytes) {
        return render(bytes, NO_LIMIT);
    }

    /// The bytes decoded as UTF-8, or `null` when they are not displayable text
    /// — not well-formed UTF-8, decoding to a string containing control
    /// characters, or starting with `0x`, which would read back as the bytes it
    /// spells. Decoding reports malformed input rather than substituting
    /// `U+FFFD`, so a binary payload is never mistaken for text that happens to
    /// contain replacement characters.
    public static String asText(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        String decoded;
        try {
            decoded = decoder.decode(ByteBuffer.wrap(bytes)).toString();
        }
        catch (CharacterCodingException e) {
            return null;
        }
        for (int i = 0; i < decoded.length(); i++) {
            if (Character.isISOControl(decoded.charAt(i))) {
                return null;
            }
        }
        return decoded.startsWith("0x") ? null : decoded;
    }

    /// The bytes as `0x`-prefixed lowercase hex, stopping once the result
    /// exceeds `maxChars`.
    ///
    /// Just past the budget rather than exactly at it: a caller caps at
    /// `maxChars` and marks what it cut, and a result trimmed to exactly
    /// `maxChars` would look complete to it and go unmarked. Overshooting by a
    /// byte keeps the value over-length, so the caller still marks it.
    ///
    /// A non-empty payload always contributes at least one byte, however small
    /// the budget. A caller's effective width is its own — a table widens a
    /// column to fit the marker it plans to add — so a bare `0x` could land
    /// inside it and print as though that were the whole value.
    public static String toHex(byte[] bytes, int maxChars) {
        if (maxChars == NO_LIMIT) {
            return toHex(bytes);
        }
        HexFormat hex = HexFormat.of();
        StringBuilder sb = new StringBuilder(Math.min(2 + 2 * bytes.length, maxChars + 2)).append("0x");
        for (int i = 0; i < bytes.length && (i == 0 || sb.length() <= maxChars); i++) {
            sb.append(hex.toHighHexDigit(bytes[i])).append(hex.toLowHexDigit(bytes[i]));
        }
        return sb.toString();
    }

    /// The bytes as `0x`-prefixed lowercase hex, however long that is.
    public static String toHex(byte[] bytes) {
        return "0x" + HexFormat.of().formatHex(bytes);
    }
}
