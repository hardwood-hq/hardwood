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
/// strictly: well-formed UTF-8 with no control characters is text, anything
/// else is binary.
///
/// Binary renders according to how much room the target surface has, which the
/// caller states through [Form]. Every value, statistic and dictionary entry in
/// the CLI and the `dive` TUI routes through here, so the same bytes never
/// render two different ways on two different screens.
public final class BinaryValues {

    /// How much room the surface receiving a rendered value has.
    public enum Form {

        /// A fixed-width table cell. Binary short enough to read in one still
        /// renders as hex; anything longer renders as `<N bytes>` rather than
        /// as a hex string the cell would truncate to a meaningless prefix.
        COMPACT,

        /// A modal, a facts pane, an untruncated table or a file export.
        /// Binary renders as complete `0x`-prefixed lowercase hex.
        FULL
    }

    /// The longest binary payload [Form#COMPACT] still renders as hex. Eight
    /// bytes come to eighteen characters with the `0x` prefix, which fits the
    /// cell budgets the CLI tables and the `dive` screens allow a value — so
    /// up to this length the hex is fully readable and strictly more useful
    /// than a byte count, and past it the cell would show a prefix that says
    /// less than the count does.
    private static final int MAX_COMPACT_HEX_BYTES = 8;

    private BinaryValues() {
    }

    /// Renders `bytes` as text when they are displayable text, and otherwise as
    /// binary in the shape `form` calls for. Empty input renders as the empty
    /// string; callers that need to distinguish "empty" from "absent" in a
    /// table cell add that marker themselves.
    public static String render(byte[] bytes, Form form) {
        if (bytes.length == 0) {
            return "";
        }
        String text = asText(bytes);
        if (text != null) {
            return text;
        }
        if (form == Form.FULL || bytes.length <= MAX_COMPACT_HEX_BYTES) {
            return toHex(bytes);
        }
        return "<" + bytes.length + " bytes>";
    }

    /// The bytes decoded as UTF-8, or `null` when they are not displayable text
    /// — either not well-formed UTF-8, or decoding to a string containing
    /// control characters. Decoding reports malformed input rather than
    /// substituting `U+FFFD`, so a binary payload is never mistaken for text
    /// that happens to contain replacement characters.
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
        return decoded;
    }

    /// The bytes as `0x`-prefixed lowercase hex.
    public static String toHex(byte[] bytes) {
        return "0x" + HexFormat.of().formatHex(bytes);
    }
}
