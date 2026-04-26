/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.Base64;

/// Best-effort pretty-printing of key/value metadata values on the Overview
/// modal. Most Parquet writers shove structured content into these strings:
/// Spark writes JSON schemas under `org.apache.spark.sql.parquet.row.metadata`,
/// and Arrow writes a base64-encoded Arrow IPC schema under `ARROW:schema`.
/// This formatter detects those two common cases and renders them more
/// usefully than raw text. Unknown values pass through unchanged.
public final class KvMetadataFormatter {

    private KvMetadataFormatter() {
    }

    public static String format(String key, String value) {
        if (value == null) {
            return "null";
        }
        // The display path must never crash the dive session for any input,
        // so the formatters wrap a final RuntimeException catch. Each
        // detector is best-effort — pretty-printers don't validate — but
        // a future change to e.g. `prettyJson` could introduce an
        // index out-of-bounds on truncated input. The fallback is the
        // raw value with a marker, which is still readable.
        try {
            if ("ARROW:schema".equalsIgnoreCase(key) || looksLikeArrowBase64(value)) {
                return renderArrow(value);
            }
            String trimmed = value.strip();
            if (looksLikeJson(trimmed)) {
                return prettyJson(trimmed);
            }
            return value;
        }
        catch (RuntimeException e) {
            return "[format failed: " + e.getClass().getSimpleName()
                    + (e.getMessage() != null ? ": " + e.getMessage() : "")
                    + "]\n\n" + value;
        }
    }

    private static boolean looksLikeArrowBase64(String value) {
        // Arrow IPC schemas start with 0xFFFFFFFF (continuation marker) which is
        // `/////` in base64. Short-circuits on the prefix without decoding.
        return value.length() > 16 && value.startsWith("/////");
    }

    private static String renderArrow(String value) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(value.strip());
        }
        catch (IllegalArgumentException e) {
            return "[base64-decode failed: " + e.getMessage() + "]\n\n" + value;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Arrow IPC schema (base64-decoded, ")
                .append(decoded.length)
                .append(" bytes)\n\n");
        sb.append(decoded.length > 256 ? "Hex dump (first 256 bytes):\n" : "Hex dump:\n");
        int limit = Math.min(decoded.length, 256);
        for (int i = 0; i < limit; i += 16) {
            sb.append(String.format("%04x  ", i));
            for (int j = 0; j < 16; j++) {
                if (i + j < limit) {
                    sb.append(String.format("%02x ", decoded[i + j] & 0xff));
                }
                else {
                    sb.append("   ");
                }
            }
            sb.append(' ');
            for (int j = 0; j < 16 && i + j < limit; j++) {
                byte b = decoded[i + j];
                sb.append((b >= 0x20 && b < 0x7f) ? (char) b : '.');
            }
            sb.append('\n');
        }
        if (decoded.length > limit) {
            sb.append("... (").append(decoded.length - limit).append(" more bytes)\n");
        }
        return sb.toString();
    }

    private static boolean looksLikeJson(String trimmed) {
        return (trimmed.startsWith("{") && trimmed.endsWith("}"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"));
    }

    /// Minimal JSON pretty-printer — walks the string once, inserts newlines
    /// and indent after braces / brackets / commas outside of strings, without
    /// parsing the structure. Doesn't validate; malformed input still
    /// produces readable output, just with potentially skewed indentation.
    static String prettyJson(String json) {
        StringBuilder sb = new StringBuilder();
        int indent = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (inString) {
                sb.append(c);
                if (escaped) {
                    escaped = false;
                }
                else if (c == '\\') {
                    escaped = true;
                }
                else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            switch (c) {
                case '"' -> {
                    inString = true;
                    sb.append(c);
                }
                case '{', '[' -> {
                    sb.append(c);
                    indent++;
                    sb.append('\n').append("  ".repeat(indent));
                }
                case '}', ']' -> {
                    indent = Math.max(0, indent - 1);
                    sb.append('\n').append("  ".repeat(indent)).append(c);
                }
                case ',' -> {
                    sb.append(c).append('\n').append("  ".repeat(indent));
                }
                case ':' -> sb.append(c).append(' ');
                case ' ', '\t', '\n', '\r' -> {
                    // swallow — we insert our own whitespace
                }
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
