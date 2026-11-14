/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;

/// A page's header parsed ahead of the page's decode, or the failure its parse met, which the
/// decode then fails with: whoever parses a header hands on either, so that no reader parses it a
/// second time.
///
/// @param header the header and its length, `null` where its parse failed
/// @param failure what its parse failed with, `null` where it parsed
public record ParsedHeader(PageFormatProbe.HeaderAt header, RuntimeException failure) {

    public ParsedHeader {
        if ((header == null) == (failure == null)) {
            throw new IllegalArgumentException("A parsed header holds a header or a failure, not both or neither");
        }
    }

    /// The header of `page`, the page's bytes as stored, header first; only the header's bytes are
    /// read.
    public static ParsedHeader of(ByteBuffer page) {
        try {
            DataPage parsed = DataPage.parse(page);
            return new ParsedHeader(new PageFormatProbe.HeaderAt(parsed.header(), parsed.headerSize()), null);
        }
        catch (RuntimeException e) {
            return new ParsedHeader(null, e);
        }
    }

    /// The header `header` parsed into.
    public static ParsedHeader of(PageFormatProbe.HeaderAt header) {
        return new ParsedHeader(header, null);
    }

    /// The header.
    ///
    /// @throws RuntimeException the failure its parse met, where it failed
    public PageFormatProbe.HeaderAt orThrow() {
        if (failure != null) {
            throw failure;
        }
        return header;
    }
}
