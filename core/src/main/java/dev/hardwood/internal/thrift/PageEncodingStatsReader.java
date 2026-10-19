/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;

/// Reads a Thrift-encoded `list<PageEncodingStats>` into an unmodifiable list: one entry per
/// (page type, encoding) pair written in a column chunk.
///
/// The field is optional and purely informational, so no shape of it fails the footer read: a
/// list that cannot be decoded as written is reported as absent (an empty list) and logged at
/// WARNING. An empty list carries no such claim, since callers already handle writers that omit
/// the field entirely.
class PageEncodingStatsReader {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(PageEncodingStatsReader.class.getName()));

    /// Reads an encoding stats list from the given reader, which must be positioned right after
    /// the list field header has been consumed (i.e. ready to read the list header).
    ///
    /// The reader is always left positioned on the byte after the list, so the rest of the
    /// footer parses regardless of what this field contained.
    static List<PageEncodingStats> read(ThriftCompactReader reader) {
        long listHeader =
                reader.acceptListHeader(Codes.STRUCT);
        if (listHeader == ThriftCompactReader.ABSENT_LIST) {
            return List.of();
        }
        List<PageEncodingStats> result = new ArrayList<>(ThriftCompactReader.listSize(listHeader));
        for (int i = 0; i < ThriftCompactReader.listSize(listHeader); i++) {
            PageEncodingStats stats = readStats(reader);
            if (stats != null) {
                result.add(stats);
            }
        }
        if (result.size() != ThriftCompactReader.listSize(listHeader)) {
            LOG.get().log(System.Logger.Level.WARNING, () -> "Ignoring " + reader.fieldHere() + ": "
                    + (ThriftCompactReader.listSize(listHeader) - result.size()) + " of "
                    + ThriftCompactReader.listSize(listHeader)
                    + " entries are missing a required field");
            return List.of();
        }
        return Collections.unmodifiableList(result);
    }

    /// Reads a single PageEncodingStats Thrift struct (field 1: page_type, field 2: encoding,
    /// field 3: count), all three required by the format and all three `i32`.
    ///
    /// Returns `null` for a struct that does not carry all three at that wire type, or whose
    /// count is negative. The struct is consumed either way, leaving the reader on the next
    /// element.
    private static PageEncodingStats readStats(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.PAGE_ENCODING_STATS);
        try {
            PageType pageType = null;
            Encoding encoding = null;
            int count = -1;

            while (true) {
                int header = reader.readFieldHeader();
                if (header == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                // Every field defined on this struct is an i32, so one wire-type gate covers
                // all three: anything else is a field this version cannot use.
                if (!reader.acceptField(header, Codes.I32)) {
                    continue;
                }
                switch (ThriftCompactReader.fieldId(header)) {
                    case 1 -> pageType = ThriftEnumLookup.pageType(reader.readI32());
                    case 2 -> encoding = ThriftEnumLookup.encoding(reader.readI32());
                    case 3 -> count = reader.readI32();
                    default -> reader.skipField(Codes.I32);
                }
            }

            if (pageType == null || encoding == null || count < 0) {
                return null;
            }
            return new PageEncodingStats(pageType, encoding, count);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }
}
