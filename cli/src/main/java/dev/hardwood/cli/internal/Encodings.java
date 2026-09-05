/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;

/// How a column chunk's values are encoded, as opposed to the flat `encodings`
/// list the chunk declares. Shared by `dive` and `hardwood inspect columns` so
/// the same chunk reads the same on both.
public final class Encodings {

    private Encodings() {
    }

    /// The encodings the chunk's *data* pages use. Read from `encoding_stats`
    /// where the writer records it, since that distinguishes dictionary-encoded
    /// data pages from the dictionary page itself and exposes a mid-chunk
    /// fallback — `PLAIN+DICT`, a dictionary the writer abandoned partway — that
    /// the flat `encodings` list cannot express. Falls back to that list,
    /// without the level encodings, when the field is absent.
    public static Set<Encoding> dataPages(ColumnMetaData metaData) {
        Set<Encoding> encodings = new TreeSet<>();
        for (PageEncodingStats stat : metaData.encodingStats()) {
            if (stat.count() > 0
                    && (stat.pageType() == PageType.DATA_PAGE || stat.pageType() == PageType.DATA_PAGE_V2)) {
                encodings.add(stat.encoding());
            }
        }
        if (encodings.isEmpty()) {
            encodings.addAll(metaData.encodings());
            // The level streams are RLE regardless of how the values are
            // encoded, so it says nothing about the values themselves.
            encodings.remove(Encoding.RLE);
            encodings.remove(Encoding.BIT_PACKED);
        }
        return encodings;
    }

    /// Whether the chunk records `encoding_stats`, and so whether
    /// [#dataPages] says anything the declared list does not.
    public static boolean hasEncodingStats(ColumnMetaData metaData) {
        return !metaData.encodingStats().isEmpty();
    }

    /// Bytes read at `dictionary_page_offset` to decode the dictionary page's
    /// own header. A thrift-compact page header runs to a few dozen bytes; this
    /// is generous enough that no writer's is truncated by it, and small enough
    /// that the figure costs one short read rather than the whole page.
    private static final int HEADER_PROBE_BYTES = 256;

    /// How many distinct values the chunk's dictionary holds, or -1 when the
    /// chunk has none or the header cannot be read.
    ///
    /// Only the dictionary page's *header* is read, so the cost is one short
    /// seek and no decode however large the dictionary is — the entry count is
    /// `num_values` on that header, and nothing here touches the entries.
    public static long dictionaryEntries(ColumnChunk chunk, InputFile inputFile) {
        Long offset = chunk.metaData().dictionaryPageOffset();
        if (offset == null || offset <= 0) {
            return -1;
        }
        try {
            // The offsets address the file named by file_path, not this one; a
            // header decoded from whatever sits here would be fiction.
            chunk.requireSameFile();
            int length = Math.toIntExact(
                    Math.min(HEADER_PROBE_BYTES, chunk.metaData().totalCompressedSize()));
            PageHeader header = PageHeaderReader.read(
                    new ThriftCompactReader(inputFile.readRange(offset, length)));
            return header.dictionaryPageHeader() != null
                    ? header.dictionaryPageHeader().numValues()
                    : -1;
        }
        catch (IOException | RuntimeException e) {
            // Unknown, not zero: the surfaces drop the annotation rather than
            // claim a cardinality, the same way `# Pages` renders the
            // shared absent-value marker.
            return -1;
        }
    }

    /// Abbreviated, `+`-joined label for a set of encodings. Ordered by the
    /// enum rather than by the caller's iteration order, so the same chunk
    /// reads the same however the set reached here — a `Set.of` iterates in an
    /// order that varies between JVM runs, and this string is what a reader
    /// compares between two invocations.
    ///
    /// @return the label, or [Strings#ABSENT_VALUE] for an empty set — a chunk
    ///         declaring nothing but the level encodings has no data-page
    ///         encoding to name
    public static String label(Collection<Encoding> encodings) {
        if (encodings.isEmpty()) {
            return Strings.ABSENT_VALUE;
        }
        StringBuilder label = new StringBuilder();
        for (Encoding encoding : new TreeSet<>(encodings)) {
            if (!label.isEmpty()) {
                label.append('+');
            }
            label.append(abbreviate(encoding));
        }
        return label.toString();
    }

    /// The encoding label with the dictionary's cardinality appended, as the
    /// share of the values it holds an entry for.
    ///
    /// `DICT` alone cannot separate a dictionary that pays for itself from one
    /// that is a verbatim second copy of the column: both read the same. At
    /// 100% every value is distinct, so the values are stored once in the
    /// dictionary page and once more as a stream of distinct indices, and no
    /// codec undoes the second copy — the indices are high-entropy by
    /// construction. Reading `100%` is the signal to look at the column;
    /// what to encode it as instead is not a question the footer can answer.
    ///
    /// @param entries distinct values the dictionary holds, or -1 when unknown
    /// @param values values it could hold an entry for — the present-value
    ///        count where that is known, since nulls never reach a dictionary
    public static String label(Collection<Encoding> encodings, long entries, long values) {
        String label = label(encodings);
        if (entries < 0 || values <= 0 || !usesDictionary(encodings)) {
            return label;
        }
        return label + " " + percentage(entries, values);
    }

    /// Rounds to whole percent — the reading is "all of them" or "hardly any",
    /// not a measurement. A non-zero cardinality that rounds to zero renders
    /// `<1%` so it cannot be read as "no dictionary".
    private static String percentage(long entries, long values) {
        double share = 100.0 * entries / values;
        if (share > 0 && share < 0.5) {
            return "<1%";
        }
        return Math.round(share) + "%";
    }

    private static boolean usesDictionary(Collection<Encoding> encodings) {
        return encodings.contains(Encoding.RLE_DICTIONARY)
                || encodings.contains(Encoding.PLAIN_DICTIONARY);
    }

    private static String abbreviate(Encoding encoding) {
        return switch (encoding) {
            case PLAIN_DICTIONARY, RLE_DICTIONARY -> "DICT";
            case DELTA_BINARY_PACKED -> "DELTA";
            case DELTA_LENGTH_BYTE_ARRAY -> "DELTA_LEN";
            case DELTA_BYTE_ARRAY -> "DELTA_BA";
            case BYTE_STREAM_SPLIT -> "BSS";
            default -> encoding.name();
        };
    }
}
