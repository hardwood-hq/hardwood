/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// Built from hand-assembled `encoding_stats` rather than a fixture: the
/// interesting shapes — a dictionary abandoned mid-chunk, a chunk with no page
/// stats at all — are ones no checked-in file happens to have.
class EncodingsTest {

    @Test
    void dataPageEncodingsSeparateTheDictionaryPageFromTheDataPages() {
        assertThat(label(chunkEncodedAs(
                stat(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                stat(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 10))))
                .isEqualTo("DICT");

        assertThat(label(chunkEncodedAs(
                stat(PageType.DATA_PAGE, Encoding.PLAIN, 10))))
                .isEqualTo("PLAIN");

        // The writer started with a dictionary and abandoned it mid-chunk.
        assertThat(label(chunkEncodedAs(
                stat(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                stat(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 4),
                stat(PageType.DATA_PAGE, Encoding.PLAIN, 6))))
                .isEqualTo("PLAIN+DICT");

        assertThat(label(chunkEncodedAs(
                stat(PageType.DATA_PAGE_V2, Encoding.DELTA_BINARY_PACKED, 3))))
                .isEqualTo("DELTA");
    }

    /// Without `encoding_stats` the flat list is all there is. The level
    /// streams are RLE whatever the values use, so they say nothing about the
    /// values and are dropped.
    @Test
    void dataPageEncodingsFallBackToTheFlatListWithoutPageStats() {
        ColumnMetaData chunk = chunkDeclaring(
                List.of(Encoding.RLE, Encoding.PLAIN, Encoding.RLE_DICTIONARY));

        assertThat(label(chunk)).isEqualTo("PLAIN+DICT");
        // Nothing to add: the declared list is where the label came from, so
        // `dive` has no second row to show beside it.
        assertThat(Encodings.hasEncodingStats(chunk)).isFalse();
    }

    /// A chunk declaring nothing but the level encodings leaves the set empty,
    /// and every surface uses the shared absent-value marker.
    @Test
    void anEmptySetRendersAsTheSharedPlaceholder() {
        ColumnMetaData chunk = chunkDeclaring(List.of(Encoding.RLE, Encoding.BIT_PACKED));

        assertThat(Encodings.dataPages(chunk)).isEmpty();
        assertThat(Encodings.label(Encodings.dataPages(chunk))).isEqualTo("—");
        assertThat(Encodings.label(Encodings.dataPages(chunk), 3, 10)).isEqualTo("—");
    }

    /// A page type the writer recorded but never wrote contributes nothing.
    @Test
    void aZeroCountPageTypeIsNotAnEncodingInUse() {
        assertThat(label(chunkEncodedAs(
                stat(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 10),
                stat(PageType.DATA_PAGE, Encoding.BYTE_STREAM_SPLIT, 0))))
                .isEqualTo("DICT");
    }

    @Test
    void encodingStatsArePresentWhenTheWriterRecordedThem() {
        assertThat(Encodings.hasEncodingStats(
                chunkEncodedAs(stat(PageType.DATA_PAGE, Encoding.PLAIN, 1)))).isTrue();
    }

    /// The whole point of the figure: `DICT` alone reads the same for a
    /// dictionary that pays for itself and one that is a second copy of the
    /// column, and only the cardinality separates them.
    @Test
    void theCardinalitySeparatesAUsefulDictionaryFromAVerbatimCopy() {
        Set<Encoding> dict = Set.of(Encoding.RLE_DICTIONARY);

        assertThat(Encodings.label(dict, 20_000, 20_000)).isEqualTo("DICT 100%");
        assertThat(Encodings.label(dict, 10, 20_000)).isEqualTo("DICT <1%");
        assertThat(Encodings.label(dict, 5_000, 20_000)).isEqualTo("DICT 25%");
    }

    /// `<1%` rather than `0%`, which would read as "no dictionary" beside a
    /// label that just said there is one.
    @Test
    void aCardinalityRoundingToZeroIsNotRenderedAsZero() {
        assertThat(Encodings.label(Set.of(Encoding.RLE_DICTIONARY), 1, 1_000_000))
                .isEqualTo("DICT <1%");
    }

    /// A chunk whose dictionary page could not be read gets no annotation
    /// rather than a fabricated one, and a chunk that uses no dictionary has
    /// nothing for the figure to qualify.
    @Test
    void theCardinalityIsDroppedWhereItSaysNothing() {
        assertThat(Encodings.label(Set.of(Encoding.RLE_DICTIONARY), -1, 20_000))
                .isEqualTo("DICT");
        assertThat(Encodings.label(Set.of(Encoding.PLAIN), 20_000, 20_000))
                .isEqualTo("PLAIN");
        assertThat(Encodings.label(Set.of(Encoding.RLE_DICTIONARY), 10, 0))
                .isEqualTo("DICT");
    }

    /// A mid-chunk fallback still carries the cardinality: the dictionary the
    /// writer built before giving up is exactly what the figure measures.
    @Test
    void aFallbackChunkStillCarriesItsCardinality() {
        assertThat(Encodings.label(Set.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY),
                16_000, 20_000)).isEqualTo("PLAIN+DICT 80%");
    }

    /// `Set.of` iterates in an order that varies between JVM runs, so a label
    /// built in the caller's order would differ between two invocations
    /// against the same file.
    @Test
    void theLabelIsOrderedByTheEnumNotByTheCallersIteration() {
        assertThat(Encodings.label(List.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN)))
                .isEqualTo("PLAIN+DICT");
        assertThat(Encodings.label(List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY)))
                .isEqualTo("PLAIN+DICT");
    }

    private static String label(ColumnMetaData chunk) {
        return Encodings.label(Encodings.dataPages(chunk));
    }

    private static PageEncodingStats stat(PageType pageType, Encoding encoding, int count) {
        return new PageEncodingStats(pageType, encoding, count);
    }

    private static ColumnMetaData chunkEncodedAs(PageEncodingStats... stats) {
        return new ColumnMetaData(PhysicalType.INT32, List.of(), FieldPath.of("c"),
                CompressionCodec.ZSTD, 10, 100, 50, Map.of(), 0L, null,
                null, null, null, null, List.of(stats), null);
    }

    private static ColumnMetaData chunkDeclaring(List<Encoding> encodings) {
        return new ColumnMetaData(PhysicalType.INT32, encodings, FieldPath.of("c"),
                CompressionCodec.ZSTD, 10, 100, 50, Map.of(), 0L, null,
                null, null, null, null, List.of(), null);
    }
}
