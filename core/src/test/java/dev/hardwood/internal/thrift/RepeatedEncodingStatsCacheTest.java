/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;

/// The `encoding_stats` of a column repeats in every row group's chunk of it, and the decoder
/// caches it by position — the n-th chunk of a row group is expected to carry the same stats as
/// the n-th chunk of the row group before it.
///
/// The format requires that ordering (`RowGroup.columns` "must have the same order as the
/// SchemaElement list in FileMetaData"), but the decoder treats it as a prediction and checks
/// the cached stats against the bytes it is about to read. These tests pin both halves: the
/// prediction pays off on a conforming file, and a file that breaks the ordering still decodes
/// to the stats it actually carries. A chunk that omits the field does not advance the cache
/// position, so a later chunk may be compared against another column's entry — that costs a
/// miss, never a wrong answer, because a hit requires byte-identical content.
class RepeatedEncodingStatsCacheTest {

    private static final List<PageEncodingStats> PLAIN_ONE =
            List.of(new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 1));
    private static final List<PageEncodingStats> DICT_TWO =
            List.of(new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 2));
    private static final List<PageEncodingStats> MIXED =
            List.of(new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 3));

    @Test
    void sharesOneStatsListInstanceAcrossRowGroupsOfTheSameColumn() throws IOException {
        FileMetaData decoded = roundTrip(List.of(
                List.of(PLAIN_ONE, DICT_TWO),
                List.of(PLAIN_ONE, DICT_TWO),
                List.of(PLAIN_ONE, DICT_TWO)));

        assertThat(statsOf(decoded, 0, 0)).containsExactlyElementsOf(PLAIN_ONE);
        assertThat(statsOf(decoded, 0, 1)).containsExactlyElementsOf(DICT_TWO);

        // One list per column rather than one per chunk: what the cache is for.
        for (int column = 0; column < 2; column++) {
            List<PageEncodingStats> first = statsOf(decoded, 0, column);
            assertThat(statsOf(decoded, 1, column)).isSameAs(first);
            assertThat(statsOf(decoded, 2, column)).isSameAs(first);
        }
    }

    @Test
    void decodesRowGroupsThatCarryDifferentStats() throws IOException {
        // Against the prediction, and so the case the positional cache would get wrong if it
        // trusted position instead of checking the bytes.
        FileMetaData decoded = roundTrip(List.of(
                List.of(PLAIN_ONE, DICT_TWO),
                List.of(DICT_TWO, PLAIN_ONE),
                List.of(MIXED, PLAIN_ONE)));

        assertThat(statsOf(decoded, 0, 0)).containsExactlyElementsOf(PLAIN_ONE);
        assertThat(statsOf(decoded, 1, 0)).containsExactlyElementsOf(DICT_TWO);
        assertThat(statsOf(decoded, 1, 1)).containsExactlyElementsOf(PLAIN_ONE);
        assertThat(statsOf(decoded, 2, 0)).containsExactlyElementsOf(MIXED);
        assertThat(statsOf(decoded, 2, 1)).containsExactlyElementsOf(PLAIN_ONE);
    }

    @Test
    void decodesRowGroupsOfDifferingColumnCounts() throws IOException {
        // A shorter row group leaves the cache primed past its end; the next longer one must
        // still decode every stats list it carries.
        FileMetaData decoded = roundTrip(List.of(
                List.of(PLAIN_ONE, DICT_TWO),
                List.of(PLAIN_ONE),
                List.of(PLAIN_ONE, DICT_TWO)));

        assertThat(statsOf(decoded, 0, 1)).containsExactlyElementsOf(DICT_TWO);
        assertThat(statsOf(decoded, 1, 0)).containsExactlyElementsOf(PLAIN_ONE);
        assertThat(statsOf(decoded, 2, 0)).containsExactlyElementsOf(PLAIN_ONE);
        assertThat(statsOf(decoded, 2, 1)).containsExactlyElementsOf(DICT_TWO);
        assertThat(statsOf(decoded, 2, 1)).isSameAs(statsOf(decoded, 0, 1));
    }

    @Test
    void chunkWithoutStatsDoesNotBreakLaterChunks() throws IOException {
        // The writer omits an empty stats list, so the middle column of the second row group
        // carries no field 13 at all. The chunk after it still decodes to the stats it
        // carries, and the omitted chunk reads as an empty list.
        FileMetaData decoded = roundTrip(List.of(
                List.of(PLAIN_ONE, DICT_TWO, MIXED),
                List.of(PLAIN_ONE, List.of(), MIXED),
                List.of(PLAIN_ONE, DICT_TWO, MIXED)));

        assertThat(statsOf(decoded, 1, 0)).containsExactlyElementsOf(PLAIN_ONE);
        assertThat(statsOf(decoded, 1, 1)).isEmpty();
        assertThat(statsOf(decoded, 1, 2)).containsExactlyElementsOf(MIXED);
        assertThat(statsOf(decoded, 2, 1)).containsExactlyElementsOf(DICT_TWO);
        assertThat(statsOf(decoded, 2, 2)).containsExactlyElementsOf(MIXED);
    }

    /// Writes a footer whose row groups carry the given encoding stats — one inner list per row
    /// group, one stats list per column — and reads it back.
    private static FileMetaData roundTrip(List<List<List<PageEncodingStats>>> rowGroupStats) throws IOException {
        List<RowGroup> rowGroups = new ArrayList<>();
        for (List<List<PageEncodingStats>> stats : rowGroupStats) {
            List<ColumnChunk> chunks = new ArrayList<>();
            for (int column = 0; column < stats.size(); column++) {
                chunks.add(new ColumnChunk(columnMetaData("c" + column, stats.get(column)), null, null, null, null, ""));
            }
            rowGroups.add(new RowGroup(List.copyOf(chunks), 1024, 10));
        }

        List<String> paths = List.of("c0", "c1", "c2");
        FileMetaData metaData = new FileMetaData(1, schemaElements(paths), 10,
                List.copyOf(rowGroups), Map.of(), "test", List.of());
        ThriftCompactWriter writer = new ThriftCompactWriter();
        FileMetaDataWriter.write(writer, metaData);
        return FileMetaDataReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));
    }

    private static ColumnMetaData columnMetaData(String path, List<PageEncodingStats> stats) {
        return new ColumnMetaData(PhysicalType.DOUBLE, List.of(Encoding.PLAIN),
                FieldPath.of(path), CompressionCodec.UNCOMPRESSED,
                10, 80, 80, Map.of(), 4, null, null, null, null, null, stats, null);
    }

    private static List<SchemaElement> schemaElements(List<String> paths) {
        List<SchemaElement> elements = new ArrayList<>();
        elements.add(SchemaElement.group("schema", RepetitionType.REQUIRED, paths.size()));
        for (String path : paths) {
            elements.add(SchemaElement.primitive(path, PhysicalType.DOUBLE, RepetitionType.REQUIRED));
        }
        return elements;
    }

    private static List<PageEncodingStats> statsOf(FileMetaData metaData, int rowGroup, int column) {
        return metaData.rowGroups().get(rowGroup).columns().get(column).metaData().encodingStats();
    }
}
