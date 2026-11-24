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

import dev.hardwood.internal.metadata.SchemaElements;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;

/// The `path_in_schema` of a column repeats in every row group's chunk of it, and the decoder
/// caches it by position — the n-th chunk of a row group is expected to name the same column as
/// the n-th chunk of the row group before it.
///
/// The format requires that ordering (`RowGroup.columns` "must have the same order as the
/// SchemaElement list in FileMetaData"), but the decoder treats it as a prediction and checks
/// the cached path against the bytes it is about to read. These tests pin both halves: the
/// prediction pays off on a conforming file, and a file that breaks the ordering still decodes
/// to the paths it actually carries.
class RepeatedPathCacheTest {

    @Test
    void sharesOnePathInstanceAcrossRowGroupsOfTheSameColumn() throws IOException {
        FileMetaData decoded = roundTrip(List.of(
                List.of("a", "b", "c"),
                List.of("a", "b", "c"),
                List.of("a", "b", "c")));

        assertThat(pathsOf(decoded, 0)).containsExactly("a", "b", "c");
        assertThat(pathsOf(decoded, 1)).containsExactly("a", "b", "c");
        assertThat(pathsOf(decoded, 2)).containsExactly("a", "b", "c");

        // One FieldPath per column rather than one per chunk: what the cache is for.
        for (int column = 0; column < 3; column++) {
            FieldPath first = pathAt(decoded, 0, column);
            assertThat(pathAt(decoded, 1, column)).isSameAs(first);
            assertThat(pathAt(decoded, 2, column)).isSameAs(first);
        }
    }

    @Test
    void decodesRowGroupsThatOrderTheirColumnsDifferently() throws IOException {
        // Against the spec, and so the case the positional prediction would get wrong if it
        // trusted position instead of checking the bytes.
        FileMetaData decoded = roundTrip(List.of(
                List.of("a", "b", "c"),
                List.of("c", "a", "b"),
                List.of("b", "c", "a")));

        assertThat(pathsOf(decoded, 0)).containsExactly("a", "b", "c");
        assertThat(pathsOf(decoded, 1)).containsExactly("c", "a", "b");
        assertThat(pathsOf(decoded, 2)).containsExactly("b", "c", "a");
    }

    @Test
    void decodesRowGroupsOfDifferingColumnCounts() throws IOException {
        // A shorter row group leaves the cache primed past its end; the next longer one must
        // still decode every path it carries.
        FileMetaData decoded = roundTrip(List.of(
                List.of("a", "b", "c"),
                List.of("a"),
                List.of("a", "b", "c")));

        assertThat(pathsOf(decoded, 0)).containsExactly("a", "b", "c");
        assertThat(pathsOf(decoded, 1)).containsExactly("a");
        assertThat(pathsOf(decoded, 2)).containsExactly("a", "b", "c");
    }

    @Test
    void decodesNestedPathsOfSeveralComponents() throws IOException {
        FileMetaData decoded = roundTrip(List.of(
                List.of("address.zip", "address.city"),
                List.of("address.zip", "address.city")));

        assertThat(pathAt(decoded, 0, 0).elements()).containsExactly("address", "zip");
        assertThat(pathAt(decoded, 1, 1).elements()).containsExactly("address", "city");
        assertThat(pathAt(decoded, 1, 0)).isSameAs(pathAt(decoded, 0, 0));
    }

    /// Writes a footer whose row groups carry the given column paths — one inner list per row
    /// group, each element a dot-separated path — and reads it back.
    private static FileMetaData roundTrip(List<List<String>> rowGroupPaths) throws IOException {
        List<RowGroup> rowGroups = new ArrayList<>();
        for (List<String> paths : rowGroupPaths) {
            List<ColumnChunk> chunks = new ArrayList<>();
            for (String path : paths) {
                chunks.add(new ColumnChunk(columnMetaData(path), null, null, null, null, ""));
            }
            rowGroups.add(new RowGroup(List.copyOf(chunks), 1024, 10));
        }

        FileMetaData metaData = new FileMetaData(1, schemaElements(rowGroupPaths.get(0)), 10,
                List.copyOf(rowGroups), Map.of(), "test", List.of());
        ThriftCompactWriter writer = new ThriftCompactWriter();
        FileMetaDataWriter.write(writer, metaData);
        return FileMetaDataReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));
    }

    private static ColumnMetaData columnMetaData(String path) {
        return new ColumnMetaData(PhysicalType.DOUBLE, List.of(Encoding.PLAIN),
                FieldPath.of(path.split("\\.")), CompressionCodec.UNCOMPRESSED,
                10, 80, 80, Map.of(), 4, null, null, null, null, null, List.of(), null);
    }

    private static List<SchemaElement> schemaElements(List<String> paths) {
        List<SchemaElement> elements = new ArrayList<>();
        elements.add(SchemaElements.group("schema", RepetitionType.REQUIRED, paths.size()));
        for (String path : paths) {
            elements.add(SchemaElements.primitive(path.replace('.', '_'), PhysicalType.DOUBLE, RepetitionType.REQUIRED));
        }
        return elements;
    }

    private static List<String> pathsOf(FileMetaData metaData, int rowGroup) {
        return metaData.rowGroups().get(rowGroup).columns().stream()
                .map(chunk -> String.join(".", chunk.metaData().pathInSchema().elements()))
                .toList();
    }

    private static FieldPath pathAt(FileMetaData metaData, int rowGroup, int column) {
        return metaData.rowGroups().get(rowGroup).columns().get(column).metaData().pathInSchema();
    }
}
