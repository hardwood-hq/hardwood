/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// [NestedLevelComputer#computeLayers], one walk for every leaf, against a reference that walks
/// the schema from the root to each leaf separately, over the schema of every fixture in the
/// test resources except those whose footer does not open.
class ComputeLayersTest {

    /// Fixtures whose footer the reader refuses: two encrypted files and one with a negative page
    /// offset.
    private static final Set<String> UNOPENABLE = Set.of(
            "encrypted_footer.parquet",
            "encrypted_plaintext_footer.parquet",
            "negative_data_page_offset.parquet");

    @Test
    void matchesPerLeafWalkOnEveryFixture() throws IOException {
        int compared = 0;
        try (Stream<Path> files = Files.list(Path.of("src/test/resources"))) {
            for (Path file : files.filter(p -> p.toString().endsWith(".parquet")).sorted().toList()) {
                if (UNOPENABLE.contains(file.getFileName().toString())) {
                    continue;
                }
                FileSchema schema = schemaOf(file);
                NestedLevelComputer.Layers[] layers = NestedLevelComputer.computeLayers(schema);
                assertThat(layers).as(file.toString()).hasSize(schema.getColumnCount());
                for (int c = 0; c < schema.getColumnCount(); c++) {
                    NestedLevelComputer.Layers expected = referenceLayers(schema.getRootNode(), c);
                    assertThat(layers[c].kinds()).as(file + " column " + c).containsExactly(expected.kinds());
                    assertThat(layers[c].defThresholds()).as(file + " column " + c)
                            .containsExactly(expected.defThresholds());
                    assertThat(layers[c].itemDefThresholds()).as(file + " column " + c)
                            .containsExactly(expected.itemDefThresholds());
                    assertThat(layers[c].itemRepThresholds()).as(file + " column " + c)
                            .containsExactly(expected.itemRepThresholds());
                }
                compared++;
            }
        }
        assertThat(compared).isGreaterThan(100);
    }

    private static FileSchema schemaOf(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return reader.getFileSchema();
        }
    }

    private static NestedLevelComputer.Layers referenceLayers(SchemaNode.GroupNode root, int columnIndex) {
        List<LayerKind> kinds = new ArrayList<>();
        List<Integer> thresholds = new ArrayList<>();
        assertThat(walkToLeaf(root, columnIndex, kinds, thresholds, false)).isTrue();
        int[] threshArr = new int[thresholds.size()];
        for (int i = 0; i < threshArr.length; i++) {
            threshArr[i] = thresholds.get(i);
        }
        return NestedLevelComputer.Layers.of(kinds.toArray(new LayerKind[0]), threshArr);
    }

    private static boolean walkToLeaf(SchemaNode node, int columnIndex, List<LayerKind> kinds,
                                      List<Integer> thresholds, boolean parentWasListMap) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                if (prim.columnIndex() != columnIndex) {
                    yield false;
                }
                if (!parentWasListMap && prim.repetitionType() == RepetitionType.REPEATED) {
                    kinds.add(LayerKind.REPEATED);
                    thresholds.add(prim.maxDefinitionLevel() - 1);
                }
                yield true;
            }
            case SchemaNode.GroupNode group -> {
                boolean isListOrMap = group.isList() || group.isMap();
                int added = 0;
                if (!parentWasListMap && group.repetitionType() == RepetitionType.REPEATED) {
                    kinds.add(LayerKind.REPEATED);
                    thresholds.add(group.maxDefinitionLevel() - 1);
                    added++;
                }
                if (isListOrMap) {
                    kinds.add(LayerKind.REPEATED);
                    thresholds.add(group.maxDefinitionLevel());
                    added++;
                }
                else if (!parentWasListMap && group.repetitionType() == RepetitionType.OPTIONAL) {
                    kinds.add(LayerKind.STRUCT);
                    thresholds.add(group.maxDefinitionLevel());
                    added++;
                }
                for (SchemaNode child : group.children()) {
                    if (walkToLeaf(child, columnIndex, kinds, thresholds, isListOrMap)) {
                        yield true;
                    }
                }
                for (int k = 0; k < added; k++) {
                    kinds.removeLast();
                    thresholds.removeLast();
                }
                yield false;
            }
        };
    }
}
