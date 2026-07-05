/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;

/// Equal values of a dictionary-encoded column materialise to the same interned
/// `String` instance, instead of a fresh allocation per value.
class DictionaryStringReuseTest {

    /// `dictionary_uncompressed.parquet` has a dictionary-encoded `category`
    /// column with values `[A, B, A, C, B]` — A and B each appear twice.
    @Test
    void repeatedDictionaryValuesReuseTheSameStringInstance() throws Exception {
        Path file = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        List<String> categories = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                categories.add(rows.getString("category"));
            }
        }

        assertThat(categories).containsExactly("A", "B", "A", "C", "B");
        // The two "A"s come from the same dictionary entry — one interned instance.
        assertThat(categories.get(0)).isSameAs(categories.get(2));
        // Likewise the two "B"s.
        assertThat(categories.get(1)).isSameAs(categories.get(4));
    }

    /// On the nested path, a dictionary-encoded value is interned too: re-reading
    /// the same cell returns the same instance (a cache hit), rather than a fresh
    /// `String` per call. `nested_dict_batch_boundary.parquet` has a
    /// dictionary-encoded `nested.name` field.
    @Test
    void nestedDictionaryStringsAreInterned() throws Exception {
        Path file = Paths.get("src/test/resources/nested_dict_batch_boundary.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqStruct nested = rows.getStruct("nested");
                if (nested == null) {
                    continue;
                }
                String name = nested.getString("name");
                if (name == null) {
                    continue;
                }
                // Re-reading the same dictionary-encoded cell returns the same
                // interned instance.
                assertThat(nested.getString("name")).isSameAs(name);
                return;
            }
            throw new AssertionError("fixture had no non-null nested.name to assert on");
        }
    }

    /// Cross-batch: a value recurs across multiple batches of one chunk yet
    /// interns to a single instance (per chunk, not per batch).
    /// `dict_cross_batch.parquet` has 600k rows (more than one row-reader batch)
    /// with 3 distinct values in a single row group.
    @Test
    void dictionaryStringsAreInternedAcrossBatches() throws Exception {
        Path file = Paths.get("src/test/resources/dict_cross_batch.parquet");

        Set<String> instances = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> values = new HashSet<>();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rr = reader.rowReader()) {
            while (rr.hasNext()) {
                rr.next();
                String v = rr.getString("label");
                instances.add(v);
                values.add(v);
                rows++;
            }
        }

        assertThat(rows).isEqualTo(600_000L);
        assertThat(values).hasSize(3);
        // One instance per distinct value over the whole chunk — the cache spans
        // batches, so a per-batch count (which would exceed 3) is ruled out.
        assertThat(instances).hasSize(3);
    }

    /// The generic `getValue` and the list `get(i)` / `strings()` / `values()`
    /// accessors return the interned instance for equal dictionary values — and
    /// the same instance as the typed `getString` for the same cell.
    @Test
    void genericAndListStringAccessorsAreInterned() throws Exception {
        Path file = Paths.get("src/test/resources/dict_nested_repeats.parquet");

        Set<String> listInstances = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> listValues = new HashSet<>();
        Set<String> nameInstances = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> nameValues = new HashSet<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rr = reader.rowReader()) {
            while (rr.hasNext()) {
                rr.next();
                PqList tags = rr.getList("tags");
                List<String> asStrings = tags.strings();
                List<Object> asValues = tags.values();
                for (int i = 0; i < tags.size(); i++) {
                    String element = (String) tags.get(i);
                    listInstances.add(element);
                    listValues.add(element);
                    // The list views return the same interned instances as get(i).
                    listInstances.add(asStrings.get(i));
                    listInstances.add((String) asValues.get(i));
                }
                PqStruct info = rr.getStruct("info");
                if (info != null && info.getString("name") != null) {
                    String typed = info.getString("name");
                    assertThat((String) info.getValue("name")).isSameAs(typed);
                    nameInstances.add(typed);
                    nameValues.add(typed);
                }
            }
        }

        // Values repeat, and every distinct value is a single interned instance
        // across get(i) / strings() / values().
        assertThat(listValues.size()).isGreaterThan(1);
        assertThat(listInstances).hasSameSizeAs(listValues);
        assertThat(nameValues.size()).isGreaterThan(1);
        assertThat(nameInstances).hasSameSizeAs(nameValues);
    }

    /// A null dictionary value reads back as null on both the typed and generic
    /// accessors. `nested_dict_batch_boundary.parquet` has null `nested.name`s.
    @Test
    void nullDictionaryStringReadsAsNull() throws Exception {
        Path file = Paths.get("src/test/resources/nested_dict_batch_boundary.parquet");

        boolean sawNullName = false;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rr = reader.rowReader()) {
            while (rr.hasNext()) {
                rr.next();
                PqStruct nested = rr.getStruct("nested");
                if (nested == null) {
                    continue;
                }
                if (nested.getString("name") == null) {
                    sawNullName = true;
                    assertThat(nested.getValue("name")).isNull();
                }
            }
        }
        assertThat(sawNullName).isTrue();
    }

    /// `getBinary` still returns the raw bytes for a dictionary-encoded column —
    /// the interning path leaves the packed-byte representation intact.
    @Test
    void getBinaryStillReturnsBytesForDictionaryColumn() throws Exception {
        Path file = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rr = reader.rowReader()) {
            rr.next();
            assertThat(rr.getBinary("category")).isEqualTo("A".getBytes(StandardCharsets.UTF_8));
        }
    }

    /// Cross-CHUNK straddle: `dict_cross_chunk.parquet` has two row groups (two
    /// dictionaries) with disjoint pools — rows 0-99 from `{alpha,bravo,charlie}`,
    /// rows 100-199 from `{delta,echo,foxtrot}` — and is small enough (200 rows,
    /// below the 16384 batch floor) that the whole file is one row-reader batch
    /// spanning both chunks. The batch keeps chunk 0's dictionary and byte-decodes
    /// chunk 1's values; a regression that indexed chunk 1's ordinals into chunk
    /// 0's dictionary would return chunk 0's strings (silently wrong) or go out of
    /// bounds. The disjoint pools make any mis-resolution observable.
    @Test
    void dictionaryStraddlingTwoChunksResolvesEachAgainstItsOwnDictionary() throws Exception {
        Path file = Paths.get("src/test/resources/dict_cross_chunk.parquet");
        String[] poolA = {"alpha", "bravo", "charlie"};
        String[] poolB = {"delta", "echo", "foxtrot"};

        List<String> values = new ArrayList<>();
        Set<String> chunk0Instances = Collections.newSetFromMap(new IdentityHashMap<>());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rr = reader.rowReader()) {
            int row = 0;
            while (rr.hasNext()) {
                rr.next();
                String v = rr.getString("label");
                values.add(v);
                if (row < 100) {
                    chunk0Instances.add(v);
                }
                row++;
            }
        }

        assertThat(values).hasSize(200);
        for (int i = 0; i < 200; i++) {
            String[] pool = i < 100 ? poolA : poolB;
            assertThat(values.get(i)).as("row %d", i).isEqualTo(pool[i % 3]);
        }
        // Row 102 is a chunk-1 value ("delta") absent from chunk 0's dictionary;
        // if its ordinal were resolved against chunk 0's dictionary it would read
        // a chunk-0 entry instead — the exact straddle regression.
        assertThat(values.get(102)).isEqualTo("delta");
        // Chunk 0 is the batch's adopted dictionary, so its three values still
        // intern to one instance each — straddle does not disable interning for
        // the kept chunk.
        assertThat(chunk0Instances).hasSize(3);
    }

    /// The generic and typed `PqMap` key/value accessors return the interned
    /// instance for equal dictionary values, like the list and struct paths.
    /// `dict_string_map.parquet` has 200 rows of a `map<string,string>` `props`
    /// column (keys `color`/`size`; values from a small dictionary).
    @Test
    void genericMapKeyAndValueAccessorsAreInterned() throws Exception {
        Path file = Paths.get("src/test/resources/dict_string_map.parquet");

        Set<String> keyInstances = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> keyValues = new HashSet<>();
        Set<String> valueInstances = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> valueValues = new HashSet<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rr = reader.rowReader()) {
            while (rr.hasNext()) {
                rr.next();
                PqMap props = rr.getMap("props");
                for (PqMap.Entry entry : props.getEntries()) {
                    String typedKey = entry.getStringKey();
                    String typedValue = entry.getStringValue();
                    keyInstances.add(typedKey);
                    keyValues.add(typedKey);
                    valueInstances.add(typedValue);
                    valueValues.add(typedValue);
                    // The generic accessors resolve the same cell through the same
                    // per-chunk cache, so they hand back the same interned instance.
                    assertThat((String) entry.getKey()).isSameAs(typedKey);
                    assertThat((String) entry.getValue()).isSameAs(typedValue);
                }
            }
        }

        // Keys and values recur across rows; each distinct one is a single
        // interned instance across the chunk.
        assertThat(keyValues).containsExactlyInAnyOrder("color", "size");
        assertThat(keyInstances).hasSameSizeAs(keyValues);
        assertThat(valueValues.size()).isGreaterThan(1);
        assertThat(valueInstances).hasSameSizeAs(valueValues);
    }

    /// The column reader's `getStrings()` interns on the unfiltered flat path: it
    /// resolves through the same per-chunk cache as the row reader, so equal
    /// values in a batch are one shared instance rather than a fresh decode each.
    /// `dictionary_uncompressed.parquet` is a flat `category` column
    /// `[A, B, A, C, B]` — A and B each appear twice in a single batch.
    @Test
    void columnReaderInternsFlatDictionaryStrings() throws Exception {
        Path file = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader category = reader.columnReader("category")) {
            assertThat(category.nextBatch()).isTrue();
            String[] values = category.getStrings();

            assertThat(values).containsExactly("A", "B", "A", "C", "B");
            // The two "A"s and the two "B"s each collapse to one interned instance.
            assertThat(values[0]).isSameAs(values[2]);
            assertThat(values[1]).isSameAs(values[4]);
            assertThat(category.nextBatch()).isFalse();
        }
    }

    /// The column reader interns on the struct-only nested path too: a `STRUCT`
    /// chain has no repeated layer, so the leaf passes through compaction with its
    /// dictionary intact. `dict_nested_repeats.parquet` leaf 0 is `info.name`,
    /// drawn from a 4-value pool with the struct null on every 13th row.
    @Test
    void columnReaderInternsStructOnlyNestedDictionaryStrings() throws Exception {
        Path file = Paths.get("src/test/resources/dict_nested_repeats.parquet");

        Set<String> instances = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> values = new HashSet<>();
        boolean sawNull = false;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader name = reader.buildColumnReader(0).build()) {
            while (name.nextBatch()) {
                for (String n : name.getStrings()) {
                    if (n == null) {
                        sawNull = true;
                        continue;
                    }
                    instances.add(n);
                    values.add(n);
                }
            }
        }

        // Values repeat across the batch; every distinct one is a single interned
        // instance on the struct-only pass-through path.
        assertThat(values).containsExactlyInAnyOrder("alpha", "beta", "gamma", "delta");
        assertThat(instances).hasSameSizeAs(values);
        // Null struct positions still read back as null.
        assertThat(sawNull).isTrue();
    }

    /// The compacted nested path interns too: reading the `tags` list compacts the
    /// leaf (`compactBinary`), which carries the chunk dictionary through — so
    /// `getStrings()` returns the exact values in leaf order *and* reuses one
    /// interned instance per distinct value. `dict_nested_repeats.parquet` leaf 1
    /// is the `tags` `list<string>` element.
    @Test
    void columnReaderInternsListDictionaryStrings() throws Exception {
        Path file = Paths.get("src/test/resources/dict_nested_repeats.parquet");
        String[] pool = {"alpha", "beta", "gamma", "delta"};
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < i % 3; j++) {
                expected.add(pool[(i + j) % 4]);
            }
        }

        List<String> actual = new ArrayList<>();
        Set<String> instances = Collections.newSetFromMap(new IdentityHashMap<>());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader tags = reader.buildColumnReader(1).build()) {
            while (tags.nextBatch()) {
                String[] batch = tags.getStrings();
                Collections.addAll(actual, batch);
                Collections.addAll(instances, batch);
            }
        }

        // Parity in leaf order, and every distinct value is one interned instance.
        assertThat(actual).isEqualTo(expected);
        assertThat(new HashSet<>(actual)).containsExactlyInAnyOrder("alpha", "beta", "gamma", "delta");
        assertThat(instances).hasSize(4);
    }

    /// The compacted flat path interns too: a filter drops rows and compacts the
    /// leaf (`compactBinary`), which carries the chunk dictionary through.
    /// `dictionary_uncompressed.parquet` is `id` + dictionary `category`
    /// `[A, B, A, C, B]`; `id < 4` keeps `[A, B, A]`, so the two kept "A"s are one
    /// interned instance.
    @Test
    void columnReaderInternsFilteredFlatDictionaryStrings() throws Exception {
        Path file = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader category = reader.buildColumnReader("category")
                        .filter(FilterPredicate.lt("id", 4L)).build()) {
            assertThat(category.nextBatch()).isTrue();
            String[] values = category.getStrings();

            assertThat(values).containsExactly("A", "B", "A");
            assertThat(values[0]).isSameAs(values[2]);
        }
    }
}
