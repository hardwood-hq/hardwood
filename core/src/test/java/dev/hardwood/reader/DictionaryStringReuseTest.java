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
}
