/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;

/// Issue #636: equal values of a dictionary-encoded column materialise to the
/// same interned `String` instance, instead of a fresh allocation per value.
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
}
