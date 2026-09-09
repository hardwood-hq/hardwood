/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// A legacy INT96 timestamp reads as an [Instant] wherever it sits (#1151).
///
/// INT96 carries no logical type, so the decode cannot be driven by an annotation
/// the way every other timestamp is; it hangs off the physical type alone. The
/// struct, list and map flyweights each reach a leaf through the same
/// `SchemaNode`-aware shim, and this pins all three to the reading the flat
/// accessor already gives — see [LocalTimestampTest] for the flat column.
///
/// Fixture: `tools/simple-datagen.py` → `nested_int96_timestamp_test.parquet`.
/// One row holding the same instant as a top-level leaf, a struct field, a list
/// element and a map value.
class NestedInt96TimestampTest {

    private static final Path FILE =
            Paths.get("src/test/resources/nested_int96_timestamp_test.parquet");

    private static final Instant FIRST = Instant.parse("2026-03-05T09:30:00.123456Z");
    private static final Instant SECOND = Instant.parse("2026-03-05T17:45:30Z");

    /// A top-level leaf in a file whose schema is nested is read by
    /// `NestedBatchDataView`, not by the flat reader and not by a flyweight.
    @Test
    void topLevelLeafDecodesToInstant() throws IOException {
        withFirstRow(rows -> assertThat(rows.getTimestamp("at")).isEqualTo(FIRST));
    }

    @Test
    void structFieldDecodesToInstant() throws IOException {
        withFirstRow(rows -> assertThat(rows.getStruct("event").getTimestamp("at")).isEqualTo(FIRST));
    }

    @Test
    void listElementsDecodeToInstants() throws IOException {
        withFirstRow(rows -> assertThat(rows.getList("samples").timestamps())
                .containsExactly(FIRST, SECOND));
    }

    @Test
    void mapValueDecodesToInstant() throws IOException {
        withFirstRow(rows -> assertThat(rows.getMap("marks").getEntries().get(0).getTimestampValue())
                .isEqualTo(FIRST));
    }

    /// The untyped accessor sees the same decode as the typed one, so a caller
    /// walking a schema it does not know still gets an [Instant] rather than the
    /// undecoded 12-byte payload.
    @Test
    void genericListValuesDecodeToInstants() throws IOException {
        withFirstRow(rows -> assertThat(rows.getList("samples").values())
                .containsExactly(FIRST, SECOND));
    }

    private interface RowAssertion {
        void accept(RowReader rows);
    }

    private static void withFirstRow(RowAssertion assertion) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertion.accept(rows);
        }
    }
}
