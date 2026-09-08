/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.reader.SchemaIncompatibleException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests that multi-file reading validates logical type and repetition type
/// compatibility across files (issue #202).
class SchemaCompatibilityTest {

    @Test
    void metadataAccessDoesNotValidateSchemaCompatibility() throws Exception {
        Path micros = Paths.get("src/test/resources/compat_ts_micros.parquet");
        Path millis = Paths.get("src/test/resources/compat_ts_millis.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(micros, millis))) {
            assertThat(parquet.getFileMetaData(1).numRows()).isEqualTo(2);
            // The second file is planned when the read arrives at it, so the mismatch is
            // raised by the reading loop rather than by the call that builds the reader.
            // Asserting on rowReader() alone would pass only while the first file is small
            // enough that no batch is published before the second file is planned.
            assertThatThrownBy(() -> {
                try (RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class);
        }
    }

    @Test
    void rejectTimestampUnitMismatch() {
        Path micros = Paths.get("src/test/resources/compat_ts_micros.parquet");
        Path millis = Paths.get("src/test/resources/compat_ts_millis.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(micros, millis));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_ts_millis.parquet] column ts — Incompatible logical type: expected"
                            + " TIMESTAMP(MICROS, UTC) but found TIMESTAMP(MILLIS, UTC)");
        }
    }

    @Test
    void rejectDecimalScaleMismatch() {
        Path dec10_2 = Paths.get("src/test/resources/compat_decimal_10_2.parquet");
        Path dec10_4 = Paths.get("src/test/resources/compat_decimal_10_4.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(dec10_2, dec10_4));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_decimal_10_4.parquet] column amount — Incompatible logical type:"
                            + " expected DECIMAL(10, 2) but found DECIMAL(10, 4)");
        }
    }

    @Test
    void rejectRepetitionTypeMismatch() {
        Path required = Paths.get("src/test/resources/compat_required.parquet");
        Path optional = Paths.get("src/test/resources/compat_optional_value.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(required, optional));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_optional_value.parquet] column value — Incompatible repetition type:"
                            + " expected REQUIRED but found OPTIONAL");
        }
    }

    @Test
    void rejectLogicalTypePresenceMismatch() {
        Path tsMicros = Paths.get("src/test/resources/compat_ts_micros.parquet");
        Path plainInt = Paths.get("src/test/resources/compat_plain_int64.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(tsMicros, plainInt));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_plain_int64.parquet] column ts — Incompatible logical type: expected"
                            + " TIMESTAMP(MICROS, UTC) but found null");
        }
    }

    @Test
    void rejectFixedLenByteArrayWidthMismatch() {
        Path flba4 = Paths.get("src/test/resources/compat_flba_4.parquet");
        Path flba8 = Paths.get("src/test/resources/compat_flba_8.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(flba4, flba8));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_flba_8.parquet] column v — Incompatible type length: expected 4 but"
                            + " found 8");
        }
    }

    /// The leaf `g.v` is REQUIRED in both files; only its ancestor group's
    /// repetition type differs, which changes the maximum definition level and
    /// therefore how nulls are read.
    @Test
    void rejectAncestorNullabilityMismatch() {
        Path required = Paths.get("src/test/resources/compat_nested_req_group.parquet");
        Path optional = Paths.get("src/test/resources/compat_nested_opt_group.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(required, optional));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_nested_opt_group.parquet] column g.v — Incompatible maximum"
                            + " definition level: expected 0 but found 1");
        }
    }

    /// The leaf `g.list.element` is OPTIONAL in both files and carries the same
    /// maximum definition level (three nullable ancestors either way); only the
    /// middle group's repeatedness differs, which changes where record boundaries
    /// fall. Read by ordinal alone this pair silently loses rows: the leaf's own
    /// repetition type, physical type and definition levels all agree.
    @Test
    void rejectAncestorRepeatednessMismatch() {
        Path repeated = Paths.get("src/test/resources/compat_maxrep_list.parquet");
        Path nonRepeated = Paths.get("src/test/resources/compat_maxrep_struct.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(repeated, nonRepeated));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_maxrep_struct.parquet] column g.list.element — Incompatible maximum"
                            + " repetition level: expected 1 but found 0");
        }
    }

    /// The reverse order of [#rejectAncestorRepeatednessMismatch]: with the
    /// non-repeated file as the reference the mismatch produced no exception at
    /// all, only extra rows whose values had silently vanished.
    @Test
    void rejectAncestorRepeatednessMismatchInEitherOrder() {
        Path repeated = Paths.get("src/test/resources/compat_maxrep_list.parquet");
        Path nonRepeated = Paths.get("src/test/resources/compat_maxrep_struct.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(nonRepeated, repeated));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_maxrep_list.parquet] column g.list.element — Incompatible maximum"
                            + " repetition level: expected 0 but found 1");
        }
    }

    @Test
    void acceptCompatibleSchemas() throws Exception {
        // Same file twice should always be compatible
        Path micros = Paths.get("src/test/resources/compat_ts_micros.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(micros, micros));
             RowReader reader = parquet.rowReader()) {

            int count = 0;
            while (reader.hasNext()) {
                reader.next();
                count++;
            }
            assertThat(count).isEqualTo(4);
        }
    }
}
