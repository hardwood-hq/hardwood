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

import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.reader.SchemaIncompatibleException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests that [FileManager] validates logical type and repetition type
/// compatibility across files in multi-file reading (issue #202).
class SchemaCompatibilityTest {

    @Test
    void rejectTimestampUnitMismatch() {
        Path micros = Paths.get("src/test/resources/compat_ts_micros.parquet");
        Path millis = Paths.get("src/test/resources/compat_ts_millis.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(micros, millis));
                     RowReader reader = parquet.createRowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_ts_millis.parquet] Column 'ts' has incompatible logical type:" +
                            " expected TimestampType[isAdjustedToUTC=true, unit=MICROS]" +
                            " but found TimestampType[isAdjustedToUTC=true, unit=MILLIS]");
        }
    }

    @Test
    void rejectDecimalScaleMismatch() {
        Path dec10_2 = Paths.get("src/test/resources/compat_decimal_10_2.parquet");
        Path dec10_4 = Paths.get("src/test/resources/compat_decimal_10_4.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(dec10_2, dec10_4));
                     RowReader reader = parquet.createRowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_decimal_10_4.parquet] Column 'amount' has incompatible logical type:" +
                            " expected DecimalType[scale=2, precision=10]" +
                            " but found DecimalType[scale=4, precision=10]");
        }
    }

    @Test
    void rejectRepetitionTypeMismatch() {
        Path required = Paths.get("src/test/resources/compat_required.parquet");
        Path optional = Paths.get("src/test/resources/compat_optional_value.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(required, optional));
                     RowReader reader = parquet.createRowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_optional_value.parquet] Column 'value' has incompatible repetition type:" +
                            " expected REQUIRED but found OPTIONAL");
        }
    }

    @Test
    void rejectLogicalTypePresenceMismatch() {
        Path tsMicros = Paths.get("src/test/resources/compat_ts_micros.parquet");
        Path plainInt = Paths.get("src/test/resources/compat_plain_int64.parquet");

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(tsMicros, plainInt));
                     RowReader reader = parquet.createRowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_plain_int64.parquet] Column 'ts' has incompatible logical type:" +
                            " expected TimestampType[isAdjustedToUTC=true, unit=MICROS] but found null");
        }
    }

    @Test
    void acceptCompatibleSchemas() throws Exception {
        // Same file twice should always be compatible
        Path micros = Paths.get("src/test/resources/compat_ts_micros.parquet");

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(micros, micros));
             RowReader reader = parquet.createRowReader()) {

            int count = 0;
            while (reader.hasNext()) {
                reader.next();
                count++;
            }
            assertThat(count).isEqualTo(4);
        }
    }
}
