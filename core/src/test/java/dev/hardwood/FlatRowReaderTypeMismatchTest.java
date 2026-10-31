/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A primitive accessor called on a column of another physical type names the file, the column
/// and its type, the type it was asked for, and the accessor that reads the column.
class FlatRowReaderTypeMismatchTest {

    private static final String PREFIX = "[primitive_types_test.parquet] ";

    @Test
    void primitiveAccessorOfAnotherTypeNamesTheColumnItsTypeAndTheAccessorThatFits() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/primitive_types_test.parquet")));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            assertThatThrownBy(() -> rows.getInt("long_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'long_col' is INT64, which cannot be read as an int;"
                            + " read it with getLong");
            assertThatThrownBy(() -> rows.getLong("int_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'int_col' is INT32, which cannot be read as a long;"
                            + " read it with getInt");
            assertThatThrownBy(() -> rows.getDouble("float_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'float_col' is FLOAT, which cannot be read as a double;"
                            + " read it with getFloat");
            assertThatThrownBy(() -> rows.getBoolean("double_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'double_col' is DOUBLE, which cannot be read as a boolean;"
                            + " read it with getDouble");
            assertThatThrownBy(() -> rows.getBinary("bool_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'bool_col' is BOOLEAN, which cannot be read as bytes;"
                            + " read it with getBoolean");
            assertThatThrownBy(() -> rows.getLong("string_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'string_col' is BYTE_ARRAY annotated STRING, which cannot"
                            + " be read as a long; read it with getString");
            assertThatThrownBy(() -> rows.getInt("binary_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'binary_col' is BYTE_ARRAY, which cannot be read as an int;"
                            + " read it with getBinary");
        }
    }

    /// `getFloat` reads a column that is not a `FLOAT` as a `FLOAT16`, so on any other column it
    /// fails through the same check the nested reader makes, with the same message.
    @Test
    void getFloatOnAnotherTypeFailsAsTheNestedReaderDoes() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/primitive_types_test.parquet")));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            assertThatThrownBy(() -> rows.getFloat("long_col"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(PREFIX + "Column 'long_col' is INT64, which cannot be read as a float");
        }
    }

    @Test
    void primitiveAccessorOfItsOwnTypeReadsTheValue() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/primitive_types_test.parquet")));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            assertThat(rows.getInt("int_col")).isEqualTo(1);
            assertThat(rows.getLong("long_col")).isEqualTo(100L);
            assertThat(rows.getFloat("float_col")).isEqualTo(1.5f);
            assertThat(rows.getDouble("double_col")).isEqualTo(10.5);
            assertThat(rows.getBoolean("bool_col")).isTrue();
            assertThat(rows.getString("string_col")).isEqualTo("hello");
            assertThat(rows.getBinary("binary_col")).containsExactly(0, 1, 2);
        }
    }
}
