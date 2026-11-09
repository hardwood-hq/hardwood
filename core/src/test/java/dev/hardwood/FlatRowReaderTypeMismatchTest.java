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

    /// The hint names the accessor the column's annotation reads it with, and the one its physical
    /// type reads it with where the annotation has none of its own.
    @Test
    void hintNamesTheAccessorOfTheColumnsAnnotation() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/logical_types_test.parquet")));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            String prefix = "[logical_types_test.parquet] ";
            assertThatThrownBy(() -> rows.getLong("birth_date"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'birth_date' is INT32 annotated DATE, which cannot be read as"
                            + " a long; read it with getDate");
            assertThatThrownBy(() -> rows.getInt("created_at_millis"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'created_at_millis' is INT64 annotated TIMESTAMP(MILLIS, UTC),"
                            + " which cannot be read as an int; read it with getTimestamp");
            assertThatThrownBy(() -> rows.getInt("wake_time_micros"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'wake_time_micros' is INT64 annotated TIME(MICROS, local),"
                            + " which cannot be read as an int; read it with getTime");
            assertThatThrownBy(() -> rows.getLong("balance"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'balance' is FIXED_LEN_BYTE_ARRAY annotated DECIMAL(10, 2),"
                            + " which cannot be read as a long; read it with getDecimal");
            assertThatThrownBy(() -> rows.getLong("account_id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'account_id' is FIXED_LEN_BYTE_ARRAY annotated UUID,"
                            + " which cannot be read as a long; read it with getUuid");
            assertThatThrownBy(() -> rows.getInt("profile_json"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'profile_json' is BYTE_ARRAY annotated JSON,"
                            + " which cannot be read as an int; read it with getString");
            assertThatThrownBy(() -> rows.getInt("bson_payload"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'bson_payload' is BYTE_ARRAY annotated BSON,"
                            + " which cannot be read as an int; read it with getBinary");
            assertThatThrownBy(() -> rows.getLong("tiny_int"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(prefix + "Column 'tiny_int' is INT32 annotated INT_8,"
                            + " which cannot be read as a long; read it with getInt");
        }
    }

    @Test
    void hintNamesGetFloatForAFloat16Column() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/float16_logical_type_test.parquet")));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            assertThatThrownBy(() -> rows.getDouble("half"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[float16_logical_type_test.parquet] Column 'half' is FIXED_LEN_BYTE_ARRAY"
                            + " annotated FLOAT16, which cannot be read as a double; read it with getFloat");
        }
    }

    @Test
    void hintNamesGetTimestampForAnInt96Column() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/int96_timestamp_test.parquet")));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            assertThatThrownBy(() -> rows.getLong("ts"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[int96_timestamp_test.parquet] Column 'ts' is INT96, which cannot be read as"
                            + " a long; read it with getTimestamp");
        }
    }

    /// A primitive accessor checks the type ahead of the value's presence, so a wrong-type call on
    /// a null cell fails as on any other rather than as a null. The object accessors answer the
    /// null test first and read a null value as `null` whatever the column holds.
    @Test
    void wrongTypePrimitiveOnANullCellFailsAsATypeMismatch() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/delta_binary_packed_optional_test.parquet")));
             RowReader rows = reader.rowReader()) {
            for (int i = 0; i < 3; i++) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
            }
            assertThat(rows.isNull("optional_value")).isTrue();

            String message = "[delta_binary_packed_optional_test.parquet] Column 'optional_value' is INT32,"
                    + " which cannot be read as %s";
            assertThatThrownBy(() -> rows.getLong("optional_value"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message.formatted("a long; read it with getInt"));
            assertThatThrownBy(() -> rows.getDouble("optional_value"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message.formatted("a double; read it with getInt"));
            assertThat(rows.getBinary("optional_value")).isNull();
            assertThatThrownBy(() -> rows.getFloat("optional_value"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message.formatted("a float"));
        }
    }

    /// A reader that never received a batch has no typed slot filled, which is no type mismatch.
    @Test
    void accessorOnAReaderWithNoRowsFailsAsNoCurrentRow() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/empty_row_group.parquet")));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isFalse();
            assertThatThrownBy(() -> rows.getLong("id"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No current row; call next() before reading column 'id'");
        }
    }
}
