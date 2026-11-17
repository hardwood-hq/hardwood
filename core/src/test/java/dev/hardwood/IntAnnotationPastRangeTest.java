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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An `INT(8)` or `INT(16)` column whose file stores values outside the range the annotation
/// states. `getValue` refuses such a value, naming the file and the column; `getInt` returns it
/// as stored, and a predicate compares the stored value.
class IntAnnotationPastRangeTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/int_past_annotation.parquet");

    @Test
    void getIntReadsTheStoredValue() throws Exception {
        List<Integer> i8Ints = new ArrayList<>();
        List<Integer> i16Ints = new ArrayList<>();
        List<Integer> u8Ints = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                i8Ints.add(rows.getInt("i8"));
                i16Ints.add(rows.getInt("i16"));
                u8Ints.add(rows.getInt("u8"));
            }
        }

        assertThat(i8Ints).containsExactly(-1000, -129, -128, 127, 128, 200, 1000, 100000);
        assertThat(i16Ints).containsExactly(-40000, -32769, -32768, 32767, 32768, 40000, 70000, 100000);
        assertThat(u8Ints).containsExactly(-1000, -1, 0, 127, 128, 255, 1000, 100000);
    }

    /// Rows 2 and 3 of each column hold the bounds of its annotation.
    @Test
    void getValueReadsAValueInsideTheAnnotation() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            advanceTo(rows, 2);
            assertThat(rows.getValue("i8")).isEqualTo((byte) -128);
            assertThat(rows.getValue("i16")).isEqualTo((short) -32768);
            assertThat(rows.getValue("u8")).isEqualTo(0);
            rows.next();
            assertThat(rows.getValue("i8")).isEqualTo((byte) 127);
            assertThat(rows.getValue("i16")).isEqualTo((short) 32767);
            assertThat(rows.getValue("u8")).isEqualTo(127);
        }
    }

    static Stream<Arguments> valuesPastTheAnnotation() {
        return Stream.of(
                Arguments.of("i8", 1, "The INT_8 value -129 lies outside its range, which holds -128 to 127"),
                Arguments.of("i8", 4, "The INT_8 value 128 lies outside its range, which holds -128 to 127"),
                Arguments.of("i8", 6, "The INT_8 value 1000 lies outside its range, which holds -128 to 127"),
                Arguments.of("i16", 1,
                        "The INT_16 value -32769 lies outside its range, which holds -32768 to 32767"),
                Arguments.of("i16", 4,
                        "The INT_16 value 32768 lies outside its range, which holds -32768 to 32767"),
                Arguments.of("u8", 1, "The UINT_8 value 4294967295 lies outside its range, which holds 0 to 255"),
                Arguments.of("u8", 6, "The UINT_8 value 1000 lies outside its range, which holds 0 to 255"));
    }

    @ParameterizedTest
    @MethodSource("valuesPastTheAnnotation")
    void getValueRefusesAValuePastTheAnnotation(String column, int row, String message) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            advanceTo(rows, row);
            assertThatThrownBy(() -> rows.getValue(column))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("[int_past_annotation.parquet: column '" + column + "'] " + message);
        }
    }

    /// The predicate on the stored value `1000` matches its row.
    @Test
    void predicatesCompareTheStoredValue() throws Exception {
        assertThat(matchingInts("i8", FilterPredicate.eq("i8", 1000))).containsExactly(1000);
        assertThat(matchingInts("i8", FilterPredicate.gt("i8", 127))).containsExactly(128, 200, 1000, 100000);
        assertThat(matchingInts("i16", FilterPredicate.lt("i16", -32768))).containsExactly(-40000, -32769);
    }

    private static void advanceTo(RowReader rows, int row) throws Exception {
        for (int i = 0; i <= row; i++) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
        }
    }

    private static List<Integer> matchingInts(String column, FilterPredicate filter) throws Exception {
        List<Integer> matched = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getInt(column));
            }
        }
        return matched;
    }
}
