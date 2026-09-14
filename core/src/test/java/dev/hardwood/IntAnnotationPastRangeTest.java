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

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// An `INT(8)` or `INT(16)` column whose file stores values outside the range the annotation
/// states. `getValue` narrows such a value on a signed column to the annotation's `Byte` or
/// `Short`; `getInt` returns it as stored, and a predicate compares the stored value.
class IntAnnotationPastRangeTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/int_past_annotation.parquet");

    @Test
    void getValueNarrowsWhileGetIntReadsTheStoredValue() throws Exception {
        List<Object> i8Values = new ArrayList<>();
        List<Integer> i8Ints = new ArrayList<>();
        List<Object> i16Values = new ArrayList<>();
        List<Integer> i16Ints = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                i8Values.add(rows.getValue("i8"));
                i8Ints.add(rows.getInt("i8"));
                i16Values.add(rows.getValue("i16"));
                i16Ints.add(rows.getInt("i16"));
            }
        }

        assertThat(i8Ints).containsExactly(-1000, -129, -128, 127, 128, 200, 1000, 100000);
        assertThat(i8Values).containsExactly((byte) 24, (byte) 127, (byte) -128, (byte) 127, (byte) -128,
                (byte) -56, (byte) -24, (byte) -96);
        assertThat(i16Ints).containsExactly(-40000, -32769, -32768, 32767, 32768, 40000, 70000, 100000);
        assertThat(i16Values).containsExactly((short) 25536, (short) 32767, (short) -32768, (short) 32767,
                (short) -32768, (short) -25536, (short) 4464, (short) -31072);
    }

    /// An unsigned annotation is not narrowed: `getValue` returns the stored `Integer`.
    @Test
    void getValuePassesAnUnsignedValueThrough() throws Exception {
        List<Object> u8Values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                u8Values.add(rows.getValue("u8"));
            }
        }

        assertThat(u8Values).containsExactly(-1000, -1, 0, 127, 128, 255, 1000, 100000);
    }

    /// `1000` narrows to the byte `-24`: the predicate on the stored value matches the row, and a
    /// predicate on the narrowed value does not.
    @Test
    void predicatesCompareTheStoredValue() throws Exception {
        assertThat(matchingInts("i8", FilterPredicate.eq("i8", 1000))).containsExactly(1000);
        assertThat(matchingInts("i8", FilterPredicate.eq("i8", -24))).isEmpty();
        assertThat(matchingInts("i8", FilterPredicate.gt("i8", 127))).containsExactly(128, 200, 1000, 100000);
        assertThat(matchingInts("i16", FilterPredicate.lt("i16", -32768))).containsExactly(-40000, -32769);
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
