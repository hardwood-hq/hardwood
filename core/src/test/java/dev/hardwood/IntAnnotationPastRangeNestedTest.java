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
import java.time.DateTimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Values past their annotation inside a struct, a list, a map and a shredded Variant. An `INT_8`
/// value is refused with a plain [ParquetReadException], so a library caller never sees the
/// reader's internal refusal type, and the `TIME` outside a day with a `DateTimeException`; both
/// messages name the file and the column. The value next to a refused one reads as usual.
class IntAnnotationPastRangeNestedTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/undecodable_nested_values.parquet");

    private static final String INT_8_300 = "The INT_8 value 300 lies outside its range, which holds -128 to 127";

    @Test
    void structFieldIsRefused() throws Throwable {
        onFirstRow(rows -> assertThatThrownBy(() -> rows.getStruct("s").getValue("i8"))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage(prefix("s.i8") + INT_8_300));
    }

    @Test
    void listElementIsRefused() throws Throwable {
        onFirstRow(rows -> {
            assertThat(rows.getList("l").get(2)).isEqualTo((byte) 3);
            assertThatThrownBy(() -> rows.getList("l").get(3))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage(prefix("l.list.element") + INT_8_300);
        });
    }

    @Test
    void mapKeyAndValueAreRefused() throws Throwable {
        onFirstRow(rows -> {
            assertThatThrownBy(() -> rows.getMap("m").getEntries().get(0).getKey())
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage(prefix("m.key_value.key") + INT_8_300);
            assertThatThrownBy(() -> rows.getMap("m").getEntries().get(0).getValue())
                    .isExactlyInstanceOf(DateTimeException.class)
                    .hasMessage(prefix("m.key_value.value")
                            + "The TIME(MILLIS) value 86400000 lies outside a day, which holds 0 to 86399999");
        });
    }

    @Test
    void shreddedVariantIsRefused() throws Throwable {
        onFirstRow(rows -> assertThatThrownBy(() -> rows.getVariant("var"))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage(prefix("var") + INT_8_300));
    }

    private static String prefix(String column) {
        return "[undecodable_nested_values.parquet: column '" + column + "'] ";
    }

    private static void onFirstRow(ThrowingConsumer<RowReader> check) throws Throwable {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            check.accept(rows);
        }
    }
}
