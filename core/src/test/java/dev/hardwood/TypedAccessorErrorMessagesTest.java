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
import dev.hardwood.row.PqList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Coverage for `PqList` typed-iterator behavior on wrong element types
/// (hardwood#460): wrong-type access surfaces as a [ClassCastException] from
/// the element decode rather than a typed pre-flight validation.
class TypedAccessorErrorMessagesTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/typed_accessors_issue_445.parquet");

    @Test
    void pqListWrongElementTypeFailsOnElementDecode() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            // `intervals` is a List<INTERVAL>; iterating it as dates fails on the
            // first element decode rather than at iterator construction.
            PqList intervals = rowReader.getList("intervals");
            assertThatThrownBy(() -> intervals.dates().get(0))
                    .isInstanceOf(ClassCastException.class);
        }
    }

    @Test
    void pqListTypedIteratorOnRightElementTypeStillWorks() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            PqList intervals = rowReader.getList("intervals");
            assertThat(intervals.intervals()).hasSize(2);
        }
    }
}
