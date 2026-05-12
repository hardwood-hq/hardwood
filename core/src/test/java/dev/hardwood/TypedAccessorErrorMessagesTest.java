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

/// Coverage for the `PqList` typed-iterator validation hoist
/// (hardwood#469): wrong-type iteration must fail at iterator construction
/// with a field-named error, not lazily on the first element decode.
class TypedAccessorErrorMessagesTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/typed_accessors_issue_445.parquet");

    @Test
    void pqListTypedIteratorValidatesAtConstructionNotFirstElement() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            // `intervals` is a List<INTERVAL>; calling .dates() on it must
            // throw immediately at iterator construction with a field-named
            // error, not later when the first element is decoded.
            PqList intervals = rowReader.getList("intervals");
            assertThatThrownBy(intervals::dates)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("DateType");
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
