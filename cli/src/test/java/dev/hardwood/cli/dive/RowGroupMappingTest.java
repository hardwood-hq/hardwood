/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Covers the row ↔ row group mapping the Data preview jumps through:
/// [ParquetModel#firstRowOf] and [ParquetModel#rowGroupOf].
class RowGroupMappingTest {

    /// 3 row groups of 100 rows each: rows 0..99, 100..199, 200..299.
    private static final String FIXTURE = "filter_pushdown_int.parquet";

    private ParquetModel model;

    @BeforeEach
    void open() throws IOException {
        Path path = Path.of(RowGroupMappingTest.class.getResource("/" + FIXTURE).getPath());
        model = ParquetModel.open(InputFile.of(path), FIXTURE);
    }

    @AfterEach
    void close() throws IOException {
        model.close();
    }

    @Test
    void firstRowOfEachRowGroup() {
        assertThat(model.rowGroupCount()).as("row groups in the fixture").isEqualTo(3);
        assertThat(model.firstRowOf(0)).as("first row of RG 0").isEqualTo(0L);
        assertThat(model.firstRowOf(1)).as("first row of RG 1").isEqualTo(100L);
        assertThat(model.firstRowOf(2)).as("first row of RG 2").isEqualTo(200L);
    }

    @Test
    void firstRowOfRejectsARowGroupPastTheLast() {
        assertThatThrownBy(() -> model.firstRowOf(3))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Row group 3 is out of range for " + FIXTURE + ", which has 3 row groups");
    }

    @Test
    void firstRowOfRejectsANegativeRowGroup() {
        assertThatThrownBy(() -> model.firstRowOf(-1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Row group -1 is out of range for " + FIXTURE + ", which has 3 row groups");
    }

    @Test
    void rowGroupOfPlacesRowsOnBothSidesOfEveryBoundary() {
        assertThat(model.rowGroupOf(0)).as("row group of row 0").isEqualTo(0);
        assertThat(model.rowGroupOf(99)).as("row group of row 99").isEqualTo(0);
        assertThat(model.rowGroupOf(100)).as("row group of row 100").isEqualTo(1);
        assertThat(model.rowGroupOf(199)).as("row group of row 199").isEqualTo(1);
        assertThat(model.rowGroupOf(200)).as("row group of row 200").isEqualTo(2);
        assertThat(model.rowGroupOf(299)).as("row group of the last row").isEqualTo(2);
    }

    @Test
    void rowGroupOfRejectsARowPastTheLast() {
        assertThatThrownBy(() -> model.rowGroupOf(300))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Row 300 is out of range for " + FIXTURE + ", which has 300 rows");
    }

    @Test
    void rowGroupOfRejectsANegativeRow() {
        assertThatThrownBy(() -> model.rowGroupOf(-1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Row -1 is out of range for " + FIXTURE + ", which has 300 rows");
    }

    /// An empty row group repeats its predecessor's cumulative count, so the
    /// binary search can land on one. No row is inside it, and the mapping
    /// must answer with the row group that actually holds the row.
    @Test
    void rowGroupOfSkipsEmptyRowGroups() {
        // RG 0: rows 0..9, RG 1: empty, RG 2: empty, RG 3: rows 10..19.
        long[] firstRows = {0, 10, 10, 10, 20};

        assertThat(ParquetModel.rowGroupOf(firstRows, 0)).as("row group of row 0").isEqualTo(0);
        assertThat(ParquetModel.rowGroupOf(firstRows, 9)).as("row group of row 9").isEqualTo(0);
        assertThat(ParquetModel.rowGroupOf(firstRows, 10)).as("row group of row 10").isEqualTo(3);
        assertThat(ParquetModel.rowGroupOf(firstRows, 19)).as("row group of row 19").isEqualTo(3);
    }

    /// A file whose first row groups are empty: the search lands on index 0
    /// for row 0, and the walk has to move off it.
    @Test
    void rowGroupOfSkipsLeadingEmptyRowGroups() {
        // RG 0: empty, RG 1: empty, RG 2: rows 0..4.
        long[] firstRows = {0, 0, 0, 5};

        assertThat(ParquetModel.rowGroupOf(firstRows, 0)).as("row group of row 0").isEqualTo(2);
        assertThat(ParquetModel.rowGroupOf(firstRows, 4)).as("row group of row 4").isEqualTo(2);
    }
}
