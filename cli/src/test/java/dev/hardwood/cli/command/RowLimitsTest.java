/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/// [RowLimits#resolveWindow] against synthetic metadata — no fixture file
/// needed, since it takes a plain [FileMetaData].
class RowLimitsTest {

    @Test
    void rowGroupResolvesToItsFirstRowAndSize() {
        FileMetaData metadata = fileWithRowGroups(10, 5, 20);

        RowLimits.RowWindow window = RowLimits.resolveWindow(metadata, null, 1, 0);

        assertThat(window.skip()).isEqualTo(10L);
        assertThat(window.limit()).isEqualTo(5L);
    }

    @Test
    void noRowLimitIsAnExplicitWindowOfEveryRow() {
        FileMetaData metadata = fileWithRowGroups(10, 5, 20);

        assertThat(RowLimits.resolveWindow(metadata, null, null, 0).limit()).isEqualTo(RowLimits.RowWindow.NO_LIMIT);
        assertThat(RowLimits.resolveWindow(metadata, 3L, null, 0).limit()).isEqualTo(RowLimits.RowWindow.NO_LIMIT);
    }

    @Test
    void aWindowOfNoRowsCannotBeBuilt() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new RowLimits.RowWindow(10, 0))
                .withMessage("A row window reads at least one row; use NO_LIMIT for every row");
    }

    @Test
    void anEmptyRowGroupIsRefusedRatherThanReadAsNoLimit() {
        FileMetaData metadata = fileWithRowGroups(10, 0, 20);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> RowLimits.resolveWindow(metadata, null, 1, 0))
                .withMessage("Row group 1 has no rows");
    }

    @Test
    void anEmptyLeadingRowGroupIsRefused() {
        FileMetaData metadata = fileWithRowGroups(0, 10);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> RowLimits.resolveWindow(metadata, null, 0, 0))
                .withMessage("Row group 0 has no rows");
    }

    private static FileMetaData fileWithRowGroups(long... rowCounts) {
        long total = 0;
        List<RowGroup> rowGroups = new ArrayList<>();
        for (long rowCount : rowCounts) {
            rowGroups.add(new RowGroup(List.of(), 0, rowCount));
            total += rowCount;
        }
        return new FileMetaData(2, List.of(), total, rowGroups, Map.of(), null, List.of());
    }
}
