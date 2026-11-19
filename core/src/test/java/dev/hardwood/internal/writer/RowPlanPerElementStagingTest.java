/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.PrecisionLossPolicy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// When the row writer has to submit a batch before it holds a slice's records: once an array
/// staging an entry per list or map element reaches half the slice ceiling, and not before.
class RowPlanPerElementStagingTest {

    private static final int THRESHOLD = RowGroupBuffer.MAX_SLICE_RECORDS / 2;

    @Test
    void listElementsReachTheThresholdBeforeTheRecords() {
        RowPlan plan = RowPlan.build(idAndTags(), PrecisionLossPolicy.REJECT);
        // Three elements per record: the threshold falls inside a record.
        int records = (THRESHOLD - 1) / 3;
        stage(plan, records, 3);
        assertThat(plan.perElementStagesFull()).isFalse();
        stage(plan, 1, 3);
        assertThat(plan.perElementStagesFull()).isTrue();
    }

    /// A flat leaf beside a list stages one entry per record, so it does not count, however
    /// many records the batch holds.
    @Test
    void aFlatLeafBesideAListDoesNotCount() {
        RowPlan plan = RowPlan.build(idAndTags(), PrecisionLossPolicy.REJECT);
        stage(plan, RowGroupBuffer.MAX_SLICE_RECORDS, 0);
        assertThat(plan.perElementStagesFull()).isFalse();
    }

    /// Empty inner lists stage no leaf entry but an offset each in the inner list.
    @Test
    void emptyInnerListsOfANestedListCount() {
        FileSchema schema = FileSchema.builder("schema")
                .list("grid", RepetitionType.REQUIRED, element -> element.list(RepetitionType.REQUIRED,
                        inner -> inner.primitive(PhysicalType.INT64, RepetitionType.REQUIRED)))
                .build();
        RowPlan plan = RowPlan.build(schema, PrecisionLossPolicy.REJECT);
        int records = (THRESHOLD - 1) / 3;
        for (int r = 0; r < records; r++) {
            plan.writeRecord(row -> row.setList("grid", grid -> grid.addList(i -> { }).addList(i -> { }).addList(i -> { })));
        }
        assertThat(plan.perElementStagesFull()).isFalse();
        plan.writeRecord(row -> row.setList("grid", grid -> grid.addList(i -> { }).addList(i -> { }).addList(i -> { })));
        assertThat(plan.perElementStagesFull()).isTrue();
    }

    /// A record that fails leaves nothing staged, its elements included.
    @Test
    void aRolledBackRecordDoesNotCount() {
        RowPlan plan = RowPlan.build(idAndTags(), PrecisionLossPolicy.REJECT);
        stage(plan, THRESHOLD - 1, 1);
        assertThatThrownBy(() -> plan.writeRecord(row -> {
            row.setLong("id", -1).setList("tags", tags -> tags.addLong(1).addLong(2));
            throw new IllegalArgumentException("filler failed");
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("filler failed");
        assertThat(plan.perElementStagesFull()).isFalse();
        stage(plan, 1, 1);
        assertThat(plan.perElementStagesFull()).isTrue();
    }

    private static FileSchema idAndTags() {
        return FileSchema.builder("schema")
                .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .list("tags", RepetitionType.REQUIRED,
                        element -> element.primitive(PhysicalType.INT64, RepetitionType.REQUIRED))
                .build();
    }

    private static void stage(RowPlan plan, int records, int elementsPerRecord) {
        for (int r = 0; r < records; r++) {
            long id = r;
            plan.writeRecord(row -> row.setLong("id", id).setList("tags", tags -> {
                for (int e = 0; e < elementsPerRecord; e++) {
                    tags.addLong(e);
                }
            }));
        }
    }
}
