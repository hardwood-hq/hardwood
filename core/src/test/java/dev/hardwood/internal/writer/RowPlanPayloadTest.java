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

/// The staged variable-width payload the row writer submits a batch on: the bytes of every
/// binary column's staged values, without those of a record that failed, and none after the
/// batch is reset.
class RowPlanPayloadTest {

    @Test
    void countsTheValuesOfEveryBinaryColumn() {
        RowPlan plan = RowPlan.build(twoBlobs(), PrecisionLossPolicy.REJECT);
        plan.writeRecord(row -> row.setLong("id", 1).setBinary("a", new byte[10]).setBinary("b", new byte[100]));
        plan.writeRecord(row -> row.setLong("id", 2).setBinary("a", new byte[20]).setNull("b"));
        assertThat(plan.variableWidthBytes()).isEqualTo(130);
    }

    /// A record that fails after staging values in both columns leaves the count as it was.
    @Test
    void aRolledBackRecordDoesNotCount() {
        RowPlan plan = RowPlan.build(twoBlobs(), PrecisionLossPolicy.REJECT);
        plan.writeRecord(row -> row.setLong("id", 1).setBinary("a", new byte[10]).setBinary("b", new byte[100]));
        assertThatThrownBy(() -> plan.writeRecord(row -> {
            row.setLong("id", 2).setBinary("a", new byte[1000]).setBinary("b", new byte[2000]);
            throw new IllegalArgumentException("filler failed");
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("filler failed");
        assertThat(plan.variableWidthBytes()).isEqualTo(110);
    }

    @Test
    void resetEmptiesTheCount() {
        RowPlan plan = RowPlan.build(twoBlobs(), PrecisionLossPolicy.REJECT);
        plan.writeRecord(row -> row.setLong("id", 1).setBinary("a", new byte[10]).setBinary("b", new byte[100]));
        plan.reset();
        assertThat(plan.variableWidthBytes()).isZero();
        plan.writeRecord(row -> row.setLong("id", 2).setBinary("a", new byte[5]).setNull("b"));
        assertThat(plan.variableWidthBytes()).isEqualTo(5);
    }

    private static FileSchema twoBlobs() {
        return FileSchema.builder("schema")
                .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .primitive("a", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .primitive("b", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                .build();
    }
}
