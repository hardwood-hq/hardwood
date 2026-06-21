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
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests that `FileMetaData.column_orders` is decoded and surfaced.
///
/// `primitive_types_test.parquet` (written by PyArrow) carries one `ColumnOrder` per leaf; PyArrow
/// emits the type-defined ordering for every column, floats and doubles included.
class ColumnOrdersTest {

    private static final Path FILE = Paths.get("src/test/resources/primitive_types_test.parquet");

    @Test
    void columnOrdersSurfaceTypeDefinedOrderForEveryColumn() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            List<ColumnOrder> orders = reader.getFileMetaData().columnOrders();
            // int, long, float, double, bool, string, binary
            assertThat(orders).hasSize(7);
            assertThat(orders).containsOnly(ColumnOrder.TYPE_DEFINED_ORDER);
        }
    }
}
