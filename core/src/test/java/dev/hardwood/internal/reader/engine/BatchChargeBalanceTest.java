/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// A read whose consumer took every batch holds nothing on its context's ledger once it is
/// quiescent, before it closes: what its decodes' claims settled to, the bytes their batch
/// holders' arrays gained and lost, is what the batches return as the consumer takes them.
/// Covers nullable, plain and dictionary numeric and binary columns, a dictionary falling back to
/// plain, and the delta binary encodings, on a pool and without one.
class BatchChargeBalanceTest {

    private static final int BATCH_SIZE = 1024;

    @ParameterizedTest
    @CsvSource({
            "dict_plain_fallback_nulls.parquet, 0", "dict_plain_fallback_nulls.parquet, 4",
            "dict_cross_batch.parquet, 0", "dict_cross_batch.parquet, 4",
            "delta_length_byte_array_test.parquet, 4",
            "delta_byte_array_test.parquet, 4",
            "delta_binary_packed_optional_test.parquet, 4",
            "compat_optional_value.parquet, 4",
            "filter_pushdown_int.parquet, 0", "filter_pushdown_int.parquet, 4" })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadWhoseConsumerTookEveryBatchHoldsNothing(String name, int threads) throws Exception {
        InputFile file = InputFile.of(Path.of("src/test/resources", name));
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Path.of("src/test/resources", name)))) {
            schema = reader.getFileSchema();
        }
        List<BatchExchange<?>> exchanges = new ArrayList<>();
        for (int c = 0; c < schema.getColumnCount(); c++) {
            exchanges.add(BatchExchange.detachingFlat(schema.getColumn(c).name(), BATCH_SIZE));
        }
        List<NestedLevelComputer.Layers> layers = Collections.nCopies(exchanges.size(), null);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads);
                ReadEngine engine = ReadEngines.prepared(List.of(file), context, ColumnProjection.all(), null, 0)
                        .start(ProjectedSchema.create(schema, ColumnProjection.all()), layers,
                                NestedIndexMode.ALL_ITEMS, false, BATCH_SIZE, BATCH_SIZE, exchanges, null)) {
            takeEveryBatch(exchanges);

            assertThat(engine.quiescentHeldBytes()).isZero();
        }
    }

    /// Takes a batch of each column in turn until every column has ended.
    private static void takeEveryBatch(List<BatchExchange<?>> exchanges) throws Exception {
        boolean[] ended = new boolean[exchanges.size()];
        int open = exchanges.size();
        while (open > 0) {
            for (int c = 0; c < exchanges.size(); c++) {
                if (!ended[c]) {
                    Object batch = exchanges.get(c).poll();
                    if (batch == null) {
                        exchanges.get(c).checkError();
                        ended[c] = true;
                        open--;
                    }
                }
            }
        }
    }
}
