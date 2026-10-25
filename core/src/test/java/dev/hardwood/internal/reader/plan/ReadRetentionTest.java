/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.engine.RowGroupIterator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// A read of many files drops what it holds of each file once every column has passed it: but for
/// the first file's, its footer. A read of a million small files would otherwise hold a million
/// parsed footers until it closes.
///
/// Uses compat_order_ab.parquet: one row group of two columns, read as twenty files.
class ReadRetentionTest {

    private static final Path FIXTURE = Path.of("src/test/resources/compat_order_ab.parquet");
    private static final int FILES = 20;
    private static final int COLUMNS = 2;
    /// More rows than a file holds, so that each file is one batch.
    private static final int BATCH_SIZE = 1024;

    @ParameterizedTest(name = "in memory: {0}")
    @ValueSource(booleans = {true, false})
    void aReadDropsTheFootersOfTheFilesItHasPassed(boolean inMemory) throws Exception {
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            files.add(inMemory ? InputFile.of(FIXTURE) : new CountingInputFile(InputFile.of(FIXTURE)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            FileMetadataCache cache = new FileMetadataCache(files, context);
            try (RowGroupIterator iterator = RowGroupIterators.standalone(cache, context)) {
                FileMetadataCache.PreparedFile first = cache.getFile(0);
                ProjectedSchema decoded = ProjectedSchema.create(first.schema(), ColumnProjection.all());
                iterator.initialize(first.schema(), first.rowGroups(), ReadProjection.of(decoded), null, true, 0, 0);
                List<BatchExchange<?>> exchanges = new ArrayList<>();
                List<NestedLevelComputer.Layers> flat = new ArrayList<>();
                for (int column = 0; column < COLUMNS; column++) {
                    exchanges.add(BatchExchange.detachingFlat(first.schema().getColumn(column).name(), BATCH_SIZE));
                    flat.add(null);
                }
                iterator.start(decoded, flat, NestedIndexMode.ALL_ITEMS, false, BATCH_SIZE, BATCH_SIZE, 0, exchanges,
                        null);
                int read = 0;
                while (takeBatch(exchanges)) {
                    // One batch per file: every column has published the batch of file `read`, so it
                    // has passed every file before it
                    for (int passed = 1; passed < read; passed++) {
                        assertThat(cache.holdsFooter(passed)).as("footer %d after file %d", passed, read).isFalse();
                    }
                    read++;
                }
                assertThat(read).isEqualTo(FILES);
                assertThat(cache.holdsFooter(0)).as("the first file's footer").isTrue();
            }
        }
    }

    /// Takes the next batch of every column; `false` once the read has ended.
    private static boolean takeBatch(List<BatchExchange<?>> exchanges) throws Exception {
        boolean taken = false;
        for (BatchExchange<?> exchange : exchanges) {
            BatchExchange.Batch batch = (BatchExchange.Batch) exchange.poll();
            if (batch == null || batch.recordCount == 0) {
                exchange.checkError();
            }
            else {
                taken = true;
            }
        }
        return taken;
    }
}
