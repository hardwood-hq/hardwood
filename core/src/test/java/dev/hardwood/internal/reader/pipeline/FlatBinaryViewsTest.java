/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.DictionaryBinaryBatchMatcher;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryEqBatchMatcher;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.engine.PreparedRead;
import dev.hardwood.internal.reader.engine.ReadEngine;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// A flat binary column's batches get a window of every value's view as their cells complete once
/// a reader has taken a window: a reader of entry ids alone, or a filter that decides by entry ids,
/// never gets one, nor view arrays for its dictionary values.
///
/// The fixture has two row groups of 100 dictionary-encoded strings, read in batches of 10.
class FlatBinaryViewsTest {


    private static final Path FILE = Path.of("src/test/resources/dict_cross_chunk.parquet");

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aReaderOfIdsOnlyNeverGetsViews() throws Exception {
        List<BinaryBatchValues> batches = read(false, null);

        assertThat(batches).hasSize(20);
        assertThat(batches).allSatisfy(values -> {
            assertThat(values.viewArraysAllocated()).isFalse();
            assertThat(values.dictionary).isNotNull();
        });
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aDictionaryAwareFilterWritesNoViews() throws Exception {
        byte[] literal = "x".getBytes(StandardCharsets.UTF_8);

        List<BinaryBatchValues> batches = read(false, new DictionaryBinaryBatchMatcher(
                new BinaryEqBatchMatcher(literal, Comparison.BYTE_STRING)));

        assertThat(batches).hasSize(20);
        assertThat(batches).allSatisfy(values -> {
            assertThat(values.viewArraysAllocated()).isFalse();
            assertThat(values.hasWindow()).isFalse();
        });
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aReaderOfWindowsHasThemBuiltAsCellsComplete() throws Exception {
        List<BinaryBatchValues> batches = read(true, null);

        assertThat(batches).hasSize(20);
        assertThat(batches.getFirst().hasWindow()).isFalse();
        assertThat(batches.getLast().hasWindow()).as("a batch completed after the request").isTrue();
        // Batches with and without a window alike carry each value's bytes
        for (BinaryBatchValues values : batches) {
            for (int i = 0; i < 10; i++) {
                int start = values.start(i);
                assertThat(new String(values.buffer(i), start, values.end(i) - start, StandardCharsets.UTF_8))
                        .isEqualTo(values.stringAt(i));
            }
        }
    }

    /// Reads the fixture's column in batches of 10, taking the first batch's window if
    /// `readViews`. On a context of zero threads, so that no job of the read runs before the
    /// consumer polls.
    private static List<BinaryBatchValues> read(boolean readViews, ColumnBatchMatcher filter) throws Exception {
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            schema = reader.getFileSchema();
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0)) {
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detaching(
                    schema.getColumn(0).name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.matches = new long[1];
                        return b;
                    }, batch -> batch.capacity, 10);
            ProjectedSchema decoded = ProjectedSchema.create(schema, ColumnProjection.all());
            List<NestedLevelComputer.Layers> flat = new ArrayList<>();
            flat.add(null);
            PreparedRead prepared = ReadEngines.prepared(List.of(inputFile), context, ColumnProjection.all(), null, 0);
            try (ReadEngine engine = prepared.start(decoded, flat, NestedIndexMode.ALL_ITEMS, false, 10, 10,
                    List.of(exchange), new ColumnBatchMatcher[]{ filter })) {
                List<BinaryBatchValues> batches = new ArrayList<>();
                BatchExchange.Batch batch;
                while ((batch = exchange.poll()) != null) {
                    BinaryBatchValues values = (BinaryBatchValues) batch.values;
                    if (readViews && batches.isEmpty()) {
                        values.window(batch.recordCount);
                    }
                    batches.add(values);
                }
                exchange.checkError();
                return batches;
            }
        }
    }
}
