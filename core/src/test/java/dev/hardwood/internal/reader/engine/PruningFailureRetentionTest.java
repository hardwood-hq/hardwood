/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A row group whose pruning fails returns the ranges it held, the ones it shares with its window
/// included: once the read is quiescent, before it closes, it holds nothing in R.
///
/// Uses bloom_filter_multi_rg_dict_index_test.parquet: five row groups of 200 rows, one window.
/// Under `value == 8015 OR id == 50`, statistics drop row groups 1 to 3 and leave row group 0 to
/// its Bloom filters and row group 4 to its dictionary page; row group 0 is kept and reads the
/// window's page index. Row group 4's dictionary page fails while it holds the window's Bloom
/// filters and page index. It is the last row group, so no work past the failure, which the read
/// never starts, holds anything.
class PruningFailureRetentionTest {

    private static final Path FIXTURE = Path.of("src/test/resources/bloom_filter_multi_rg_dict_index_test.parquet");
    private static final int BATCH_SIZE = 100;

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aRowGroupWhosePruningFailsHoldsNothingInR() throws Exception {
        InputFile file = new FailingInputFile(InputFile.of(FIXTURE));
        file.open();
        BatchExchange<?> exchange = BatchExchange.detachingFlat("code", BATCH_SIZE);
        List<NestedLevelComputer.Layers> layers = new ArrayList<>();
        layers.add(null);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            PreparedRead prepared = ReadEngines.prepared(List.of(file), context, ColumnProjection.columns("code"),
                    FilterPredicate.or(FilterPredicate.eq("value", 8015L), FilterPredicate.eq("id", 50L)), 0);
            try (ReadEngine engine = prepared.start(ProjectedSchema.create(prepared.schema(),
                    ColumnProjection.columns("code")), layers, NestedIndexMode.ALL_ITEMS, false, BATCH_SIZE,
                    BATCH_SIZE, List.of(exchange), null)) {
                assertThatThrownBy(() -> {
                    while (exchange.poll() != null) {
                        // drains to the failure
                    }
                    exchange.checkError();
                })
                        .rootCause()
                        .hasMessage("injected");

                engine.awaitQuiescent();
                assertThat(context.readAheadBuffer().held()).isZero();
            }
        }
    }

    /// Fails the read of the last row group's dictionary page with "injected".
    private static final class FailingInputFile implements InputFile {

        private final InputFile delegate;

        FailingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (FetchReason.current().equals("rg=4 pruning")) {
                throw new IOException("injected");
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
