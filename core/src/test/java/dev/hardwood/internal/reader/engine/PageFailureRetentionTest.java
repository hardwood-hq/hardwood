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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page whose chunk's dictionary fails returns the ranges it held: once the read is quiescent,
/// before it closes, it holds nothing in R.
///
/// The file is one `INT64` column of one row group with a page index, whose dictionary page holds
/// 1 MiB of zeros, more than the read's working-set limit lets it decode. Its data page lies in
/// the same range and has arrived when the dictionary fails. The file is read through a wrapper,
/// so that its ranges are fetched and charged to R rather than sliced from the bytes it holds.
class PageFailureRetentionTest {

    private static final String MAX_WORKING_SET_BYTES_PROPERTY = "hardwood.internal.maxWorkingSetBytes";
    private static final int DICTIONARY_BYTES = 1024 * 1024;
    private static final int PAGE_VALUES = 1024;

    @AfterEach
    void clearLimit() {
        System.clearProperty(MAX_WORKING_SET_BYTES_PROPERTY);
        System.clearProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aPageWhoseDictionaryFailsHoldsNothingInR() throws Exception {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(2L * DICTIONARY_BYTES - 1));
        // the dictionary page and the data page each a request of its own
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, "1");
        InputFile file = new FetchedFile(InputFile.of(ByteBuffer.wrap(RleExpansionFile.buildWide(1, 1,
                DICTIONARY_BYTES, PAGE_VALUES, true))));
        file.open();
        BatchExchange<?> exchange = BatchExchange.detachingFlat("c0", PAGE_VALUES);
        List<NestedLevelComputer.Layers> layers = new ArrayList<>();
        layers.add(null);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            PreparedRead prepared = ReadEngines.prepared(List.of(file), context, ColumnProjection.all(), null, 0);
            try (ReadEngine engine = prepared.start(ProjectedSchema.create(prepared.schema(), ColumnProjection.all()),
                    layers, NestedIndexMode.ALL_ITEMS, false, PAGE_VALUES, PAGE_VALUES, List.of(exchange), null)) {
                assertThatThrownBy(() -> {
                    while (exchange.poll() != null) {
                        // drains to the failure
                    }
                    exchange.checkError();
                })
                        .isInstanceOf(ParquetReadException.class)
                        .hasMessage("[<memory>: row group 0, column 'c0', dictionary page] Dictionary page needs "
                                + "2097152 bytes to decode, which takes the read's working set to 2097152 bytes, "
                                + "more than its limit of 2097151 (hardwood.internal.maxWorkingSetBytes)");

                engine.awaitQuiescent();
                assertThat(context.readAheadBuffer().held()).isZero();
            }
        }
    }

    /// Reads `delegate` without exposing the bytes it holds.
    private record FetchedFile(InputFile delegate) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
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
