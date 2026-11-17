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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// A read's decodes, and the dictionary parses its dictionary pruning runs, whose buffers
/// exceed the retention size run beside the one its consumer's next batch needs only within the
/// context's heap quota.
class LargeDecodeTaskTest {

    private static final int COLUMNS = 8;
    private static final int PAGE_VALUES = 1024;
    /// what each page's decode decompresses: the heap holds a few of them, not all at once
    private static final int PAGE_BYTES = 160 * 1024 * 1024;
    private static final int ROW_GROUPS = 16;
    /// what each row group's dictionary decompresses to; its parse holds as much again in entries
    private static final int DICTIONARY_BYTES = 128 * 1024 * 1024;
    /// the heap quota of the pages' read: room for one page's buffers and the pages' batches, so
    /// that no decode beside the one need forces holds its buffers
    private static final int PAGE_QUOTA = PAGE_BYTES + 1024 * 1024;
    /// the heap quota of the dictionaries' read: room for none of the parses' buffers, so that only
    /// the parse need forces holds them
    private static final int DICTIONARY_QUOTA = 1024 * 1024;

    @AfterEach
    void clearBuffer() {
        System.clearProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY);
    }

    /// Eight columns whose one data page each decompresses to 160 MiB, a few KiB on disk, read
    /// under [#PAGE_QUOTA] on a pool of eight threads and on none: the read returns every row
    /// in a heap that cannot hold the eight pages' buffers at once.
    @ParameterizedTest
    @ValueSource(ints = { 8, 0 })
    void largePagesOfEveryColumnReadInAHeapSmallerThanTheirSum(int threads) throws IOException {
        assertThat(Runtime.getRuntime().maxMemory()).isLessThan((long) COLUMNS * PAGE_BYTES);
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, Integer.toString(PAGE_QUOTA));
        byte[] file = RleExpansionFile.buildWide(COLUMNS, 1, Long.BYTES, PAGE_VALUES, false, PAGE_BYTES);

        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("c7")).isZero();
                count++;
            }
            assertThat(count).isEqualTo(PAGE_VALUES);
        }
    }

    /// Sixteen row groups whose one column's dictionary decompresses to 128 MiB of zeros, a few
    /// KiB on disk, read under a filter for a value none holds, under [#DICTIONARY_QUOTA] on a pool of eight
    /// threads and on none: each row group's dictionary pruning parses its
    /// dictionary and drops it, and the read returns no row in a heap that cannot hold eight
    /// parses at once.
    @ParameterizedTest
    @ValueSource(ints = { 8, 0 })
    void largeDictionariesPruneEveryRowGroupInAHeapSmallerThanTheirSum(int threads) throws IOException {
        assertThat(Runtime.getRuntime().maxMemory()).isLessThan(8L * 2 * DICTIONARY_BYTES);
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, Integer.toString(DICTIONARY_QUOTA));
        byte[] file = RleExpansionFile.buildWide(1, ROW_GROUPS, DICTIONARY_BYTES, PAGE_VALUES);

        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("c0", 1L)).build()) {
            assertThat(rows.hasNext()).isFalse();
        }
    }
}
