/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.io.InputFileCloser;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// A read of many files drops what it holds of each file once every column has passed it: the
/// file's work items and, but for the first file's, its footer. A read of a million small files
/// would otherwise hold a million parsed footers until it closes.
///
/// Uses compat_order_ab.parquet: one row group of two columns, read as twenty files.
class ReadRetentionTest {

    private static final Path FIXTURE = Path.of("src/test/resources/compat_order_ab.parquet");
    private static final int FILES = 20;
    private static final int COLUMNS = 2;
    /// The files after the one reached whose footers a read of files not in memory reads ahead.
    private static final int FILES_AHEAD = 2;
    /// The first file, the next unit's, and the files ahead of that.
    private static final int MOST_FILES_OPENED = 2 + FILES_AHEAD;

    @ParameterizedTest(name = "in memory: {0}")
    @ValueSource(booleans = {true, false})
    void aReadDropsTheFootersAndWorkItemsOfTheFilesItHasPassed(boolean inMemory) throws Exception {
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            files.add(inMemory ? InputFile.of(FIXTURE) : new CountingInputFile(InputFile.of(FIXTURE)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            FileMetadataCache cache = new FileMetadataCache(files, context);
            RowGroupIterator iterator = new RowGroupIterator(cache, ignored -> {}, context, 0, 0, 0);
            try {
                iterator.openFirst();
                iterator.initialize(ReadProjection.of(
                        ProjectedSchema.create(iterator.referenceSchema(), ColumnProjection.all())), null, true);
                int read = 0;
                for (RowGroupIterator.WorkItem workItem = iterator.workItemAt(0); workItem != null;
                        workItem = iterator.workItemAt(++read)) {
                    for (int column = 0; column < COLUMNS; column++) {
                        iterator.getColumnPlan(workItem, column);
                    }
                    for (int column = 0; column < COLUMNS; column++) {
                        iterator.releaseWorkItem(workItem);
                    }
                    // One work item per file: every column has passed this file and every one before
                    assertThat(iterator.holdsWorkItem(read)).as("work item %d", read).isFalse();
                    for (int passed = 1; passed < read; passed++) {
                        assertThat(cache.holdsFooter(passed)).as("footer %d after file %d", passed, read).isFalse();
                    }
                }
                assertThat(read).isEqualTo(FILES);
                assertThat(cache.holdsFooter(0)).as("the first file's footer").isTrue();
            }
            finally {
                iterator.close();
                cache.close();
                closeAll(files);
            }
        }
    }

    /// A read planning ahead lists a file only once its fetch sequence has started, and charged,
    /// the file's footer, so planning ahead over files that fit their tail, which no data entry
    /// counts toward F, stays within the files ahead.
    @Test
    void planningAheadListsNoFileWhoseFooterTheReadHasNotStarted() throws Exception {
        // At the default size, so every file and its data fit their tail
        String tailBytes = System.clearProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        try {
            readAheadOfAStoppedConsumer();
        }
        finally {
            if (tailBytes != null) {
                System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, tailBytes);
            }
        }
    }

    private static void readAheadOfAStoppedConsumer() throws Exception {
        List<CountingInputFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            files.add(new CountingInputFile(InputFile.of(FIXTURE)));
        }
        QuiescentExecutor speculative = new QuiescentExecutor();
        try (HardwoodContextImpl context = HardwoodContextImpl.withIoExecutor(2, speculative)) {
            FileMetadataCache cache = new FileMetadataCache(List.copyOf(files), context);
            RowGroupIterator iterator = new RowGroupIterator(cache, ignored -> {}, context, 0, 0, 0);
            try {
                iterator.openFirst();
                // The read takes the files ahead as it sets up its planning
                System.setProperty(FooterReadAhead.MAX_FILES_AHEAD_PROPERTY, Integer.toString(FILES_AHEAD));
                try {
                    iterator.initialize(ReadProjection.of(
                            ProjectedSchema.create(iterator.referenceSchema(), ColumnProjection.all())), null, true);
                }
                finally {
                    System.clearProperty(FooterReadAhead.MAX_FILES_AHEAD_PROPERTY);
                }
                RowGroupIterator.WorkItem first = iterator.workItemAt(0);
                for (int column = 0; column < COLUMNS; column++) {
                    iterator.getColumnPlan(first, column);
                }
                // A consumer that stops in the first file; planning ahead runs on the pool meanwhile
                assertThat(speculative.awaitQuiescence(10, TimeUnit.SECONDS, iterator::isPlanningAtRest))
                        .as("planning and its reads at rest").isTrue();

                long opened = files.stream().filter(file -> !file.reads().isEmpty()).count();
                assertThat(opened).as("files opened").isLessThanOrEqualTo(MOST_FILES_OPENED);
            }
            finally {
                iterator.close();
                cache.close();
                closeAll(List.copyOf(files));
            }
        }
    }

    private static void closeAll(List<InputFile> files) throws IOException {
        InputFileCloser.rethrow(InputFileCloser.closeAll(files, null));
    }
}
