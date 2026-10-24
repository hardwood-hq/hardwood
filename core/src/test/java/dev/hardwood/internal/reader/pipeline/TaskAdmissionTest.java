/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read's tasks whose buffers exceed the retention size start as its one task, beside it only
/// within the context's read-ahead buffer, and otherwise once a task of the read ends.
class TaskAdmissionTest {

    private static final long LARGE = DecodeBuffers.RETAINED_BYTES + 1L;

    @AfterEach
    void clearBuffer() {
        System.clearProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY);
    }

    @Test
    void firstLargeTaskIsTheReadsOwn() {
        WorkingSet workingSet = new WorkingSet(1, Long.MAX_VALUE, new ReadAheadBuffer(0));

        assertThat(workingSet.admit(LARGE).admission()).isEqualTo(WorkingSet.OWN_TASK);
        assertThat(workingSet.admit(LARGE).admitted()).isFalse();
    }

    @Test
    void taskWithinTheRetentionSizeStartsBesideTheReadsOwn() {
        WorkingSet workingSet = new WorkingSet(1, Long.MAX_VALUE, new ReadAheadBuffer(0));
        workingSet.admit(LARGE);

        WorkingSet.Admission small = workingSet.admit(DecodeBuffers.RETAINED_BYTES);

        assertThat(small.admitted()).isTrue();
        assertThat(small.admission()).isZero();
    }

    @Test
    void taskBesideTheReadsOwnIsChargedToTheReadAheadBuffer() {
        ReadAheadBuffer readAhead = new ReadAheadBuffer(LARGE);
        WorkingSet workingSet = new WorkingSet(1, Long.MAX_VALUE, readAhead);
        workingSet.admit(LARGE);

        long admission = workingSet.admit(LARGE).admission();

        assertThat(admission).isEqualTo(LARGE);
        assertThat(readAhead.held()).isEqualTo(LARGE);
        assertThat(workingSet.admit(LARGE).admitted()).isFalse();
        workingSet.taskEnded(admission);
        assertThat(readAhead.held()).isZero();
    }

    @Test
    void refusedTaskResumesOnceTheReadsOwnEnds() {
        WorkingSet workingSet = new WorkingSet(1, Long.MAX_VALUE, new ReadAheadBuffer(0));
        long own = workingSet.admit(LARGE).admission();
        WorkingSet.Admission refused = workingSet.admit(LARGE);
        assertThat(refused.admitted()).isFalse();
        AtomicInteger resumed = new AtomicInteger();
        refused.refused().whenMet(resumed::incrementAndGet);

        assertThat(resumed).hasValue(0);
        workingSet.taskEnded(own);

        assertThat(resumed).hasValue(1);
        assertThat(workingSet.admit(LARGE).admission()).isEqualTo(WorkingSet.OWN_TASK);
    }

    @Test
    void refusedTaskResumesAtOnceWhenATaskEndedBeforeItWaits() {
        WorkingSet workingSet = new WorkingSet(1, Long.MAX_VALUE, new ReadAheadBuffer(0));
        long own = workingSet.admit(LARGE).admission();
        WorkingSet.Admission refused = workingSet.admit(LARGE);
        assertThat(refused.admitted()).isFalse();
        workingSet.taskEnded(own);
        AtomicInteger resumed = new AtomicInteger();

        refused.refused().whenMet(resumed::incrementAndGet);

        assertThat(resumed).hasValue(1);
    }

    @Test
    void taskOutsideAReadIsNotAdmitted() {
        WorkingSet workingSet = new WorkingSet(1, Long.MAX_VALUE, null);

        assertThat(workingSet.admit(LARGE).admission()).isZero();
        assertThat(workingSet.admit(LARGE).admission()).isZero();
    }

    @Test
    void pageIsSizedByItsDeclaredBytesWhateverValueCountItDeclares() throws IOException {
        ColumnSchema column = optionalColumn();
        int bytes = 2 * DecodeBuffers.RETAINED_BYTES;
        PageHeader header = new PageHeader(PageType.DATA_PAGE, bytes, 1, null, null, null, null);
        PageInfo page = new PageInfo(new DataPage(header, 0, ByteBuffer.allocate(1)), column, null, null,
                PageRowMask.ALL, 0);

        assertThat(page.taskBytes()).isEqualTo(bytes);
    }

    @Test
    void nullPlaceholderIsSizedByTheValuesItStandsFor() throws IOException {
        ColumnSchema column = optionalColumn();
        int values = 8 * 1024 * 1024;
        PageInfo placeholder = PageInfo.nullPlaceholder(values, column, null, values);

        // Its definition levels, an `int` each
        assertThat(placeholder.taskBytes()).isEqualTo((long) Integer.BYTES * values);
    }

    private static ColumnSchema optionalColumn() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Path.of("src/test/resources/filter_all_pages_match.parquet")))) {
            ColumnSchema column = reader.getFileSchema().getColumn(0);
            assertThat(column.maxDefinitionLevel()).isEqualTo(1);
            assertThat(column.maxRepetitionLevel()).isZero();
            return column;
        }
    }

    /// Dictionaries and pages above the retention size in every column, read with no read-ahead
    /// buffer, from the pages' headers and from an offset index: each of the read's large tasks
    /// waits for the one before it, and the read returns every row, on a pool and without one.
    @ParameterizedTest
    @CsvSource({ "0, false", "1, false", "4, false", "0, true", "4, true" })
    void readWithNoReadAheadBufferReadsEveryRow(int threads, boolean indexed) throws IOException {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, "0");
        int pageValues = 5 * 1024 * 1024;
        byte[] file = RleExpansionFile.buildWide(4, 3, 2 * DecodeBuffers.RETAINED_BYTES, pageValues, indexed);

        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertThat(count).isEqualTo(3L * pageValues);
        }
    }

    /// Two reads on a pool share a read-ahead buffer with room for one large task, so a task of
    /// one ending resumes the other's submission on its thread. Every submission of read A made
    /// while a release runs the waiters fails: A ends with the failure, or reads every row where
    /// none of its submissions resumed there, and B reads every row.
    @Test
    @Tag("pool")
    void failureResumingOneReadDoesNotReachAnother(@TempDir Path dir) throws Exception {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, Integer.toString(40 * 1024 * 1024));
        int pageValues = 5 * 1024 * 1024;
        Path a = dir.resolve("a.parquet");
        Path b = dir.resolve("b.parquet");
        Files.write(a, RleExpansionFile.buildWide(4, 6, 2 * DecodeBuffers.RETAINED_BYTES, pageValues));
        Files.write(b, RleExpansionFile.buildWide(4, 6, 2 * DecodeBuffers.RETAINED_BYTES, pageValues));
        IllegalStateException injected = new IllegalStateException("injected");
        AtomicInteger resumedInWake = new AtomicInteger();
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            if (page.fileName.equals("a.parquet") && StackWalker.getInstance().walk(frames -> frames.anyMatch(
                    frame -> frame.getClassName().equals(ReadAheadBuffer.class.getName())
                            && frame.getMethodName().equals("wake")))) {
                resumedInWake.incrementAndGet();
                throw injected;
            }
        };
        try (HardwoodContext context = HardwoodContext.create(2)) {
            for (int round = 0; round < 10; round++) {
                CompletableFuture<Long> readA = CompletableFuture.supplyAsync(() -> count(context, a));
                assertThat(count(context, b)).as("rows of read B, round %d", round).isEqualTo(6L * pageValues);
                // A round in which no submission of A happened to resume inside a release reads A whole
                assertThat(readA).as("read A, round %d", round).satisfiesAnyOf(
                        read -> assertThatThrownBy(read::join).hasRootCause(injected),
                        read -> assertThat(read.join()).isEqualTo(6L * pageValues));
            }
            assertThat(resumedInWake).as("submissions of A resumed inside a release").hasPositiveValue();
        }
        finally {
            ReadScheduler.submissionObserver = null;
        }
    }

    private static long count(HardwoodContext context, Path file) {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            return count;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void bufferThatIsNotAByteCountIsRejected() {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, "-1");

        assertThatThrownBy(ReadAheadBuffer::new)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("hardwood.internal.readAheadBytes must be a non-negative byte count, but was: -1");
    }
}
