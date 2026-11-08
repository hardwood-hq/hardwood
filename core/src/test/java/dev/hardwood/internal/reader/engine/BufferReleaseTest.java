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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A read gives back the buffers of a row group's data requests ([InputFile#release]) once nothing
/// reads them, and reads none of them after. Each buffer here is a copy that its release overwrites,
/// so a read of a buffer given back reads other bytes than the file's.
class BufferReleaseTest {

    private static final Path RESOURCES = Path.of("src/test/resources");
    /// Four row groups of 50 rows.
    private static final Path MULTI_ROW_GROUP = RESOURCES.resolve("differential/diff_order_multi.parquet");
    /// Five row groups of 200 rows, dictionary-encoded, with Bloom filters and a page index.
    private static final Path PRUNABLE = RESOURCES.resolve("bloom_filter_multi_rg_dict_index_test.parquet");
    /// A context with a pool, and one without, whose reads run on the thread that reads the rows.
    private static final int[] THREADS = { 0, 4 };

    /// A tail too short to hold the data of a file, so that the data are requests of their own.
    private static final String SHORT_TAIL_BYTES = "64";
    /// The rows of the file of [#wideFile()].
    private static final int WIDE_ROWS = 40_000;

    /// How the read gets the bytes of a file: the tail it takes, and the maximum size of a request.
    /// A `null` value keeps the default.
    private enum Shape {
        /// Each data range is a request.
        SHORT_TAIL(SHORT_TAIL_BYTES, null),
        /// Each column chunk is cut into many requests, and a page that crosses a cut is a copy.
        SMALL_REQUESTS(SHORT_TAIL_BYTES, "4096"),
        /// The data of a small file are slices of its tail, which the read does not give back.
        DEFAULT_TAIL(null, null);

        final String tailBytes;
        final String maxSpanBytes;

        Shape(String tailBytes, String maxSpanBytes) {
            this.tailBytes = tailBytes;
            this.maxSpanBytes = maxSpanBytes;
        }

        void apply() {
            set(TailedInputFile.TAIL_BYTES_PROPERTY, tailBytes);
            set(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, maxSpanBytes);
        }
    }

    private String tailBytes;
    private String maxSpanBytes;

    @BeforeEach
    void shortTail() {
        tailBytes = System.getProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        maxSpanBytes = System.getProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY);
        Shape.SHORT_TAIL.apply();
    }

    @AfterEach
    void restoreProperties() {
        set(TailedInputFile.TAIL_BYTES_PROPERTY, tailBytes);
        set(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, maxSpanBytes);
    }

    private static void set(String property, String value) {
        if (value == null) {
            System.clearProperty(property);
        }
        else {
            System.setProperty(property, value);
        }
    }

    static Stream<Arguments> fixtures() throws IOException {
        List<String> names;
        try (Stream<Path> files = Files.list(RESOURCES)) {
            names = files.map(f -> f.getFileName().toString())
                    .filter(name -> name.endsWith(".parquet"))
                    .sorted()
                    .toList();
        }
        return names.stream().flatMap(name -> Arrays.stream(THREADS).mapToObj(threads -> Arguments.of(name, threads)));
    }

    /// Every fixture reads the same rows, or fails the same way, through a file that frees each
    /// buffer it gives back, and is given back only buffers it returned, each once, before it closes.
    /// The fixtures that fail to read cover the give-backs of a failed walk, placement and decode.
    /// Each [Shape] of the requests is read.
    @ParameterizedTest(name = "{0} threads={1}")
    @MethodSource("fixtures")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aReadReadsNoBufferItGaveBack(String fixture, int threads) {
        Path path = RESOURCES.resolve(fixture);
        for (Shape shape : Shape.values()) {
            shape.apply();
            for (long head : new long[]{ 0, 7 }) {
                Query query = reader -> head > 0 ? reader.buildRowReader().head(head).build() : reader.rowReader();
                Outcome expected = read(List.of(InputFile.of(path)), threads, query);
                ReleasingInputFile file = new ReleasingInputFile(InputFile.of(path));
                assertThat(read(List.of(file), threads, query)).as("%s head %d", shape, head).isEqualTo(expected);
                assertThat(file.misuse()).as("%s head %d", shape, head).isEmpty();
            }
        }
    }

    /// A filtered read, whose Bloom filter, dictionary and page-index reads are the file's to free
    /// and whose pages a mask drops, reads the same rows.
    @ParameterizedTest(name = "threads={0}")
    @ValueSource(ints = { 0, 4 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aFilteredReadReadsNoBufferItGaveBack(int threads) {
        List<FilterPredicate> filters = List.of(
                FilterPredicate.eq("code", 3),
                FilterPredicate.eq("name", "x".repeat(5)),
                FilterPredicate.gt("id", 650L),
                FilterPredicate.or(FilterPredicate.eq("id", 10L), FilterPredicate.eq("id", 990L)));
        for (FilterPredicate filter : filters) {
            Query query = reader -> reader.buildRowReader().filter(filter).build();
            Outcome expected = read(List.of(InputFile.of(PRUNABLE)), threads, query);
            assertThat(expected.failure()).isNull();
            ReleasingInputFile file = new ReleasingInputFile(InputFile.of(PRUNABLE));
            assertThat(read(List.of(file), threads, query)).as("%s", filter).isEqualTo(expected);
            assertThat(file.released()).as("%s", filter).isNotEmpty();
            assertThat(file.misuse()).as("%s", filter).isEmpty();
        }
    }

    /// A read of several files, here two of the same content, gives each buffer back to the file
    /// that returned it.
    @ParameterizedTest(name = "threads={0}")
    @ValueSource(ints = { 0, 4 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadOfSeveralFilesReadsNoBufferItGaveBack(int threads) {
        Query query = ParquetFileReader::rowReader;
        Outcome expected = read(List.of(InputFile.of(PRUNABLE), InputFile.of(PRUNABLE)), threads, query);
        assertThat(expected.failure()).isNull();
        ReleasingInputFile first = new ReleasingInputFile(InputFile.of(PRUNABLE));
        ReleasingInputFile second = new ReleasingInputFile(InputFile.of(PRUNABLE));
        assertThat(read(List.of(first, second), threads, query)).isEqualTo(expected);
        for (ReleasingInputFile file : List.of(first, second)) {
            assertThat(file.released()).isNotEmpty();
            assertThat(file.misuse()).isEmpty();
        }
    }

    /// The buffers of the row groups the read has passed are given back while it goes on, and the
    /// buffers of every row group once it closes.
    @ParameterizedTest(name = "threads={0}")
    @ValueSource(ints = { 0, 4 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void buffersOfPassedRowGroupsAreGivenBackBeforeTheReadEnds(int threads) throws Exception {
        ReleasingInputFile file = new ReleasingInputFile(InputFile.of(MULTI_ROW_GROUP));
        List<RowGroup> rowGroups;
        try (HardwoodContext context = HardwoodContext.create(threads);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             RowReader rows = reader.rowReader()) {
            rowGroups = reader.getFileMetaData().rowGroups();
            assertThat(rowGroups).hasSize(4);
            // into the fourth row group, past the first three
            for (int i = 0; i < 160; i++) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
            }
            for (int rowGroup = 0; rowGroup < 3; rowGroup++) {
                List<ByteBuffer> data = file.readsWithin(rowGroups.get(rowGroup));
                assertThat(data).as("row group %d", rowGroup).isNotEmpty();
                file.awaitReleased(data);
            }
        }
        for (int rowGroup = 0; rowGroup < rowGroups.size(); rowGroup++) {
            assertThat(file.notReleased(file.readsWithin(rowGroups.get(rowGroup)))).as("row group %d", rowGroup)
                    .isEmpty();
        }
        assertThat(file.misuse()).isEmpty();
    }

    /// The read gives back the buffer of a request when it has decoded each page of the request,
    /// before it passes the row group. Here, the read decodes only some batches ahead of the rows
    /// that the test reads, thus its walk gets to the end of the row group only near its last row.
    @ParameterizedTest(name = "threads={0}")
    @ValueSource(ints = { 0, 4 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void buffersOfDecodedRequestsAreGivenBackInsideARowGroup(int threads, @TempDir Path dir) throws Exception {
        Path path = dir.resolve("wide.parquet");
        Files.write(path, wideFile());
        set(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, "256");
        ReleasingInputFile file = new ReleasingInputFile(InputFile.of(path));
        try (HardwoodContext context = HardwoodContext.create(threads);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             RowReader rows = reader.rowReader()) {
            List<RowGroup> rowGroups = reader.getFileMetaData().rowGroups();
            assertThat(rowGroups).hasSize(1);
            long read = 0;
            if (threads > 0) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                read++;
                file.awaitAnyReleased();
            }
            else {
                // Without a pool, the read runs only while the test reads rows
                while (file.released().isEmpty() && rows.hasNext()) {
                    rows.next();
                    read++;
                }
            }
            assertThat(file.released()).isNotEmpty();
            assertThat(read).as("rows read before the first give-back").isLessThan(WIDE_ROWS / 2);
            assertThat(file.notReleased(file.readsWithin(rowGroups.getFirst())))
                    .as("buffers of the row group not given back")
                    .isNotEmpty();
        }
        assertThat(file.misuse()).isEmpty();
    }

    /// One row group of one `FIXED_LEN_BYTE_ARRAY` column whose rows all have the same value. The
    /// file is small, but the row reader sizes its batches by the width of the column, so the read
    /// decodes few rows ahead.
    private static byte[] wideFile() throws IOException {
        int width = 4096;
        int rowCount = WIDE_ROWS;
        FileSchema schema = FileSchema.builder("wide")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, width)
                .build();
        byte[][] values = new byte[rowCount][];
        Arrays.fill(values, new byte[width]);
        WriterConfig config = WriterConfig.builder()
                .rowGroupTargetRows(rowCount)
                .rowGroupBufferTargetBytes(Long.MAX_VALUE)
                .pageTargetRows(500)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed("v", values));
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// The rows of a read, or the failure it ends with.
    private record Outcome(List<String> rows, String failure) {
    }

    @FunctionalInterface
    private interface Query {
        RowReader rows(ParquetFileReader reader) throws Exception;
    }

    /// Reads `files` as one dataset on a context of `threads`, each field rendered as text.
    private static Outcome read(List<InputFile> files, int threads, Query query) {
        List<String> rows = new ArrayList<>();
        try (HardwoodContext context = HardwoodContext.create(threads);
             ParquetFileReader reader = files.size() == 1
                     ? ParquetFileReader.open(files.getFirst(), context)
                     : ParquetFileReader.openAll(files, context);
             RowReader rowReader = query.rows(reader)) {
            while (rowReader.hasNext()) {
                rowReader.next();
                StringBuilder row = new StringBuilder();
                for (int f = 0; f < rowReader.getFieldCount(); f++) {
                    Object value = rowReader.getValue(f);
                    row.append(value instanceof byte[] bytes ? Arrays.toString(bytes) : String.valueOf(value))
                            .append('|');
                }
                rows.add(row.toString());
            }
        }
        catch (Exception e) {
            // the rows read before a failure depend on how far the read got
            return new Outcome(List.of(), e.getClass().getName() + ": " + e.getMessage());
        }
        return new Outcome(rows, null);
    }

    /// A file whose every read is a direct copy of its own, overwritten as the read gives it back,
    /// which records what is given back amiss.
    private static final class ReleasingInputFile implements InputFile {

        /// What a buffer given back holds: no valid page header or value run starts with it.
        private static final byte POISON = (byte) 0xA5;

        private record Read(long offset, int length, ByteBuffer bytes) {
        }

        private final InputFile delegate;
        private final List<Read> reads = new CopyOnWriteArrayList<>();
        private final Map<ByteBuffer, Boolean> live = new IdentityHashMap<>();
        private final Set<ByteBuffer> released = Collections.newSetFromMap(new IdentityHashMap<>());
        private final List<String> misuse = new CopyOnWriteArrayList<>();
        private boolean closed;

        ReleasingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        List<String> misuse() {
            return misuse;
        }

        /// The buffers given back so far.
        List<ByteBuffer> released() {
            synchronized (live) {
                return List.copyOf(released);
            }
        }

        /// The buffers of `buffers` not given back.
        List<ByteBuffer> notReleased(List<ByteBuffer> buffers) {
            synchronized (live) {
                return buffers.stream().filter(b -> !released.contains(b)).toList();
            }
        }

        /// The buffers of the reads inside `rowGroup`'s column chunks.
        List<ByteBuffer> readsWithin(RowGroup rowGroup) {
            long start = Long.MAX_VALUE;
            long end = 0;
            for (ColumnChunk chunk : rowGroup.columns()) {
                start = Math.min(start, chunk.chunkStartOffset());
                end = Math.max(end, chunk.chunkStartOffset() + chunk.metaData().totalCompressedSize());
            }
            List<ByteBuffer> within = new ArrayList<>();
            for (Read read : reads) {
                if (read.offset() >= start && read.offset() + read.length() <= end) {
                    within.add(read.bytes());
                }
            }
            return within;
        }

        /// Waits until a buffer is given back.
        void awaitAnyReleased() throws InterruptedException {
            synchronized (live) {
                while (released.isEmpty()) {
                    live.wait();
                }
            }
        }

        /// Waits until every buffer of `buffers` is given back: a buffer is given back on whichever
        /// thread ends its last page, which can come after the rows the test reads.
        void awaitReleased(List<ByteBuffer> buffers) throws InterruptedException {
            synchronized (live) {
                while (!released.containsAll(buffers)) {
                    live.wait();
                }
            }
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            ByteBuffer source = delegate.readRange(offset, length);
            ByteBuffer copy = ByteBuffer.allocateDirect(length);
            copy.put(source.duplicate()).clear();
            synchronized (live) {
                live.put(copy, Boolean.TRUE);
            }
            reads.add(new Read(offset, length, copy));
            return copy;
        }

        @Override
        public void release(ByteBuffer buffer) {
            synchronized (live) {
                if (closed) {
                    misuse.add("released after close");
                }
                if (live.remove(buffer) == null) {
                    misuse.add("released a buffer not live");
                    return;
                }
                for (int i = 0; i < buffer.capacity(); i++) {
                    buffer.put(i, POISON);
                }
                released.add(buffer);
                live.notifyAll();
            }
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
        public Optional<String> identity() throws IOException {
            return delegate.identity();
        }

        @Override
        public void close() throws IOException {
            synchronized (live) {
                closed = true;
                live.clear();
            }
            delegate.close();
        }
    }
}
