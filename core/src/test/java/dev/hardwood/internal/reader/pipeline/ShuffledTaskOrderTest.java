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
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.engine.ReadEngine;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// Reads columns whose cells span several pages with their page tasks run one at a time in
/// seeded random orders ([TaskOrderExecutor#shuffling]), and checks every read against the
/// values written, or, for a fixture, against a read on a context's own pool.
///
/// The orders reach the steps of the cell protocol that depend on which task comes first, and
/// the test counts each, so that a change that stops the reads from reaching one fails here:
///
/// - the pages of a binary cell fill their segments in any order, each copying its `PLAIN` values'
///   bytes into chunks of its own ([BinaryFill]), and the page that fills the cell's last segment,
///   not necessarily the cell's last page, completes it;
/// - a page of a column decoded through levels measured before the page ahead of it waits, and
///   the task of the page ahead places both ([ScheduledNestedColumn]), including pages that begin
///   inside a record, whose leading values go into the cell of the record before; every cell of
///   such a column is filled inside a page task, none on its fetch steps.
///
/// The fetch steps run on the executor's thread too, among the page tasks. A failure names the
/// seed; the order a seed gives also depends on when the executor's wait for more tasks times
/// out, so a seed reproduces an order only most of the time.
class ShuffledTaskOrderTest {

    private static final int SEEDS = 10;
    private static final int THREADS = 4;
    /// The pages a read with [#THREADS] threads has in flight at most.
    private static final int IN_FLIGHT = 2 * THREADS;

    private static final int ROWS = 400;
    private static final String LONG_RECORDS = "src/test/resources/nested_v1_long_record_pages.parquet";

    @TempDir
    static Path dir;

    private static Path strings;
    private static Path lists;
    private static List<String> plainValues;
    private static List<String> dictValues;
    private static List<String> listRecords;

    private OrderCounts counts;

    @BeforeAll
    static void writeFiles() throws Exception {
        writeStrings();
        writeLists();
    }

    @BeforeEach
    void install() {
        counts = new OrderCounts();
        TaskOrderExecutor.install();
    }

    @AfterEach
    void uninstall() {
        TaskOrderExecutor.uninstall();
        ScheduledNestedColumn.fillObserver = null;
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void binaryCellsFillCorrectlyInEveryTaskOrder() throws Exception {
        for (long seed = 0; seed < SEEDS; seed++) {
            for (int batchSize : new int[]{ 7, 16 }) {
                assertThat(readFlat(seed, strings, "plain", batchSize))
                        .as("plain, seed %d, batch %d", seed, batchSize).isEqualTo(plainValues);
                assertThat(readFlat(seed, strings, "dict", batchSize))
                        .as("dict, seed %d, batch %d", seed, batchSize).isEqualTo(dictValues);
            }
        }
        // The counts come from the scheduler's submissions, which a read the read engine drives
        // makes none of; its pages still run in the seeded orders, and their values are checked.
        if (!ReadEngine.supports()) {
            assertThat(counts.outOfOrder).as("pages run ahead of an earlier page of their column").isPositive();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void nestedPagesArePlacedInPageOrderInEveryTaskOrder() throws Exception {
        List<String> longRecords = readList(HardwoodContextImpl.create(THREADS), Path.of(LONG_RECORDS), 3);
        ScheduledNestedColumn.fillObserver = (column, cell) -> counts.filled();
        for (long seed = 0; seed < SEEDS; seed++) {
            for (int batchSize : new int[]{ 3, 11 }) {
                assertThat(readList(seed, lists, batchSize))
                        .as("lists, seed %d, batch %d", seed, batchSize).isEqualTo(listRecords);
            }
            assertThat(readList(seed, Path.of(LONG_RECORDS), 3))
                    .as("long records, seed %d", seed).isEqualTo(longRecords);
        }
        // The counts come from the scheduler's submissions and the scheduled column's fills, which a
        // read the read engine drives makes none of; its pages still run in the seeded orders, and
        // their records are checked.
        if (!ReadEngine.supports()) {
            assertThat(counts.outOfOrder).as("pages measured ahead of an earlier page of their column").isPositive();
            assertThat(counts.outOfOrderInsideRecord).as("of them, pages beginning inside a record").isPositive();
            assertThat(counts.fills).as("cells filled").isPositive();
            assertThat(counts.fillsOutsideTasks).as("cells filled outside a page task").isZero();
        }
    }

    // ==================== Reads ====================

    private List<String> readFlat(long seed, Path file, String column, int batchSize) throws Exception {
        TaskOrderExecutor executor = TaskOrderExecutor.shuffling(seed, IN_FLIGHT, counts);
        List<String> values = new ArrayList<>();
        try (HardwoodContextImpl context = executor.context(THREADS);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
             ColumnReader columnReader = reader.buildColumnReader(column).batchSize(batchSize).build()) {
            while (columnReader.nextBatch()) {
                String[] strings = columnReader.getStrings();
                for (int i = 0; i < columnReader.getRecordCount(); i++) {
                    values.add(strings[i]);
                }
            }
        }
        assertThat(executor.failure()).isNull();
        return values;
    }

    private List<String> readList(long seed, Path file, int batchSize) throws Exception {
        TaskOrderExecutor executor = TaskOrderExecutor.shuffling(seed, IN_FLIGHT, counts);
        List<String> records = readList(executor.context(THREADS), file, batchSize);
        assertThat(executor.failure()).isNull();
        return records;
    }

    /// The records of `file`'s `tags` list, each as `null` or its elements in brackets.
    private static List<String> readList(HardwoodContext context, Path file, int batchSize) throws Exception {
        List<String> records = new ArrayList<>();
        try (context;
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
             ColumnReader columnReader = reader
                     .buildColumnReader(reader.getFileSchema().getColumn("tags.list.element").columnIndex())
                     .batchSize(batchSize).build()) {
            while (columnReader.nextBatch()) {
                int[] offsets = columnReader.getLayerOffsets(0);
                Validity lists = columnReader.getLayerValidity(0);
                String[] elements = columnReader.getStrings();
                for (int r = 0; r < columnReader.getRecordCount(); r++) {
                    records.add(lists.isNull(r) ? null
                            : String.valueOf(Arrays.asList(elements).subList(offsets[r], offsets[r + 1])));
                }
            }
        }
        return records;
    }

    // ==================== Order counts ====================

    /// Counts the orders the tasks ran in. Runs on the
    /// executor's one task thread, apart from [#submitted].
    private static final class OrderCounts implements TaskOrderExecutor.Listener {

        private final Map<ScheduledColumn<?>, List<PageTask>> submitted = new IdentityHashMap<>();
        private final Set<PageTask> ran = Collections.newSetFromMap(new IdentityHashMap<>());
        private int outOfOrder;
        private int outOfOrderInsideRecord;
        private final Set<PageTask> ranAhead = Collections.newSetFromMap(new IdentityHashMap<>());
        /// The thread running a task, `null` between tasks.
        private Thread running;
        private int fills;
        private int fillsOutsideTasks;

        /// On the thread filling a cell.
        synchronized void filled() {
            fills++;
            if (Thread.currentThread() != running) {
                fillsOutsideTasks++;
            }
        }

        @Override
        public synchronized void submitted(ScheduledColumn<?> column, PageTask page) {
            submitted.computeIfAbsent(column, c -> new ArrayList<>()).add(page);
        }

        @Override
        public synchronized void beforeRun(ScheduledColumn<?> column, PageTask page) {
            running = Thread.currentThread();
            for (PageTask earlier : submitted.get(column)) {
                if (earlier == page) {
                    break;
                }
                if (!ran.contains(earlier)) {
                    outOfOrder++;
                    ranAhead.add(page);
                    break;
                }
            }
        }

        @Override
        public synchronized void afterRun(ScheduledColumn<?> column, PageTask page) {
            running = null;
            ran.add(page);
            // A page measured ahead of the page before it waits for it, unplaced, with its measure
            if (ranAhead.remove(page) && page instanceof NestedPageTask nested && nested.measure != null
                    && nested.measure.leadingValues() > 0) {
                outOfOrderInsideRecord++;
            }
        }
    }

    // ==================== Files ====================

    /// Two optional binary columns in pages of five rows and row groups of 150: `plain` in `PLAIN`,
    /// `dict` dictionary-encoded, each with nulls.
    private static void writeStrings() throws Exception {
        byte[][] plain = new byte[ROWS][];
        boolean[] plainNulls = new boolean[ROWS];
        byte[][] dict = new byte[ROWS][];
        boolean[] dictNulls = new boolean[ROWS];
        plainValues = new ArrayList<>();
        dictValues = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            plainNulls[i] = i % 7 == 3 || i % 11 == 0;
            String p = "p".repeat(i % 13) + i;
            plain[i] = p.getBytes(StandardCharsets.UTF_8);
            plainValues.add(plainNulls[i] ? null : p);
            dictNulls[i] = i % 9 == 4;
            String d = "entry-" + (i % 5);
            dict[i] = d.getBytes(StandardCharsets.UTF_8);
            dictValues.add(dictNulls[i] ? null : d);
        }
        FileSchema schema = FileSchema.builder("strings")
                .addColumn("plain", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                .addColumn("dict", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding("plain", ColumnEncoding.PLAIN)
                .pageTargetRows(5)
                .rowGroupTargetRows(150)
                .build();
        strings = dir.resolve("strings.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(strings), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes("plain", plain, plainNulls).bytes("dict", dict, dictNulls));
        }
    }

    /// An optional list of optional strings, `tags`, with null and empty lists, in pages of three
    /// rows and row groups of 150.
    private static void writeLists() throws Exception {
        int[] offsets = new int[ROWS + 1];
        boolean[] listNulls = new boolean[ROWS];
        List<byte[]> elements = new ArrayList<>();
        List<Boolean> elementNulls = new ArrayList<>();
        listRecords = new ArrayList<>();
        for (int r = 0; r < ROWS; r++) {
            listNulls[r] = r % 10 == 7;
            List<String> record = new ArrayList<>();
            if (!listNulls[r]) {
                for (int k = 0; k < r % 9; k++) {
                    boolean isNull = (r + k) % 6 == 5;
                    String value = "e" + r + "." + k + "x".repeat(k);
                    elements.add(value.getBytes(StandardCharsets.UTF_8));
                    elementNulls.add(isNull);
                    record.add(isNull ? null : value);
                }
            }
            offsets[r + 1] = elements.size();
            listRecords.add(listNulls[r] ? null : String.valueOf(record));
        }
        boolean[] nulls = new boolean[elementNulls.size()];
        for (int i = 0; i < nulls.length; i++) {
            nulls[i] = elementNulls.get(i);
        }
        FileSchema schema = FileSchema.builder("lists")
                .list("tags", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL))
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding(ColumnEncoding.PLAIN)
                .pageTargetRows(3)
                .rowGroupTargetRows(150)
                .build();
        lists = dir.resolve("lists.parquet");
        byte[][] values = elements.toArray(new byte[0][]);
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(lists), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.list("tags", offsets, Validity.ofNulls(listNulls))
                    .bytes("tags.list.element", values, nulls));
        }
    }
}
