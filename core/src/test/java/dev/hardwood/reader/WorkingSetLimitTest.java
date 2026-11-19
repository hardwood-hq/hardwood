/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.Compression;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read's working set, the dictionary and data page each decoded column holds, is held to the
/// working-set limit before anything is allocated for it.
///
/// The files are `INT64` columns whose dictionary page holds 1 MiB of zeros and whose data page
/// holds 1,024 values in 4 bytes. Each column holds its dictionary's entries (1,048,576 bytes) and
/// its page's batches (8,192 bytes); the read holds the largest task buffers once, the
/// dictionary page's decompressed bytes (1,048,576 bytes).
class WorkingSetLimitTest {

    private static final String MAX_WORKING_SET_BYTES_PROPERTY = "hardwood.internal.maxWorkingSetBytes";


    private static final int DICTIONARY_BYTES = 1024 * 1024;
    private static final int PAGE_VALUES = 1024;
    private static final int LARGE_VALUE_BYTES = 4 * 1024 * 1024;
    private static final long COLUMN_BYTES = DICTIONARY_BYTES + 8_192;

    /// The working set of a read of `columns` columns.
    private static long workingSet(int columns) {
        return columns * COLUMN_BYTES + DICTIONARY_BYTES;
    }

    @AfterEach
    void clearLimit() {
        System.clearProperty(MAX_WORKING_SET_BYTES_PROPERTY);
        System.clearProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY);
        System.clearProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY);
    }

    @Test
    void readAtTheLimitReads() throws IOException {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet(4)));

        assertThat(countRows(RleExpansionFile.buildWide(4, 1, DICTIONARY_BYTES, PAGE_VALUES), 4))
                .isEqualTo(PAGE_VALUES);
    }

    @Test
    void readOneBytePastTheLimitFails() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet(1) - 1));
        byte[] file = RleExpansionFile.buildWide(1, 1, DICTIONARY_BYTES, PAGE_VALUES);

        // one column: its dictionary and then its data page are the only charges, in that order
        assertThatThrownBy(() -> countRows(file, 0))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c0', page 0] Data page needs 8196 bytes to decode, "
                        + "which takes the read's working set to 2105344 bytes, more than its limit of 2105343 "
                        + "(hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void rowGroupsInBatchesOfTheirOwnCountOneDictionary() throws IOException {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet(1)));
        byte[] file = RleExpansionFile.buildWide(1, 3, DICTIONARY_BYTES, PAGE_VALUES);

        assertThat(countValues(file, PAGE_VALUES)).isEqualTo(3 * PAGE_VALUES);
    }

    @Test
    void batchSpanningRowGroupsCountsTheDictionaryOfEach() throws IOException {
        // One batch holds the rows of all three row groups, so their three dictionaries at once
        long workingSet = 3L * DICTIONARY_BYTES + 8_192 + DICTIONARY_BYTES;
        byte[] file = RleExpansionFile.buildWide(1, 3, DICTIONARY_BYTES, PAGE_VALUES);

        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet));
        assertThat(countValues(file, 3 * PAGE_VALUES)).isEqualTo(3 * PAGE_VALUES);

        // The same total crosses the limit whichever order the charges come in; which charge
        // crosses it depends on that order. The read plans the third row group behind the first
        // page's task, so the third dictionary crosses.
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet - 1));
        String crossing = "[<memory>: row group 2, column 'c0', dictionary page] Dictionary page needs 2097152 bytes";
        assertThatThrownBy(() -> countValues(file, 3 * PAGE_VALUES))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(crossing + " to decode, which takes the read's working set to 4202496 bytes, more than "
                        + "its limit of 4202495 (hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void dictionaryPruningReadCountsOnceTheColumnTakesIt() {
        // Pruning reads each row group's dictionary to look for the literal, which it holds, so
        // every row group stays and its column takes the dictionary instead of parsing it again.
        // Each parse fits the limit alone; the batch holding all three row groups' rows does not.
        // The column's walk takes the third row group's dictionary before the first page is
        // decoded, so that charge crosses.
        String crossing = "[<memory>: row group 2, column 'c0'] Dictionary page needs 1048576 bytes";
        long workingSet = 3L * DICTIONARY_BYTES + 8_192 + 4;
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet - 1));
        byte[] file = RleExpansionFile.buildWide(1, 3, DICTIONARY_BYTES, PAGE_VALUES);

        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(0);
                    ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                    RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("c0", 0L)).build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(crossing + " to decode, which takes the read's working set to 3153924 bytes, more "
                        + "than its limit of 3153923 (hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void dictionaryLargerThanTheLimitFailsBeforeItIsDecompressed() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(2L * DICTIONARY_BYTES - 1));
        byte[] file = RleExpansionFile.buildWide(1, 1, DICTIONARY_BYTES, PAGE_VALUES);

        assertThatThrownBy(() -> countRows(file, 4))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c0', dictionary page] Dictionary page needs 2097152 "
                        + "bytes to decode, which takes the read's working set to 2097152 bytes, more than its "
                        + "limit of 2097151 (hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void limitThatIsNotAByteCountIsRejected() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "0");
        byte[] file = RleExpansionFile.buildWide(1, 1, DICTIONARY_BYTES, PAGE_VALUES);

        assertThatThrownBy(() -> countRows(file, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("hardwood.internal.maxWorkingSetBytes must be a positive byte count, but was: 0");
    }

    /// Every way a read reaches a page charges it before decompressing it, so a limit one byte
    /// short of the working set a read reaches fails it at the page that reaches it: a nested
    /// column's batch, binary columns, `DATA_PAGE_V2`, the page index's plan and the column reader,
    /// and a nested column's page ([#nestedPageChargesBeforeDecompressing]). Each limit is above
    /// every page's stored bytes, which a smaller limit refuses before the page is read
    /// ([#pageStoringMoreThanTheLimitFailsBeforeItIsRead]). Without a pool the read's order is the
    /// same each run, so the same charges are made and the same page fails.
    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
            compat_maxrep_list.parquet                | 119  | [compat_maxrep_list.parquet: row group 0, column 'g.list.element', page 0] Batch of values from 1 pages needs 120 bytes to decode, which takes the read's working set to 120 bytes, more than its limit of 119 (hardwood.internal.maxWorkingSetBytes)
            misaligned_pages_nested_v2.parquet        | 2127 | [misaligned_pages_nested_v2.parquet: row group 0, column 'narrow', page 0] Data page needs 2128 bytes to decode, which takes the read's working set to 2128 bytes, more than its limit of 2127 (hardwood.internal.maxWorkingSetBytes)
            dict_string_map.parquet                   | 1599 | [dict_string_map.parquet: row group 0, column 'id', dictionary page] Dictionary page needs 1600 bytes to decode, which takes the read's working set to 1600 bytes, more than its limit of 1599 (hardwood.internal.maxWorkingSetBytes)
            page_index_test.parquet                   | 9375 | [page_index_test.parquet: row group 0, column 'id', page 0] Data page needs 8192 bytes to decode, which takes the read's working set to 9376 bytes, more than its limit of 9375 (hardwood.internal.maxWorkingSetBytes)
            delta_binary_packed_optional_test.parquet | 409  | [delta_binary_packed_optional_test.parquet: row group 0, column 'id', page 0] Data page needs 410 bytes to decode, which takes the read's working set to 410 bytes, more than its limit of 409 (hardwood.internal.maxWorkingSetBytes)
            """)
    void everyPathChargesBeforeDecompressing(String file, long limit, String message) {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(limit));

        assertThatThrownBy(() -> readAll(Path.of("src/test/resources", file)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(message);
    }

    /// A page whose stored bytes alone exceed the limit fails before any of its bytes are
    /// requested: its decode would hold them at once, so the read cannot hold it, and fetching
    /// or copying it first can run out of memory before the working set is charged. The file is
    /// a 4 MiB value written uncompressed: once in `PLAIN`, a data page of its own, and twice in `AUTO`,
    /// which stores it once in the dictionary page.
    @ParameterizedTest
    @EnumSource(names = { "PLAIN", "AUTO" })
    void pageStoringMoreThanTheLimitFailsBeforeItIsRead(ColumnEncoding encoding) throws IOException {
        byte[] value = new byte[LARGE_VALUE_BYTES];
        new Random(1).nextBytes(value);
        ByteBuffer file = writeValues(encoding == ColumnEncoding.AUTO ? new byte[][] { value, value } : new byte[][] { value },
                encoding);
        ColumnMetaData chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file.duplicate()))) {
            chunk = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
        }
        boolean dictionary = chunk.dictionaryPageOffset() != null;
        long stored = dictionary ? chunk.dataPageOffset() - chunk.dictionaryPageOffset() : chunk.totalCompressedSize();
        AtomicLong bytesRead = new AtomicLong();
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Integer.toString(LARGE_VALUE_BYTES / 4));

        assertThatThrownBy(() -> readAll(new ReadCountingFile(file, bytesRead)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[large.parquet: row group 0, column 'blob', " + (dictionary ? "dictionary page" : "page 0")
                        + "] " + (dictionary ? "Dictionary page" : "Data page") + " stores " + stored
                        + " bytes, more than the read's working-set limit of " + LARGE_VALUE_BYTES / 4
                        + " (hardwood.internal.maxWorkingSetBytes)");
        assertThat(dictionary).isEqualTo(encoding == ColumnEncoding.AUTO);
        assertThat(bytesRead.get()).isLessThan(stored);
    }

    /// A page a header scan finds, in a chunk without an offset index, is refused the same way once
    /// the scan reaches its header, before it is decoded.
    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
            inline_null_pages_v2.parquet | 799 | [inline_null_pages_v2.parquet: row group 0, column 'id', page 0] Data page stores 872 bytes, more than the read's working-set limit of 799 (hardwood.internal.maxWorkingSetBytes)
            dict_string_map.parquet      | 821 | [dict_string_map.parquet: row group 0, column 'id', dictionary page] Dictionary page stores 822 bytes, more than the read's working-set limit of 821 (hardwood.internal.maxWorkingSetBytes)
            """)
    void scannedPageStoringMoreThanTheLimitFailsBeforeItIsDecoded(String file, long limit, String message) {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(limit));

        assertThatThrownBy(() -> readAll(Path.of("src/test/resources", file)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(message);
    }

    /// A page a header scan finds is refused at its header, before the scan fetches its body, also
    /// where a row limit plans only the start of its chunk and the scan fetches the rest piece by
    /// piece. The chunk has no offset index; its page, or its dictionary page, holds an 8 MiB value.
    /// Requests of 256 KiB and no read-ahead leave the scan only the requests it waits on.
    @ParameterizedTest
    @CsvSource({ "PLAIN, 0", "PLAIN, 1", "AUTO, 0", "AUTO, 1" })
    void scannedPageStoringMoreThanTheLimitFailsBeforeItIsFetched(ColumnEncoding encoding, long maxRows)
            throws IOException {
        byte[] value = new byte[2 * LARGE_VALUE_BYTES];
        new Random(1).nextBytes(value);
        boolean dictionary = encoding == ColumnEncoding.AUTO;
        // many small values after it, on pages of their own, so that a row limit of one row plans
        // only the start of the chunk
        byte[][] values = new byte[100_000][];
        Arrays.fill(values, new byte[] { 'x' });
        values[0] = value;
        values[1] = dictionary ? value : values[1];
        ByteBuffer written = writeValues(values, encoding);
        byte[] bytes = new byte[written.remaining()];
        written.get(bytes);
        ByteBuffer file = ByteBuffer.wrap(FooterRewriter.withoutPageIndex(bytes));
        ColumnMetaData chunk = chunk(file);
        // the large value's data page stores what the chunk of a file of it alone does
        long stored = dictionary ? chunk.dataPageOffset() - chunk.dictionaryPageOffset()
                : storedBytes(writeValues(new byte[][] { value }, encoding));
        AtomicLong bytesRead = new AtomicLong();
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Integer.toString(LARGE_VALUE_BYTES / 4));
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, Integer.toString(256 * 1024));
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, "0");

        assertThatThrownBy(() -> readAll(new ReadCountingFile(file, bytesRead), maxRows))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[large.parquet: row group 0, column 'blob', " + (dictionary ? "dictionary page" : "page 0")
                        + "] " + (dictionary ? "Dictionary page" : "Data page") + " stores " + stored
                        + " bytes, more than the read's working-set limit of " + LARGE_VALUE_BYTES / 4
                        + " (hardwood.internal.maxWorkingSetBytes)");
        assertThat(bytesRead.get()).isLessThan(stored);
    }

    /// A dictionary page storing more than the limit is not read to prune by: a filter on its
    /// column leaves the row group to the read, which refuses the page before fetching it. The
    /// column holds two 4 MiB values on either side of the filter's literal, so statistics leave
    /// the row group open, twice each, so that the dictionary holds both.
    @Test
    void dictionaryStoringMoreThanTheLimitIsNotReadToPrune() throws IOException {
        byte[] low = new byte[LARGE_VALUE_BYTES];
        byte[] high = new byte[LARGE_VALUE_BYTES];
        Random random = new Random(1);
        random.nextBytes(low);
        random.nextBytes(high);
        low[0] = 0;
        high[0] = (byte) 0xFF;
        ByteBuffer file = writeValues(new byte[][] { low, high, low, high }, ColumnEncoding.AUTO);
        ColumnMetaData chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file.duplicate()))) {
            chunk = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
        }
        long stored = chunk.dataPageOffset() - chunk.dictionaryPageOffset();
        AtomicLong bytesRead = new AtomicLong();
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Integer.toString(LARGE_VALUE_BYTES / 4));

        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(0);
                    ParquetFileReader reader = ParquetFileReader.open(new ReadCountingFile(file, bytesRead), context);
                    RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("blob", new byte[] { 'q' })).build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[large.parquet: row group 0, column 'blob', dictionary page] Dictionary page stores " + stored
                        + " bytes, more than the read's working-set limit of " + LARGE_VALUE_BYTES / 4
                        + " (hardwood.internal.maxWorkingSetBytes)");
        assertThat(bytesRead.get()).isLessThan(stored);
    }

    /// A nested column's page is charged before it is decompressed. The fixtures' nested pages
    /// store more than they decode to, so the file is written here: a `list<int64>` of 1,000 rows
    /// of eight zeros, GZIP-compressed into a page of a few hundred bytes.
    @Test
    void nestedPageChargesBeforeDecompressing() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .list("values", RepetitionType.REQUIRED, element -> element.primitive(PhysicalType.INT64,
                        RepetitionType.REQUIRED))
                .build();
        int[] offsets = new int[1_001];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = 8 * i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, WriterConfig.builder()
                .compression(Compression.of(CompressionCodec.GZIP))
                .encoding(ColumnEncoding.PLAIN)
                .build())) {
            writer.columnWriter().writeBatch(batch -> batch.list("values", offsets)
                    .longs("values.list.element", new long[8_000]));
        }
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "1024");

        assertThatThrownBy(() -> readAll(InputFile.of(out.buffer())))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'values.list.element', page 0] Data page needs 289027 bytes "
                        + "to decode, which takes the read's working set to 289027 bytes, more than its limit of 1024 "
                        + "(hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void columnReaderChargesBeforeDecompressing() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "409");

        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(0);
                    ParquetFileReader reader = ParquetFileReader.open(
                            InputFile.of(Path.of("src/test/resources/delta_binary_packed_optional_test.parquet")), context);
                    ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    // drains the column
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[delta_binary_packed_optional_test.parquet: row group 0, column 'id', page 0] Data page needs 410 "
                        + "bytes to decode, which takes the read's working set to 410 bytes, more than its limit of 409 "
                        + "(hardwood.internal.maxWorkingSetBytes)");
    }

    private static void readAll(Path file) throws IOException {
        readAll(InputFile.of(file));
    }

    private static void readAll(InputFile file) throws IOException {
        readAll(file, 0);
    }

    /// Reads every row of `file`, or its first `maxRows` where that is positive.
    private static void readAll(InputFile file, long maxRows) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(0);
                ParquetFileReader reader = ParquetFileReader.open(file, context);
                RowReader rows = (maxRows > 0 ? reader.buildRowReader().head(maxRows) : reader.buildRowReader()).build()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    /// The values of `file`'s one column, read in batches of `batchSize` rows on a context
    /// without threads, each checked to be the dictionary's one value.
    private static long countValues(byte[] file, int batchSize) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(0);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                ColumnReader column = reader.buildColumnReader(0).batchSize(batchSize).build()) {
            long count = 0;
            while (column.nextBatch()) {
                long[] values = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    assertThat(values[i]).isZero();
                }
                count += column.getRecordCount();
            }
            return count;
        }
    }

    /// The rows of `file`, read on a context of `threads` threads, each checked to hold the
    /// dictionary's one value in every column.
    private static long countRows(byte[] file, int threads) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("c0")).isZero();
                count++;
            }
            return count;
        }
    }

    /// One required binary column `blob` holding `values`, uncompressed, in `encoding`.
    private static ByteBuffer writeValues(byte[][] values, ColumnEncoding encoding) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .primitive("blob", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .compression(Compression.uncompressed())
                .encoding(encoding)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }
        return out.buffer();
    }

    /// The metadata of `file`'s one column chunk.
    private static ColumnMetaData chunk(ByteBuffer file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file.duplicate()))) {
            return reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
        }
    }

    /// The stored bytes of `file`'s one column chunk.
    private static long storedBytes(ByteBuffer file) throws IOException {
        return chunk(file).totalCompressedSize();
    }

    /// Serves `bytes` through [#readRange] alone, as a file not in memory is read, and counts the
    /// bytes read.
    private record ReadCountingFile(ByteBuffer bytes, AtomicLong bytesRead) implements InputFile {

        @Override
        public void open() {
        }

        @Override
        public ByteBuffer readRange(long offset, int length) {
            bytesRead.addAndGet(length);
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(0, bytes, Math.toIntExact(offset), length);
            return copy;
        }

        @Override
        public long length() {
            return bytes.limit();
        }

        @Override
        public String name() {
            return "large.parquet";
        }

        @Override
        public void close() {
        }
    }
}
