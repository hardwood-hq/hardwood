/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A file that holds the tail its open fetched ([TailHolder]) is read whole in that request when
/// no longer than the tail, has its footer from it otherwise, releases the tail once the read is
/// done with the file, and is opened with the smaller tail when a `MetadataSource` supplies the
/// footers (#1399).
class TailHolderTest {

    private static final int TAIL = 256 * 1024;
    private static final int SUPPLIED_TAIL = 64 * 1024;
    private static final int SMALL_ROWS = 2_000;

    @Test
    void aFileWithinTheTailIsReadInItsOpenRequest() throws IOException {
        byte[] bytes = smallFile(0);
        assertThat(bytes.length).isLessThan(TAIL);
        TailFile file = tailFile(bytes, 0);

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            assertThat(rows(reader)).isEqualTo(SMALL_ROWS);
        }

        assertThat(file.requests()).containsExactly(new TailFile.Request(0, bytes.length, "open-tail"));
    }

    @Test
    void aFileLargerThanTheTailHasItsFooterFromTheOpenRequest() throws IOException {
        byte[] bytes = largeFile(0);
        assertThat(bytes.length).isGreaterThan(TAIL);
        TailFile file = tailFile(bytes, 0);

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            assertThat(rows(reader)).isEqualTo(100_000);
        }

        List<TailFile.Request> requests = file.requests();
        assertThat(requests.getFirst()).isEqualTo(new TailFile.Request(bytes.length - TAIL, TAIL, "open-tail"));
        assertThat(requests).as("requests after the open").hasSizeGreaterThan(1)
                .noneSatisfy(request -> assertThat(request.reason()).startsWith("footer-"));
    }

    @Test
    void theTailIsReleasedOnceTheReadIsDoneWithTheFile() throws IOException {
        List<TailFile> files = smallTailFiles(6);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(List.copyOf(files), context)) {
            assertThat(files.getFirst().tailBytes()).as("tail of the first file once opened").isPositive();
            assertThat(rows(reader)).isEqualTo(6L * SMALL_ROWS);

            // Before the reader closes its files
            for (TailFile file : files) {
                assertThat(file.tailBytes()).as("tail of " + file.name()).isZero();
                assertThat(file.releases()).as("releases of " + file.name()).isEqualTo(1);
                assertThat(file.requests()).as("requests of " + file.name()).hasSize(1);
            }
            assertThat(context.readAheadBuffer().held()).as("read-ahead held once read").isZero();
        }
    }

    @Test
    void aFileStatisticsPruneReleasesItsTailWhenPlanned() throws IOException {
        List<TailFile> files = smallTailFiles(6);
        // Keeps the rows of the first file alone
        FilterPredicate filter = FilterPredicate.lt("id", (long) SMALL_ROWS);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(List.copyOf(files), context);
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.all()).filter(filter).build()) {
            long rows = 0;
            while (cols.nextBatch()) {
                rows += cols.getRecordCount();
            }
            assertThat(rows).isEqualTo(SMALL_ROWS);

            for (TailFile file : files) {
                assertThat(file.tailBytes()).as("tail of " + file.name()).isZero();
                assertThat(file.requests()).as("requests of " + file.name()).hasSize(1);
            }
            assertThat(context.readAheadBuffer().held()).as("read-ahead held once read").isZero();
        }
    }

    @Test
    void aSuppliedFooterKeepsTheOpenRequestAtItsSize() throws IOException {
        List<byte[]> contents = List.of(largeFile(0), largeFile(1), largeFile(2));
        MetadataSource source = cachingSource();
        try (HardwoodContext context = HardwoodContext.builder().threads(2).metadataSource(source).build()) {
            // Warms the source, which reads each footer from the file on its first request
            try (ParquetFileReader reader = ParquetFileReader.openAll(tailFiles(contents), context)) {
                assertThat(rows(reader)).isEqualTo(3 * 100_000L);
            }

            List<InputFile> files = tailFiles(contents);
            try (ParquetFileReader reader = ParquetFileReader.openAll(files, context)) {
                assertThat(rows(reader)).isEqualTo(3 * 100_000L);
                for (InputFile file : files) {
                    TailFile tailFile = (TailFile) file;
                    assertThat(tailFile.openedForSuppliedFooter()).as("supplied open of " + file.name()).isTrue();
                    assertThat(tailFile.openBytes()).as("open of " + file.name()).isEqualTo(SUPPLIED_TAIL);
                }
            }
        }
    }

    private static long rows(ParquetFileReader reader) throws IOException {
        long rows = 0;
        try (ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.all()).build()) {
            while (cols.nextBatch()) {
                rows += cols.getRecordCount();
            }
        }
        return rows;
    }

    /// Caches each footer by file name; the files of this test have no identity, so the trailer
    /// check guards a served footer.
    private static MetadataSource cachingSource() {
        Map<String, ParsedFooter> cache = new ConcurrentHashMap<>();
        return file -> {
            ParsedFooter footer = cache.get(file.name());
            if (footer == null) {
                footer = ParsedFooter.readFrom(file);
                cache.put(file.name(), footer);
            }
            return footer;
        };
    }

    private static TailFile tailFile(byte[] bytes, int index) {
        return new TailFile(bytes, "file-" + index, TAIL, SUPPLIED_TAIL);
    }

    private static List<InputFile> tailFiles(List<byte[]> contents) {
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < contents.size(); i++) {
            files.add(tailFile(contents.get(i), i));
        }
        return files;
    }

    private static List<TailFile> smallTailFiles(int count) throws IOException {
        List<TailFile> files = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            files.add(tailFile(smallFile(i), i));
        }
        return files;
    }

    /// One row group of [#SMALL_ROWS] rows: `id` counts on from the rows of the files before, so a
    /// range predicate on it prunes whole files.
    private static byte[] smallFile(int fileIndex) throws IOException {
        long[] ids = new long[SMALL_ROWS];
        double[] values = new double[SMALL_ROWS];
        for (int i = 0; i < SMALL_ROWS; i++) {
            ids[i] = (long) fileIndex * SMALL_ROWS + i;
            values[i] = ids[i] * 1.5 + 0.25;
        }
        return write(ids, values);
    }

    /// One row group of 100,000 rows whose values do not compress: about 1.6 MB.
    private static byte[] largeFile(int fileIndex) throws IOException {
        int rows = 100_000;
        Random random = new Random(fileIndex);
        long[] ids = new long[rows];
        double[] values = new double[rows];
        for (int i = 0; i < rows; i++) {
            ids[i] = random.nextLong();
            values[i] = random.nextDouble();
        }
        return write(ids, values);
    }

    private static byte[] write(long[] ids, double[] values) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, WriterConfig.builder().build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("id", ids).doubles("v", values));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
