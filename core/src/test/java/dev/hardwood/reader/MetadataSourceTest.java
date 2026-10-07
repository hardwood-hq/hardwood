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
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.reader.CountingInputFile;
import dev.hardwood.internal.reader.CountingInputFile.Read;
import dev.hardwood.internal.reader.ParsedFooterImpl;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.reader.StaleMetadataException.Check;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/// A [MetadataSource] installed on a context supplies the footer of every file a reader reads,
/// and the reader checks each supplied footer against the file before using it.
class MetadataSourceTest {

    private static final Path FILE = Path.of("src/test/resources/plain_uncompressed.parquet");
    private static final Path LARGER_FILE = Path.of("src/test/resources/page_index_test.parquet");
    private static final Path PART_0 = Path.of("src/test/resources/multi_file_part0.parquet");
    private static final Path PART_1 = Path.of("src/test/resources/multi_file_part1.parquet");
    private static final Path ANNOTATED = Path.of("src/test/resources/annotated_repeated_group_test.parquet");
    private static final String STALE = "Footer from the MetadataSource does not describe the file: ";

    @TempDir
    Path tempDir;

    @Test
    void aSuppliedFooterIsCheckedThroughTheTrailerAlone() throws Exception {
        ParsedFooter footer = footerOf(InputFile.of(FILE));
        CountingInputFile withoutSource = new CountingInputFile(InputFile.of(FILE));
        CountingInputFile withSource = new CountingInputFile(InputFile.of(FILE));
        long trailer = Files.size(FILE) - 8;

        try (ParquetFileReader reader = ParquetFileReader.open(withoutSource)) {
            assertThat(withoutSource.reads()).containsExactly(
                    new Read(trailer, 8, "footer-info"),
                    new Read(trailer - footer.footerLength(), Math.toIntExact(footer.footerLength()), "footer-body"));
        }
        try (HardwoodContext context = contextServing(file -> footer);
                ParquetFileReader reader = ParquetFileReader.open(withSource, context)) {
            assertThat(withSource.reads()).containsExactly(new Read(trailer, 8, "footer-info"));
            assertThat(reader.getFileMetaData()).isSameAs(footer.metaData());
            assertThat(reader.getFileSchema()).isSameAs(((ParsedFooterImpl) footer).fileFooter().schema());
            assertThat(rowCount(reader)).isEqualTo(footer.metaData().numRows());
        }
    }

    @Test
    void theSuppliedSchemaIsTheOneANormalOpenDerives() throws Exception {
        // foo_list is a repeated group annotated LIST outside a LIST group; the reader drops the
        // annotation and reads it as a list of structs.
        ParsedFooter footer = footerOf(InputFile.of(ANNOTATED));

        try (HardwoodContext context = contextServing(file -> footer);
                ParquetFileReader served = ParquetFileReader.open(InputFile.of(ANNOTATED), context);
                ParquetFileReader read = ParquetFileReader.open(InputFile.of(ANNOTATED))) {
            assertThat(served.getFileSchema().getField("foo_list")).isInstanceOfSatisfying(SchemaNode.GroupNode.class,
                    group -> assertThat(group.isStruct()).isTrue());
            assertThat(served.getFileSchema().toString()).isEqualTo(read.getFileSchema().toString());
        }
    }

    @Test
    void everyFileOfAMultiFileReadIsServedByTheSource() throws Exception {
        CachingSource source = new CachingSource();
        long expectedRows = rowCount(PART_0) + rowCount(PART_1);
        try (HardwoodContext context = contextServing(source)) {
            assertThat(rowCountOfAll(context, InputFile.of(PART_0), InputFile.of(PART_1))).isEqualTo(expectedRows);

            CountingInputFile part0 = new CountingInputFile(InputFile.of(PART_0));
            CountingInputFile part1 = new CountingInputFile(InputFile.of(PART_1));
            assertThat(rowCountOfAll(context, part0, part1)).isEqualTo(expectedRows);

            assertThat(footerReads(part0)).containsExactly("footer-info");
            assertThat(footerReads(part1)).containsExactly("footer-info");
        }
        assertThat(source.misses).containsExactly(PART_0.getFileName().toString(), PART_1.getFileName().toString());
        assertThat(source.calls).hasSize(4);
    }

    @Test
    void aStaleLaterFileFailsTheReadWhenItIsReached() throws Exception {
        ParsedFooter part0Footer = footerOf(InputFile.of(PART_0));
        InputFile part1 = InputFile.of(PART_1);
        String part1Identity = identityOf(PART_1);

        try (HardwoodContext context = contextServing(file -> part0Footer);
                ParquetFileReader reader = ParquetFileReader.openAll(List.of(InputFile.of(PART_0), part1), context)) {
            assertThatThrownBy(() -> {
                try (RowReader rows = reader.rowReader()) {
                    while (rows.hasNext()) {
                        rows.next();
                    }
                }
            }).isInstanceOfSatisfying(StaleMetadataException.class, e -> {
                assertThat(e.check()).isEqualTo(Check.IDENTITY);
                assertThat(e.fileName()).isEqualTo("multi_file_part1.parquet");
                assertThat(e.sourceIdentity()).isEqualTo(part0Footer.sourceIdentity());
                assertThat(e.fileIdentity()).contains(part1Identity);
            }).hasMessage("[multi_file_part1.parquet] " + STALE + "it was read from content with identity '"
                    + part0Footer.sourceIdentity().orElseThrow() + "', the file has identity '" + part1Identity + "'");
        }
    }

    @Test
    void aFooterOfOtherContentFailsTheIdentityCheck() throws Exception {
        Path original = copy(FILE, "original.parquet");
        Path copy = copy(FILE, "copy.parquet");
        ParsedFooter footer = footerOf(InputFile.of(original));
        String copyIdentity = identityOf(copy);

        assertStale(InputFile.of(copy), footer, Check.IDENTITY, "[copy.parquet] " + STALE
                + "it was read from content with identity '" + footer.sourceIdentity().orElseThrow()
                + "', the file has identity '" + copyIdentity + "'");
    }

    @Test
    void aFooterOfAFileOfAnotherLengthFailsTheTrailerCheck() throws Exception {
        byte[] shorter = Files.readAllBytes(FILE);
        byte[] longer = Files.readAllBytes(LARGER_FILE);
        ParsedFooter footer = footerOf(InputFile.of(ByteBuffer.wrap(shorter)));

        assertStale(InputFile.of(ByteBuffer.wrap(longer)), footer, Check.TRAILER, "[<memory>] " + STALE
                + "it was read from a file of " + shorter.length + " bytes, the file has " + longer.length);
    }

    @Test
    void aFooterOfAnotherLengthFailsTheTrailerCheck() throws Exception {
        byte[] bytes = Files.readAllBytes(FILE);
        ParsedFooter footer = footerOf(InputFile.of(ByteBuffer.wrap(bytes)));
        byte[] patched = bytes.clone();
        ByteBuffer.wrap(patched).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(patched.length - 8, Math.toIntExact(footer.footerLength() - 1));

        assertStale(InputFile.of(ByteBuffer.wrap(patched)), footer, Check.TRAILER, "[<memory>] " + STALE
                + "it is " + footer.footerLength() + " bytes long, the file's trailer records "
                + (footer.footerLength() - 1));
    }

    @Test
    void aTruncatedFileFailsTheTrailerCheck() throws Exception {
        byte[] bytes = Files.readAllBytes(FILE);
        ParsedFooter footer = footerOf(InputFile.of(ByteBuffer.wrap(bytes)));
        long footerStart = bytes.length - 8 - footer.footerLength();
        byte[] truncated = Arrays.copyOf(bytes, Math.toIntExact(footerStart - 1));

        assertStale(InputFile.of(ByteBuffer.wrap(truncated)), footer, Check.TRAILER, "[<memory>] " + STALE
                + "it was read from a file of " + bytes.length + " bytes, the file has " + truncated.length);
    }

    @Test
    void aFooterWhoseRowGroupsDoNotHoldItsRowsOpensAsWithoutASource() throws Exception {
        byte[] incoherent = FooterRewriter.rewrite(Files.readAllBytes(FILE), metaData -> new FileMetaData(
                metaData.version(), metaData.schema(), metaData.numRows() + 1, metaData.rowGroups(),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders()));
        ParsedFooter footer = footerOf(InputFile.of(ByteBuffer.wrap(incoherent)));

        long withoutSource;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(incoherent)))) {
            withoutSource = rowCount(reader);
        }
        try (HardwoodContext context = contextServing(file -> footer);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(incoherent)), context)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(footer.metaData().numRows());
            assertThat(rowCount(reader)).isEqualTo(withoutSource);
        }
    }

    @Test
    void anEmptyIdentityOnEitherSideSkipsTheIdentityCheck() throws Exception {
        byte[] bytes = Files.readAllBytes(FILE);
        ParsedFooter inMemory = footerOf(InputFile.of(ByteBuffer.wrap(bytes)));
        ParsedFooter local = footerOf(InputFile.of(FILE));

        assertThat(inMemory.sourceIdentity()).isEmpty();
        try (HardwoodContext context = contextServing(file -> inMemory);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE), context)) {
            assertThat(rowCount(reader)).isEqualTo(inMemory.metaData().numRows());
        }
        try (HardwoodContext context = contextServing(file -> local);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)), context)) {
            assertThat(rowCount(reader)).isEqualTo(local.metaData().numRows());
        }
    }

    @Test
    void aSourceReturningNoFooterFailsTheOpen() throws Exception {
        try (HardwoodContext context = contextServing(file -> null)) {
            assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(FILE), context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("[plain_uncompressed.parquet] MetadataSource returned no footer");
        }
    }

    @Test
    void aSourceFailureKeepsItsTypeAndNamesTheFile() throws Exception {
        try (HardwoodContext context = contextServing(file -> {
            throw new IllegalStateException("Footer cache is offline");
        })) {
            assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(FILE), context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("[plain_uncompressed.parquet] Footer cache is offline");
        }
    }

    @Test
    void aSourceIOExceptionReachesTheCaller() throws Exception {
        IOException failure = new IOException("Footer cache is unreachable");
        try (HardwoodContext context = contextServing(file -> {
            throw failure;
        })) {
            assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(FILE), context))
                    .isSameAs(failure);
        }
    }

    @Test
    void aSourceFailureOnALaterFileNamesThatFile() throws Exception {
        IOException failure = new IOException("Footer cache is unreachable");
        try (HardwoodContext context = contextServing(file -> {
            if (file.name().equals(PART_1.getFileName().toString())) {
                throw failure;
            }
            return ParsedFooter.readFrom(file);
        });
                ParquetFileReader reader = ParquetFileReader.openAll(
                        List.of(InputFile.of(PART_0), InputFile.of(PART_1)), context)) {
            assertThatThrownBy(() -> reader.getFileMetaData(1))
                    .isExactlyInstanceOf(IOException.class)
                    .hasMessage("[multi_file_part1.parquet] Failed to read metadata")
                    .cause().isSameAs(failure);
        }
        try (HardwoodContext context = contextServing(file -> {
            if (file.name().equals(PART_1.getFileName().toString())) {
                throw new IllegalStateException("Footer cache is offline");
            }
            return ParsedFooter.readFrom(file);
        });
                ParquetFileReader reader = ParquetFileReader.openAll(
                        List.of(InputFile.of(PART_0), InputFile.of(PART_1)), context)) {
            assertThatThrownBy(() -> reader.getFileMetaData(1))
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessage("[multi_file_part1.parquet] Footer cache is offline");
        }
    }

    @Test
    void aCorruptFooterFailsAServedOpenAsItFailsAReadersOwnOpen() throws Exception {
        byte[] corrupt = Files.readAllBytes(FILE);
        int footerLength = ByteBuffer.wrap(corrupt).order(ByteOrder.LITTLE_ENDIAN).getInt(corrupt.length - 8);
        Arrays.fill(corrupt, corrupt.length - 8 - footerLength, corrupt.length - 8, (byte) 0xFF);

        Throwable withoutSource = catchThrowable(() -> ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(corrupt))));
        assertThat(withoutSource).isInstanceOf(ParquetReadException.class);
        try (HardwoodContext context = contextServing(ParsedFooter::readFrom)) {
            assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(corrupt)), context))
                    .isExactlyInstanceOf(withoutSource.getClass())
                    .hasMessage(withoutSource.getMessage());
        }
    }

    @Test
    void readingAFooterFromAFileNotOpenedIsTheCallersError() throws Exception {
        try (InputFile file = InputFile.of(FILE)) {
            assertThatThrownBy(() -> ParsedFooter.readFrom(file))
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessage("File not opened: plain_uncompressed.parquet");
        }
    }

    @Test
    void theBuilderRefusesANullSourceAndTooFewThreads() {
        assertThatThrownBy(() -> HardwoodContext.builder().metadataSource(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("source");
        assertThatThrownBy(() -> HardwoodContext.builder().threads(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("threads must be at least 1, but was 0");
    }

    private void assertStale(InputFile file, ParsedFooter footer, Check check, String message) throws Exception {
        try (HardwoodContext context = contextServing(f -> footer)) {
            assertThatThrownBy(() -> ParquetFileReader.open(file, context))
                    .isInstanceOfSatisfying(StaleMetadataException.class,
                            e -> assertThat(e.check()).isEqualTo(check))
                    .hasMessage(message);
        }
    }

    private Path copy(Path source, String name) throws IOException {
        return Files.copy(source, tempDir.resolve(name));
    }

    private static HardwoodContext contextServing(MetadataSource source) {
        return HardwoodContext.builder().threads(2).metadataSource(source).build();
    }

    private static ParsedFooter footerOf(InputFile file) throws IOException {
        try (file) {
            file.open();
            return ParsedFooter.readFrom(file);
        }
    }

    private static String identityOf(Path path) throws IOException {
        try (InputFile file = InputFile.of(path)) {
            file.open();
            return file.identity().orElseThrow();
        }
    }

    private static List<String> footerReads(CountingInputFile file) {
        List<String> reasons = new ArrayList<>();
        for (Read read : file.reads()) {
            if (read.reason().startsWith("footer-")) {
                reasons.add(read.reason());
            }
        }
        return reasons;
    }

    private static long rowCount(Path path) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            return rowCount(reader);
        }
    }

    private static long rowCountOfAll(HardwoodContext context, InputFile... files) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(files), context)) {
            return rowCount(reader);
        }
    }

    private static long rowCount(ParquetFileReader reader) throws IOException {
        long rows = 0;
        try (RowReader rowReader = reader.rowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }

    /// Caches footers by file name and identity, reading one on a miss, and records each call.
    private static final class CachingSource implements MetadataSource {

        private final Map<String, ParsedFooter> cache = new ConcurrentHashMap<>();
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final List<String> misses = new CopyOnWriteArrayList<>();

        @Override
        public ParsedFooter footerOf(InputFile file) throws IOException {
            calls.add(file.name());
            String key = file.name() + "|" + file.identity().orElseThrow();
            ParsedFooter footer = cache.get(key);
            if (footer == null) {
                misses.add(file.name());
                footer = ParsedFooter.readFrom(file);
                cache.put(key, footer);
            }
            return footer;
        }
    }
}
