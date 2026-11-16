/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.fuzz;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Named;

import dev.hardwood.MisstatedPageFile;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.thrift.DeepNestingFile;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;

/// The seed inputs of the fuzz targets: the small Parquet files of the core test resources and
/// files crafted to break a bound, and the footers, page headers and page indexes cut from them.
final class Seeds {

    /// Files above this size are left out, as each mutation of a seed costs in proportion to it.
    private static final long MAX_FILE_BYTES = 64 * 1024;
    /// Page headers taken from each column chunk, from its first page.
    private static final int PAGES_PER_CHUNK = 3;
    /// The longest run of a chunk's first pages a page seed holds.
    private static final int MAX_CHUNK_BYTES = 16 * 1024;

    private static final Path RESOURCES = Path.of("src/test/resources");

    private static final List<Named<byte[]>> FILES = readFiles();

    static {
        // The reader logs a warning for most malformed inputs, which a fuzzing run writes millions
        // of; every target takes its seeds from here, so loading them silences it
        Configurator.setLevel("dev.hardwood", Level.OFF);
    }

    private Seeds() {
    }

    /// Whole files.
    static Stream<Named<byte[]>> files() {
        return FILES.stream();
    }

    /// The footer of each file, the bytes between the magic number and the footer length.
    static Stream<Named<byte[]>> footers() {
        Stream<Named<byte[]>> crafted = Stream.of(
                Named.of("footer skipping a value nested past the bound",
                        DeepNestingFile.footerSkipping(DeepNestingFile.MAX_SKIP_DEPTH + 1)),
                Named.of("footer of a schema nested past the bound",
                        DeepNestingFile.footerOfSchema(DeepNestingFile.MAX_SCHEMA_DEPTH + 1)));
        return Stream.concat(crafted, slices(Seeds::footer));
    }

    /// The headers of the first pages of each column chunk.
    static Stream<Named<byte[]>> pageHeaders() {
        return slices(Seeds::pageHeaders);
    }

    /// The first pages of the first row group's column chunks, each with its column's shape and
    /// its codec ([ChunkInput]).
    static Stream<Named<byte[]>> chunks() {
        return slices(Seeds::chunks);
    }

    /// Compressed bytes for each decompressor ([DecompressorFuzzTest]): the bodies of the first
    /// pages of each column chunk, with the size their headers declare, and samples compressed by
    /// each codec's compressor, each from a heap buffer and from a direct one.
    static Stream<Named<byte[]>> compressed() {
        Stream<Named<byte[]>> samples = Stream.of(CompressionCodec.values()).flatMap(Seeds::samples);
        return Stream.concat(samples, slices(Seeds::pageBodies));
    }

    /// The offset index of each column chunk that has one.
    static Stream<Named<byte[]>> offsetIndexes() {
        return slices(file -> chunkRegions(file, chunk -> region(chunk.offsetIndexOffset(), chunk.offsetIndexLength())));
    }

    /// The column index of each column chunk that has one.
    static Stream<Named<byte[]>> columnIndexes() {
        return slices(file -> chunkRegions(file, chunk -> region(chunk.columnIndexOffset(), chunk.columnIndexLength())));
    }

    /// The bloom filter of each column chunk that has one, its header followed by its bitset.
    static Stream<Named<byte[]>> bloomFilters() {
        return slices(file -> chunkRegions(file,
                chunk -> region(chunk.metaData().bloomFilterOffset(), chunk.metaData().bloomFilterLength())));
    }

    private static List<Named<byte[]>> readFiles() {
        List<Named<byte[]>> files = new ArrayList<>();
        try (Stream<Path> paths = Files.list(RESOURCES)) {
            for (Path path : paths.sorted().toList()) {
                if (path.toString().endsWith(".parquet") && Files.size(path) <= MAX_FILE_BYTES) {
                    files.add(Named.of(path.getFileName().toString(), Files.readAllBytes(path)));
                }
            }
            for (CompressionCodec codec : List.of(CompressionCodec.GZIP, CompressionCodec.SNAPPY,
                    CompressionCodec.ZSTD, CompressionCodec.LZ4_RAW, CompressionCodec.BROTLI)) {
                files.add(Named.of(codec + " page header understating its page",
                        MisstatedPageFile.build(codec, MisstatedPageFile.UNDERSTATED)));
            }
            files.add(Named.of("data page declaring 2^31 - 1 values in one RLE run",
                    RleExpansionFile.build(Integer.MAX_VALUE, 1, Integer.MAX_VALUE)));
            files.add(Named.of("schema nested past the bound",
                    DeepNestingFile.fileOf(DeepNestingFile.footerOfSchema(DeepNestingFile.MAX_SCHEMA_DEPTH + 1))));
            files.add(Named.of("footer skipping a value nested past the bound",
                    DeepNestingFile.fileOf(DeepNestingFile.footerSkipping(DeepNestingFile.MAX_SKIP_DEPTH + 1))));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return files;
    }

    /// The slices `cut` takes from each file that parses, named after the file.
    private static Stream<Named<byte[]>> slices(Function<byte[], List<byte[]>> cut) {
        return FILES.stream().flatMap(file -> {
            List<byte[]> slices;
            try {
                slices = cut.apply(file.getPayload());
            }
            catch (RuntimeException e) {
                // A crafted or malformed file seeds the file target only
                return Stream.empty();
            }
            List<Named<byte[]>> named = new ArrayList<>(slices.size());
            for (int i = 0; i < slices.size(); i++) {
                named.add(Named.of(file.getName() + " #" + i, slices.get(i)));
            }
            return named.stream();
        });
    }

    private static List<byte[]> footer(byte[] file) {
        int lengthAt = file.length - 8;
        int length = ByteBuffer.wrap(file, lengthAt, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return List.of(Arrays.copyOfRange(file, lengthAt - length, lengthAt));
    }

    private static FileMetaData metaData(byte[] file) {
        return FileMetaDataReader.read(reader(footer(file).get(0)));
    }

    private static List<byte[]> pageHeaders(byte[] file) {
        List<byte[]> headers = new ArrayList<>();
        for (RowGroup rowGroup : metaData(file).rowGroups()) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                for (Page page : pages(file, chunk.metaData())) {
                    headers.add(Arrays.copyOfRange(file, page.at(), page.bodyAt()));
                }
            }
        }
        return headers;
    }

    private static List<byte[]> chunks(byte[] file) {
        FileMetaData metaData = metaData(file);
        FileSchema schema = ParquetMetadataReader.schemaOf(metaData);
        List<byte[]> chunks = new ArrayList<>();
        if (metaData.rowGroups().isEmpty()) {
            return chunks;
        }
        List<ColumnChunk> columns = metaData.rowGroups().get(0).columns();
        for (int c = 0; c < columns.size(); c++) {
            ColumnMetaData chunk = columns.get(c).metaData();
            int from = Math.toIntExact(firstPageOffset(chunk));
            int to = from;
            for (Page page : pages(file, chunk)) {
                if (page.end() - from > MAX_CHUNK_BYTES) {
                    break;
                }
                to = page.end();
            }
            byte[] input = ChunkInput.encode(schema.getColumn(c), chunk.codec(), Arrays.copyOfRange(file, from, to));
            if (to > from && input != null) {
                chunks.add(input);
            }
        }
        return chunks;
    }

    /// The compressed body of each of the first pages of each column chunk, after the selector of
    /// each decompressor of the chunk's codec and the size the page header declares.
    private static List<byte[]> pageBodies(byte[] file) {
        List<byte[]> bodies = new ArrayList<>();
        for (RowGroup rowGroup : metaData(file).rowGroups()) {
            for (ColumnChunk column : rowGroup.columns()) {
                ColumnMetaData chunk = column.metaData();
                for (Page page : pages(file, chunk)) {
                    PageHeader header = page.header();
                    byte[] body = Arrays.copyOfRange(file, page.bodyAt(), page.end());
                    // a V2 page compresses its values alone, after the levels its header counts
                    int levels = header.dataPageHeaderV2() == null ? 0
                            : header.dataPageHeaderV2().repetitionLevelsByteLength()
                                    + header.dataPageHeaderV2().definitionLevelsByteLength();
                    if (levels > body.length) {
                        continue;
                    }
                    for (int selector : DecompressorFuzzTest.selectors(chunk.codec())) {
                        bodies.add(compressedInput(selector, header.uncompressedPageSize() - levels,
                                Arrays.copyOfRange(body, levels, body.length)));
                    }
                }
            }
        }
        return bodies;
    }

    /// Samples compressed by `codec`'s compressor, for each of its decompressors, from a heap
    /// buffer and from a direct one; none for a codec without a compressor.
    private static Stream<Named<byte[]>> samples(CompressionCodec codec) {
        Compressor compressor;
        try {
            compressor = new CompressorFactory().getCompressor(codec);
        }
        catch (UnsupportedOperationException e) {
            return Stream.empty();
        }
        byte[] repeated = new byte[4096];
        Arrays.fill(repeated, (byte) 'a');
        byte[] counting = new byte[1024];
        for (int i = 0; i < counting.length; i++) {
            counting[i] = (byte) (i * 31);
        }
        List<Named<byte[]>> samples = new ArrayList<>();
        for (byte[] sample : List.of(new byte[0], "Hardwood".getBytes(StandardCharsets.US_ASCII), repeated, counting)) {
            byte[] compressed = compressor.compress(sample, 0, sample.length);
            for (int selector : DecompressorFuzzTest.selectors(codec)) {
                for (int direct : new int[]{ 0, DecompressorFuzzTest.DIRECT_BIT }) {
                    samples.add(Named.of(codec + " sample of " + sample.length + " bytes, selector " + (selector | direct),
                            compressedInput(selector | direct, sample.length, compressed)));
                }
            }
        }
        return samples.stream();
    }

    private static byte[] compressedInput(int selector, int size, byte[] compressed) {
        byte[] input = new byte[DecompressorFuzzTest.HEADER_BYTES + compressed.length];
        input[0] = (byte) selector;
        input[1] = (byte) size;
        input[2] = (byte) (size >>> 8);
        input[3] = (byte) (size >>> 16);
        System.arraycopy(compressed, 0, input, DecompressorFuzzTest.HEADER_BYTES, compressed.length);
        return input;
    }

    /// A page of a file: where its header starts, where its body starts and ends, and its header.
    private record Page(int at, int bodyAt, int end, PageHeader header) {
    }

    /// The first [#PAGES_PER_CHUNK] pages of a column chunk.
    private static List<Page> pages(byte[] file, ColumnMetaData chunk) {
        List<Page> pages = new ArrayList<>();
        int at = Math.toIntExact(firstPageOffset(chunk));
        long end = at + chunk.totalCompressedSize();
        for (int page = 0; page < PAGES_PER_CHUNK && at < end; page++) {
            ThriftCompactReader reader = reader(Arrays.copyOfRange(file, at, file.length));
            PageHeader header = PageHeaderReader.read(reader);
            int bodyAt = Math.toIntExact(at + reader.getBytesRead());
            pages.add(new Page(at, bodyAt, Math.addExact(bodyAt, header.compressedPageSize()), header));
            at = pages.getLast().end();
        }
        return pages;
    }

    private static long firstPageOffset(ColumnMetaData chunk) {
        return chunk.dictionaryPageOffset() != null && chunk.dictionaryPageOffset() > 0
                ? chunk.dictionaryPageOffset()
                : chunk.dataPageOffset();
    }

    private static List<byte[]> chunkRegions(byte[] file, Function<ColumnChunk, long[]> region) {
        List<byte[]> regions = new ArrayList<>();
        for (RowGroup rowGroup : metaData(file).rowGroups()) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                long[] offsetAndLength = region.apply(chunk);
                if (offsetAndLength != null) {
                    int from = Math.toIntExact(offsetAndLength[0]);
                    regions.add(Arrays.copyOfRange(file, from, Math.addExact(from, Math.toIntExact(offsetAndLength[1]))));
                }
            }
        }
        return regions;
    }

    private static long[] region(Long offset, Integer length) {
        return offset == null || length == null ? null : new long[]{ offset, length };
    }

    static ThriftCompactReader reader(byte[] bytes) {
        return new ThriftCompactReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }
}
