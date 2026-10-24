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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;

import dev.hardwood.MisstatedPageFile;
import dev.hardwood.internal.metadata.PageHeader;
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

/// The seed inputs of the fuzz targets: the small Parquet files of the core test resources and
/// files crafted to break a bound, and the footers, page headers and page indexes cut from them.
final class Seeds {

    /// Files above this size are left out, as each mutation of a seed costs in proportion to it.
    private static final long MAX_FILE_BYTES = 64 * 1024;
    /// Page headers taken from each column chunk, from its first page.
    private static final int PAGES_PER_CHUNK = 3;

    private static final Path RESOURCES = Path.of("src/test/resources");

    private static final List<Named<byte[]>> FILES = readFiles();

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
                ColumnMetaData metaData = chunk.metaData();
                long at = metaData.dictionaryPageOffset() != null && metaData.dictionaryPageOffset() > 0
                        ? metaData.dictionaryPageOffset()
                        : metaData.dataPageOffset();
                long end = at + metaData.totalCompressedSize();
                for (int page = 0; page < PAGES_PER_CHUNK && at < end; page++) {
                    ThriftCompactReader reader = reader(Arrays.copyOfRange(file, Math.toIntExact(at), file.length));
                    PageHeader header = PageHeaderReader.read(reader);
                    headers.add(Arrays.copyOfRange(file, Math.toIntExact(at), Math.toIntExact(at + reader.getBytesRead())));
                    at += reader.getBytesRead() + header.compressedPageSize();
                }
            }
        }
        return headers;
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
