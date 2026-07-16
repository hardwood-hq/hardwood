/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;

/// Assembles same-schema Parquet files by copying their encoded bodies and rebuilding the footer.
public final class ParquetFileStitcher {
    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int MAGIC_SIZE = 4;
    private static final int FOOTER_TRAILER_SIZE = 8;
    private static final int COPY_BUFFER_SIZE = 8 * 1024 * 1024;

    private ParquetFileStitcher() {}

    public static void stitch(List<InputFile> inputs, OutputFile output) throws IOException {
        Objects.requireNonNull(inputs, "inputs must not be null");
        Objects.requireNonNull(output, "output must not be null");
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("At least one input file is required");
        }

        List<Source> sources = new ArrayList<>(inputs.size());
        try {
            for (InputFile input : inputs) {
                input.open();
                FileMetaData metadata = ParquetMetadataReader.readMetadata(input);
                sources.add(new Source(input, metadata, footerStart(input)));
            }
            validateCompatible(sources);
            write(sources, output);
        }
        finally {
            IOException failure = null;
            for (InputFile input : inputs) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    }
                    else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static void write(List<Source> sources, OutputFile output) throws IOException {
        output.create();
        try {
            output.write(ByteBuffer.wrap(MAGIC));
            List<RowGroup> rowGroups = new ArrayList<>();
            long numRows = 0;
            int ordinal = 0;
            for (Source source : sources) {
                long delta = Math.subtractExact(output.position(), MAGIC_SIZE);
                copyBody(source, output);
                for (RowGroup rowGroup : source.metadata().rowGroups()) {
                    rowGroups.add(relocate(rowGroup, delta, ordinal++));
                    numRows = Math.addExact(numRows, rowGroup.numRows());
                }
            }

            FileMetaData first = sources.get(0).metadata();
            FileMetaData merged = new FileMetaData(first.version(), first.schema(), numRows,
                    List.copyOf(rowGroups), first.keyValueMetadata(), "hardwood merge",
                    first.columnOrders());
            ThriftCompactWriter footer = new ThriftCompactWriter();
            FileMetaDataWriter.write(footer, merged);
            byte[] bytes = footer.toByteArray();
            output.write(ByteBuffer.wrap(bytes));
            output.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(bytes.length).flip());
            output.write(ByteBuffer.wrap(MAGIC));
            output.close();
        }
        catch (IOException | RuntimeException e) {
            try {
                output.discard();
            }
            catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private static void copyBody(Source source, OutputFile output) throws IOException {
        long offset = MAGIC_SIZE;
        while (offset < source.footerStart()) {
            int length = (int) Math.min(COPY_BUFFER_SIZE, source.footerStart() - offset);
            output.write(source.input().readRange(offset, length));
            offset += length;
        }
    }

    private static RowGroup relocate(RowGroup rowGroup, long delta, int ordinal) {
        List<ColumnChunk> chunks = new ArrayList<>(rowGroup.columns().size());
        for (ColumnChunk chunk : rowGroup.columns()) {
            chunks.add(relocate(chunk, delta));
        }
        Long fileOffset = add(rowGroup.fileOffset(), delta);
        Short outputOrdinal = ordinal <= Short.MAX_VALUE ? (short) ordinal : null;
        return new RowGroup(List.copyOf(chunks), rowGroup.totalByteSize(), rowGroup.numRows(),
                fileOffset, rowGroup.totalCompressedSize(), outputOrdinal);
    }

    private static ColumnChunk relocate(ColumnChunk chunk, long delta) {
        ColumnMetaData m = chunk.metaData();
        ColumnMetaData relocated = new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(),
                m.codec(), m.numValues(), m.totalUncompressedSize(), m.totalCompressedSize(),
                m.keyValueMetadata(), Math.addExact(m.dataPageOffset(), delta),
                add(m.indexPageOffset(), delta), add(m.dictionaryPageOffset(), delta),
                m.statistics(), m.geospatialStatistics(), add(m.bloomFilterOffset(), delta),
                m.bloomFilterLength());
        return new ColumnChunk(relocated, add(chunk.offsetIndexOffset(), delta),
                chunk.offsetIndexLength(), add(chunk.columnIndexOffset(), delta),
                chunk.columnIndexLength());
    }

    private static Long add(Long value, long delta) {
        return value == null ? null : Math.addExact(value, delta);
    }

    private static void validateCompatible(List<Source> sources) {
        FileMetaData first = sources.get(0).metadata();
        for (int i = 1; i < sources.size(); i++) {
            FileMetaData candidate = sources.get(i).metadata();
            if (!first.schema().equals(candidate.schema())) {
                throw new IllegalArgumentException("Schema mismatch: " + sources.get(i).input().name());
            }
            if (!first.columnOrders().equals(candidate.columnOrders())) {
                throw new IllegalArgumentException("Column-order mismatch: " + sources.get(i).input().name());
            }
            if (!metadataEquals(first.keyValueMetadata(), candidate.keyValueMetadata())) {
                throw new IllegalArgumentException("File metadata mismatch: " + sources.get(i).input().name());
            }
        }
    }

    private static boolean metadataEquals(Map<String, String> first, Map<String, String> second) {
        return first.equals(second);
    }

    private static long footerStart(InputFile input) throws IOException {
        long length = input.length();
        ByteBuffer trailer = input.readRange(length - FOOTER_TRAILER_SIZE, FOOTER_TRAILER_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = trailer.getInt();
        return Math.subtractExact(length - FOOTER_TRAILER_SIZE, footerLength);
    }

    private record Source(InputFile input, FileMetaData metadata, long footerStart) {}
}
