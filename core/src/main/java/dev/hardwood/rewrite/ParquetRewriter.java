/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.rewrite;

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

/// Rewrites complete Parquet row groups without decoding their pages.
///
/// This first runtime increment implements the compaction path: one or more inputs
/// with identical schemas are concatenated into one output file. Encoded column chunks
/// are copied in bounded ranges and their page offsets are rebased. Page indexes and
/// bloom filters are deliberately omitted until their writer-side serializers land;
/// no source offset is ever retained in the destination footer.
public final class ParquetRewriter {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int TRANSFER_BYTES = 8 * 1024 * 1024;

    private ParquetRewriter() {
    }

    /// Byte-copies one Parquet file to a newly assembled output file.
    public static RewriteResult rewrite(InputFile input, OutputFile output) throws IOException {
        return rewrite(List.of(input), output);
    }

    /// Concatenates complete row groups from schema-identical inputs into one output.
    ///
    /// Ownership of every input and the output is transferred to this method. Inputs
    /// are validated before the output is created. On any failure after creation, the
    /// output is discarded rather than published as a partial Parquet file.
    public static RewriteResult rewrite(List<InputFile> inputs, OutputFile output) throws IOException {
        Objects.requireNonNull(inputs, "inputs must not be null");
        Objects.requireNonNull(output, "output must not be null");
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("At least one input file is required");
        }

        List<Source> sources = new ArrayList<>(inputs.size());
        Throwable failure = null;
        boolean outputCreated = false;
        try {
            for (InputFile input : inputs) {
                Objects.requireNonNull(input, "input file must not be null");
                input.open();
                sources.add(new Source(input, ParquetMetadataReader.readMetadata(input)));
            }
            validateSchemas(sources);

            output.create();
            outputCreated = true;
            output.write(ByteBuffer.wrap(MAGIC));

            List<RowGroup> rewrittenGroups = new ArrayList<>();
            long rows = 0;
            long copiedBytes = 0;
            for (Source source : sources) {
                for (RowGroup rowGroup : source.metadata().rowGroups()) {
                    List<ColumnChunk> rewrittenChunks = new ArrayList<>(rowGroup.columns().size());
                    for (ColumnChunk chunk : rowGroup.columns()) {
                        long sourceStart = chunk.chunkStartOffset();
                        long length = chunk.metaData().totalCompressedSize();
                        long destinationStart = output.position();
                        copy(source.input(), sourceStart, length, output);
                        long delta = Math.subtractExact(destinationStart, sourceStart);
                        rewrittenChunks.add(rebase(chunk, delta));
                        copiedBytes = Math.addExact(copiedBytes, length);
                    }
                    rewrittenGroups.add(new RowGroup(List.copyOf(rewrittenChunks),
                            rowGroup.totalByteSize(), rowGroup.numRows()));
                    rows = Math.addExact(rows, rowGroup.numRows());
                }
            }

            FileMetaData first = sources.get(0).metadata();
            FileMetaData rewritten = new FileMetaData(first.version(), first.schema(), rows,
                    List.copyOf(rewrittenGroups), commonMetadata(sources),
                    "hardwood-rewriter", first.columnOrders());
            writeFooter(output, rewritten);
            output.close();
            return new RewriteResult(sources.size(), rewrittenGroups.size(), rows, copiedBytes);
        }
        catch (IOException | RuntimeException | Error e) {
            failure = e;
            if (outputCreated) {
                try {
                    output.discard();
                }
                catch (IOException discardFailure) {
                    e.addSuppressed(discardFailure);
                }
            }
            throw e;
        }
        finally {
            closeInputs(sources, failure);
        }
    }

    private static void validateSchemas(List<Source> sources) {
        FileMetaData first = sources.get(0).metadata();
        for (int i = 1; i < sources.size(); i++) {
            FileMetaData candidate = sources.get(i).metadata();
            if (!first.schema().equals(candidate.schema())) {
                throw new IllegalArgumentException("Input schema mismatch: "
                        + sources.get(i).input().name() + " differs from " + sources.get(0).input().name());
            }
            if (!first.columnOrders().equals(candidate.columnOrders())) {
                throw new IllegalArgumentException("Input column-order mismatch: "
                        + sources.get(i).input().name() + " differs from " + sources.get(0).input().name());
            }
        }
    }

    private static Map<String, String> commonMetadata(List<Source> sources) {
        Map<String, String> common = new java.util.LinkedHashMap<>(sources.get(0).metadata().keyValueMetadata());
        for (int i = 1; i < sources.size(); i++) {
            Map<String, String> candidate = sources.get(i).metadata().keyValueMetadata();
            common.entrySet().removeIf(entry -> !Objects.equals(candidate.get(entry.getKey()), entry.getValue()));
        }
        return Map.copyOf(common);
    }

    private static void copy(InputFile input, long offset, long length, OutputFile output) throws IOException {
        if (offset < 0 || length < 0 || Math.addExact(offset, length) > input.length()) {
            throw new IOException("Invalid column chunk range in " + input.name()
                    + ": offset=" + offset + ", length=" + length);
        }
        long copied = 0;
        while (copied < length) {
            int block = (int) Math.min(TRANSFER_BYTES, length - copied);
            output.write(input.readRange(Math.addExact(offset, copied), block));
            copied += block;
        }
    }

    private static ColumnChunk rebase(ColumnChunk chunk, long delta) {
        ColumnMetaData source = chunk.metaData();
        long dataPageOffset = Math.addExact(source.dataPageOffset(), delta);
        Long dictionaryPageOffset = source.dictionaryPageOffset() == null
                ? null : Math.addExact(source.dictionaryPageOffset(), delta);
        ColumnMetaData rebased = new ColumnMetaData(source.type(), source.encodings(),
                source.pathInSchema(), source.codec(), source.numValues(),
                source.totalUncompressedSize(), source.totalCompressedSize(),
                source.keyValueMetadata(), dataPageOffset, dictionaryPageOffset,
                source.statistics(), source.geospatialStatistics(), null, null);
        return new ColumnChunk(rebased, null, null, null, null);
    }

    private static void writeFooter(OutputFile output, FileMetaData metadata) throws IOException {
        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, metadata);
        byte[] bytes = footer.toByteArray();
        output.write(ByteBuffer.wrap(bytes));
        output.write(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(bytes.length).flip());
        output.write(ByteBuffer.wrap(MAGIC));
    }

    private static void closeInputs(List<Source> sources, Throwable primary) throws IOException {
        IOException closeFailure = null;
        for (Source source : sources) {
            try {
                source.input().close();
            }
            catch (IOException e) {
                if (primary != null) {
                    primary.addSuppressed(e);
                }
                else if (closeFailure == null) {
                    closeFailure = e;
                }
                else {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        if (primary == null && closeFailure != null) {
            throw closeFailure;
        }
    }

    private record Source(InputFile input, FileMetaData metadata) {
    }
}
