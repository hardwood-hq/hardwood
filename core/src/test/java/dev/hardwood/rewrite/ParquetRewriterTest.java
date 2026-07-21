/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.rewrite;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetRewriterTest {

    @Test
    void rewritesOneFileWithoutDecodingColumnChunks() throws Exception {
        byte[] source = parquet(oneColumn("id"), new int[] { 1, 2, 3, 4 });
        ColumnChunk sourceChunk = firstChunk(source);
        byte[] sourceChunkBytes = chunkBytes(source, sourceChunk);

        ByteBufferOutputFile destination = new ByteBufferOutputFile();
        RewriteResult result = ParquetRewriter.rewrite(
                InputFile.of(ByteBuffer.wrap(source)), destination);
        byte[] rewritten = destination.toByteArray();

        assertThat(result).isEqualTo(new RewriteResult(1, 1, 4, sourceChunkBytes.length));
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(rewritten)))) {
            assertThat(readInts(reader)).containsExactly(1, 2, 3, 4);
            assertThat(reader.getFileMetaData().createdBy()).isEqualTo("hardwood-rewriter");
            ColumnChunk rewrittenChunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0);
            assertThat(chunkBytes(rewritten, rewrittenChunk)).isEqualTo(sourceChunkBytes);
            assertThat(rewrittenChunk.offsetIndexOffset()).isNull();
            assertThat(rewrittenChunk.columnIndexOffset()).isNull();
            assertThat(rewrittenChunk.metaData().bloomFilterOffset()).isNull();
        }
    }

    @Test
    void mergesSchemaIdenticalFilesInInputOrder() throws Exception {
        FileSchema schema = oneColumn("id");
        byte[] first = parquet(schema, new int[] { 1, 2 });
        byte[] second = parquet(schema, new int[] { 3, 4, 5 });

        ByteBufferOutputFile destination = new ByteBufferOutputFile();
        RewriteResult result = ParquetRewriter.rewrite(List.of(
                InputFile.of(ByteBuffer.wrap(first)),
                InputFile.of(ByteBuffer.wrap(second))), destination);

        assertThat(result.inputFiles()).isEqualTo(2);
        assertThat(result.rowGroups()).isEqualTo(2);
        assertThat(result.rows()).isEqualTo(5);
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(destination.toByteArray())))) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(2);
            assertThat(readInts(reader)).containsExactly(1, 2, 3, 4, 5);
        }
    }

    @Test
    void rebasesDictionaryAndDataPageOffsetsByTheSameDelta() throws Exception {
        byte[] source = parquet(oneColumn("id"), new int[] { 10, 20, 10, 30 });
        ColumnChunk before = firstChunk(source);
        assertThat(before.metaData().dictionaryPageOffset()).isNotNull();

        ByteBufferOutputFile destination = new ByteBufferOutputFile();
        ParquetRewriter.rewrite(InputFile.of(ByteBuffer.wrap(source)), destination);
        ColumnChunk after = firstChunk(destination.toByteArray());

        long dictionaryDelta = after.metaData().dictionaryPageOffset()
                - before.metaData().dictionaryPageOffset();
        long dataDelta = after.metaData().dataPageOffset() - before.metaData().dataPageOffset();
        assertThat(dictionaryDelta).isEqualTo(dataDelta);
        assertThat(after.chunkStartOffset()).isEqualTo(4L);
    }

    @Test
    void rejectsMismatchedSchemasBeforeCreatingOutput() throws Exception {
        byte[] first = parquet(oneColumn("id"), new int[] { 1 });
        byte[] second = parquet(oneColumn("other"), new int[] { 2 });
        ByteBufferOutputFile destination = new ByteBufferOutputFile();

        assertThatThrownBy(() -> ParquetRewriter.rewrite(List.of(
                InputFile.of(ByteBuffer.wrap(first)),
                InputFile.of(ByteBuffer.wrap(second))), destination))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema mismatch");
        assertThatThrownBy(destination::toByteArray)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not closed");
    }

    private static FileSchema oneColumn(String name) {
        return FileSchema.builder("schema")
                .addColumn(name, PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    private static byte[] parquet(FileSchema schema, int[] values) throws Exception {
        ByteBufferOutputFile output = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(output, schema)) {
            writer.writeBatch(batch -> batch.ints(0, values));
        }
        return output.toByteArray();
    }

    private static ColumnChunk firstChunk(byte[] parquet) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(parquet)))) {
            return reader.getFileMetaData().rowGroups().get(0).columns().get(0);
        }
    }

    private static byte[] chunkBytes(byte[] parquet, ColumnChunk chunk) {
        int from = Math.toIntExact(chunk.chunkStartOffset());
        int to = Math.toIntExact(from + chunk.metaData().totalCompressedSize());
        return java.util.Arrays.copyOfRange(parquet, from, to);
    }

    private static int[] readInts(ParquetFileReader reader) {
        int[] result = new int[Math.toIntExact(reader.getFileMetaData().numRows())];
        int position = 0;
        try (ColumnReader column = reader.columnReader(0)) {
            while (column.nextBatch()) {
                int count = column.getValueCount();
                System.arraycopy(column.getInts(), 0, result, position, count);
                position += count;
            }
        }
        return result;
    }
}
