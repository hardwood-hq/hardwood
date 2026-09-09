/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.writer.WriterTestSupport.columnMeta;
import static dev.hardwood.writer.WriterTestSupport.expectedNullable;
import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static dev.hardwood.writer.WriterTestSupport.oneOptionalColumn;
import static dev.hardwood.writer.WriterTestSupport.readInts;
import static dev.hardwood.writer.WriterTestSupport.readNullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Page compression: that a codec is applied, that both sizes are accounted for, and that a
/// codec the writer refuses fails before any bytes are written.
///
/// Whether the compressed bytes are a form other implementations accept is not decidable
/// from here — `WriterDifferentialTest` reads them back through DuckDB and the interop gate
/// through parquet-java.
class WriterCompressionTest {

    @Test
    void compressesPagesWithZstdByDefault() throws Exception {
        // The default codec is ZSTD, so a file written with no override records ZSTD and reads
        // back through the reader's ZSTD path.
        int[] values = new int[1_000];
        int[] palette = { 3, 3, 7, 3, 9 };
        for (int i = 0; i < values.length; i++) {
            values[i] = palette[i % palette.length];
        }

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(columnMeta(reader, 0).codec()).isEqualTo(CompressionCodec.ZSTD);
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void zstdShrinksACompressiblePageAndAccountsForBothSizes() throws Exception {
        // A large single-valued PLAIN column (dictionary disabled) has a highly compressible
        // body, so ZSTD stores far fewer bytes than it holds — proving the compress step ran
        // and that the compressed and uncompressed sizes are tracked independently, at both the
        // chunk-metadata and the page-header level.
        int n = 10_000;
        int[] values = new int[n];
        Arrays.fill(values, 42);

        WriterConfig config = WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }
        byte[] bytes = InMemoryFiles.toByteArray(out);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.codec()).isEqualTo(CompressionCodec.ZSTD);
            assertThat(meta.totalCompressedSize()).isLessThan(meta.totalUncompressedSize());

            int offset = Math.toIntExact(meta.dataPageOffset());
            ThriftCompactReader thrift = new ThriftCompactReader(ByteBuffer.wrap(bytes), offset);
            PageHeader header = PageHeaderReader.read(thrift);
            assertThat(header.compressedPageSize()).isLessThan(header.uncompressedPageSize());
            // The CRC covers the stored (compressed) bytes.
            int bodyStart = offset + thrift.getBytesRead();
            CRC32 crc = new CRC32();
            crc.update(bytes, bodyStart, header.compressedPageSize());
            assertThat(header.crc().intValue()).isEqualTo((int) crc.getValue());

            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void uncompressedCodecStoresBodiesVerbatim() throws Exception {
        // With the UNCOMPRESSED codec the stored bytes are the body bytes, so the chunk's
        // compressed and uncompressed sizes are equal.
        int[] values = { 1, 2, 3, 4, 5 };

        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.codec()).isEqualTo(CompressionCodec.UNCOMPRESSED);
            assertThat(meta.totalCompressedSize()).isEqualTo(meta.totalUncompressedSize());
            assertThat(readInts(reader, 0)).containsExactly(values);
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = CompressionCodec.class, names = { "LZ4", "LZO" })
    void refusedCodecFailsBeforeAnyBytesAreWritten(CompressionCodec codec, @TempDir Path dir) {
        // The codec resolves ahead of out.create(), so a codec this writer does not produce
        // leaves no file at the destination rather than an empty or headerless one.
        Path file = dir.resolve("refused.parquet");
        WriterConfig config = WriterConfig.builder().codec(codec).build();

        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.of(file), oneColumn(), config))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThat(Files.exists(file)).isFalse();
    }

    @Test
    void compressionComposesWithNullsAcrossPages() throws Exception {
        // ZSTD compression under a tiny page target: many compressed pages, each carrying a
        // def-level stream and PLAIN values, must all decompress and reassemble the nulls.
        int n = 4_000;
        int[] values = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
            nulls[i] = i % 4 == 0;
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(256).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(columnMeta(reader, 0).codec()).isEqualTo(CompressionCodec.ZSTD);
            assertThat(readNullable(reader, 0)).isEqualTo(expectedNullable(values, nulls));
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = CompressionCodec.class,
            names = { "UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4_RAW", "BROTLI" })
    void everyWritableCodecRoundTripsThroughTheReader(CompressionCodec codec) throws Exception {
        // Nulls and a small page target put several compressed pages in the chunk, each carrying
        // a def-level stream ahead of its values, so what round-trips is a page body of the shape
        // the writer actually produces rather than a lone value section.
        int n = 4_000;
        int[] values = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            values[i] = i % 250;
            nulls[i] = i % 7 == 0;
        }

        WriterConfig config = WriterConfig.builder().codec(codec).pageTargetBytes(1024).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.codec()).as("declared codec").isEqualTo(codec);
            if (codec == CompressionCodec.UNCOMPRESSED) {
                assertThat(meta.totalCompressedSize()).as("%s stored size", codec)
                        .isEqualTo(meta.totalUncompressedSize());
            }
            else {
                assertThat(meta.totalCompressedSize()).as("%s stored size", codec)
                        .isLessThan(meta.totalUncompressedSize());
            }
            assertThat(readNullable(reader, 0)).as("%s values", codec)
                    .isEqualTo(expectedNullable(values, nulls));
        }
    }
}
