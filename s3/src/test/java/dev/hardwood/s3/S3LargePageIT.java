/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.Compression;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page whose stored bytes alone exceed the read's working-set limit fails an S3 read before any
/// of its bytes are requested. Fetching it first would allocate a direct buffer for each of its
/// ranges, which can exhaust direct memory before the working set is charged.
class S3LargePageIT {

    private static final String MAX_WORKING_SET_BYTES_PROPERTY = "hardwood.internal.maxWorkingSetBytes";
    private static final int VALUE_BYTES = 4 * 1024 * 1024;
    private static final String KEY = "large-page.parquet";

    static TestBucket bucket = S3Proxy.get().bucketFor(S3LargePageIT.class);

    static S3Source source;

    /// The stored bytes of the file's one data page, its header included.
    static long pageBytes;

    @BeforeAll
    static void setup() throws Exception {
        bucket.create();
        byte[] value = new byte[VALUE_BYTES];
        new Random(1).nextBytes(value);
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("blob", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .compression(Compression.uncompressed())
                .encoding(ColumnEncoding.PLAIN)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, new byte[][] { value }));
        }
        ByteBuffer bytes = out.buffer();
        try (FileChannel channel = FileChannel.open(bucket.objectPath(KEY), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(bytes.duplicate());
        }
        source = S3Source.builder()
                .endpoint(bucket.endpoint())
                .pathStyle(true)
                .credentials(S3Credentials.of(S3Proxy.ACCESS_KEY, S3Proxy.SECRET_KEY))
                .build();
        try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(bucket.name(), KEY))) {
            pageBytes = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData()
                    .totalCompressedSize();
        }
    }

    @AfterEach
    void clearLimit() {
        System.clearProperty(MAX_WORKING_SET_BYTES_PROPERTY);
    }

    @AfterAll
    static void tearDown() {
        source.close();
        bucket.delete();
    }

    @Test
    void pageStoringMoreThanTheLimitFailsBeforeItIsFetched() throws Exception {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Integer.toString(VALUE_BYTES / 4));
        S3InputFile file = source.inputFile(bucket.name(), KEY);

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    RowReader rows = reader.buildRowReader().build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[s3://" + bucket.name() + "/" + KEY + ": row group 0, column 'blob', page 0] Data page stores "
                        + pageBytes + " bytes, more than the read's working-set limit of " + VALUE_BYTES / 4
                        + " (hardwood.internal.maxWorkingSetBytes)");
        assertThat(file.networkBytesFetched()).isLessThan(pageBytes);
    }
}
