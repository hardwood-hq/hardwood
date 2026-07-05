/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/// Shared parquet-java writer plumbing for the benchmark corpus generators. Every
/// fixture is written the same way — DataPageV2 (or V1 via `-Dperf.pageVersion=v1`),
/// uncompressed, dictionary-free, 3-level compliant lists — so a scan measures level
/// handling and value copy rather than codec or dictionary cost.
public final class BenchmarkWriter {

    /// Mean list length for the variable-length fixtures.
    public static final int MEAN_LIST_LEN = 16;

    private BenchmarkWriter() {
    }

    /// An [AvroParquetWriter] over `path` for `schema`, configured for the benchmark
    /// corpus. Overwrites any existing file.
    public static ParquetWriter<GenericRecord> create(Path path, Schema schema) throws IOException {
        Configuration conf = new Configuration();
        // 3-level compliant lists (`group list { element }`) so the leaf path is the
        // standard `field.list.element` the readers resolve.
        conf.setBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, false);
        org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString());
        return AvroParquetWriter.<GenericRecord>builder(hPath)
                .withSchema(schema)
                .withConf(conf)
                .withWriterVersion(writerVersion())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withRowGroupSize(512L * 1024 * 1024)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build();
    }

    /// A list length drawn around [#MEAN_LIST_LEN], never zero (empty lists are
    /// introduced only through a null-list path where a fixture wants them).
    public static int randomLen(Random rng) {
        return 1 + rng.nextInt(2 * MEAN_LIST_LEN);
    }

    /// A union `[null, type]` — an optional field. Null comes first so a `null` field
    /// default validates against the union.
    public static Schema optional(Schema type) {
        return Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), type));
    }

    public static boolean present(Path path) throws IOException {
        return Files.exists(path) && Files.size(path) > 0;
    }

    private static WriterVersion writerVersion() {
        String version = System.getProperty("perf.pageVersion", "v2");
        return "v1".equalsIgnoreCase(version) ? WriterVersion.PARQUET_1_0 : WriterVersion.PARQUET_2_0;
    }
}
