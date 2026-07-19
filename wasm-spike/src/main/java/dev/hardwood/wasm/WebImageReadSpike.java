/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.StringJoiner;

import dev.hardwood.internal.reader.ByteBufferInputFile;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Entry point demonstrating a JNI-free Parquet read path.
///
/// The core method [#describe(byte[])] takes the raw bytes of a Parquet file and
/// returns a text summary, using only pure-Java code: an in-memory
/// [ByteBufferInputFile] (no filesystem, no memory mapping) and pure-Java
/// decompressors (no JNI, no FFM). It is the shape a GraalVM Web Image build would
/// expose to its JavaScript wrapper — bytes from a browser-dropped file in, summary
/// out — with no dependency on a terminal, a filesystem, or native libraries.
///
/// The [#main(String[])] wrapper reads a Parquet file from the local filesystem so
/// the same path can be exercised and verified on a normal JVM.
public final class WebImageReadSpike {

    private WebImageReadSpike() {
    }

    /// Read a Parquet file from an in-memory byte array and return a text summary
    /// (schema plus row count). Uses a single-threaded, pure-Java, JNI-free read path.
    public static String describe(byte[] parquetBytes) throws IOException {
        // FFM libdeflate is unavailable in a WebAssembly build; force the pure-Java GZIP path.
        System.setProperty("hardwood.uselibdeflate", "false");

        ByteBufferInputFile inputFile = new ByteBufferInputFile(ByteBuffer.wrap(parquetBytes));

        // Single decoding thread: a WebAssembly runtime has no thread pool to draw on.
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1, PureJavaDecompressors.overrides());
                ParquetFileReader reader = ParquetFileReader.open(inputFile, context)) {

            String schemaSummary = describeSchema(reader.getFileSchema());

            long rows = 0;
            try (RowReader rowReader = reader.rowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rows++;
                }
            }

            return schemaSummary + "\nrows: " + rows;
        }
    }

    /// Read only the file schema (footer/metadata), without scanning rows. This avoids the
    /// reader's concurrent row-decoding machinery, which the single-threaded WebAssembly
    /// runtime cannot run.
    public static String describeSchemaOnly(byte[] parquetBytes) throws IOException {
        System.setProperty("hardwood.uselibdeflate", "false");
        ByteBufferInputFile inputFile = new ByteBufferInputFile(ByteBuffer.wrap(parquetBytes));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1, PureJavaDecompressors.overrides());
                ParquetFileReader reader = ParquetFileReader.open(inputFile, context)) {
            return describeSchema(reader.getFileSchema());
        }
    }

    private static String describeSchema(FileSchema schema) {
        StringJoiner columns = new StringJoiner(", ");
        for (ColumnSchema column : schema.getColumns()) {
            columns.add(column.name());
        }
        return "schema: " + schema.getName()
                + "\ncolumns (" + schema.getColumnCount() + "): " + columns;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 1) {
            // JVM / Node path: read the file from the local filesystem.
            System.out.println(describe(Files.readAllBytes(Path.of(args[0]))));
            return;
        }
        System.err.println("usage: WebImageReadSpike <file.parquet>");
    }
}
