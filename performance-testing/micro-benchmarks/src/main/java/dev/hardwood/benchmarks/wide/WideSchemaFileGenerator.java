/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.wide;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

/// Generates the wide-schema corpus for [WideSchemaMetadataBenchmark]: for a given column
/// count, one file of that many `REQUIRED DOUBLE` columns spread over [#ROW_GROUPS] row
/// groups, so its footer carries `columns × ROW_GROUPS` `ColumnChunk` structures. Each file
/// is skipped when already present.
///
/// The files carry only as many rows as it takes to produce the row groups
/// ([#ROWS_PER_ROW_GROUP] each): the benchmark measures footer decode, so the values exist
/// only to give every column chunk a page and non-degenerate statistics. Row groups are cut
/// deterministically by sizing the row-group target to exactly one batch of `PLAIN`
/// dictionary-free values, and the produced file is verified to hold [#ROW_GROUPS] of them.
public final class WideSchemaFileGenerator {

    /// Row groups per file, so a file has `columns × ROW_GROUPS` column chunks.
    public static final int ROW_GROUPS = 10;

    /// Rows per row group. Small on purpose — the fixture is a metadata fixture.
    public static final int ROWS_PER_ROW_GROUP = 8;

    private static final int VALUE_BYTES = Double.BYTES;

    private WideSchemaFileGenerator() {
    }

    /// The column counts swept by default, matching the reference blog post.
    public static int[] defaultColumnCounts() {
        return new int[]{ 10, 100, 1_000, 10_000, 100_000 };
    }

    public static Path file(Path dir, int columns) {
        return dir.resolve("wide_float64_" + columns + ".parquet");
    }

    /// The name of the column at `index`, zero-padded so a file's column names all have the
    /// same length and the footer size stays linear in the column count.
    public static String columnName(int index) {
        return String.format("col_%06d", index);
    }

    /// Writes the fixture for `columns` columns if it is not already present.
    public static void ensureFile(Path dir, int columns) throws IOException {
        Path path = file(dir, columns);
        if (Files.exists(path)) {
            return;
        }
        Files.createDirectories(dir);
        System.out.printf("Generating wide-schema fixture (%,d columns x %d row groups)...%n",
                columns, ROW_GROUPS);

        FileSchema schema = schema(columns);
        // One row group's worth of PLAIN values, so each batch fills the row-group budget
        // exactly and the writer cuts a row group at every batch boundary. The page target
        // is one row group per page as well, which keeps the writer's per-column pending
        // buffers small — they are sized from it, and there are up to 100,000 of them.
        long rowGroupBufferTargetBytes = (long) ROWS_PER_ROW_GROUP * VALUE_BYTES * columns;
        WriterConfig config = WriterConfig.builder()
                .pageTargetBytes(ROWS_PER_ROW_GROUP * VALUE_BYTES)
                .rowGroupBufferTargetBytes(rowGroupBufferTargetBytes)
                .encoding(ColumnEncoding.PLAIN)
                .codec(CompressionCodec.UNCOMPRESSED)
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
            ColumnWriter columnWriter = writer.columnWriter();
            for (int group = 0; group < ROW_GROUPS; group++) {
                int groupIndex = group;
                columnWriter.writeBatch(batch -> {
                    for (int c = 0; c < columns; c++) {
                        batch.doubles(c, values(groupIndex, c));
                    }
                });
            }
        }

        verify(path, columns);
    }

    /// One row group's values for one column: distinct across columns and row groups so
    /// every chunk's statistics carry a distinct min/max pair, as they would in real data.
    private static double[] values(int rowGroup, int column) {
        double[] values = new double[ROWS_PER_ROW_GROUP];
        double base = rowGroup * 1_000_000.0 + column;
        for (int i = 0; i < ROWS_PER_ROW_GROUP; i++) {
            values[i] = base + i * 0.5;
        }
        return values;
    }

    private static FileSchema schema(int columns) {
        FileSchema.Builder builder = FileSchema.builder("wide");
        for (int c = 0; c < columns; c++) {
            builder.addColumn(columnName(c), PhysicalType.DOUBLE, RepetitionType.REQUIRED);
        }
        return builder.build();
    }

    /// Fails the generation if the written file does not have the intended shape — a footer
    /// with fewer row groups than asked for would silently shrink the metadata under
    /// measurement.
    private static void verify(Path path, int columns) throws IOException {
        try (InputFile input = InputFile.of(path)) {
            input.open();
            FileMetaData metaData = ParquetMetadataReader.readMetadata(input);
            if (metaData.rowGroups().size() != ROW_GROUPS) {
                throw new IllegalStateException(path + ": expected " + ROW_GROUPS + " row groups but wrote "
                        + metaData.rowGroups().size());
            }
            System.out.printf("  %s: %,d columns, %d row groups, %,d column chunks, %,d bytes on disk%n",
                    path.getFileName(), columns, ROW_GROUPS, (long) columns * ROW_GROUPS, Files.size(path));
        }
    }

    /// Generates the fixtures for the given column counts, or the defaults when none are given.
    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : BenchmarkData.dir());
        int[] columnCounts = defaultColumnCounts();
        if (args.length > 1) {
            columnCounts = new int[args.length - 1];
            for (int i = 1; i < args.length; i++) {
                columnCounts[i - 1] = Integer.parseInt(args[i]);
            }
        }
        for (int columns : columnCounts) {
            ensureFile(dir, columns);
        }
        System.out.println("Wide-schema corpus ready in " + dir);
    }
}
