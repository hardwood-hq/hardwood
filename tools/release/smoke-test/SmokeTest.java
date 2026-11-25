/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.s3.S3InputFile;
import dev.hardwood.s3.S3Source;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.Compression;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

/// The release smoke test: a user's program, run on the JARs that resolve.sh resolved for
/// the staged release. It uses the public API only, writes a Parquet file with the Snappy
/// codec (an optional dependency the resolution project declares, as a user would) and
/// reads it back, and sets up an S3 source and file, without any network access, so that
/// hardwood-s3's classes are loaded and linked against hardwood-core.
///
/// Usage: java -cp '<dir>/lib/*' tools/release/smoke-test/SmokeTest.java <RELEASE_VERSION>
public class SmokeTest {

    private static final int ROWS = 10_000;

    public static void main(String[] args) throws Exception {
        check(args.length == 1, "usage: SmokeTest <RELEASE_VERSION>");
        String version = ParquetFileReader.class.getPackage().getImplementationVersion();
        check(args[0].equals(version), "hardwood-core on the classpath is " + version + ", not " + args[0]);

        Path file = Files.createTempFile("hardwood-smoke-test", ".parquet");
        try {
            write(file);
            read(file);
        }
        finally {
            Files.delete(file);
        }
        s3();
        System.out.println("Wrote and read " + ROWS + " rows and set up an S3 file with Hardwood " + version);
    }

    /// Builds what a read from S3 starts with; `open()` would send the first request.
    private static void s3() {
        try (S3Source source = S3Source.builder().region("us-east-1").anonymous().build()) {
            S3InputFile file = source.inputFile("s3://hardwood-smoke-test/smoke.parquet");
            check(file.name().contains("smoke.parquet"), "the S3 file is named " + file.name());
        }
    }

    private static void write(Path file) throws Exception {
        FileSchema schema = FileSchema.builder("trip")
                .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .primitive("station", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        col -> col.logicalType(LogicalType.string()))
                .primitive("distance", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        long[] ids = new long[ROWS];
        byte[][] stations = new byte[ROWS][];
        double[] distances = new double[ROWS];
        for (int i = 0; i < ROWS; i++) {
            ids[i] = i;
            stations[i] = station(i).getBytes(StandardCharsets.UTF_8);
            distances[i] = i * 0.5;
        }
        WriterConfig config = WriterConfig.builder()
                .compression(Compression.snappy())
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .longs("id", ids)
                    .bytes("station", stations)
                    .doubles("distance", distances));
        }
    }

    private static void read(Path file) throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = fileReader.rowReader()) {
            check(fileReader.getFileMetaData().numRows() == ROWS,
                    "the footer counts " + fileReader.getFileMetaData().numRows() + " rows, not " + ROWS);
            int row = 0;
            while (rows.hasNext()) {
                rows.next();
                check(rows.getLong("id") == row, "row " + row + " has id " + rows.getLong("id"));
                check(rows.getString("station").equals(station(row)),
                        "row " + row + " has station " + rows.getString("station"));
                check(rows.getDouble("distance") == row * 0.5,
                        "row " + row + " has distance " + rows.getDouble("distance"));
                row++;
            }
            check(row == ROWS, "read " + row + " rows, not " + ROWS);
        }
    }

    private static String station(int row) {
        return "station-" + (row % 17);
    }

    /// Fails with an AssertionError whether or not assertions are enabled.
    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
