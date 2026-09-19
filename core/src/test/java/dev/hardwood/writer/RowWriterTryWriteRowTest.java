/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `tryWriteRow` returns a data-dependent rejection without failing the writer. Builder
/// misuse, a filler throw, and a flush failure still fail it, as `writeRow` does for every
/// exception.
class RowWriterTryWriteRowTest {

    private static final String FAILED = "A previous write failed; the writer accepts no more data";

    @TempDir
    Path dir;

    @Test
    void rangeRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.intType(8, true))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            assertThat(rows.tryWriteRow(row -> row.setInt("v", 1)) == RowWriteResult.Staged.INSTANCE).isTrue();
            assertThat(rows.tryWriteRow(row -> row.setInt("v", 200)))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("v");
                        assertThat(rejected.message()).isEqualTo(
                                "Field v: 200 is out of range for a INT_8 column");
                    });
            assertThat(rows.tryWriteRow(row -> row.setInt("v", 2)) == RowWriteResult.Staged.INSTANCE).isTrue();
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
            assertThat(WriterTestSupport.readInts(reader, 0)).containsExactly(1, 2);
        }
    }

    @Test
    void requiredFieldRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThat(rows.tryWriteRow(row -> { }))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("id");
                        assertThat(rejected.message()).isEqualTo(
                                "Field id is REQUIRED; it must be set to a non-null value in every record");
                    });
            rows.writeRow(row -> row.setInt("id", 2));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
            assertThat(WriterTestSupport.readInts(reader, 0)).containsExactly(1, 2);
        }
    }

    @Test
    void fixedLengthRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4)
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setBinary("v", new byte[] { 1, 2, 3, 4 }));
            assertThat(rows.tryWriteRow(row -> row.setBinary("v", new byte[] { 1, 2, 3 })))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("v");
                        assertThat(rejected.message()).isEqualTo(
                                "Field v: value is 3 bytes but the column is FIXED_LEN_BYTE_ARRAY(4)");
                    });
            rows.writeRow(row -> row.setBinary("v", new byte[] { 5, 6, 7, 8 }));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
        }
    }

    @Test
    void dateRangeRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.date())
                .build();
        LocalDate tooFar = LocalDate.ofEpochDay((long) Integer.MAX_VALUE + 1);

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setDate("v", LocalDate.of(2020, 1, 1)));
            assertThat(rows.tryWriteRow(row -> row.setDate("v", tooFar)))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("v");
                        assertThat(rejected.message()).isEqualTo(
                                "Field v: " + tooFar + " is out of range for a DATE column");
                    });
            rows.writeRow(row -> row.setDate("v", LocalDate.of(2021, 1, 1)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
        }
    }

    @Test
    void timestampOverflowRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS))
                .build();
        Instant beyondNanos = LocalDateTime.of(2263, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setTimestamp("v", Instant.EPOCH));
            assertThat(rows.tryWriteRow(row -> row.setTimestamp("v", beyondNanos)))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("v");
                        assertThat(rejected.message()).isEqualTo(
                                "Field v: 2263-01-01T00:00:00Z is outside the range an INT64 TIMESTAMP(NANOS) "
                                        + "column can represent; a FIXED_LEN_BYTE_ARRAY(12) column holds it");
                    });
            rows.writeRow(row -> row.setTimestamp("v", Instant.ofEpochSecond(1)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
        }
    }

    @Test
    void precisionLossRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setTimestamp("v", Instant.ofEpochSecond(1)));
            assertThat(rows.tryWriteRow(row -> row.setTimestamp("v", Instant.ofEpochSecond(1, 500_000))))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("v");
                        assertThat(rejected.message()).isEqualTo(
                                "Field v: value has finer precision than the column's TIMESTAMP(MILLIS) unit. "
                                        + "Truncate it at the call site (for example "
                                        + "Instant.truncatedTo(ChronoUnit.MILLIS)), declare the column at a finer unit, "
                                        + "or configure WriterConfig.precisionLossPolicy(TRUNCATE)");
                    });
            rows.writeRow(row -> row.setTimestamp("v", Instant.ofEpochSecond(2)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
        }
    }

    @Test
    void decimalPrecisionRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED, LogicalType.decimal(5, 2))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setDecimal("v", new BigDecimal("1.00")));
            assertThat(rows.tryWriteRow(row -> row.setDecimal("v", new BigDecimal("12345.67"))))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("v");
                        assertThat(rejected.message()).isEqualTo(
                                "Field v: 12345.67 has precision 7, exceeding the column's DECIMAL(5, 2)");
                    });
            rows.writeRow(row -> row.setDecimal("v", new BigDecimal("2.00")));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
        }
    }

    @Test
    void intervalRangeRejectionIsReturnedAndTheWriterStaysWritable() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 12,
                        LogicalType.interval())
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInterval("v", new PqInterval(0, 0, 0)));
            assertThat(rows.tryWriteRow(row -> row.setInterval("v", new PqInterval(-1, 0, 0))))
                    .isInstanceOfSatisfying(RowWriteResult.Rejected.class, rejected -> {
                        assertThat(rejected.fieldPath()).isEqualTo("v");
                        assertThat(rejected.message()).isEqualTo(
                                "Field v: INTERVAL months is -1, outside the unsigned 32-bit range the format "
                                        + "stores");
                    });
            rows.writeRow(row -> row.setInterval("v", new PqInterval(1, 0, 0)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
        }
    }

    @Test
    void writeRowStillFailsTheWriterOnADataRejection() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.intType(8, true))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("v", 1));
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("v", 200)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field v: 200 is out of range for a INT_8 column");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("v", 2)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void unknownFieldFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThatThrownBy(() -> rows.tryWriteRow(row -> row.setInt("nope", 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("No field named 'nope' in the record; it has id");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("id", 3)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void fieldSetTwiceFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThatThrownBy(() -> rows.tryWriteRow(row -> row.setInt("id", 2).setInt("id", 3)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field id is already set in this record");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("id", 4)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void wrongSetterFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThatThrownBy(() -> rows.tryWriteRow(row -> row.setLong("id", 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field id is INT32; setLong requires an INT64 column");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("id", 3)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void fillerThrownIllegalArgumentExceptionFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThatThrownBy(() -> rows.tryWriteRow(row -> {
                throw new IllegalArgumentException("bad age");
            }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("bad age");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("id", 3)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void flushFailureOnAStagedBatchFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");
        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn(),
                flushPerRecord(), FAILING_CODEC);
        RowWriter rows = writer.rowWriter();

        assertThatThrownBy(() -> {
            for (int i = 0; i < 4096; i++) {
                int value = i;
                rows.tryWriteRow(row -> row.setInt(0, value));
            }
        }).isInstanceOf(ParquetWriteException.class)
          .hasMessage("codec unavailable");
        assertThatThrownBy(() -> rows.writeRow(row -> row.setInt(0, -1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(FAILED);

        writer.close();
        assertNothingAt(file);
    }

    @Test
    void everyRecordRejectedPublishesAZeroRowFile() throws Exception {
        Path file = dir.resolve("out.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.intType(8, true))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            assertThat(rows.tryWriteRow(row -> row.setInt("v", 200))).isInstanceOf(RowWriteResult.Rejected.class);
            assertThat(rows.tryWriteRow(row -> row.setInt("v", 300))).isInstanceOf(RowWriteResult.Rejected.class);
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(0);
        }
    }

    private static void assertNothingAt(Path file) throws IOException {
        try (Stream<Path> entries = Files.list(file.getParent())) {
            assertThat(entries).isEmpty();
        }
    }

    private static WriterConfig flushPerRecord() {
        return WriterConfig.builder()
                .codec(CompressionCodec.GZIP)
                .rowGroupTargetRows(1)
                .build();
    }

    private static final Compressor FAILING_CODEC = new Compressor() {
        @Override
        public byte[] compress(byte[] data, int offset, int length) {
            throw new ParquetWriteException("codec unavailable");
        }

        @Override
        public String getName() {
            return "FAILING";
        }
    };
}
