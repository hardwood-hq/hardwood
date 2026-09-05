/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `type_length` is optional in the format, so a footer can declare a
/// `FIXED_LEN_BYTE_ARRAY` column without saying how wide its values are — and nothing on
/// the write path can produce one, so these files are built by rewriting the footer of a
/// well-formed file.
///
/// A read that touches such a column fails as the read is planned, naming the column and
/// the file it came from. A read that does not touch it, and the metadata accessors, are
/// unaffected: an unreadable column is not an unreadable file.
class FixedWidthSchemaValidationTest {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    @TempDir
    Path tempDir;

    @Test
    void aTouchedColumnWithoutAWidthNamesTheColumnAndTheFile() throws Exception {
        Path file = fileWithDigestWidth(null, "no-width.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThatThrownBy(() -> reader.buildRowReader().projection(ColumnProjection.columns("digest")).build())
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[no-width.parquet] Column 'digest' is a FIXED_LEN_BYTE_ARRAY "
                            + "that declares no type length");
        }
    }

    @Test
    void aWidthOfZeroIsRejectedRatherThanDecodingToEmptyValues() throws Exception {
        Path file = fileWithDigestWidth(0, "zero-width.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThatThrownBy(() -> reader.buildRowReader().projection(ColumnProjection.columns("digest")).build())
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[zero-width.parquet] Column 'digest' declares a FIXED_LEN_BYTE_ARRAY "
                            + "type length of 0, which must be positive");
        }
    }

    @Test
    void aNegativeWidthIsRejectedRatherThanSizingAnArrayFromIt() throws Exception {
        Path file = fileWithDigestWidth(-4, "negative-width.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThatThrownBy(() -> reader.columnReaders(ColumnProjection.columns("digest")))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[negative-width.parquet] Column 'digest' declares a FIXED_LEN_BYTE_ARRAY "
                            + "type length of -4, which must be positive");
        }
    }

    @Test
    void aFilterOnAWidthlessDecimalColumnFailsAsThePredicateResolves() throws Exception {
        Path file = fileWithPriceWidth(null, "no-decimal-width.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThatThrownBy(() -> reader.buildRowReader()
                    .projection(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.gt("price", new BigDecimal("1.00")))
                    .build())
                            .isInstanceOf(SchemaIncompatibleException.class)
                            .hasMessage("[no-decimal-width.parquet] Column 'price' is a FIXED_LEN_BYTE_ARRAY "
                                    + "that declares no type length");
        }
    }

    @Test
    void aColumnTheReadDoesNotTouchLeavesTheReadAlone() throws Exception {
        Path file = fileWithDigestWidth(null, "untouched.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.buildRowReader()
                        .projection(ColumnProjection.columns("id")).build()) {
            List<Integer> ids = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getInt("id"));
            }
            assertThat(ids).containsExactly(1, 2, 3);
        }
    }

    @Test
    void theSchemaStaysReportableAsTheFooterStatesIt() throws Exception {
        Path file = fileWithDigestWidth(null, "reportable.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileSchema schema = reader.getFileSchema();
            assertThat(schema.getColumn("digest").type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
            assertThat(schema.getColumn("digest").typeLength()).isNull();
        }
    }

    @Test
    void aWellFormedWidthStillReads() throws Exception {
        Path file = writeToTempFile(validFile(), "well-formed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("digest")).build()) {
            assertThatCode(() -> {
                while (rows.hasNext()) {
                    rows.next();
                    assertThat(rows.getBinary("digest")).hasSize(4);
                }
            }).doesNotThrowAnyException();
        }
    }

    // ==================== Fixtures ====================

    /// A well-formed three-column file: `id` `INT32`, `digest` `FIXED_LEN_BYTE_ARRAY(4)`,
    /// and `price` a `FIXED_LEN_BYTE_ARRAY(8)` `DECIMAL` for the predicate path.
    private static byte[] validFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("digest", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4)
                .addColumn("price", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 8,
                        new LogicalType.DecimalType(2, 18))
                .build();

        byte[][] digests = { bytes("aaaa"), bytes("bbbb"), bytes("cccc") };
        byte[][] prices = { unscaled(100), unscaled(250), unscaled(375) };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints(0, new int[] { 1, 2, 3 })
                    .fixed(1, digests)
                    .fixed(2, prices));
        }
        return out.toByteArray();
    }

    private Path fileWithDigestWidth(Integer width, String fileName) throws IOException {
        return writeToTempFile(withTypeLength(validFile(), "digest", width), fileName);
    }

    private Path fileWithPriceWidth(Integer width, String fileName) throws IOException {
        return writeToTempFile(withTypeLength(validFile(), "price", width), fileName);
    }

    private Path writeToTempFile(byte[] content, String fileName) throws IOException {
        Path file = tempDir.resolve(fileName);
        Files.write(file, content);
        return file;
    }

    /// Rewrites `file`'s footer with `columnName`'s `type_length` set to `width`, dropping
    /// the field entirely when `width` is `null`. The data pages are untouched, so what
    /// changes is only what the file claims about the column.
    private static byte[] withTypeLength(byte[] file, String columnName, Integer width) throws IOException {
        FileMetaData metaData = ParquetMetadataReader.readMetadata(InputFile.of(ByteBuffer.wrap(file)));

        List<SchemaElement> patched = new ArrayList<>(metaData.schema().size());
        for (SchemaElement element : metaData.schema()) {
            patched.add(element.name().equals(columnName)
                    ? new SchemaElement(element.name(), element.type(), width, element.repetitionType(),
                            element.numChildren(), element.convertedType(), element.scale(),
                            element.precision(), element.fieldId(), element.logicalType())
                    : element);
        }

        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, new FileMetaData(metaData.version(), patched, metaData.numRows(),
                metaData.rowGroups(), metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders()));
        byte[] footerBytes = footer.toByteArray();

        int dataLength = file.length - MAGIC.length - Integer.BYTES - footerLength(file);
        ByteBuffer rewritten = ByteBuffer
                .allocate(dataLength + footerBytes.length + Integer.BYTES + MAGIC.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        rewritten.put(file, 0, dataLength);
        rewritten.put(footerBytes);
        rewritten.putInt(footerBytes.length);
        rewritten.put(MAGIC);
        return rewritten.array();
    }

    private static int footerLength(byte[] file) {
        return ByteBuffer.wrap(file, file.length - MAGIC.length - Integer.BYTES, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /// The 8-byte big-endian two's complement of `value`, the `DECIMAL` encoding the
    /// `price` column declares.
    private static byte[] unscaled(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }
}
