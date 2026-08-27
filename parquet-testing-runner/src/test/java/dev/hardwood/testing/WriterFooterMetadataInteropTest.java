/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// The interop gate over the footer's `key_value_metadata`: what Hardwood stamps on a file is
/// what an independent implementation reads back out of it.
///
/// The field is the one Hardwood's own round trip cannot vouch for on its own. It carries
/// `ARROW:schema`, the pandas descriptor and the table-format stamps, which means the consumer
/// that has to find them is never Hardwood — a reader that agrees with the writer about a
/// malformed `list<KeyValue>` would hide the break from both halves of a Hardwood round trip.
class WriterFooterMetadataInteropTest {

    private static final String COLUMN = "v";

    /// `column_orders` gives `min_value` / `max_value` their comparison semantics. parquet-java
    /// defaults a missing list to type-defined order when it builds the high-level schema, so this
    /// asserts both views: the raw footer field is present with one entry per leaf, and the schema
    /// parquet-java exposes marks every leaf as `TYPE_DEFINED_ORDER`.
    @Test
    void parquetJavaReadsColumnOrdersForEveryLeafColumn(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("column-orders.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), columnOrderSchema())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .longs("id", new long[]{ 1L, 2L, 3L })
                    .bytes("person.name", new byte[][] { utf8("ada"), utf8("alan"), utf8("grace") })
                    .doubles("person.score", new double[]{ 1.5, 2.5, 3.5 })
                    .booleans("active", new boolean[]{ true, false, true }));
        }

        ParquetMetadata footer = ParquetJavaReader.readFooter(file);
        List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
        assertThat(columns).extracting(column -> String.join(".", column.getPath()))
                .containsExactly("id", "person.name", "person.score", "active");

        List<ColumnOrder> columnOrders = ParquetJavaReader.readFormatFooter(file).getColumn_orders();
        assertThat(columnOrders).as("raw footer column_orders")
                .isNotNull()
                .hasSameSizeAs(columns)
                .allSatisfy(order -> assertThat(order.isSetTYPE_ORDER()).isTrue());
        assertThat(columns).allSatisfy(column -> assertThat(column.getPrimitiveType()
                .columnOrder()
                .getColumnOrderName()).isEqualTo(ColumnOrderName.TYPE_DEFINED_ORDER));
    }

    /// A STRING column orders by unsigned bytes. These two valid UTF-8 values straddle the signed
    /// byte boundary, so a deprecated signed `BYTE_ARRAY` comparison would reverse the bounds.
    @Test
    void parquetJavaReadsStringBoundsUsingUnsignedByteOrder(@TempDir Path dir) throws IOException {
        byte[] ascii = utf8("a");
        byte[] highBit = utf8("\u0080");
        Path file = dir.resolve("string-order.parquet");
        FileSchema schema = FileSchema.builder("string-order")
                .addColumn(COLUMN, PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        new LogicalType.StringType())
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(COLUMN, new byte[][] { highBit, ascii }));
        }

        Statistics<?> statistics = ParquetJavaReader.readFooter(file).getBlocks().get(0)
                .getColumns().get(0).getStatistics();
        assertThat(statistics.genericGetMin()).as("min")
                .isEqualTo(Binary.fromConstantByteArray(ascii));
        assertThat(statistics.genericGetMax()).as("max")
                .isEqualTo(Binary.fromConstantByteArray(highBit));
    }

    @Test
    void parquetJavaReadsTheMetadataHardwoodWrote(@TempDir Path dir) throws IOException {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("ARROW:schema", "AAAAgAAAABAAAAAAAAoADgAGAAUACAAKAAAAAAEEABAAAAAA");
        metadata.put("org.apache.spark.sql.parquet.row.metadata", "{\"type\":\"struct\",\"fields\":[]}");
        metadata.put("unicode.value", "grüße — 🌲");

        Path file = write(dir, metadata);

        ParquetMetadata footer = ParquetJavaReader.readFooter(file);
        assertThat(footer.getFileMetaData().getKeyValueMetaData())
                .containsAllEntriesOf(metadata);
    }

    /// `KeyValue.value` is optional, and a key carrying no value is what a reader reports as a
    /// null value. parquet-java must see the same thing, or a file round-tripped through
    /// Hardwood would gain an empty string where the original had nothing.
    @Test
    void parquetJavaSeesAKeyWithNoValue(@TempDir Path dir) throws IOException {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("valueless", null);
        metadata.put("empty", "");

        Path file = write(dir, metadata);

        Map<String, String> readBack = ParquetJavaReader.readFooter(file)
                .getFileMetaData().getKeyValueMetaData();
        assertThat(readBack).containsEntry("valueless", null);
        assertThat(readBack).containsEntry("empty", "");
    }

    /// A file given no metadata carries none, rather than an empty `list<KeyValue>` that a
    /// stricter consumer could balk at.
    @Test
    void aFileGivenNoMetadataCarriesNone(@TempDir Path dir) throws IOException {
        Path file = write(dir, Map.of());

        assertThat(ParquetJavaReader.readFooter(file).getFileMetaData().getKeyValueMetaData())
                .isEmpty();
    }

    /// The identifier a caller sets must stay parseable by parquet-java's `VersionParser`, on
    /// which its writer-specific correctness workarounds turn.
    @Test
    void parquetJavaParsesACallerSuppliedCreatedBy(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("created-by.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema())) {
            writer.createdBy("myapp version 2.1.0 (build deadbeef)");
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[]{ 1, 2, 3 }));
        }

        assertThat(ParquetJavaReader.readFooter(file).getFileMetaData().getCreatedBy())
                .isEqualTo("myapp version 2.1.0 (build deadbeef)");
    }

    private static Path write(Path dir, Map<String, String> metadata) throws IOException {
        Path file = dir.resolve("footer-metadata.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema())) {
            writer.keyValueMetadata(metadata);
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[]{ 1, 2, 3 }));
        }
        return file;
    }

    private static FileSchema schema() {
        return FileSchema.builder("footer-metadata")
                .addColumn(COLUMN, PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    private static FileSchema columnOrderSchema() {
        return FileSchema.builder("column-orders")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .struct("person", RepetitionType.REQUIRED, person -> person
                        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                new LogicalType.StringType())
                        .addColumn("score", PhysicalType.DOUBLE, RepetitionType.REQUIRED))
                .addColumn("active", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
