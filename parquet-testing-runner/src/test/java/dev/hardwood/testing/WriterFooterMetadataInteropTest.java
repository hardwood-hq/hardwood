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
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// The interop gate over the footer fields a value comparison cannot see: `key_value_metadata`,
/// `created_by` and `column_orders`. What Hardwood stamps on a file is what an independent
/// implementation reads back out of it.
///
/// These are the fields Hardwood's own round trip cannot vouch for. `key_value_metadata` carries
/// `ARROW:schema`, the pandas descriptor and the table-format stamps, which means the consumer
/// that has to find them is never Hardwood — a reader that agrees with the writer about a
/// malformed `list<KeyValue>` would hide the break from both halves of a Hardwood round trip.
/// `column_orders` is the same blind spot in a different place: it declares the order the bounds
/// were computed in, which a reader sharing the writer's conventions never has to be told.
class WriterFooterMetadataInteropTest {

    private static final String COLUMN = "v";

    /// `column_orders` gives `min_value` / `max_value` their comparison semantics: the format
    /// leaves their meaning undefined without it, so a file that lost the list carries bounds a
    /// reader holding the format to its word has to discard.
    ///
    /// The assertion is over the **raw** footer because the decoded view cannot fail.
    /// `PrimitiveType` defaults a missing column order to type-defined for every type but `INT96`
    /// and `INTERVAL`, so the schema parquet-java exposes reports `TYPE_DEFINED_ORDER` whether the
    /// field reached the wire or not. Only `getColumn_orders()` tells the two files apart, by
    /// coming back null.
    @Test
    void parquetJavaReadsColumnOrdersForEveryLeafColumn(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("column-orders.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), columnOrderSchema())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .longs("id", new long[]{ 1L, 2L, 3L })
                    .bytes("person.name", new byte[][]{ utf8("ada"), utf8("alan"), utf8("grace") })
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
                                LogicalType.string())
                        .addColumn("score", PhysicalType.DOUBLE, RepetitionType.REQUIRED))
                .addColumn("active", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
