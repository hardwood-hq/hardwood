package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.StructBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VariantWriteTest {

    private static final byte[] EMPTY_METADATA = {0x01, 0x00, 0x00};

    private static final Path FILE_OFFSET_SIZE2 =
            Paths.get("src/test/resources/variant_metadata_offset_size2.parquet");
    private static final byte[] TRUE_VALUE = {0x04};

    @Test
    void setVariantOnAPlainStructFails() throws IOException {
        assertWriteFails(row -> row.setInt("id", 0).setVariant("meta", null),
                "Field meta is a struct group; setVariant applies to a VARIANT group");
    }

    @Test
    void setVariantNullWritesANullVariant() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .variant("payload", RepetitionType.OPTIONAL)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try(ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setVariant("payload", null));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getVariant("payload")).isNull();
        }
    }

    @Test
    void setVariantWritesTheStoredBytesOfAVariant() throws IOException {
        PqVariant variant = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);

        FileSchema schema = FileSchema.builder("schema")
                .variant("payload", RepetitionType.OPTIONAL)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setVariant("payload", variant));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant back = rows.getVariant("payload");
            assertThat(back.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(back.value()).isEqualTo(TRUE_VALUE);
        }
    }

    @Test
    void setVariantByIndexWritesTheStoredBytesOfAVariant() throws IOException {
        PqVariant variant = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);

        FileSchema schema = FileSchema.builder("schema")
                .variant("payload", RepetitionType.OPTIONAL).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setVariant(0, variant));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant back = rows.getVariant("payload");
            assertThat(back.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(back.value()).isEqualTo(TRUE_VALUE);
        }
    }

    @Test
    void setVariantInsideAStructWritesTheStoredBytesOfAVariant() throws IOException {
        PqVariant variant = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);

        FileSchema schema = FileSchema.builder("schema")
                .primitive("id", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .variant("nested", RepetitionType.OPTIONAL))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setInt("id", 1)
                    .setStruct("s", s -> s.setVariant("nested", variant)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant back = rows.getStruct("s").getVariant("nested");
            assertThat(back.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(back.value()).isEqualTo(TRUE_VALUE);
        }
    }

    @Test
    void setStructOnAVariantGroupFails() throws IOException {
        assertWriteFails(row -> row.setInt("id", 0).setStruct("payload", payload -> { }),
                "Field payload is a VARIANT group; setStruct applies to a struct group");
    }

    @Test
    void addStructOnAVariantListElementFails() throws IOException {
        assertWriteFails(row -> row.setInt("id", 0)
                        .setList("attachments", list -> list.addStruct(s -> { })),
                "The element of list attachments is a VARIANT group; addStruct applies to a struct element");
    }

    @Test
    void addVariantWritesTheStoredBytesOfAListElement() throws IOException {
        PqVariant variant = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);

        FileSchema schema = FileSchema.builder("schema")
                .list("attachments", RepetitionType.OPTIONAL, element -> element.variant(RepetitionType.OPTIONAL))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setList("attachments", list -> list.addVariant(variant)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            List<PqVariant> back = rows.getList("attachments").variants();
            assertThat(back).hasSize(1);
            assertThat(back.get(0).metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(back.get(0).value()).isEqualTo(TRUE_VALUE);
        }
    }

    @Test
    void setStructOnAVariantMapValueFails() throws IOException {
        assertWriteFails(row -> row.setInt("id", 0)
                        .setMap("renditions", map -> map
                                .addEntry(entry -> entry.setString("key", "a").setStruct("value", v -> { }))),
                "Field renditions.key_value.value is a VARIANT group; setStruct applies to a struct group");
    }

    @Test
    void setVariantInAMapValueWritesTheStoredBytesOfAVariant() throws IOException {
        PqVariant variant = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);

        FileSchema schema = FileSchema.builder("schema")
                .map("renditions", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        key -> key.logicalType(LogicalType.string()),
                        value -> value.variant(RepetitionType.OPTIONAL))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setMap("renditions", map -> map
                    .addEntry(e -> e.setString("key", "a").setVariant("value", variant))));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant back = rows.getMap("renditions").getEntries().get(0).getVariantValue(); // guess
            assertThat(back.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(back.value()).isEqualTo(TRUE_VALUE);
        }
    }

    @Test
    void setValueAcceptsAVariant() throws IOException {
        PqVariant variant = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);

        FileSchema schema = FileSchema
                .builder("schema")
                .variant("payload", RepetitionType.OPTIONAL)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setValue("payload", variant));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant back = rows.getVariant("payload");
            assertThat(back.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(back.value()).isEqualTo(TRUE_VALUE);
        }
    }

    @Test
    void setValueByIndexAcceptsAVariant() throws IOException {
        PqVariant variant = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);

        FileSchema schema = FileSchema
                .builder("schema")
                .variant("payload", RepetitionType.OPTIONAL)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setValue(0, variant));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant back = rows.getVariant("payload");
            assertThat(back.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(back.value()).isEqualTo(TRUE_VALUE);
        }
    }

    @Test
    void setVariantWritesRowsOfDifferentShapesInOneColumn() throws IOException {
        PqVariant yes = new PqVariantImpl(EMPTY_METADATA, TRUE_VALUE);
        PqVariant answer = new PqVariantImpl(EMPTY_METADATA,
                new byte[]{0x14, 0x2A, 0x00, 0x00, 0x00});   // INT32 42
        PqVariant greeting = new PqVariantImpl(EMPTY_METADATA,
                new byte[]{0x09, 'h', 'i'});                  // short string "hi"

        PqVariant object;
        try (ParquetFileReader source = ParquetFileReader.open(InputFile.of(FILE_OFFSET_SIZE2));
             RowReader sourceRows = source.rowReader()) {
            sourceRows.next();
            object = sourceRows.getVariant("var");
        }
        byte[] objectMetadata = object.metadata();
        byte[] objectValue = object.value();

        FileSchema schema = FileSchema
                .builder("schema")
                .variant("payload", RepetitionType.OPTIONAL).build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setVariant("payload", yes));
            writer.rowWriter().writeRow(row -> row.setVariant("payload", answer));
            writer.rowWriter().writeRow(row -> row.setVariant("payload", greeting));
            writer.rowWriter().writeRow(row -> row.setVariant("payload", object));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant first = rows.getVariant("payload");
            assertThat(first.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(first.value()).isEqualTo(TRUE_VALUE);

            rows.next();
            PqVariant second = rows.getVariant("payload");
            assertThat(second.asInt()).isEqualTo(42);

            rows.next();
            PqVariant third = rows.getVariant("payload");
            assertThat(third.asString()).isEqualTo("hi");

            rows.next();
            PqVariant fourth = rows.getVariant("payload");
            assertThat(fourth.metadata()).isEqualTo(objectMetadata);
            assertThat(fourth.value()).isEqualTo(objectValue);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void columnWriterWritesAVariantColumnFromItsTwoByteColumns() throws IOException {
        byte[][] metadata = { EMPTY_METADATA, EMPTY_METADATA };
        byte[][] values = { TRUE_VALUE, new byte[] { 0x0c, 0x2a } };

        FileSchema schema = FileSchema.builder("schema")
                .variant("payload", RepetitionType.OPTIONAL)
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .bytes("payload.metadata", metadata)
                    .bytes("payload.value", values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant first = rows.getVariant("payload");
            assertThat(first.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(first.value()).isEqualTo(TRUE_VALUE);

            rows.next();
            PqVariant second = rows.getVariant("payload");
            assertThat(second.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(second.value()).isEqualTo(new byte[] { 0x0c, 0x2a });
        }
    }

    @Test
    void columnWriterWritesANullVariantRow() throws IOException {
        boolean[] nulls = { false, true, false };
        byte[][] metadata = { EMPTY_METADATA, new byte[0], EMPTY_METADATA };
        byte[][] values = { TRUE_VALUE, new byte[0], new byte[] { 0x0c, 0x2a } };

        FileSchema schema = FileSchema.builder("schema")
                .variant("payload", RepetitionType.OPTIONAL)
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .struct("payload", Validity.ofNulls(nulls))
                    .bytes("payload.metadata", metadata)
                    .bytes("payload.value", values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqVariant first = rows.getVariant("payload");
            assertThat(first.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(first.value()).isEqualTo(TRUE_VALUE);

            rows.next();
            assertThat(rows.isNull("payload")).isTrue();

            rows.next();
            PqVariant third = rows.getVariant("payload");
            assertThat(third.metadata()).isEqualTo(EMPTY_METADATA);
            assertThat(third.value()).isEqualTo(new byte[] { 0x0c, 0x2a });
        }
    }

    private static void assertWriteFails(Consumer<StructBuilder> row, String message) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .primitive("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("meta", RepetitionType.OPTIONAL, meta -> meta
                        .primitive("note", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                col -> col.logicalType(LogicalType.string())))
                .variant("payload", RepetitionType.OPTIONAL)
                .list("attachments", RepetitionType.OPTIONAL, element -> element.variant(RepetitionType.OPTIONAL))
                .map("renditions", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        key -> key.logicalType(LogicalType.string()),
                        value -> value.variant(RepetitionType.OPTIONAL))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.rowWriter().writeRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message);
        }
    }
}
