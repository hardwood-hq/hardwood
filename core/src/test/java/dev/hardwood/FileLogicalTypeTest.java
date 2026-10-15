/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqFile;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriteResult;
import dev.hardwood.writer.StructBuilder;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/// `FILE` groups written through the row writer and read back through every accessor that
/// reaches one: the row, a nested struct, a list element and a map value.
class FileLogicalTypeTest {

    private static final byte[] PNG = {(byte) 0x89, 'P', 'N', 'G'};

    /// One value of each form the format defines, in row order.
    private static final List<PqFile> FORMS = List.of(
            PqFile.inline(PNG),
            PqFile.inline(new byte[0]).withContentType("text/plain"),
            PqFile.external("s3://bucket/cat.jpg").withContentType("image/jpeg").withChecksum("ETAG:\"9b2c\""),
            PqFile.external("bundle.tar", 51200),
            PqFile.external("s3://bucket/bundle.tar", 4096, 51200)
                    .withChecksum("SHA-256:" + "0".repeat(64)),
            PqFile.external("s3://bucket/bundle.tar", 0, 0).withInline(new byte[0]),
            PqFile.external("s3://bucket/bundle.tar", 8, PNG.length).withInline(PNG).withContentType("image/png"));

    private static byte[] file;

    @BeforeAll
    static void writeFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .file("photo", RepetitionType.OPTIONAL)
                .struct("meta", RepetitionType.OPTIONAL, meta -> meta
                        .file("thumbnail", RepetitionType.OPTIONAL))
                .list("attachments", RepetitionType.OPTIONAL, element -> element.file(RepetitionType.OPTIONAL))
                .map("renditions", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.file(RepetitionType.OPTIONAL))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < FORMS.size(); i++) {
                PqFile form = FORMS.get(i);
                int id = i;
                writer.rowWriter().writeRow(row -> row
                        .setInt("id", id)
                        .setFile("photo", form)
                        .setStruct("meta", meta -> meta.setFile("thumbnail", form))
                        .setList("attachments", list -> list.addFile(form).addFile(null).addFile(form))
                        .setMap("renditions", map -> map
                                .addEntry(entry -> entry.setString("key", "a").setFile("value", form))
                                .addEntry(entry -> entry.setString("key", "b").setFile("value", null))));
            }
            // A row whose FILE fields are all null.
            writer.rowWriter().writeRow(row -> row.setInt("id", FORMS.size()));
        }
        file = InMemoryFiles.toByteArray(out);
    }

    @Test
    void declaresTheGroupWithAllSixChildren() throws IOException {
        try (ParquetFileReader reader = open()) {
            SchemaNode.GroupNode photo = (SchemaNode.GroupNode) reader.getFileSchema().getField("photo");

            assertThat(photo.logicalType()).isEqualTo(LogicalType.file());
            assertThat(photo.isFile()).isTrue();
            assertThat(photo.children())
                    .extracting(SchemaNode::name, SchemaNode::repetitionType)
                    .containsExactly(
                            tuple("uri", RepetitionType.OPTIONAL), tuple("offset", RepetitionType.OPTIONAL),
                            tuple("size", RepetitionType.OPTIONAL), tuple("content_type", RepetitionType.OPTIONAL),
                            tuple("checksum", RepetitionType.OPTIONAL), tuple("inline", RepetitionType.OPTIONAL));
            assertThat(reader.getFileSchema().toSchemaElements())
                    .filteredOn(element -> element.name().equals("photo"))
                    .extracting(SchemaElement::logicalType, SchemaElement::convertedType)
                    .containsExactly(tuple(LogicalType.file(), null));
        }
    }

    @Test
    void readsEachFormBackThroughGetFile() throws IOException {
        List<PqFile> photos = new ArrayList<>();
        List<PqFile> byIndex = new ArrayList<>();
        readRows(rows -> {
            photos.add(rows.getFile("photo"));
            byIndex.add(rows.getFile(1));
        });

        List<PqFile> expected = new ArrayList<>(FORMS);
        expected.add(null);
        assertThat(photos).isEqualTo(expected);
        assertThat(byIndex).isEqualTo(expected);
    }

    @Test
    void readsANestedFileThroughTheStruct() throws IOException {
        List<PqFile> thumbnails = new ArrayList<>();
        readRows(rows -> {
            PqStruct meta = rows.getStruct("meta");
            thumbnails.add(meta == null ? null : meta.getFile("thumbnail"));
        });

        assertThat(thumbnails.subList(0, FORMS.size())).isEqualTo(FORMS);
        assertThat(thumbnails.get(FORMS.size())).isNull();
    }

    @Test
    void readsListElementsThroughFilesAndGet() throws IOException {
        List<List<PqFile>> viaFiles = new ArrayList<>();
        List<List<Object>> viaGet = new ArrayList<>();
        readRows(rows -> {
            PqList attachments = rows.getList("attachments");
            if (attachments != null) {
                viaFiles.add(attachments.files());
                viaGet.add(attachments.values());
            }
        });

        assertThat(viaFiles).hasSize(FORMS.size());
        for (int i = 0; i < FORMS.size(); i++) {
            assertThat(viaFiles.get(i)).containsExactly(FORMS.get(i), null, FORMS.get(i));
            assertThat(viaGet.get(i)).containsExactly(FORMS.get(i), null, FORMS.get(i));
        }
    }

    @Test
    void readsMapValuesThroughGetFileValueAndGetValue() throws IOException {
        List<PqFile> viaTyped = new ArrayList<>();
        List<Object> viaGeneric = new ArrayList<>();
        List<Object> viaKey = new ArrayList<>();
        readRows(rows -> {
            PqMap renditions = rows.getMap("renditions");
            if (renditions != null) {
                for (PqMap.Entry entry : renditions.getEntries()) {
                    viaTyped.add(entry.getFileValue());
                    viaGeneric.add(entry.getValue());
                }
                viaKey.add(renditions.getValue("a"));
            }
        });

        List<PqFile> expected = new ArrayList<>();
        for (PqFile form : FORMS) {
            expected.add(form);
            expected.add(null);
        }
        assertThat(viaTyped).isEqualTo(expected);
        assertThat(viaGeneric).isEqualTo(new ArrayList<Object>(expected));
        assertThat(viaKey).isEqualTo(new ArrayList<Object>(FORMS));
    }

    @Test
    void genericRouteReturnsThePqFileAndRawRouteTheStruct() throws IOException {
        List<Object> values = new ArrayList<>();
        List<Object> raw = new ArrayList<>();
        List<Object> rawElements = new ArrayList<>();
        readRows(rows -> {
            values.add(rows.getValue("photo"));
            raw.add(rows.getRawValue("photo"));
            PqList attachments = rows.getList("attachments");
            if (attachments != null) {
                rawElements.add(attachments.getRaw(0));
            }
        });

        assertThat(values.get(4)).isEqualTo(FORMS.get(4));
        assertThat(raw.get(4)).isInstanceOf(PqStruct.class);
        PqStruct stored = (PqStruct) raw.get(4);
        assertThat(stored.getString("uri")).isEqualTo("s3://bucket/bundle.tar");
        assertThat(stored.getLong("offset")).isEqualTo(4096);
        assertThat(stored.isNull("inline")).isTrue();
        assertThat(rawElements.get(4)).isInstanceOf(PqStruct.class);
        assertThat(values.get(FORMS.size())).isNull();
        assertThat(raw.get(FORMS.size())).isNull();
    }

    @Test
    void projectingOneChildProjectsTheWholeGroup() throws IOException {
        List<PqFile> photos = new ArrayList<>();
        try (ParquetFileReader reader = open();
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("photo.uri"))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                photos.add(rows.getFile("photo"));
            }
        }

        assertThat(photos.subList(0, FORMS.size())).isEqualTo(FORMS);
    }

    @Test
    void getFileOnAFieldNotAnnotatedFileFails() throws IOException {
        try (ParquetFileReader reader = open(); RowReader rows = reader.rowReader()) {
            rows.next();

            assertThatThrownBy(() -> rows.getFile("meta"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field 'meta' is not annotated as FILE");
        }
    }

    @Test
    void filesAndGetFileValueOnPrimitiveElementsFail() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .list("tags", RepetitionType.OPTIONAL, element -> element.primitive(PhysicalType.INT32,
                        RepetitionType.OPTIONAL))
                .map("labels", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row
                    .setList("tags", list -> list.addInt(1))
                    .setMap("labels", map -> map.addEntry(entry -> entry.setString("key", "a").setInt("value", 1))));
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();

            assertThatThrownBy(() -> rows.getList("tags").files())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The element of list 'tags' is not annotated as FILE");
            assertThatThrownBy(() -> rows.getMap("labels").getEntries().get(0).getFileValue())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The value of map 'labels' is not annotated as FILE");
        }
    }

    @Test
    void writesTheStoredBytesOfEachField() throws IOException {
        List<String> uris = new ArrayList<>();
        try (ParquetFileReader reader = open(); RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqStruct stored = (PqStruct) rows.getRawValue("photo");
                uris.add(stored == null || stored.isNull("uri") ? null
                        : new String(stored.getBinary("uri"), StandardCharsets.UTF_8));
            }
        }

        assertThat(uris).containsExactly(null, null, "s3://bucket/cat.jpg", "bundle.tar",
                "s3://bucket/bundle.tar", "s3://bucket/bundle.tar", "s3://bucket/bundle.tar", null);
    }

    @Test
    void setStructOnAFileGroupFails() throws IOException {
        assertWriteFails(row -> row.setInt("id", 0).setStruct("photo", photo -> { }),
                "Field photo is a FILE group; setStruct applies to a struct group");
    }

    @Test
    void setFileOnAPlainStructFails() throws IOException {
        assertWriteFails(row -> row.setInt("id", 0).setFile("meta", PqFile.inline(PNG)),
                "Field meta is a struct group; setFile applies to a FILE group");
    }

    @Test
    void addStructOnAFileElementFails() throws IOException {
        assertWriteFails(row -> row.setInt("id", 0).setList("attachments", list -> list.addStruct(s -> { })),
                "The element of list attachments is a FILE group; addStruct applies to a struct element");
    }

    @Test
    void setFileRejectsAValueSettingAFieldTheGroupLacks() throws IOException {
        // A FILE group declaring only the children an external reference needs, as a schema read
        // from a file can.
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("photo", RepetitionType.OPTIONAL, 2, LogicalType.file()),
                SchemaElement.primitive("uri", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()),
                SchemaElement.primitive("content_type", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        LogicalType.string())));
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setFile("photo", PqFile.external("s3://bucket/a")));

            assertThat(writer.rowWriter().tryWriteRow(row -> row.setFile("photo", PqFile.external("s3://bucket/a", 10))))
                    .isEqualTo(new RowWriteResult.Rejected("photo", "FILE group photo has no 'size' child, which the value sets"));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.rowWriter().writeRow(
                    row -> row.setFile("photo", PqFile.external("s3://bucket/a", 10))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("FILE group photo has no 'size' child, which the value sets");
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getFile("photo")).isEqualTo(PqFile.external("s3://bucket/a"));
            assertThat(rows.hasNext()).isFalse();
        }
    }

    /// The writer keeps a reference to a written value's inline bytes rather than a copy; the
    /// value still reads its own bytes afterwards, and the file holds them.
    @Test
    void writingAValueLeavesItsInlineBytesIntact() throws IOException {
        FileSchema schema = FileSchema.builder("schema").file("photo", RepetitionType.OPTIONAL).build();
        PqFile photo = PqFile.inline(PNG);
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setFile("photo", photo));
            writer.rowWriter().writeRow(row -> row.setFile("photo", photo));
        }

        assertThat(photo.inline()).isEqualTo(PNG);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqFile first = rows.getFile("photo");
            rows.next();
            PqFile second = rows.getFile("photo");
            // A value read earlier keeps its bytes after the reader moves on.
            assertThat(first.inline()).isEqualTo(PNG);
            assertThat(second.inline()).isEqualTo(PNG);
        }
    }

    /// A value the reader returns refers to the batch it was read from, whose arrays no later
    /// batch refills, so values held across many batches and row groups keep their own bytes, on
    /// every route and whether the `inline` columns are dictionary-encoded or plain.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void valuesHeldAcrossBatchesKeepTheirInlineBytes(boolean dictionary) throws IOException {
        int rowCount = 300_000;
        FileSchema schema = FileSchema.builder("schema")
                .file("photo", RepetitionType.REQUIRED)
                .struct("meta", RepetitionType.REQUIRED, meta -> meta.file("thumbnail", RepetitionType.REQUIRED))
                .list("attachments", RepetitionType.REQUIRED, element -> element.file(RepetitionType.REQUIRED))
                .map("renditions", RepetitionType.REQUIRED, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.file(RepetitionType.REQUIRED))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema,
                WriterConfig.builder().rowGroupTargetRows(70_000).build())) {
            for (int i = 0; i < rowCount; i++) {
                int row = i;
                writer.rowWriter().writeRow(r -> r
                        .setFile("photo", heldValue(row, 0, dictionary))
                        .setStruct("meta", meta -> meta.setFile("thumbnail", heldValue(row, 1, dictionary)))
                        .setList("attachments", list -> list
                                .addFile(heldValue(row, 2, dictionary))
                                .addFile(heldValue(row, 3, dictionary)))
                        .setMap("renditions", map -> map.addEntry(entry -> entry
                                .setString("key", "a").setFile("value", heldValue(row, 4, dictionary)))));
            }
        }
        List<PqFile> held = new ArrayList<>(rowCount * 5);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(5);
            for (ColumnChunk column : reader.getFileMetaData().rowGroups().get(0).columns()) {
                if (column.metaData().pathInSchema().leafName().equals("inline")) {
                    assertThat(column.metaData().encodings().contains(Encoding.RLE_DICTIONARY))
                            .as("%s", column.metaData().pathInSchema()).isEqualTo(dictionary);
                }
            }
            while (rows.hasNext()) {
                rows.next();
                held.add(rows.getFile("photo"));
                held.add(rows.getStruct("meta").getFile("thumbnail"));
                held.addAll(rows.getList("attachments").files());
                held.add(rows.getMap("renditions").getEntries().get(0).getFileValue());
            }
        }

        assertThat(held).hasSize(rowCount * 5);
        for (int i = 0; i < held.size(); i++) {
            assertThat(held.get(i)).as("value %d", i).isEqualTo(heldValue(i / 5, i % 5, dictionary));
        }
    }

    /// Value `route` of row `row`: distinct per row for a plain column, one of five per route for a
    /// dictionary-encoded one, so a value reading another row's bytes fails either way.
    private static PqFile heldValue(int row, int route, boolean dictionary) {
        int value = dictionary ? (row + route) % 5 : row * 5 + route;
        return PqFile.inline(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    @Test
    void aReadValueCopiesItsInlineBytesOnEachCallAndHashesLikeAnEqualValue() throws IOException {
        try (ParquetFileReader reader = open(); RowReader rows = reader.rowReader()) {
            rows.next();
            PqFile read = rows.getFile("photo");
            byte[] first = read.inline();
            first[0] = 0;

            assertThat(read.inline()).isEqualTo(PNG);
            assertThat(read).isEqualTo(FORMS.get(0)).hasSameHashCodeAs(FORMS.get(0));
            assertThat(FORMS.get(0)).isEqualTo(read);
            assertThat(read).hasToString(FORMS.get(0).toString());
        }
    }

    @Test
    void theWriterChecksAnyImplementationOfPqFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema").file("photo", RepetitionType.OPTIONAL).build();
        PqFile valid = new StoredFile("s3://b/k", 4, 5, "");
        PqFile emptyUri = new StoredFile("", 4, 5, null);
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setFile("photo", valid));
            writer.rowWriter().writeRow(row -> row.setFile("photo", valid.withChecksum("MD5:" + "0".repeat(32))));

            assertThat(writer.rowWriter().tryWriteRow(row -> row.setFile("photo", emptyUri)))
                    .isEqualTo(new RowWriteResult.Rejected("photo",
                            "FILE group photo value is invalid: neither 'inline' nor 'uri' is set"));
        }
        List<PqFile> photos = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                photos.add(rows.getFile("photo"));
            }
        }

        // The empty content type is not set, so it is not written.
        PqFile expected = PqFile.external("s3://b/k", 4, 5);
        assertThat(photos).containsExactly(expected, expected.withChecksum("MD5:" + "0".repeat(32)));
        // Equality holds among the library's own values only, so it stays symmetric.
        assertThat(expected).isNotEqualTo(new StoredFile("s3://b/k", 4, 5, null));
        assertThat(photos.get(0)).isNotEqualTo(new StoredFile("s3://b/k", 4, 5, null));
    }

    /// A `PqFile` implemented outside Hardwood, holding whatever it is given.
    private record StoredFile(String uri, long offset, long size, String contentType) implements PqFile {

        @Override
        public boolean hasOffset() {
            return true;
        }

        @Override
        public boolean hasSize() {
            return true;
        }

        @Override
        public String checksum() {
            return null;
        }

        @Override
        public byte[] inline() {
            return null;
        }
    }

    @Test
    void theWriterRefusesAnEmptyFileGroup() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 2),
                SchemaElement.primitive("id", PhysicalType.INT32, RepetitionType.REQUIRED),
                SchemaElement.group("photo", RepetitionType.OPTIONAL, 0, LogicalType.file())));

        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.inMemory(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Group photo is annotated FILE but has no children; none of its values could be read"
                        + " back as written");
    }

    @Test
    void theRowWriterRejectsAMalformedChecksum() throws IOException {
        FileSchema schema = FileSchema.builder("schema").file("photo", RepetitionType.OPTIONAL).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            assertThat(writer.rowWriter().tryWriteRow(row -> row.setFile("photo",
                    PqFile.external("s3://b/k").withChecksum("SHA-256:ABC"))))
                    .isEqualTo(new RowWriteResult.Rejected("photo", "FILE group photo value is invalid: 'checksum' "
                            + "digest of SHA-256 must be 64 lowercase hex characters: SHA-256:ABC"));
            assertThat(writer.rowWriter().tryWriteRow(row -> row.setFile("photo",
                    PqFile.external("s3://b/k").withChecksum("md5sum"))))
                    .isEqualTo(new RowWriteResult.Rejected("photo", "FILE group photo value is invalid: 'checksum' "
                            + "is not of the form <algorithm>:<digest>: md5sum"));
            for (String token : List.of("MD5:5d41402abc4b2a76b9719d911017c592", "CRC32:3610a686",
                    "CRC32C:9a71bb4c", "ETAG:\"Opaque-Tag\"", "XXH3:Anything Goes")) {
                writer.rowWriter().writeRow(row -> row.setFile("photo", PqFile.external("s3://b/k").withChecksum(token)));
            }
        }
        List<String> checksums = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                checksums.add(rows.getFile("photo").checksum());
            }
        }
        assertThat(checksums).containsExactly("MD5:5d41402abc4b2a76b9719d911017c592", "CRC32:3610a686",
                "CRC32C:9a71bb4c", "ETAG:\"Opaque-Tag\"", "XXH3:Anything Goes");
    }

    @Test
    void fileOnALeafIsRefused() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("photo", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.file()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FILE annotates a group, not a primitive column: photo; declare it with the file "
                        + "builder verb instead");
    }

    private static void assertWriteFails(Consumer<StructBuilder> row, String message) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .file("photo", RepetitionType.OPTIONAL)
                .struct("meta", RepetitionType.OPTIONAL, meta -> meta
                        .addColumn("note", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
                .list("attachments", RepetitionType.OPTIONAL, element -> element.file(RepetitionType.OPTIONAL))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.rowWriter().writeRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message);
        }
    }

    private static void readRows(Consumer<RowReader> perRow) throws IOException {
        try (ParquetFileReader reader = open(); RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                perRow.accept(rows);
            }
        }
    }

    private static ParquetFileReader open() throws IOException {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
    }
}
