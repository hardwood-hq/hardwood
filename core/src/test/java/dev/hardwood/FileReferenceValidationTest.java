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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqFile;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.StructBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `FILE` values and groups no writer here produces: each is written as a plain struct whose
/// footer is then rewritten to annotate the group `FILE`, so the reader meets what a foreign
/// writer could have stored.
class FileReferenceValidationTest {

    private static final byte[] BYTES = {1, 2, 3};

    /// One row per rule the format states, then values the rules accept, in row order.
    private static final List<Consumer<StructBuilder>> ROWS = List.of(
            photo -> { },
            photo -> photo.setLong("offset", 0).setLong("size", 3).setBinary("inline", BYTES),
            photo -> photo.setString("uri", "s3://b/k").setLong("offset", 0),
            photo -> photo.setLong("size", 3).setBinary("inline", BYTES),
            photo -> photo.setString("uri", "s3://b/k").setLong("size", -1),
            photo -> photo.setString("uri", "s3://b/k").setLong("offset", -5).setLong("size", 3),
            photo -> photo.setString("uri", "").setBinary("inline", BYTES).setString("content_type", ""),
            photo -> photo.setString("uri", "s3://b/k").setLong("offset", 7).setLong("size", 3));

    private static final List<String> VIOLATIONS = List.of(
            "neither 'inline' nor 'uri' is set",
            "'offset' is set without 'uri'",
            "'offset' is set without 'size'",
            "'size' is set without 'uri'",
            "'size' is negative: -1",
            "'offset' is negative: -5");

    private static byte[] file;

    @BeforeAll
    static void writeFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .struct("photo", RepetitionType.OPTIONAL, photo -> photo
                        .addColumn("uri", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                        .addColumn("offset", PhysicalType.INT64, RepetitionType.OPTIONAL)
                        .addColumn("size", PhysicalType.INT64, RepetitionType.OPTIONAL)
                        .addColumn("content_type", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                LogicalType.string())
                        .addColumn("inline", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (Consumer<StructBuilder> photo : ROWS) {
                writer.rowWriter().writeRow(row -> row.setStruct("photo", photo));
            }
        }
        file = FooterRewriter.rewrite(InMemoryFiles.toByteArray(out), annotating("photo"));
    }

    @Test
    void eachRuleTheValueBreaksFailsTheRead() throws IOException {
        try (ParquetFileReader reader = open(file); RowReader rows = reader.rowReader()) {
            for (String violation : VIOLATIONS) {
                rows.next();
                String message = "[<memory>] FILE value of column 'photo' is invalid: " + violation;

                assertThatThrownBy(() -> rows.getFile("photo"))
                        .isInstanceOf(ParquetReadException.class)
                        .hasMessage(message);
                assertThatThrownBy(() -> rows.getValue("photo"))
                        .isInstanceOf(ParquetReadException.class)
                        .hasMessage(message);
                assertThat(rows.getRawValue("photo")).isInstanceOf(PqStruct.class);
            }
        }
    }

    @Test
    void valuesAfterAnInvalidOneRead() throws IOException {
        List<PqFile> valid = new ArrayList<>();
        try (ParquetFileReader reader = open(file); RowReader rows = reader.rowReader()) {
            for (int row = 0; rows.hasNext(); row++) {
                rows.next();
                if (row >= VIOLATIONS.size()) {
                    valid.add(rows.getFile("photo"));
                }
            }
        }

        // An empty string is not set: the empty uri and content_type read as absent.
        assertThat(valid).containsExactly(PqFile.inline(BYTES), PqFile.external("s3://b/k", 7, 3));
    }

    /// An invalid value below the top level fails on every route that reads it as a `PqFile`, the
    /// message naming the group by its full path, while `toString` of the enclosing struct, list or
    /// map renders the stored fields.
    @Test
    void anInvalidNestedValueFailsOnEveryRouteAndNamesItsPath() throws IOException {
        Consumer<FileSchema.StructBuilder> children = group -> group
                .addColumn("uri", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                .addColumn("size", PhysicalType.INT64, RepetitionType.OPTIONAL);
        FileSchema schema = FileSchema.builder("schema")
                .struct("meta", RepetitionType.OPTIONAL, meta -> meta.struct("photo", RepetitionType.OPTIONAL, children))
                .list("attachments", RepetitionType.OPTIONAL, element -> element.struct(RepetitionType.OPTIONAL, children))
                .map("renditions", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.struct(RepetitionType.OPTIONAL, children))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            // Each group holds a size and neither inline nor uri.
            writer.rowWriter().writeRow(row -> row
                    .setStruct("meta", meta -> meta.setStruct("photo", photo -> photo.setLong("size", 3)))
                    .setList("attachments", list -> list.addStruct(element -> element.setLong("size", 3)))
                    .setMap("renditions", map -> map.addEntry(entry -> entry.setString("key", "a")
                            .setStruct("value", value -> value.setLong("size", 3)))));
        }
        byte[] nested = FooterRewriter.rewrite(InMemoryFiles.toByteArray(out),
                annotating("photo", "element", "value"));

        try (ParquetFileReader reader = open(nested); RowReader rows = reader.rowReader()) {
            rows.next();
            PqStruct meta = rows.getStruct("meta");
            PqList attachments = rows.getList("attachments");
            PqMap.Entry rendition = rows.getMap("renditions").getEntries().get(0);

            assertInvalid(() -> meta.getFile("photo"), "meta.photo");
            assertInvalid(() -> meta.getValue("photo"), "meta.photo");
            assertInvalid(() -> attachments.files().get(0), "attachments.list.element");
            assertInvalid(() -> attachments.get(0), "attachments.list.element");
            assertInvalid(rendition::getFileValue, "renditions.key_value.value");
            assertInvalid(rendition::getValue, "renditions.key_value.value");

            assertThat(meta).hasToString("PqStruct{photo=PqStruct{uri=null, size=3}}");
            assertThat(attachments).hasToString("[PqStruct{uri=null, size=3}]");
            assertThat(rows.getMap("renditions")).hasToString("{a=PqStruct{uri=null, size=3}}");
            assertThat(rendition).hasToString("a=PqStruct{uri=null, size=3}");
        }
    }

    @Test
    void aChildTheFormatDoesNotDefineFailsTheOpen() throws IOException {
        assertMalformed(group("photo", leaf("uri", PhysicalType.BYTE_ARRAY, LogicalType.string()),
                        leaf("name", PhysicalType.BYTE_ARRAY, LogicalType.string())),
                "FILE group 'photo' has a child 'name' the format does not define; a FILE group's children are "
                        + "uri, offset, size, content_type, checksum, inline");
    }

    @Test
    void aChildOfAnotherTypeFailsTheOpen() throws IOException {
        assertMalformed(group("photo", leaf("uri", PhysicalType.BYTE_ARRAY, LogicalType.string()),
                        leaf("offset", PhysicalType.INT32, null)),
                "FILE group 'photo' child 'offset' must be an OPTIONAL INT64 with no annotation or annotated "
                        + "INT_64, found OPTIONAL INT32 with no annotation");
        assertMalformed(group("photo", leaf("uri", PhysicalType.BYTE_ARRAY, LogicalType.string()),
                        leaf("size", PhysicalType.INT64, LogicalType.intType(64, false))),
                "FILE group 'photo' child 'size' must be an OPTIONAL INT64 with no annotation or annotated "
                        + "INT_64, found OPTIONAL INT64 annotated UINT_64");
        assertMalformed(group("photo", leaf("uri", PhysicalType.BYTE_ARRAY, null)),
                "FILE group 'photo' child 'uri' must be an OPTIONAL BYTE_ARRAY annotated STRING, "
                        + "found OPTIONAL BYTE_ARRAY with no annotation");
        assertMalformed(group("photo", leaf("inline", PhysicalType.BYTE_ARRAY, LogicalType.string())),
                "FILE group 'photo' child 'inline' must be an OPTIONAL BYTE_ARRAY with no annotation, "
                        + "found OPTIONAL BYTE_ARRAY annotated STRING");
    }

    @Test
    void aRequiredChildFailsTheOpen() throws IOException {
        assertMalformed(List.of(
                        SchemaElement.root("schema", 1),
                        SchemaElement.group("photo", RepetitionType.OPTIONAL, 1, LogicalType.file()),
                        SchemaElement.primitive("uri", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                LogicalType.string())),
                "FILE group 'photo' child 'uri' must be an OPTIONAL BYTE_ARRAY annotated STRING, "
                        + "found REQUIRED BYTE_ARRAY annotated STRING");
    }

    @Test
    void aRepeatedChildNameFailsTheOpen() throws IOException {
        assertMalformed(group("photo", leaf("uri", PhysicalType.BYTE_ARRAY, LogicalType.string()),
                        leaf("uri", PhysicalType.BYTE_ARRAY, LogicalType.string())),
                "FILE group 'photo' has two children named 'uri'");
    }

    @Test
    void offsetAndSizeAnnotatedInt64Open() {
        FileSchema schema = FileSchema.fromSchemaElements(group("photo",
                leaf("uri", PhysicalType.BYTE_ARRAY, LogicalType.string()),
                leaf("offset", PhysicalType.INT64, LogicalType.intType(64, true)),
                leaf("size", PhysicalType.INT64, LogicalType.intType(64, true))));

        assertThat(((SchemaNode.GroupNode) schema.getField("photo")).isFile()).isTrue();
    }

    /// A group may define none of the children. Without a column it stores no nulls, so every row
    /// reads as a present value with no field set, which is invalid.
    @Test
    void anEmptyGroupOpensAndEachValueFailsOnRead() throws IOException {
        FileSchema plain = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 2),
                SchemaElement.primitive("id", PhysicalType.INT32, RepetitionType.REQUIRED),
                SchemaElement.group("photo", RepetitionType.OPTIONAL, 0)));
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, plain)) {
            writer.rowWriter().writeRow(row -> row.setInt("id", 1));
        }
        byte[] empty = FooterRewriter.rewrite(InMemoryFiles.toByteArray(out), annotating("photo"));

        try (ParquetFileReader reader = open(empty); RowReader rows = reader.rowReader()) {
            rows.next();

            assertThat(rows.getInt("id")).isEqualTo(1);
            assertThat(((PqStruct) rows.getRawValue("photo")).getFieldCount()).isZero();
            assertThatThrownBy(() -> rows.getFile("photo"))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("[<memory>] FILE value of column 'photo' is invalid: neither 'inline' nor 'uri' is set");
        }
    }

    @Test
    void anyPresentSubsetInAnyOrderOpens() throws IOException {
        FileSchema schema = FileSchema.fromSchemaElements(group("photo",
                leaf("content_type", PhysicalType.BYTE_ARRAY, LogicalType.string()),
                leaf("inline", PhysicalType.BYTE_ARRAY, null)));

        assertThat(schema.getField("photo")).isInstanceOfSatisfying(SchemaNode.GroupNode.class,
                photo -> assertThat(photo.isFile()).isTrue());
    }

    /// A file whose schema is `elements`, which `fromSchemaElements` refuses and which opening the
    /// same footer reports as a read failure.
    private static void assertMalformed(List<SchemaElement> elements, String message) throws IOException {
        assertThatThrownBy(() -> FileSchema.fromSchemaElements(elements))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);

        byte[] malformed = FooterRewriter.rewrite(file, metaData -> new FileMetaData(metaData.version(),
                elements, metaData.numRows(), List.of(), metaData.keyValueMetadata(), metaData.createdBy(),
                List.of()));
        assertThatThrownBy(() -> open(malformed))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] " + message);
    }

    private static List<SchemaElement> group(String name, SchemaElement... children) {
        List<SchemaElement> elements = new ArrayList<>();
        elements.add(SchemaElement.root("schema", 1));
        elements.add(SchemaElement.group(name, RepetitionType.OPTIONAL, children.length, LogicalType.file()));
        elements.addAll(List.of(children));
        return elements;
    }

    private static SchemaElement leaf(String name, PhysicalType type, LogicalType annotation) {
        return SchemaElement.primitive(name, type, RepetitionType.OPTIONAL, annotation);
    }

    private static void assertInvalid(ThrowingCallable read, String path) {
        assertThatThrownBy(read)
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] FILE value of column '" + path + "' is invalid: neither 'inline' nor 'uri' is set");
    }

    /// Annotates the groups named `names` `FILE`.
    private static UnaryOperator<FileMetaData> annotating(String... names) {
        List<String> annotated = List.of(names);
        return metaData -> {
            List<SchemaElement> schema = new ArrayList<>();
            for (SchemaElement element : metaData.schema()) {
                schema.add(annotated.contains(element.name()) && element.numChildren() != null
                        ? new SchemaElement(element.name(), element.type(), element.typeLength(),
                                element.repetitionType(), element.numChildren(), element.convertedType(),
                                element.scale(), element.precision(), element.fieldId(), LogicalType.file())
                        : element);
            }
            return new FileMetaData(metaData.version(), schema, metaData.numRows(), metaData.rowGroups(),
                    metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
        };
    }

    private static ParquetFileReader open(byte[] bytes) throws IOException {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)));
    }
}
