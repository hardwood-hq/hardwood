/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The schemas the writer refuses, and the point at which it refuses them.
///
/// Whether a schema can be turned into bytes is a property of the schema alone, so it is
/// settled by [ParquetFileWriter#create] before the destination is opened — ahead of both
/// views, and therefore identically for both. A shape reaching a view would otherwise be
/// reported at a different moment and, historically, with a different exception type
/// depending on which one met it.
///
/// The rules here are about *producing* a shape. Rules about *addressing* one — two sibling
/// fields sharing a name, and the legacy two-level entries a row builder cannot navigate to —
/// belong to the row layer alone and are asserted in `RowWriterRulesTest`, since the columnar
/// API addresses by index and path and is unharmed by them.
///
/// A schema the writer accepts must produce a file other implementations read, so the shapes
/// pinned as accepted here are the ones checked against PyArrow.
class WriterSchemaShapeTest {

    private static final Path NO_COLUMNS = Path.of("src/test/resources/no_columns.parquet");

    /// A schema with no columns leaves the writer nothing to write. `FileSchema.Builder`
    /// refuses to build one, so the schema here comes the other way: read from
    /// `no_columns.parquet`, an Arrow-written file whose root has no children. That is the
    /// route such a schema actually arrives by, and it makes the case a real one rather than
    /// a hand-assembled root.
    @Test
    void rejectsSchemaWithNoColumns() throws IOException {
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NO_COLUMNS))) {
            schema = reader.getFileSchema();
        }
        assertThat(schema.getColumnCount()).isZero();
        ByteBufferOutputFile out = new ByteBufferOutputFile();

        assertThatThrownBy(() -> ParquetFileWriter.create(out, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Schema schema has no columns; the writer requires at least one column");

        // Refused before the destination was opened, so there is nothing at it to clean up.
        assertThatThrownBy(out::position)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsUnsupportedPhysicalType() {
        // INT96 is deprecated and never produced.
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT96, RepetitionType.REQUIRED)
                .build();

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Writer does not support INT96 columns yet; column v is INT96");
    }

    /// A `REPEATED` leaf with no annotated parent has no layer, and the columnar API has no
    /// verb that could supply such a column's entry offsets — so every value would be written
    /// as its own one-element list, silently changing the record count of anything copied
    /// through. `FileSchema.Builder` refuses the shape, but `fromSchemaElements` does not, and
    /// that is the schema a file being copied arrives with.
    @Test
    void rejectsRepeatedLeaf() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.primitive("nums", PhysicalType.INT32, RepetitionType.REPEATED)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Field nums is a REPEATED leaf outside a LIST or MAP group; the annotation is "
                         + "what gives a repeated field a layer to be addressed through, so the writer "
                         + "cannot produce it")
                ;
    }

    /// The same shape nested inside a struct, so the rejection is known to walk the schema
    /// rather than to check the top level only.
    @Test
    void rejectsRepeatedLeafInsideAStruct() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("outer", RepetitionType.REQUIRED, 1),
                SchemaElement.primitive("nums", PhysicalType.INT32, RepetitionType.REPEATED)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Field outer.nums is a REPEATED leaf outside a LIST or MAP group; the "
                         + "annotation is what gives a repeated field a layer to be addressed through, so "
                         + "the writer cannot produce it");
    }

    /// A `REPEATED` group carrying no `LIST` or `MAP` annotation is the two-level shape one
    /// level up: the reader recognizes it, and the writer has no verb for its offsets.
    @Test
    void rejectsRepeatedGroupWithoutListOrMapAnnotation() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("g", RepetitionType.REPEATED, 1),
                SchemaElement.primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Group g is REPEATED but carries no LIST or MAP annotation, and is not the "
                         + "entry group of one; the writer cannot produce it")
                ;
    }

    /// The `key_value` group of a `MAP` and the `list` group of a `LIST` are `REPEATED` and
    /// carry no annotation of their own. They are the scaffolding of an annotated ancestor
    /// rather than a bare repeated group, and must not be caught by the rule above.
    @Test
    void acceptsTheRepeatedScaffoldingOfListsAndMaps() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("items", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThat(writer).isNotNull();
        }
    }

    /// A key-only `MAP`, which older writers produce and the reader accepts. The `key_value`
    /// group holds one field rather than two, which is a shape the writer can shred.
    @Test
    void acceptsAKeyOnlyMap() throws Exception {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("props", RepetitionType.OPTIONAL, 1, LogicalType.map()),
                SchemaElement.group("key_value", RepetitionType.REPEATED, 1),
                SchemaElement.primitive("key", PhysicalType.INT32, RepetitionType.REQUIRED)));

        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThat(writer).isNotNull();
        }
    }

    /// The annotation accounts for the repetition of the group's entry, not for the group's
    /// own: a `REPEATED` `LIST` group declares two repetition levels where the shredder derives
    /// one layer, and nothing addresses the outer one. PyArrow refuses such a file outright —
    /// "LIST-annotated groups must not be repeated".
    @Test
    void rejectsAnAnnotatedGroupThatIsItselfRepeated() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("items", RepetitionType.REPEATED, 1, LogicalType.list()),
                SchemaElement.group("list", RepetitionType.REPEATED, 1),
                SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.OPTIONAL)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Group items is annotated LIST and is itself REPEATED; the annotation accounts "
                         + "for the repetition of its entry only, so nothing supplies the group's own "
                         + "entry offsets")
                ;
    }

    /// The same rule one level in, where the annotated group stands in the entry position of
    /// another. Being an annotated group's entry excuses a `REPEATED` group from carrying an
    /// annotation of its own; it does not excuse one that carries an annotation from the rule
    /// that annotation brings with it.
    @Test
    void rejectsAnAnnotatedGroupThatIsItselfRepeatedInsideAList() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("items", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElement.group("inner", RepetitionType.REPEATED, 1, LogicalType.list()),
                SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.OPTIONAL)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Group items.inner is annotated LIST and is itself REPEATED; the annotation "
                         + "accounts for the repetition of its entry only, so nothing supplies the "
                         + "group's own entry offsets")
                ;
    }

    /// An annotation over an entry that does not repeat carries no repetition at all, so the
    /// `list(...)` offsets the group's path accepts have nothing to drive. PyArrow refuses the
    /// file — "Non-repeated nodes in a LIST-annotated group are not supported".
    @Test
    void rejectsAnAnnotatedGroupWhoseEntryIsNotRepeated() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("items", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.OPTIONAL)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Group items is annotated LIST but its entry element is OPTIONAL rather than "
                         + "REPEATED; the annotation then carries no repetition and the group's entry "
                         + "offsets have nowhere to go")
                ;
    }

    /// A sibling beside the entry would take the annotated group's repetition layer without
    /// repeating, so its levels and the schema's would disagree.
    @Test
    void rejectsAnAnnotatedGroupHoldingMoreThanItsEntry() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("items", RepetitionType.OPTIONAL, 2, LogicalType.list()),
                SchemaElement.primitive("a", PhysicalType.INT32, RepetitionType.OPTIONAL),
                SchemaElement.primitive("b", PhysicalType.INT32, RepetitionType.OPTIONAL)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Group items is annotated LIST but holds 2 fields where the layout requires "
                         + "exactly one, its REPEATED entry")
                ;
    }

    /// A `MAP`'s entry is a key/value pair, so a leaf there is not the legacy two-level form a
    /// `LIST` accepts but a map with nowhere to put a key. PyArrow refuses the file —
    /// "Key-value node must be a group".
    @Test
    void rejectsAMapWhoseEntryIsALeaf() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("props", RepetitionType.OPTIONAL, 1, LogicalType.map()),
                SchemaElement.primitive("key", PhysicalType.INT32, RepetitionType.REPEATED)));

        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Group props is annotated MAP but its entry key is a leaf where the layout "
                         + "requires a repeated group of key and value")
                ;
    }

    /// The legacy two-level list, whose entry is the element itself. The annotation supplies
    /// the layer, so the shape is producible and the record count survives a copy — which is
    /// the whole point of admitting it, since `fromSchemaElements` is how a file being copied
    /// arrives. Read back through PyArrow this is `[[1, 2], [], [3, 4, 5]]`.
    @Test
    void acceptsALegacyTwoLevelListWithALeafEntry() throws Exception {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("items", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.REPEATED)));

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("items", new int[] { 0, 2, 2, 5 })
                    .ints("items.element", new int[] { 1, 2, 3, 4, 5 }));
        }

        assertThat(listOfInts(out)).containsExactly(List.of(1, 2), List.of(), List.of(3, 4, 5));
    }

    /// The other legacy two-level form: the entry is a group of several fields and is therefore
    /// itself the element, a list of structs. It repeats under the annotation exactly as a leaf
    /// entry does, so the same rule admits it.
    @Test
    void acceptsALegacyTwoLevelListOfStructs() throws Exception {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("items", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElement.group("element", RepetitionType.REPEATED, 2),
                SchemaElement.primitive("a", PhysicalType.INT32, RepetitionType.REQUIRED),
                SchemaElement.primitive("b", PhysicalType.INT32, RepetitionType.REQUIRED)));

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("items", new int[] { 0, 2, 3 })
                    .ints("items.element.a", new int[] { 1, 2, 3 })
                    .ints("items.element.b", new int[] { 10, 20, 30 }));
        }

        // Both leaves repeat under the one layer the annotation supplies, so they stay aligned.
        assertThat(listOfInts(out, "items.element.a")).containsExactly(List.of(1, 2), List.of(3));
        assertThat(listOfInts(out, "items.element.b")).containsExactly(List.of(10, 20), List.of(30));
    }

    /// The legacy `MAP` form, where only the inner `key_value` group is annotated and the
    /// reader resolves the outer group to a map. Its `key_value` carries `MAP_KEY_VALUE`, not
    /// `MAP`, so the entry rules must read it as the entry rather than as an annotation of its
    /// own. Taken from a real file rather than assembled here, since that is the shape a copy
    /// arrives with.
    @Test
    void acceptsTheLegacyKeyValueMapAnnotation() throws Exception {
        Path fixture = Path.of("src/test/resources/map_annotation_legacy_key_value_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture));
                ParquetFileWriter writer =
                        ParquetFileWriter.create(new ByteBufferOutputFile(), reader.getFileSchema())) {
            assertThat(writer).isNotNull();
        }
    }

    /// A nullable struct directly enclosing a `LIST` or `MAP` is producible (#1026): a `STRUCT`
    /// layer never remaps its item scope, so the shredder levels the repeated field beneath one
    /// exactly as it levels one at the record root. Kept here, alongside the shapes this class
    /// refuses, as a regression guard for the rule this class states; the levels themselves are
    /// asserted in `WriterNestedRoundTripTest` and the batch rule the shape adds in
    /// `WriterBatchContractTest`.
    @Test
    void acceptsANullableStructEnclosingARepeatedField() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .struct("outer", RepetitionType.OPTIONAL, outer -> outer
                        .list("inner", RepetitionType.REQUIRED,
                                element -> element.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThat(writer).isNotNull();
        }
    }

    /// A repeated field resets the ancestry: a nullable struct *below* a list is the shape
    /// `WriterNestedRoundTripTest` writes, and must not be swept up by the rule above.
    @Test
    void acceptsANullableStructBelowAList() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("items", RepetitionType.REQUIRED, element -> element
                        .struct(RepetitionType.OPTIONAL,
                                inner -> inner.addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThat(writer).isNotNull();
        }
    }

    /// A schema the writer cannot produce must leave the destination as it found it.
    /// `ChannelOutputFile` streams to a temporary sibling and renames on close, so a refusal
    /// after the destination was opened orphans that sibling for good — nothing later would
    /// clean it up.
    @Test
    void anUnproducibleSchemaLeavesNothingAtTheDestination(@TempDir Path dir) throws Exception {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.primitive("nums", PhysicalType.INT32, RepetitionType.REPEATED)));
        Path file = dir.resolve("out.parquet");

        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.of(file), schema))
                .isInstanceOf(UnsupportedOperationException.class);

        try (Stream<Path> entries = Files.list(dir)) {
            assertThat(entries).isEmpty();
        }
    }

    /// Everything the schema decides is settled before the destination is opened, so what is
    /// left between `out.create()` and a usable writer is the magic write and the construction
    /// itself. Should either fail, the destination is open and holds no file: the failure path
    /// has to discard it, because `ChannelOutputFile` streams into a temporary sibling and
    /// renames only on `close()`, and nothing else would ever remove that sibling.
    @Test
    void aFailureAfterTheDestinationIsOpenedDiscardsIt(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");
        FailingWrite out = new FailingWrite(OutputFile.of(file));

        assertThatThrownBy(() -> ParquetFileWriter.create(out, oneColumn()))
                .isInstanceOf(IOException.class)
                .hasMessage("injected write failure");

        assertThat(out.discarded).isTrue();
        try (Stream<Path> entries = Files.list(dir)) {
            assertThat(entries).isEmpty();
        }
    }

    /// Reads a list column back as one list of values per record, so what the writer emitted
    /// can be compared against the lists that went in rather than against a level stream.
    private static List<List<Integer>> listOfInts(ByteBufferOutputFile out) throws IOException {
        return listOfInts(out, 0);
    }

    private static List<List<Integer>> listOfInts(ByteBufferOutputFile out, String column) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            return listOfInts(out, reader.getFileSchema().getColumn(column).columnIndex());
        }
    }

    private static List<List<Integer>> listOfInts(ByteBufferOutputFile out, int columnIndex) throws IOException {
        List<List<Integer>> records = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                ColumnReader column = reader.columnReader(columnIndex)) {
            while (column.nextBatch()) {
                int[] offsets = column.getLayerOffsets(0);
                int[] values = column.getInts();
                for (int r = 0; r < column.getRecordCount(); r++) {
                    List<Integer> entries = new ArrayList<>();
                    for (int e = offsets[r]; e < offsets[r + 1]; e++) {
                        entries.add(values[e]);
                    }
                    records.add(entries);
                }
            }
        }
        return records;
    }

    /// An `OutputFile` that opens but refuses every write, so the failure lands after the
    /// destination has been created and the discard is the only thing that can clean it up.
    private static final class FailingWrite implements OutputFile {

        private final OutputFile delegate;
        private boolean discarded;

        FailingWrite(OutputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void create() throws IOException {
            delegate.create();
        }

        @Override
        public void write(ByteBuffer data) throws IOException {
            throw new IOException("injected write failure");
        }

        @Override
        public long position() {
            return delegate.position();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void discard() throws IOException {
            discarded = true;
            delegate.discard();
        }
    }
}
