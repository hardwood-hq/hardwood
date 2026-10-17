/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnProjection;

import static dev.hardwood.arrow.ArrowTestSupport.assertSameTable;
import static dev.hardwood.arrow.ArrowTestSupport.coreFixture;
import static dev.hardwood.arrow.ArrowTestSupport.expected;
import static dev.hardwood.arrow.ArrowTestSupport.fixture;
import static dev.hardwood.arrow.ArrowTestSupport.readAll;
import static dev.hardwood.arrow.ArrowTestSupport.rows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrowBatchReaderTest {

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void allocatorIsEmpty() {
        assertThat(allocator.getAllocatedMemory()).isZero();
        allocator.close();
    }

    // ==================== Parity with the expected tables ====================

    /// Every scalar type the module maps, plain and dictionary-encoded, against the table
    /// built from the same declaration through Arrow's own setters.
    @ParameterizedTest
    @FieldSource("dev.hardwood.arrow.ArrowTestSupport#FIXTURES")
    void matchesExpectedTable(String name) throws IOException {
        try (VectorSchemaRoot expected = expected(name, allocator);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture(name)));
             ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator);
             VectorSchemaRoot actual = readAll(reader, allocator)) {
            assertSameTable(expected, actual);
        }
    }

    /// Batch boundaries change nothing: the rows of one-row batches are the rows of one batch.
    @Test
    void batchSizeDoesNotChangeTheRows() throws IOException {
        try (VectorSchemaRoot expected = expected("scalars_dict", allocator);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars_dict")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .batchSize(1)
                     .build();
             VectorSchemaRoot actual = readAll(reader, allocator)) {
            assertSameTable(expected, actual);
        }
    }

    @Test
    void schemaIsAvailableBeforeTheFirstBatch() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars")));
             ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator)) {
            assertThat(reader.getVectorSchemaRoot().getSchema().getFields()).extracting(Field::getName)
                    .startsWith("flag_req", "flag", "i32_req", "i32")
                    .endsWith("half", "span", "nothing");
        }
    }

    @Test
    void fieldIdsAreCarriedAsFieldMetadata() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("field_ids")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .projection(ColumnProjection.columns("c", "a", "b", "zero", "m"))
                     .build()) {
            List<Field> fields = reader.getVectorSchemaRoot().getSchema().getFields();
            assertThat(fields).extracting(Field::getName).containsExactly("c", "a", "b", "zero", "m");
            assertThat(fields.get(0).getMetadata()).isEqualTo(Map.of(ArrowFixtures.FIELD_ID_KEY, "42"));
            assertThat(fields.get(1).getMetadata()).isEqualTo(Map.of(ArrowFixtures.FIELD_ID_KEY, "7"));
            assertThat(fields.get(2).getMetadata()).isEmpty();
            assertThat(fields.get(3).getMetadata()).isEqualTo(Map.of(ArrowFixtures.FIELD_ID_KEY, "0"));
            // The map, its key and its value keep their ids; the entries struct carries none.
            Field m = fields.get(4);
            assertThat(m.getMetadata()).isEqualTo(Map.of(ArrowFixtures.FIELD_ID_KEY, "50"));
            Field entries = m.getChildren().get(0);
            assertThat(entries.getMetadata()).isEmpty();
            assertThat(entries.getChildren().get(0).getMetadata()).isEqualTo(Map.of(ArrowFixtures.FIELD_ID_KEY, "52"));
            assertThat(entries.getChildren().get(1).getMetadata()).isEqualTo(Map.of(ArrowFixtures.FIELD_ID_KEY, "53"));
        }
    }

    @Test
    void arrowSchemaMetadataIsIgnored() throws IOException {
        try (ParquetFileReader bare = ParquetFileReader.open(InputFile.of(fixture("scalars")));
             ParquetFileReader stored = ParquetFileReader.open(InputFile.of(fixture("scalars_stored_schema")));
             ArrowBatchReader bareReader = ArrowReaders.batchReader(bare, allocator);
             ArrowBatchReader storedReader = ArrowReaders.batchReader(stored, allocator);
             VectorSchemaRoot expected = readAll(bareReader, allocator);
             VectorSchemaRoot actual = readAll(storedReader, allocator)) {
            assertThat(stored.getFileMetaData().keyValueMetadata()).containsKey("ARROW:schema");
            assertThat(bare.getFileMetaData().keyValueMetadata()).doesNotContainKey("ARROW:schema");
            assertSameTable(expected, actual);
        }
    }

    // ==================== Projection and filtering ====================

    @Test
    void projectionOrdersColumnsAndCollapsesRepeats() throws IOException {
        try (VectorSchemaRoot expected = expected("scalars", allocator);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .projection(ColumnProjection.columns("text", "i32_req", "text", "dec256"))
                     .build();
             VectorSchemaRoot actual = readAll(reader, allocator)) {
            assertThat(actual.getSchema().getFields()).extracting(Field::getName)
                    .containsExactly("text", "i32_req", "dec256");
            List<List<Object>> expectedRows = new ArrayList<>();
            for (List<Object> row : rows(expected)) {
                expectedRows.add(Arrays.asList(
                        row.get(index(expected, "text")),
                        row.get(index(expected, "i32_req")),
                        row.get(index(expected, "dec256"))));
            }
            assertThat(rows(actual)).isEqualTo(expectedRows);
        }
    }

    /// A projection into a struct keeps the struct around the children it names, in the
    /// order it names them, with the struct's own nulls.
    @Test
    void nestedProjectionKeepsTheRequestedChildren() throws IOException {
        Path file = coreFixture("deep_nested_struct_test");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .projection(ColumnProjection.columns("account.organization.name", "customer_id"))
                     .build();
             VectorSchemaRoot root = readAll(reader, allocator)) {
            assertThat(root.getSchema().getFields()).extracting(Field::getName).containsExactly("account", "customer_id");
            assertThat(root.getSchema().findField("account").getChildren()).extracting(Field::getName)
                    .containsExactly("organization");
            assertThat(root.getVector("account").getObject(0).toString()).isEqualTo("{\"organization\":{\"name\":\"Acme Corp\"}}");
            assertThat(root.getVector("account").getObject(2).toString()).isEqualTo("{}");
            assertThat(root.getVector("account").isNull(3)).isTrue();
            assertThat(rows(root).stream().map(row -> row.get(1)).toList()).containsExactly(1, 2, 3, 4);
        }
    }

    @Test
    void filterKeepsOnlyMatchingRows() throws IOException {
        try (VectorSchemaRoot expected = expected("scalars_dict", allocator);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars_dict")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .filter(FilterPredicate.gt("i32_req", 0))
                     .build();
             VectorSchemaRoot actual = readAll(reader, allocator)) {
            int column = index(expected, "i32_req");
            List<List<Object>> expectedRows = rows(expected).stream()
                    .filter(row -> (Integer) row.get(column) > 0)
                    .toList();
            assertThat(expectedRows).hasSize(4);
            assertThat(rows(actual)).isEqualTo(expectedRows);
        }
    }

    /// The predicate may name a column the projection leaves out, nested ones included: of
    /// the four customers, only the first has a zip code; the others are null at the
    /// address, the organization and the account respectively.
    @Test
    void filterMayReferenceAnUnprojectedNestedColumn() throws IOException {
        Path file = coreFixture("deep_nested_struct_test");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader filtered = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .projection(ColumnProjection.columns("customer_id", "name"))
                     .filter(FilterPredicate.gt("account.organization.address.zip", 0))
                     .build();
             VectorSchemaRoot some = readAll(filtered, allocator)) {
            assertThat(some.getSchema().getFields()).extracting(Field::getName).containsExactly("customer_id", "name");
            assertThat(rows(some)).containsExactly(List.of(1, "Alice"));
        }
    }

    @Test
    void multiFileReadConcatenatesTheFiles() throws IOException {
        List<InputFile> files = List.of(InputFile.of(fixture("scalars")), InputFile.of(fixture("scalars_dict")));
        try (VectorSchemaRoot expected = expected("scalars", allocator);
             ParquetFileReader fileReader = ParquetFileReader.openAll(files);
             ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator);
             VectorSchemaRoot actual = readAll(reader, allocator)) {
            List<List<Object>> twice = new ArrayList<>(rows(expected));
            twice.addAll(rows(expected));
            assertThat(actual.getSchema()).isEqualTo(expected.getSchema());
            assertThat(rows(actual)).isEqualTo(twice);
        }
    }

    // ==================== Rejections ====================

    @Test
    void variantColumnsAreRejectedBeforeAnyWorkerStarts() throws IOException {
        Path file = coreFixture("avro_variant_identity_test");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file))) {
            assertThatThrownBy(() -> ArrowReaders.batchReader(fileReader, allocator))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("Column 'real_variant' is a Variant group, which hardwood-arrow does not serve");
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(fileReader, allocator)
                    .projection(ColumnProjection.columns("variant_container"))
                    .build())
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("Column 'variant_container.real_nested_variant' is a Variant group, which hardwood-arrow "
                            + "does not serve");
            // A group of the same shape without the annotation is a struct like any other.
            try (ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                    .projection(ColumnProjection.columns("plain_top", "plain_list"))
                    .build();
                 VectorSchemaRoot root = readAll(reader, allocator)) {
                assertThat(root.getRowCount()).isGreaterThan(0);
            }
        }
    }

    @Test
    void partialMapProjectionIsRejected() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("nested")))) {
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(fileReader, allocator)
                    .projection(ColumnProjection.columns("props.key_value.value"))
                    .build())
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("Column 'props' is a MAP group of which only the key or only the value is projected: "
                            + "project the map as a whole");
        }
    }

    /// A value the annotation rules out fails the batch, with nothing of it allocated and
    /// the reader unusable afterwards.
    @Test
    void valueOutsideItsAnnotationFailsTheLoad() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(coreFixture("int_past_annotation")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .projection(ColumnProjection.columns("i8"))
                     .build()) {
            assertThatThrownBy(reader::loadNextBatch)
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Column 'i8' is annotated INT_8 but holds -1000 at row 0, outside [-128, 127]");
            assertThatThrownBy(reader::loadNextBatch)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("ArrowBatchReader failed while loading a batch and cannot continue");
        }
    }

    @Test
    void nullProjectionAndAllocatorAreRejected() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars")))) {
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(fileReader, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("allocator must not be null");
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(null, allocator))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("reader must not be null");
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(fileReader, allocator).projection(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("projection must not be null");
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("batchSize must be positive: 0");
        }
    }

    // ==================== Lifecycle ====================

    @Test
    void closeBeforeTheFirstBatchReleasesTheColumns() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars")))) {
            ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator);
            reader.close();
            reader.close();
            assertThatThrownBy(reader::loadNextBatch)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("ArrowBatchReader is closed");
        }
    }

    @Test
    void closeMidReadReleasesTheLoadedBatch() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars_dict")))) {
            ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(2).build();
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(2);
            assertThat(allocator.getAllocatedMemory()).isGreaterThan(0);
            reader.close();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void exhaustionIsReportedOnEveryLaterCall() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("empty")));
             ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator)) {
            assertThat(reader.loadNextBatch()).isFalse();
            assertThat(reader.loadNextBatch()).isFalse();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isZero();
            assertThat(reader.bytesRead()).isZero();
        }
    }

    @Test
    void loadingTheNextBatchReleasesThePreviousOne() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(coreFixture("dict_plain_fallback")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(1000).build()) {
            assertThat(reader.loadNextBatch()).isTrue();
            long afterFirst = allocator.getAllocatedMemory();
            long bytesAfterFirst = reader.bytesRead();
            assertThat(bytesAfterFirst).isGreaterThan(0);
            assertThat(reader.loadNextBatch()).isTrue();
            long secondBody = reader.bytesRead() - bytesAfterFirst;
            assertThat(secondBody).isGreaterThan(0);
            // Had the first batch stayed allocated, the two bodies would both be held.
            assertThat(allocator.getAllocatedMemory()).isLessThan(afterFirst + secondBody);
        }
    }

    /// A consumer that keeps a batch transfers it out: the transferred vectors survive the
    /// next load and the reader's close, and the consumer releases them.
    @Test
    void transferredVectorsOutliveTheNextBatch() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(coreFixture("dict_plain_fallback")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator).batchSize(1000).build()) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int firstRows = root.getRowCount();
            String first = root.getVector("label").getObject(0).toString();
            TransferPair labels = root.getVector("label").getTransferPair(allocator);
            labels.transfer();
            try (VarCharVector kept = (VarCharVector) labels.getTo()) {
                assertThat(reader.loadNextBatch()).isTrue();
                reader.close();
                assertThat(kept.getValueCount()).isEqualTo(firstRows);
                assertThat(kept.getObject(0).toString()).isEqualTo(first);
                assertThat(allocator.getAllocatedMemory()).isGreaterThan(0);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void allocatorLimitFailsTheLoadAndLeavesNothingAllocated() throws IOException {
        try (BufferAllocator limited = allocator.newChildAllocator("limited", 0, 4096);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(coreFixture("dict_plain_fallback")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, limited).batchSize(1000).build()) {
            assertThatThrownBy(reader::loadNextBatch).isInstanceOf(OutOfMemoryException.class);
            // What remains is the empty root's own footprint (an offset sentinel per
            // variable-width vector), which the reader releases on close, not the loader's.
            assertThat(limited.getAllocatedMemory()).isEqualTo(footprint(reader.getVectorSchemaRoot()));
            assertThatThrownBy(reader::loadNextBatch)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("ArrowBatchReader failed while loading a batch and cannot continue");
        }
    }

    /// The bytes the root's vectors hold, whatever their value count. `getBuffers` reports
    /// nothing for a vector of no values, so the three buffers are asked for one by one.
    private static long footprint(VectorSchemaRoot root) {
        long bytes = 0;
        for (FieldVector vector : root.getFieldVectors()) {
            bytes += vector.getValidityBuffer().capacity();
            bytes += vector.getDataBuffer().capacity();
            if (vector instanceof BaseVariableWidthVector variableWidth) {
                bytes += variableWidth.getOffsetBuffer().capacity();
            }
        }
        return bytes;
    }

    /// Preparing the root for a load allocates too (an offset sentinel per variable-width
    /// vector), and a failure there fails the reader like any other.
    @Test
    void preparationFailureIsTerminal() throws IOException {
        try (BufferAllocator limited = allocator.newChildAllocator("limited", 0, 0);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, limited)
                     .projection(ColumnProjection.columns("text"))
                     .build()) {
            assertThatThrownBy(reader::loadNextBatch).isInstanceOf(OutOfMemoryException.class);
            limited.setLimit(Long.MAX_VALUE);
            assertThatThrownBy(reader::loadNextBatch)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("ArrowBatchReader failed while loading a batch and cannot continue");
        }
    }

    /// A load that fails while a previous batch sits in the root releases what it allocated
    /// and leaves the root in a state `close()` can release.
    ///
    /// The shape is the one that once leaked: two required `INT64` columns, a one-row first
    /// batch and a seventeen-row second one, and room after the first for the second batch's
    /// two data buffers but not for its validity bitmaps. A loader that left the all-present
    /// bitmaps to Arrow's `VectorLoader` had the data buffers accepted and then, with the
    /// root's old bitmap already released, the bitmap allocation refused, leaving a vector over
    /// a released buffer; the loader here allocates the bitmaps itself, so the refusal lands
    /// where everything of the batch is still its own to release.
    @Test
    void failureWhileReplacingABatchLeavesTheReaderClosable() throws IOException {
        try (BufferAllocator limited = allocator.newChildAllocator("limited", 0, Long.MAX_VALUE);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("counters")))) {
            ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, limited)
                    .filter(FilterPredicate.gtEq("n", 16L))
                    .batchSize(17)
                    .build();
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(1);
            long held = limited.getAllocatedMemory();
            // Two 17-row INT64 data buffers of 136 bytes each, which the allocator rounds to 256.
            limited.setLimit(held + 512);
            assertThatThrownBy(reader::loadNextBatch).isInstanceOf(OutOfMemoryException.class);
            assertThat(limited.getAllocatedMemory()).isEqualTo(held);
            reader.close();
            assertThat(limited.getAllocatedMemory()).isZero();
        }
    }

    private static int index(VectorSchemaRoot root, String name) {
        return root.getSchema().getFields().indexOf(root.getVector(name).getField());
    }

}
