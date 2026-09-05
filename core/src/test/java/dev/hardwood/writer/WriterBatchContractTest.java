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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static dev.hardwood.writer.WriterTestSupport.oneOptionalColumn;
import static dev.hardwood.writer.WriterTestSupport.twoColumns;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The rules the columnar write API enforces, and the point at which it enforces them.
///
/// A batch is atomic and schema-bound, so a column named twice, a ragged batch, a null mask
/// on a `REQUIRED` column or offsets that disagree with the values they index are all
/// misuse the writer rejects rather than encodes. Rejection has to happen before any bytes
/// reach the file, and a writer that cannot finish a valid file must leave none behind.
///
/// These are rules about a *batch*. Rules about the *schema* — a physical type or a shape the
/// writer cannot produce — are settled before either view exists and are asserted in
/// `WriterSchemaShapeTest`.
class WriterBatchContractTest {

    @Test
    void rejectsDuplicateColumnInBatch() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }).ints(0, new int[] { 4, 5, 6 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsSameColumnByIndexAndName() throws Exception {
        // The schema binding lets the batch see that "id" is column 0, so the collision
        // is caught eagerly rather than at write time.
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }).ints("id", new int[] { 4, 5, 6 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsUnknownColumnName() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints("nope", new int[] { 1 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsOutOfRangeColumnIndex() throws Exception {
        // A leaf-column index out of range is an index error, reported the way StructBuilder's
        // field index and the reader's positional accessors report one, so a caller holding both
        // APIs catches one exception type rather than two.
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints(1, new int[] { 1 })))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessageContaining("[0, 1)");
        }
        // Separate writer: a failed writeBatch poisons the writer, so the negative-index
        // check needs its own.
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints(-1, new int[] { 1 })))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void rejectsRaggedBatch() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), twoColumns())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }).ints(1, new int[] { 1, 2 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsBatchNotCoveringAllColumns() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), twoColumns())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullMaskOnRequiredColumn() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2 }, new boolean[] { false, true })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsValidityOnRequiredColumn() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2 }, Validity.NO_NULLS)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullMaskLengthMismatch() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneOptionalColumn())) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }, new boolean[] { false, true })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsMutatingBatchAfterWrite() throws Exception {
        // A filler that stashes the batch and mutates it after writeBatch returns must
        // fail loudly rather than silently drop the values.
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            ColumnBatch[] escaped = new ColumnBatch[1];
            writer.columnWriter().writeBatch(batch -> {
                escaped[0] = batch;
                batch.ints(0, new int[] { 1, 2, 3 });
            });
            assertThatThrownBy(() -> escaped[0].ints(0, new int[] { 4 })).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectsUseAfterClose() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn());
        ColumnWriter columns = writer.columnWriter();
        columns.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
        writer.close();
        assertThatThrownBy(() -> writer.columnWriter())
                .isInstanceOf(IllegalStateException.class);
        // A view obtained before close is rejected too, rather than writing into a finished file.
        assertThatThrownBy(() -> columns.writeBatch(batch -> batch.ints(0, new int[] { 4 })))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failedCloseLeavesNoFileAtTarget(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("partial.parquet");

        // Fail while writing the footer's trailing magic: the data pages and part of the
        // footer are on disk, so close() must discard rather than publish a broken file.
        FailOnSecondMagic out = new FailOnSecondMagic(OutputFile.of(file));
        ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn());
        writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));

        assertThatThrownBy(writer::close).isInstanceOf(IOException.class);
        assertThat(Files.exists(file)).isFalse();
    }

    @Test
    void rejectsListVerbOnMapPath() throws Exception {
        // A map addressed with the list verb is a wrong-kind facet and must fail eagerly.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.list("props", new int[] { 0, 1 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsMapVerbOnListPath() throws Exception {
        // The mirror of rejectsListVerbOnMapPath: a list addressed with the map verb is a
        // wrong-kind facet and must fail eagerly.
        FileSchema schema = FileSchema.builder("schema")
                .list("items", RepetitionType.OPTIONAL,
                        el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.map("items", new int[] { 0, 1 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullMapWithNonEmptyOffsets() throws Exception {
        // A null map is absent, so its offset delta must be zero, mirroring the null-list rule.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .map("props", new int[] { 0, 1, 2 }, Validity.ofNulls(new boolean[] { true, false }))
                    .ints("props.key_value.key", new int[] { 99, 5 })
                    .ints("props.key_value.value", new int[] { 1, 2 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNonMonotonicListOffsets() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .list("v", new int[] { 0, 2, 1 })
                    .ints("v.list.element", new int[] { 7 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsListOffsetsDisagreeingWithElementCount() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            // offsets claim 2 elements, but only 3 are supplied.
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .list("v", new int[] { 0, 2 })
                    .ints("v.list.element", new int[] { 1, 2, 3 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullListWithNonEmptyOffsets() throws Exception {
        // A null list is absent, so its offset delta must be zero. A non-zero span
        // contradicts the null bit and would silently drop the stray element.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            // record 0 is null yet its offsets span one element (99); record 1 is [5].
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .list("v", new int[] { 0, 1, 2 }, Validity.ofNulls(new boolean[] { true, false }))
                    .ints("v.list.element", new int[] { 99, 5 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsAbsentStructWithNonEmptyListOffsetsBelowIt() throws Exception {
        // An absent struct encloses nothing, so a list beneath it has no entries at that
        // record. The shredder stops at the absent struct and never descends into the
        // offsets, so a non-zero span there would silently drop the elements it covers.
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .list("phones", RepetitionType.REQUIRED,
                                el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            // record 0's s is absent yet its offsets span two elements (10, 20); record 1 is [30].
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .struct("s", Validity.ofNulls(new boolean[] { true, false }))
                    .list("s.phones", new int[] { 0, 2, 3 })
                    .ints("s.phones.list.element", new int[] { 10, 20, 30 })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("s")
                    .hasMessageContaining("s.phones");
        }
    }

    @Test
    void rejectsAbsentStructWithNonEmptyMapOffsetsBelowIt() throws Exception {
        // The same rule reaches a MAP through the same layer: its entries are as absent as a
        // list's where an enclosing struct is.
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .map("props", RepetitionType.REQUIRED, PhysicalType.INT32,
                                v -> v.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .struct("s", Validity.ofNulls(new boolean[] { true, false }))
                    .map("s.props", new int[] { 0, 1, 2 })
                    .ints("s.props.key_value.key", new int[] { 1, 2 })
                    .ints("s.props.key_value.value", new int[] { 10, 20 })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("s.props");
        }
    }

    @Test
    void rejectsColumnsImplyingDifferentRecordCounts() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            // r has 3 records but v's offsets describe only 2.
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .ints("r", new int[] { 0, 1, 2 })
                    .list("v", new int[] { 0, 1, 2 })
                    .ints("v.list.element", new int[] { 5, 6 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /// An [OutputFile] that throws while writing the second `PAR1` magic — the footer's
    /// trailing marker — leaving a file whose footer never completed.
    private static final class FailOnSecondMagic implements OutputFile {

        private final OutputFile delegate;
        private int magicWrites;

        FailOnSecondMagic(OutputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void create() throws IOException {
            delegate.create();
        }

        @Override
        public void write(ByteBuffer data) throws IOException {
            if (isMagic(data) && ++magicWrites == 2) {
                throw new IOException("injected footer write failure");
            }
            delegate.write(data);
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
            delegate.discard();
        }

        private static boolean isMagic(ByteBuffer data) {
            if (data.remaining() != 4) {
                return false;
            }
            int p = data.position();
            return data.get(p) == 'P' && data.get(p + 1) == 'A' && data.get(p + 2) == 'R' && data.get(p + 3) == '1';
        }
    }
}
