/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.FlatBatch;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.engine.PreparedRead;
import dev.hardwood.internal.reader.engine.ReadEngine;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ColumnRequests;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.Compression;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A batch of a nullable binary column whose values all come from dictionary pages allocates no
/// view arrays, flat or nested: its values are entry ids, and its nulls read as empty, through
/// the batch's accessors and through its window alike.
///
/// The fixture has a nullable string column and a nullable list of nullable strings, both
/// dictionary-encoded, over several pages per row group.
class DictionaryNullViewsTest {

    private static final int RECORDS = 300;
    private static final int BATCH = 32;

    @TempDir
    static Path dir;

    private static Path file;

    @BeforeAll
    static void writeFixture() throws Exception {
        byte[][] flat = new byte[RECORDS][];
        boolean[] flatNulls = new boolean[RECORDS];
        int[] offsets = new int[RECORDS + 1];
        boolean[] listNulls = new boolean[RECORDS];
        List<byte[]> elements = new ArrayList<>();
        List<Boolean> elementNulls = new ArrayList<>();
        for (int r = 0; r < RECORDS; r++) {
            flatNulls[r] = r % 5 == 0;
            flat[r] = flatNulls[r] ? null : bytes("v" + r % 3);
            listNulls[r] = r % 7 == 3;
            if (!listNulls[r]) {
                for (int k = 0; k < r % 4; k++) {
                    int i = elements.size();
                    elementNulls.add(i % 5 == 2);
                    elements.add(i % 5 == 2 ? null : bytes("e" + i % 3));
                }
            }
            offsets[r + 1] = elements.size();
        }
        boolean[] elementNullArray = new boolean[elements.size()];
        for (int i = 0; i < elementNullArray.length; i++) {
            elementNullArray[i] = elementNulls.get(i);
        }
        FileSchema schema = FileSchema.builder("schema")
                .primitive("s", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        col -> col.logicalType(LogicalType.string()))
                .list("tags", RepetitionType.OPTIONAL,
                        el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        col -> col.logicalType(LogicalType.string())))
                .build();
        WriterConfig config = WriterConfig.builder()
                .compression(Compression.uncompressed())
                .pageTargetRows(20)
                .rowGroupTargetRows(150)
                .build();
        file = dir.resolve("dictionary_nulls.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes("s", flat, flatNulls)
                    .list("tags", offsets, Validity.ofNulls(listNulls))
                    .bytes("tags.list.element", elements.toArray(new byte[0][]), elementNullArray));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aFlatBatchWithNullsHasNoViewArrays() throws Exception {
        List<FlatBatch> batches = readFlat();

        List<String> values = new ArrayList<>();
        for (FlatBatch batch : batches) {
            BinaryBatchValues binary = (BinaryBatchValues) batch.values;
            assertThat(binary.dictionary).isNotNull();
            assertThat(binary.viewArraysAllocated()).isFalse();
            for (int i = 0; i < batch.recordCount; i++) {
                boolean present = batch.validity == null || (batch.validity[i >>> 6] & (1L << i)) != 0;
                values.add(render(binary, i, present, batch.recordCount));
            }
        }
        List<String> expected = new ArrayList<>();
        for (int r = 0; r < RECORDS; r++) {
            expected.add(r % 5 == 0 ? "null" : "v" + r % 3);
        }
        assertThat(values).isEqualTo(expected);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aNestedBatchWithNullsHasNoViewArrays() throws Exception {
        List<NestedBatch> batches = readNested();

        List<String> values = new ArrayList<>();
        for (NestedBatch batch : batches) {
            BinaryBatchValues binary = (BinaryBatchValues) batch.values;
            assertThat(binary.dictionary).isNotNull();
            assertThat(binary.viewArraysAllocated()).isFalse();
            for (int i = 0; i < batch.valueCount; i++) {
                values.add(render(binary, i, batch.definitionLevels[i] == 3, batch.valueCount));
            }
        }
        List<String> expected = new ArrayList<>();
        int element = 0;
        for (int r = 0; r < RECORDS; r++) {
            int length = r % 7 == 3 ? 0 : r % 4;
            if (length == 0) {
                // A null or empty list takes one slot
                expected.add("null");
            }
            for (int k = 0; k < length; k++, element++) {
                expected.add(element % 5 == 2 ? "null" : "e" + element % 3);
            }
        }
        assertThat(values).isEqualTo(expected);
    }

    /// Value `i` as its string, `"null"` for a null, checking that its bytes read the same through
    /// the accessors and the window of the batch's `count` values, and are empty for a null.
    private static String render(BinaryBatchValues values, int i, boolean present, int count) {
        BinaryBatchValues.Window window = values.window(count);
        int start = window.starts()[i];
        String windowed = new String(window.buffers()[window.bufferIds()[i]], start, window.ends()[i] - start,
                StandardCharsets.UTF_8);
        String accessed = new String(values.byteArrayAt(i), StandardCharsets.UTF_8);
        assertThat(windowed).isEqualTo(accessed);
        assertThat(values.stringAt(i)).isEqualTo(accessed);
        if (!present) {
            assertThat(accessed).isEmpty();
            assertThat(values.end(i) - values.start(i)).isZero();
            return "null";
        }
        return accessed;
    }

    private static List<FlatBatch> readFlat() throws Exception {
        FileSchema schema = schema();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0)) {
            InputFile inputFile = InputFile.of(file);
            inputFile.open();
            BatchExchange<FlatBatch> exchange = BatchExchange.detaching(schema.getColumn(0).name(), () -> {
                FlatBatch b = new FlatBatch();
                b.matches = new long[1];
                return b;
            }, batch -> batch.capacity, BATCH);
            ProjectedSchema decoded = ProjectedSchema.create(schema, ColumnProjection.columns("s"));
            List<NestedLevelComputer.Layers> flat = new ArrayList<>();
            flat.add(null);
            PreparedRead prepared = ReadEngines.prepared(List.of(inputFile), context, ColumnProjection.columns("s"),
                    null, 0);
            try (ReadEngine engine = prepared.start(decoded, flat, NestedIndexMode.ALL_ITEMS, false, BATCH, BATCH,
                    List.of(exchange), new ColumnBatchMatcher[]{ null })) {
                List<FlatBatch> batches = new ArrayList<>();
                FlatBatch batch;
                while ((batch = exchange.poll()) != null) {
                    batches.add(batch);
                }
                exchange.checkError();
                return batches;
            }
        }
    }

    private static List<NestedBatch> readNested() throws Exception {
        FileSchema schema = schema();
        ColumnSchema column = schema.getColumn(1);
        NestedLevelComputer.Layers layers = NestedLevelComputer.computeLayers(schema)[1];
        ProjectedSchema decoded = ProjectedSchema.create(schema, ColumnRequests.ofColumns(1), false);
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            InputFile inputFile = InputFile.of(file);
            inputFile.open();
            BatchExchange<NestedBatch> exchange = BatchExchange.detachingNested(column.name(), BATCH);
            PreparedRead prepared = ReadEngines.prepared(List.of(inputFile), context,
                    ignored -> ReadProjection.of(decoded), null, 0);
            try (ReadEngine engine = prepared.start(decoded, List.of(layers), NestedIndexMode.ALL_ITEMS, true, BATCH,
                    BATCH, List.of(exchange), null)) {
                List<NestedBatch> batches = new ArrayList<>();
                NestedBatch batch;
                while ((batch = exchange.poll()) != null && batch.recordCount > 0) {
                    batches.add(batch);
                }
                exchange.checkError();
                return batches;
            }
        }
    }

    private static FileSchema schema() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return reader.getFileSchema();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
