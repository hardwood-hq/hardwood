/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static dev.hardwood.arrow.ArrowTestSupport.assertSameTable;
import static dev.hardwood.arrow.ArrowTestSupport.coreFixture;
import static dev.hardwood.arrow.ArrowTestSupport.expected;
import static dev.hardwood.arrow.ArrowTestSupport.fixture;
import static dev.hardwood.arrow.ArrowTestSupport.readAll;
import static dev.hardwood.arrow.ArrowTestSupport.validateVector;
import static dev.hardwood.arrow.ArrowTestSupport.validateVectors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Arrow dictionary-encoded output: Parquet dictionary pages mapped directly to Arrow dictionary
/// vectors, reused within a row group, replaced across row groups and on plain fallback, and
/// carried over Arrow IPC streams.
class ArrowDictionaryTest {

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void allocatorIsEmpty() {
        assertThat(allocator.getAllocatedMemory()).isZero();
        allocator.close();
    }

    @Test
    void autoDictionaryEncodingCoversBinaryFixedDecimalAndFloat16(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("all_dict_types.parquet");
        FileSchema schema = FileSchema.builder("dict_types")
                .primitive("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .primitive("text", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.string()))
                .primitive("fixed4", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(4))
                .primitive("half", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(2).logicalType(LogicalType.float16()))
                .primitive("dec128", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(9).logicalType(LogicalType.decimal(20, 3)))
                .build();
        int n = 48;
        int[] ids = new int[n];
        byte[][] text = new byte[n][];
        byte[][] fixed4 = new byte[n][];
        byte[][] half = new byte[n][];
        byte[][] dec128 = new byte[n][];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            int g = i / 24;
            int k = i % 3;
            ids[i] = i;
            nulls[i] = i % 7 == 0;
            text[i] = ("g" + g + "_" + k).getBytes(StandardCharsets.UTF_8);
            fixed4[i] = new byte[]{ (byte) ('A' + g), (byte) ('0' + k), 'x', 'y' };
            short h = Float.floatToFloat16(g * 10.0f + k * 1.5f);
            half[i] = new byte[]{ (byte) h, (byte) (h >>> 8) };
            byte[] d = new byte[9];
            d[8] = (byte) (g * 10 + k + 1);
            dec128[i] = d;
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema,
                WriterConfig.builder().rowGroupTargetRows(24).build())) {
            writer.columnWriter().writeBatch(b -> b.ints("id", ids).bytes("text", text, nulls)
                    .fixed("fixed4", fixed4, nulls).fixed("half", half, nulls).fixed("dec128", dec128, nulls));
        }
        try (ParquetFileReader plainFile = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader plainReader = ArrowReaders.batchReader(plainFile, allocator);
             VectorSchemaRoot expected = readAll(plainReader, allocator);
             ParquetFileReader dictFile = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader dictReader = ArrowReaders.buildBatchReader(dictFile, allocator)
                     .dictionaryEncoding(true).batchSize(10).build();
             VectorSchemaRoot decoded = readAll(dictReader, allocator)) {
            assertThat(dictReader.getDictionaryIds()).hasSize(4);
            assertThat(dictReader.getVectorSchemaRoot().getSchema().findField("id").getDictionary()).isNull();
            Field textField = dictReader.getVectorSchemaRoot().getSchema().findField("text");
            assertThat(textField.getDictionary()).isNotNull();
            assertThat(textField.getType()).isEqualTo(new ArrowType.Int(32, true));
            assertSameTable(expected, decoded);
        }
    }

    @Test
    void dictionaryReusesWithinRowGroupAndReplacesAcrossRowGroups() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(coreFixture("dict_cross_chunk")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .projection(ColumnProjection.columns("label"))
                     .dictionaryEncoding(true).batchSize(10).build()) {
            long dictId = reader.getVectorSchemaRoot().getSchema().findField("label").getDictionary().getId();
            assertThat(reader.loadNextBatch()).isTrue();
            Dictionary dict = reader.lookup(dictId);
            long firstAddress = dict.getVector().getDataBufferAddress();

            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(dict.getVector().getDataBufferAddress()).isEqualTo(firstAddress);

            boolean replaced = false;
            while (reader.loadNextBatch()) {
                validateVectors(reader.getVectorSchemaRoot(), reader);
                if (dict.getVector().getDataBufferAddress() != firstAddress) {
                    replaced = true;
                }
            }
            assertThat(replaced).isTrue();
        }
    }

    @Test
    void plainFallbackInt96AndExplicitColumnsMatchPlainReads() throws IOException {
        for (String name : List.of("dict_plain_fallback", "dict_int96")) {
            Path file = coreFixture(name);
            try (ParquetFileReader plainFile = ParquetFileReader.open(InputFile.of(file));
                 ArrowBatchReader plainReader = ArrowReaders.batchReader(plainFile, allocator);
                 VectorSchemaRoot expected = readAll(plainReader, allocator);
                 ParquetFileReader dictFile = ParquetFileReader.open(InputFile.of(file));
                 ArrowBatchReader dictReader = ArrowReaders.buildBatchReader(dictFile, allocator)
                         .dictionaryEncoding(true).batchSize(700).build();
                 VectorSchemaRoot decoded = readAll(dictReader, allocator)) {
                assertThat(dictReader.getDictionaryIds()).isNotEmpty();
                assertSameTable(expected, decoded);
            }
        }
        // Explicit dictionaryColumns over plain-encoded pages.
        try (VectorSchemaRoot expected = expected("scalars_dict", allocator);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars_dict")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .dictionaryColumns("text", "fixed4", "dec256").batchSize(2).build();
             VectorSchemaRoot decoded = readAll(reader, allocator)) {
            assertThat(reader.getDictionaryIds()).hasSize(3);
            assertSameTable(expected, decoded);
        }
    }

    @Test
    void nestedDictionaryListAndAllNullOptionalStruct(@TempDir Path tempDir) throws IOException {
        // Nested list with dictionary leaf.
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(coreFixture("dict_nested_repeats")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .dictionaryEncoding(true).build()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            ListVector tags = (ListVector) root.getVector("tags");
            IntVector indices = (IntVector) tags.getDataVector();
            long dictId = indices.getField().getDictionary().getId();
            int totalNonNull = 0;
            while (reader.loadNextBatch()) {
                validateVectors(root, reader);
                VarCharVector dict = (VarCharVector) reader.lookup(dictId).getVector();
                validateVector(dict);
                for (int i = 0; i < indices.getValueCount(); i++) {
                    if (!indices.isNull(i)) {
                        assertThat(dict.getObject(indices.get(i))).isNotNull();
                        totalNonNull++;
                    }
                }
            }
            assertThat(totalNonNull).isGreaterThan(0);
        }
        // Non-nullable dictionary child inside an all-null OPTIONAL struct.
        Path file = tempDir.resolve("all_null_struct_dict.parquet");
        FileSchema schema = FileSchema.builder("root")
                .struct("box", RepetitionType.OPTIONAL, s -> s
                        .primitive("label", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                c -> c.logicalType(LogicalType.string())))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema,
                WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build())) {
            writer.columnWriter().writeBatch(b -> b
                    .struct("box", Validity.ofNulls(new boolean[]{ true, true, true }))
                    .bytes("box.label", new byte[][]{ new byte[0], new byte[0], new byte[0] }));
        }
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .dictionaryColumns("box.label").build()) {
            assertThat(reader.loadNextBatch()).isTrue();
            validateVectors(reader.getVectorSchemaRoot(), reader);
            StructVector box = (StructVector) reader.getVectorSchemaRoot().getVector("box");
            assertThat(box.getNullCount()).isEqualTo(3);
            assertThat(box.getChild("label").getNullCount()).isZero();
        }
        // Dictionary-encoded MAP keys (REQUIRED STRING inside MapVector entries struct) + IPC round-trip.
        Path mapFile = tempDir.resolve("map_dict.parquet");
        FileSchema mapSchema = FileSchema.builder("root")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        k -> k.logicalType(LogicalType.string()),
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        int mapRows = 12;
        int[] offsets = new int[mapRows + 1];
        byte[][] mapKeys = new byte[mapRows * 2][];
        int[] mapVals = new int[mapRows * 2];
        for (int i = 0; i < mapRows; i++) {
            offsets[i + 1] = (i + 1) * 2;
            mapKeys[i * 2] = "k0".getBytes(StandardCharsets.UTF_8);
            mapKeys[i * 2 + 1] = "k1".getBytes(StandardCharsets.UTF_8);
            mapVals[i * 2] = i;
            mapVals[i * 2 + 1] = i + 100;
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(mapFile), mapSchema,
                WriterConfig.defaults())) {
            writer.columnWriter().writeBatch(b -> b
                    .map("props", offsets)
                    .bytes("props.key_value.key", mapKeys)
                    .ints("props.key_value.value", mapVals));
        }
        ByteArrayOutputStream mapBytes = new ByteArrayOutputStream();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(mapFile));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .dictionaryEncoding(true).build();
             ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), reader, mapBytes)) {
            assertThat(reader.getDictionaryIds()).hasSize(1);
            writer.start();
            while (reader.loadNextBatch()) {
                validateVectors(reader.getVectorSchemaRoot(), reader);
                writer.writeBatch();
            }
            writer.end();
        }
        try (ArrowStreamReader back = new ArrowStreamReader(new ByteArrayInputStream(mapBytes.toByteArray()),
                     allocator)) {
            VectorSchemaRoot root = back.getVectorSchemaRoot();
            MapVector map = (MapVector) root.getVector("props");
            StructVector entries = (StructVector) map.getDataVector();
            IntVector keyIndices = (IntVector) entries.getChild("key");
            long keyDictId = keyIndices.getField().getDictionary().getId();
            int decodedKeys = 0;
            while (back.loadNextBatch()) {
                validateVectors(root, back);
                VarCharVector keyDict = (VarCharVector) back.lookup(keyDictId).getVector();
                for (int i = 0; i < keyIndices.getValueCount(); i++) {
                    String expectedKey = (i % 2 == 0) ? "k0" : "k1";
                    assertThat(keyDict.getObject(keyIndices.get(i)).toString()).isEqualTo(expectedKey);
                    decodedKeys++;
                }
            }
            assertThat(decodedKeys).isEqualTo(mapRows * 2);
        }
    }

    @Test
    void multiFileDictionaryReadHandlesMixedDictionaryAndPlainFiles(@TempDir Path tempDir) throws IOException {
        FileSchema schema = FileSchema.builder("multi")
                .primitive("label", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.string()))
                .build();
        byte[][] repeated = new byte[24][];
        for (int i = 0; i < repeated.length; i++) {
            repeated[i] = ("k" + (i % 3)).getBytes(StandardCharsets.UTF_8);
        }
        Path dictFile = tempDir.resolve("f1_dict.parquet");
        Path plainFile = tempDir.resolve("f2_plain.parquet");
        try (ParquetFileWriter w1 = ParquetFileWriter.create(OutputFile.of(dictFile), schema, WriterConfig.defaults());
             ParquetFileWriter w2 = ParquetFileWriter.create(OutputFile.of(plainFile), schema,
                     WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build())) {
            w1.columnWriter().writeBatch(b -> b.bytes("label", repeated));
            w2.columnWriter().writeBatch(b -> b.bytes("label", repeated));
        }
        List<InputFile> files = List.of(InputFile.of(dictFile), InputFile.of(plainFile));
        try (ParquetFileReader plainFiles = ParquetFileReader.openAll(files);
             ArrowBatchReader plainReader = ArrowReaders.batchReader(plainFiles, allocator);
             VectorSchemaRoot expected = readAll(plainReader, allocator);
             ParquetFileReader dictFiles = ParquetFileReader.openAll(files);
             ArrowBatchReader dictReader = ArrowReaders.buildBatchReader(dictFiles, allocator)
                     .dictionaryEncoding(true).batchSize(10).build();
             VectorSchemaRoot decoded = readAll(dictReader, allocator)) {
            assertThat(dictReader.getDictionaryIds()).hasSize(1);
            assertSameTable(expected, decoded);
        }
    }

    @Test
    void ipcStreamRoundTripWithReplacementDictionaries() throws IOException {
        for (Path file : List.of(coreFixture("dict_cross_chunk"), coreFixture("dict_plain_fallback"))) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                 ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                         .dictionaryEncoding(true).batchSize(700).build();
                 ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), reader, bytes)) {
                writer.start();
                while (reader.loadNextBatch()) {
                    writer.writeBatch();
                }
                writer.end();
            }
            try (ParquetFileReader plainFile = ParquetFileReader.open(InputFile.of(file));
                 ArrowBatchReader plainReader = ArrowReaders.batchReader(plainFile, allocator);
                 VectorSchemaRoot expected = readAll(plainReader, allocator);
                 ArrowStreamReader back = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()),
                         allocator);
                 VectorSchemaRoot actual = readAll(back, allocator)) {
                assertSameTable(expected, actual);
            }
        }
    }

    @Test
    void multiChunkDictionariesMatchPlainOutput(@TempDir Path tempDir) throws IOException {
        // 640 distinct entries of ~1 KiB each exceed core's 512 KiB ByteArrayDictionary chunk
        // bound, exercising the multi-chunk concatenation path for both BINARY and FIXED_BINARY.
        int distinct = 640;
        int width = 1024;
        int rows = distinct * 3;
        FileSchema schema = FileSchema.builder("multi_chunk_dict")
                .primitive("text", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.string()))
                .primitive("fixed", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        c -> c.typeLength(width))
                .build();
        byte[][] distinctText = new byte[distinct][];
        byte[][] distinctFixed = new byte[distinct][];
        for (int k = 0; k < distinct; k++) {
            int len = 960 + (k % 128);
            byte[] t = new byte[len];
            Arrays.fill(t, (byte) ('a' + (k % 26)));
            byte[] tag = Integer.toString(k).getBytes(StandardCharsets.UTF_8);
            System.arraycopy(tag, 0, t, 0, tag.length);
            distinctText[k] = t;

            byte[] f = new byte[width];
            Arrays.fill(f, (byte) (k & 0x7F));
            f[0] = (byte) (k >>> 8);
            f[1] = (byte) k;
            distinctFixed[k] = f;
        }
        byte[][] text = new byte[rows][];
        byte[][] fixed = new byte[rows][];
        boolean[] nulls = new boolean[rows];
        for (int i = 0; i < rows; i++) {
            int k = i % distinct;
            text[i] = distinctText[k];
            fixed[i] = distinctFixed[k];
            nulls[i] = i % 19 == 0;
        }
        Path file = tempDir.resolve("multi_chunk_dict.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, WriterConfig.defaults())) {
            writer.columnWriter().writeBatch(b -> b.bytes("text", text, nulls).fixed("fixed", fixed, nulls));
        }
        try (ParquetFileReader plainFile = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader plainReader = ArrowReaders.batchReader(plainFile, allocator);
             VectorSchemaRoot expected = readAll(plainReader, allocator);
             ParquetFileReader dictFile = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader dictReader = ArrowReaders.buildBatchReader(dictFile, allocator)
                     .dictionaryEncoding(true).batchSize(250).build();
             VectorSchemaRoot decoded = readAll(dictReader, allocator)) {
            assertThat(dictReader.getDictionaryIds()).hasSize(2);
            for (long id : dictReader.getDictionaryIds()) {
                assertThat(dictReader.lookup(id).getVector().getValueCount()).isEqualTo(distinct);
            }
            assertSameTable(expected, decoded);
        }
    }

    @Test
    void emptyFileOomAndBuildRejections() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("empty")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
                     .dictionaryColumns("text").build()) {
            assertThat(reader.loadNextBatch()).isFalse();
        }
        try (BufferAllocator limited = allocator.newChildAllocator("limited", 0, 2048);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(coreFixture("dict_plain_fallback")));
             ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, limited)
                     .dictionaryEncoding(true).batchSize(1000).build()) {
            assertThatThrownBy(reader::loadNextBatch).isInstanceOf(OutOfMemoryException.class);
        }
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture("scalars")))) {
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(fileReader, allocator)
                    .dictionaryColumns("i32").build())
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> ArrowReaders.buildBatchReader(fileReader, allocator)
                    .dictionaryColumns("no_such_col").build())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
