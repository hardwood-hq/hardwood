/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.mixed;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.BenchmarkWriter;

/// Generates the fixed-shape numeric corpus for [MixedSchemaReadBenchmark]: the mixed
/// scalar+list schema and its flat-scalar twin (#732), a non-repeated struct and its
/// flat twin, a repeated-heavy file, and the two `> 1` repetition-layer depth files.
/// Each file is skipped when already present.
public final class MixedSchemaFileGenerator {

    private static final long SCALAR_SEED = 99L;
    private static final long LIST_SEED = 4242L;

    private MixedSchemaFileGenerator() {
    }

    // ==================== File name resolution ====================

    public static Path mixedFile(Path dir) {
        return dir.resolve("mixed.parquet");
    }

    public static Path flatScalarsFile(Path dir) {
        return dir.resolve("flat_scalars.parquet");
    }

    public static Path structFile(Path dir) {
        return dir.resolve("struct.parquet");
    }

    public static Path structFlatFile(Path dir) {
        return dir.resolve("struct_flat.parquet");
    }

    public static Path repeatedHeavyFile(Path dir) {
        return dir.resolve("repeated_heavy.parquet");
    }

    public static Path listOfListFile(Path dir) {
        return dir.resolve("list_of_list.parquet");
    }

    public static Path listOfStructFile(Path dir) {
        return dir.resolve("list_of_struct.parquet");
    }

    // ==================== Ensure entry points ====================

    public static void ensureMixed(Path dir, long rows) throws IOException {
        Path mixedPath = mixedFile(dir);
        Path flatPath = flatScalarsFile(dir);
        if (BenchmarkWriter.present(mixedPath) && BenchmarkWriter.present(flatPath)) {
            return;
        }
        Files.createDirectories(dir);
        int rowCount = Math.toIntExact(rows);
        System.out.printf("Generating mixed / flat-scalar corpus (%,d rows)...%n", rowCount);
        writeMixed(mixedPath, rowCount, true);
        writeMixed(flatPath, rowCount, false);
    }

    public static void ensureStruct(Path dir, long rows) throws IOException {
        Path structPath = structFile(dir);
        Path flatPath = structFlatFile(dir);
        if (BenchmarkWriter.present(structPath) && BenchmarkWriter.present(flatPath)) {
            return;
        }
        Files.createDirectories(dir);
        int rowCount = Math.toIntExact(rows);
        System.out.printf("Generating struct / struct-flat corpus (%,d rows)...%n", rowCount);
        writeStruct(structPath, rowCount, true);
        writeStruct(flatPath, rowCount, false);
    }

    public static void ensureRepeatedHeavy(Path dir, long totalValues) throws IOException {
        Path path = repeatedHeavyFile(dir);
        if (BenchmarkWriter.present(path)) {
            return;
        }
        Files.createDirectories(dir);
        System.out.printf("Generating repeated-heavy corpus (%,d leaves x 4 columns)...%n", totalValues);
        writeRepeatedHeavy(path, Math.toIntExact(totalValues));
    }

    public static void ensureListOfList(Path dir, long totalValues) throws IOException {
        Path path = listOfListFile(dir);
        if (BenchmarkWriter.present(path)) {
            return;
        }
        Files.createDirectories(dir);
        System.out.printf("Generating list-of-list corpus (%,d leaves)...%n", totalValues);
        writeListOfList(path, Math.toIntExact(totalValues));
    }

    public static void ensureListOfStruct(Path dir, long totalValues) throws IOException {
        Path path = listOfStructFile(dir);
        if (BenchmarkWriter.present(path)) {
            return;
        }
        Files.createDirectories(dir);
        System.out.printf("Generating list-of-struct corpus (%,d elements)...%n", totalValues);
        writeListOfStruct(path, Math.toIntExact(totalValues));
    }

    /// Generates the full mixed-schema corpus.
    public static void generateAll(Path dir, long rows, long totalValues) throws IOException {
        ensureMixed(dir, rows);
        ensureStruct(dir, rows);
        ensureRepeatedHeavy(dir, totalValues);
        ensureListOfList(dir, totalValues);
        ensureListOfStruct(dir, totalValues);
    }

    // ==================== Mixed / flat-scalar fixtures ====================

    /// 12 non-repeated scalar columns; when `withLists` is set, two `LIST<double>`
    /// columns are added, which is what routes the file onto the nested reader. The
    /// scalar stream is drawn from a fixed seed in a fixed order, so `mixed` and
    /// `flat_scalars` hold byte-identical scalar values regardless of the lists.
    private static void writeMixed(Path path, int rows, boolean withLists) throws IOException {
        Schema schema = mixedSchema(withLists);
        Random scalarRng = new Random(SCALAR_SEED);
        Random listRng = new Random(LIST_SEED);
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            for (int r = 0; r < rows; r++) {
                GenericRecord record = new GenericData.Record(schema);
                for (int c = 0; c < 4; c++) {
                    record.put("i" + c, scalarRng.nextInt());
                }
                for (int c = 0; c < 4; c++) {
                    record.put("l" + c, scalarRng.nextLong());
                }
                for (int c = 0; c < 4; c++) {
                    record.put("d" + c, scalarRng.nextGaussian());
                }
                if (withLists) {
                    record.put("vec1", shortDoubleList(listRng));
                    record.put("vec2", shortDoubleList(listRng));
                }
                writer.write(record);
            }
        }
    }

    private static List<Double> shortDoubleList(Random rng) {
        int len = 1 + rng.nextInt(8);
        List<Double> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(rng.nextGaussian());
        }
        return list;
    }

    // ==================== Struct fixtures ====================

    private static void writeStruct(Path path, int rows, boolean nested) throws IOException {
        Schema schema = nested ? structSchema() : structFlatSchema();
        Schema inner = nested ? schema.getField("s").schema() : schema;
        Random rng = new Random(SCALAR_SEED + 1);
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            for (int r = 0; r < rows; r++) {
                GenericRecord leaves = new GenericData.Record(inner);
                leaves.put("a", rng.nextLong());
                leaves.put("b", rng.nextGaussian());
                leaves.put("c", rng.nextInt());
                if (nested) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("s", leaves);
                    writer.write(record);
                }
                else {
                    writer.write(leaves);
                }
            }
        }
    }

    // ==================== Repeated-heavy / depth fixtures ====================

    private static void writeRepeatedHeavy(Path path, int totalPerColumn) throws IOException {
        Schema schema = SchemaBuilder.record("repeated").fields()
                .name("vec0").type(requiredDoubleArray()).noDefault()
                .name("vec1").type(requiredDoubleArray()).noDefault()
                .name("vec2").type(requiredDoubleArray()).noDefault()
                .name("vec3").type(requiredDoubleArray()).noDefault()
                .endRecord();
        Random shapeRng = new Random(LIST_SEED + 11);
        Random valueRng = new Random(LIST_SEED + 12);
        // Rows are bounded by the first column reaching the leaf target; every column
        // uses the same list length per row so the file stays rectangular.
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            int emitted = 0;
            while (emitted < totalPerColumn) {
                int len = Math.min(BenchmarkWriter.randomLen(shapeRng), totalPerColumn - emitted);
                GenericRecord record = new GenericData.Record(schema);
                for (int c = 0; c < 4; c++) {
                    List<Double> vec = new ArrayList<>(len);
                    for (int j = 0; j < len; j++) {
                        vec.add(valueRng.nextGaussian());
                    }
                    record.put("vec" + c, vec);
                }
                writer.write(record);
                emitted += len;
            }
        }
    }

    private static void writeListOfList(Path path, int totalLeaves) throws IOException {
        Schema doubleArray = requiredDoubleArray();
        Schema outer = Schema.createArray(doubleArray);
        Schema schema = SchemaBuilder.record("listOfList").fields()
                .name("vec").type(outer).noDefault()
                .endRecord();
        Random shapeRng = new Random(LIST_SEED + 21);
        Random valueRng = new Random(LIST_SEED + 22);
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            int emitted = 0;
            while (emitted < totalLeaves) {
                int innerLists = 1 + shapeRng.nextInt(4);
                List<List<Double>> vec = new ArrayList<>(innerLists);
                for (int il = 0; il < innerLists && emitted < totalLeaves; il++) {
                    int len = Math.min(1 + shapeRng.nextInt(BenchmarkWriter.MEAN_LIST_LEN), totalLeaves - emitted);
                    List<Double> inner = new ArrayList<>(len);
                    for (int j = 0; j < len; j++) {
                        inner.add(valueRng.nextGaussian());
                    }
                    vec.add(inner);
                    emitted += len;
                }
                GenericRecord record = new GenericData.Record(schema);
                record.put("vec", vec);
                writer.write(record);
            }
        }
    }

    private static void writeListOfStruct(Path path, int totalElements) throws IOException {
        Schema element = SchemaBuilder.record("element").fields()
                .requiredLong("a")
                .requiredDouble("b")
                .endRecord();
        Schema array = Schema.createArray(element);
        Schema schema = SchemaBuilder.record("listOfStruct").fields()
                .name("vec").type(array).noDefault()
                .endRecord();
        Random shapeRng = new Random(LIST_SEED + 31);
        Random valueRng = new Random(LIST_SEED + 32);
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            int emitted = 0;
            while (emitted < totalElements) {
                int len = Math.min(BenchmarkWriter.randomLen(shapeRng), totalElements - emitted);
                List<GenericRecord> vec = new ArrayList<>(len);
                for (int j = 0; j < len; j++) {
                    GenericRecord e = new GenericData.Record(element);
                    e.put("a", valueRng.nextLong());
                    e.put("b", valueRng.nextGaussian());
                    vec.add(e);
                    emitted++;
                }
                GenericRecord record = new GenericData.Record(schema);
                record.put("vec", vec);
                writer.write(record);
            }
        }
    }

    // ==================== Schemas ====================

    private static Schema mixedSchema(boolean withLists) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("mixed").fields();
        for (int c = 0; c < 4; c++) {
            fields = fields.requiredInt("i" + c);
        }
        for (int c = 0; c < 4; c++) {
            fields = fields.requiredLong("l" + c);
        }
        for (int c = 0; c < 4; c++) {
            fields = fields.requiredDouble("d" + c);
        }
        if (withLists) {
            fields = fields.name("vec1").type(requiredDoubleArray()).noDefault();
            fields = fields.name("vec2").type(requiredDoubleArray()).noDefault();
        }
        return fields.endRecord();
    }

    private static Schema structSchema() {
        Schema inner = SchemaBuilder.record("s").fields()
                .requiredLong("a")
                .requiredDouble("b")
                .requiredInt("c")
                .endRecord();
        return SchemaBuilder.record("structWrapper").fields()
                .name("s").type(inner).noDefault()
                .endRecord();
    }

    private static Schema structFlatSchema() {
        return SchemaBuilder.record("structFlat").fields()
                .requiredLong("a")
                .requiredDouble("b")
                .requiredInt("c")
                .endRecord();
    }

    private static Schema requiredDoubleArray() {
        return Schema.createArray(Schema.create(Schema.Type.DOUBLE));
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : BenchmarkData.dir());
        generateAll(dir, BenchmarkData.rows(), BenchmarkData.totalValues());
        System.out.println("Mixed-schema corpus ready in " + dir);
    }
}
