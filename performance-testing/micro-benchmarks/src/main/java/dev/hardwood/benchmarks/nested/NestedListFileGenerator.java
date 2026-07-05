/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.nested;

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
import dev.hardwood.benchmarks.Elem;

/// Generates the `LIST<primitive>` corpus for [NestedListReadBenchmark]: for each
/// element type and null density, a list file and a flat twin carrying the identical
/// leaf stream (same values, same null positions) — the decode floor. Files hold a
/// fixed total leaf count so scan times are comparable across type and density, and
/// each is skipped when already present.
public final class NestedListFileGenerator {

    /// Fixed 3-level list leaf path for a `LIST<primitive>` written by parquet-avro
    /// with the old 2-level structure disabled.
    public static final String LIST_LEAF = "vec.list.element";

    /// Columns in the multi-column fixture — a record of this many identical
    /// `LIST<primitive>` fields, folded through one consumer to expose the
    /// consumer-side reconstruction cost that a single-column scan cannot.
    public static final int MULTI_COLUMN_COUNT = 8;

    private static final long LIST_SEED = 4242L;

    private NestedListFileGenerator() {
    }

    /// How many leaves and lists are null in a list fixture.
    public enum NullDensity {
        NONE("none", 0.0, 0.0),
        SPARSE("sparse", 0.05, 0.05),
        DENSE("dense", 0.50, 0.10);

        private final String token;
        private final double elementNullProbability;
        private final double listNullProbability;

        NullDensity(String token, double elementNullProbability, double listNullProbability) {
            this.token = token;
            this.elementNullProbability = elementNullProbability;
            this.listNullProbability = listNullProbability;
        }

        public String token() {
            return token;
        }

        boolean nullable() {
            return this != NONE;
        }
    }

    public static Path listFile(Path dir, Elem elem, NullDensity density) {
        return dir.resolve("list_" + elem.token() + "_" + density.token() + ".parquet");
    }

    public static Path listFlatFile(Path dir, Elem elem, NullDensity density) {
        return dir.resolve("list_" + elem.token() + "_" + density.token() + "_flat.parquet");
    }

    /// Writes the list fixture and its flat twin for `(elem, density)` if either is
    /// missing. Both carry the identical leaf stream, so the flat twin is the decode
    /// floor for the list column.
    public static void ensureList(Path dir, Elem elem, NullDensity density, long totalValues)
            throws IOException {
        Path listPath = listFile(dir, elem, density);
        Path flatPath = listFlatFile(dir, elem, density);
        if (BenchmarkWriter.present(listPath) && BenchmarkWriter.present(flatPath)) {
            return;
        }
        Files.createDirectories(dir);
        int total = Math.toIntExact(totalValues);
        System.out.printf("Generating list corpus elem=%s density=%s (%,d leaves)...%n",
                elem.token(), density.token(), total);

        // The full leaf stream, generated once and shared by both files.
        Random valueRng = new Random(LIST_SEED + elem.ordinal() * 31L + density.ordinal());
        long[] longs = elem == Elem.INT64 ? new long[total] : null;
        double[] doubles = elem == Elem.FLOAT64 ? new double[total] : null;
        boolean[] leafNull = new boolean[total];
        for (int i = 0; i < total; i++) {
            leafNull[i] = density.nullable() && valueRng.nextDouble() < density.elementNullProbability;
            if (elem == Elem.INT64) {
                longs[i] = valueRng.nextLong();
            }
            else {
                doubles[i] = valueRng.nextGaussian();
            }
        }

        writeListFile(listPath, elem, density, longs, doubles, leafNull);
        writeListFlatFile(flatPath, elem, density, longs, doubles, leafNull);
    }

    private static void writeListFile(Path path, Elem elem, NullDensity density,
                                      long[] longs, double[] doubles, boolean[] leafNull)
            throws IOException {
        Schema schema = listSchema(elem, density);
        Random shapeRng = new Random(LIST_SEED + 7);
        int total = leafNull.length;
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            int idx = 0;
            while (idx < total) {
                GenericRecord record = new GenericData.Record(schema);
                if (density.nullable() && shapeRng.nextDouble() < density.listNullProbability) {
                    // A null list consumes no leaves (keeps the leaf stream aligned).
                    record.put("vec", null);
                    writer.write(record);
                    continue;
                }
                int len = Math.min(BenchmarkWriter.randomLen(shapeRng), total - idx);
                List<Object> vec = new ArrayList<>(len);
                for (int j = 0; j < len; j++) {
                    vec.add(leafNull[idx] ? null : boxed(elem, longs, doubles, idx));
                    idx++;
                }
                record.put("vec", vec);
                writer.write(record);
            }
        }
    }

    private static void writeListFlatFile(Path path, Elem elem, NullDensity density,
                                          long[] longs, double[] doubles, boolean[] leafNull)
            throws IOException {
        Schema schema = flatValueSchema(elem, density.nullable());
        int total = leafNull.length;
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            for (int i = 0; i < total; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("value", leafNull[i] ? null : boxed(elem, longs, doubles, i));
                writer.write(record);
            }
        }
    }

    private static Object boxed(Elem elem, long[] longs, double[] doubles, int idx) {
        return elem == Elem.INT64 ? Long.valueOf(longs[idx]) : Double.valueOf(doubles[idx]);
    }

    private static Schema listSchema(Elem elem, NullDensity density) {
        Schema element = primitiveSchema(elem);
        if (density.nullable()) {
            element = BenchmarkWriter.optional(element);
        }
        Schema array = Schema.createArray(element);
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("list").fields();
        if (density.nullable()) {
            return fields.name("vec").type(BenchmarkWriter.optional(array)).withDefault(null).endRecord();
        }
        return fields.name("vec").type(array).noDefault().endRecord();
    }

    private static Schema flatValueSchema(Elem elem, boolean nullable) {
        Schema value = primitiveSchema(elem);
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("flat").fields();
        if (nullable) {
            return fields.name("value").type(BenchmarkWriter.optional(value)).withDefault(null).endRecord();
        }
        return fields.name("value").type(value).noDefault().endRecord();
    }

    private static Schema primitiveSchema(Elem elem) {
        return elem == Elem.INT64 ? Schema.create(Schema.Type.LONG) : Schema.create(Schema.Type.DOUBLE);
    }

    // ==================== Multi-column fixture ====================

    /// The `MULTI_COLUMN_COUNT` list-leaf paths (`vec0.list.element`, …) of the
    /// multi-column fixture, for the column-reader fold.
    public static String[] multiListLeaves() {
        String[] leaves = new String[MULTI_COLUMN_COUNT];
        for (int c = 0; c < MULTI_COLUMN_COUNT; c++) {
            leaves[c] = "vec" + c + ".list.element";
        }
        return leaves;
    }

    /// The `MULTI_COLUMN_COUNT` top-level list field names (`vec0`, …), for the
    /// row-reader fold.
    public static String[] multiListFields() {
        String[] fields = new String[MULTI_COLUMN_COUNT];
        for (int c = 0; c < MULTI_COLUMN_COUNT; c++) {
            fields[c] = "vec" + c;
        }
        return fields;
    }

    /// The `MULTI_COLUMN_COUNT` flat-twin column names (`c0`, …), the decode floor.
    public static String[] multiFlatColumns() {
        String[] columns = new String[MULTI_COLUMN_COUNT];
        for (int c = 0; c < MULTI_COLUMN_COUNT; c++) {
            columns[c] = "c" + c;
        }
        return columns;
    }

    public static Path multiListFile(Path dir, Elem elem, NullDensity density) {
        return dir.resolve("multilist_" + elem.token() + "_" + density.token() + ".parquet");
    }

    public static Path multiListFlatFile(Path dir, Elem elem, NullDensity density) {
        return dir.resolve("multilist_" + elem.token() + "_" + density.token() + "_flat.parquet");
    }

    /// Writes the multi-column fixture and its flat twin for `(elem, density)` if
    /// either is missing: a record of `MULTI_COLUMN_COUNT` identical
    /// `LIST<primitive>` columns holding `totalValues` leaves in total (so the
    /// per-file byte volume matches the single-column fixture), plus a flat twin of
    /// the same number of primitive columns carrying the identical leaf streams.
    public static void ensureMultiList(Path dir, Elem elem, NullDensity density, long totalValues)
            throws IOException {
        Path listPath = multiListFile(dir, elem, density);
        Path flatPath = multiListFlatFile(dir, elem, density);
        if (BenchmarkWriter.present(listPath) && BenchmarkWriter.present(flatPath)) {
            return;
        }
        Files.createDirectories(dir);
        // Total leaves are split evenly across the columns, which are identical, so
        // the per-column stream is generated once and written to every column.
        int perColumn = Math.toIntExact(totalValues / MULTI_COLUMN_COUNT);
        System.out.printf("Generating multi-column list corpus elem=%s density=%s "
                        + "(%d cols x %,d leaves)...%n",
                elem.token(), density.token(), MULTI_COLUMN_COUNT, perColumn);

        Random valueRng = new Random(LIST_SEED + 101 + elem.ordinal() * 31L + density.ordinal());
        long[] longs = elem == Elem.INT64 ? new long[perColumn] : null;
        double[] doubles = elem == Elem.FLOAT64 ? new double[perColumn] : null;
        boolean[] leafNull = new boolean[perColumn];
        for (int i = 0; i < perColumn; i++) {
            leafNull[i] = density.nullable() && valueRng.nextDouble() < density.elementNullProbability;
            if (elem == Elem.INT64) {
                longs[i] = valueRng.nextLong();
            }
            else {
                doubles[i] = valueRng.nextGaussian();
            }
        }

        writeMultiListFile(listPath, elem, density, longs, doubles, leafNull);
        writeMultiFlatFile(flatPath, elem, density, longs, doubles, leafNull);
    }

    private static void writeMultiListFile(Path path, Elem elem, NullDensity density,
                                           long[] longs, double[] doubles, boolean[] leafNull)
            throws IOException {
        Schema schema = multiListSchema(elem, density);
        String[] fields = multiListFields();
        Random shapeRng = new Random(LIST_SEED + 7);
        int total = leafNull.length;
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            int idx = 0;
            while (idx < total) {
                GenericRecord record = new GenericData.Record(schema);
                if (density.nullable() && shapeRng.nextDouble() < density.listNullProbability) {
                    for (String field : fields) {
                        record.put(field, null);
                    }
                    writer.write(record);
                    continue;
                }
                int len = Math.min(BenchmarkWriter.randomLen(shapeRng), total - idx);
                for (String field : fields) {
                    List<Object> vec = new ArrayList<>(len);
                    for (int j = 0; j < len; j++) {
                        vec.add(leafNull[idx + j] ? null : boxed(elem, longs, doubles, idx + j));
                    }
                    record.put(field, vec);
                }
                idx += len;
                writer.write(record);
            }
        }
    }

    private static void writeMultiFlatFile(Path path, Elem elem, NullDensity density,
                                           long[] longs, double[] doubles, boolean[] leafNull)
            throws IOException {
        Schema schema = multiFlatSchema(elem, density.nullable());
        String[] columns = multiFlatColumns();
        int total = leafNull.length;
        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            for (int i = 0; i < total; i++) {
                GenericRecord record = new GenericData.Record(schema);
                Object value = leafNull[i] ? null : boxed(elem, longs, doubles, i);
                for (String column : columns) {
                    record.put(column, value);
                }
                writer.write(record);
            }
        }
    }

    private static Schema multiListSchema(Elem elem, NullDensity density) {
        Schema element = primitiveSchema(elem);
        if (density.nullable()) {
            element = BenchmarkWriter.optional(element);
        }
        Schema array = Schema.createArray(element);
        Schema fieldType = density.nullable() ? BenchmarkWriter.optional(array) : array;
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("multilist").fields();
        for (int c = 0; c < MULTI_COLUMN_COUNT; c++) {
            if (density.nullable()) {
                fields.name("vec" + c).type(fieldType).withDefault(null);
            }
            else {
                fields.name("vec" + c).type(fieldType).noDefault();
            }
        }
        return fields.endRecord();
    }

    private static Schema multiFlatSchema(Elem elem, boolean nullable) {
        Schema value = primitiveSchema(elem);
        Schema fieldType = nullable ? BenchmarkWriter.optional(value) : value;
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("multiflat").fields();
        for (int c = 0; c < MULTI_COLUMN_COUNT; c++) {
            if (nullable) {
                fields.name("c" + c).type(fieldType).withDefault(null);
            }
            else {
                fields.name("c" + c).type(fieldType).noDefault();
            }
        }
        return fields.endRecord();
    }

    /// Generates the list corpus for every element type and null density.
    public static void generateAll(Path dir, long totalValues) throws IOException {
        for (Elem elem : Elem.values()) {
            for (NullDensity density : NullDensity.values()) {
                ensureList(dir, elem, density, totalValues);
                ensureMultiList(dir, elem, density, totalValues);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : BenchmarkData.dir());
        generateAll(dir, BenchmarkData.totalValues());
        System.out.println("Nested-list corpus ready in " + dir);
    }
}
