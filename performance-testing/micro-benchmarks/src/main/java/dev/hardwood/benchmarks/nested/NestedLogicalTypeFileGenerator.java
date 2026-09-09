/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.nested;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.BenchmarkWriter;
import dev.hardwood.benchmarks.nested.NestedListFileGenerator.NullDensity;

/// Generates the `LIST<annotated>` corpus for [NestedLogicalTypeReadBenchmark]: a list
/// column per logical type and null density, holding a fixed total leaf count so scan
/// times are comparable across type and density. Each file is skipped when present.
///
/// The three types span the representations a leaf decode starts from — a 4-byte
/// int32, an 8-byte int64, and a fixed-width byte array — so the corpus covers each
/// way an annotated value reaches its Java type.
public final class NestedLogicalTypeFileGenerator {

    /// Fixed 3-level list leaf path, matching [NestedListFileGenerator#LIST_LEAF].
    public static final String LIST_LEAF = "vec.list.element";

    /// `DECIMAL(20, 4)` needs 9 bytes, which is what the fixed element declares.
    public static final int DECIMAL_PRECISION = 20;
    public static final int DECIMAL_SCALE = 4;
    private static final int DECIMAL_BYTES = 9;

    private static final long SEED = 8484L;

    private NestedLogicalTypeFileGenerator() {
    }

    /// The annotated element types the corpus covers, each named by the physical type
    /// its decode reads from.
    public enum LogicalElem {
        /// `DATE` over `INT32`.
        DATE("date"),
        /// `TIMESTAMP(MICROS, isAdjustedToUTC = true)` over `INT64`.
        TIMESTAMP("timestamp"),
        /// `DECIMAL(20, 4)` over `FIXED_LEN_BYTE_ARRAY(9)`.
        DECIMAL("decimal");

        private final String token;

        LogicalElem(String token) {
            this.token = token;
        }

        public String token() {
            return token;
        }
    }

    public static Path listFile(Path dir, LogicalElem elem, NullDensity density) {
        return dir.resolve("list_logical_" + elem.token() + "_" + density.token() + ".parquet");
    }

    /// Writes the list file for `elem` / `density` if it is not already present.
    public static void ensureList(Path dir, LogicalElem elem, NullDensity density, long totalValues)
            throws IOException {
        Path path = listFile(dir, elem, density);
        if (BenchmarkWriter.present(path)) {
            return;
        }
        Files.createDirectories(dir);
        int total = Math.toIntExact(totalValues);
        System.out.printf("Generating logical-type list corpus elem=%s density=%s (%,d leaves)...%n",
                elem.token(), density.token(), total);
        writeListFile(path, elem, density, total);
    }

    private static void writeListFile(Path path, LogicalElem elem, NullDensity density, int total)
            throws IOException {
        Random valueRng = new Random(SEED);
        Random shapeRng = new Random(SEED + 1);
        Schema schema = listSchema(elem, density.nullable());
        Schema elementSchema = elementSchema(elem);

        try (ParquetWriter<GenericRecord> writer = BenchmarkWriter.create(path, schema)) {
            int idx = 0;
            while (idx < total) {
                GenericRecord record = new GenericData.Record(schema);
                if (density.nullable() && shapeRng.nextDouble() < density.listNullProbability()) {
                    // A null list consumes no leaves (keeps the leaf stream aligned).
                    record.put("vec", null);
                    writer.write(record);
                    continue;
                }
                int len = Math.min(BenchmarkWriter.randomLen(shapeRng), total - idx);
                List<Object> vec = new ArrayList<>(len);
                for (int j = 0; j < len; j++) {
                    boolean isNull = density.nullable()
                            && valueRng.nextDouble() < density.elementNullProbability();
                    vec.add(isNull ? null : value(elem, elementSchema, valueRng));
                    idx++;
                }
                record.put("vec", vec);
                writer.write(record);
            }
        }
    }

    /// One element value in the representation parquet-avro writes it as. No logical-type
    /// conversions are registered on the writer, so each is the underlying physical value.
    private static Object value(LogicalElem elem, Schema elementSchema, Random rng) {
        return switch (elem) {
            // Days since the epoch, spread over roughly 25 years.
            case DATE -> rng.nextInt(9000);
            // Microseconds since the epoch, spread over roughly four months.
            case TIMESTAMP -> 1_600_000_000_000_000L + rng.nextInt(10_000_000);
            case DECIMAL -> new GenericData.Fixed(elementSchema,
                    unscaledBytes(rng.nextLong() >> 24));
        };
    }

    /// `unscaled` as [#DECIMAL_BYTES] bytes, big-endian two's complement — the layout
    /// Parquet stores a `FIXED_LEN_BYTE_ARRAY` decimal in. Sign-extends the narrower
    /// array [BigInteger] produces.
    private static byte[] unscaledBytes(long unscaled) {
        byte[] minimal = BigInteger.valueOf(unscaled).toByteArray();
        byte[] padded = new byte[DECIMAL_BYTES];
        byte sign = (byte) (unscaled < 0 ? 0xFF : 0x00);
        int copyFrom = Math.max(0, minimal.length - DECIMAL_BYTES);
        int copyLen = minimal.length - copyFrom;
        java.util.Arrays.fill(padded, 0, DECIMAL_BYTES - copyLen, sign);
        System.arraycopy(minimal, copyFrom, padded, DECIMAL_BYTES - copyLen, copyLen);
        return padded;
    }

    private static Schema listSchema(LogicalElem elem, boolean nullable) {
        Schema element = elementSchema(elem);
        if (nullable) {
            element = BenchmarkWriter.optional(element);
        }
        Schema array = Schema.createArray(element);
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("logicalList").fields();
        if (nullable) {
            return fields.name("vec").type(BenchmarkWriter.optional(array)).withDefault(null).endRecord();
        }
        return fields.name("vec").type(array).noDefault().endRecord();
    }

    private static Schema elementSchema(LogicalElem elem) {
        return switch (elem) {
            case DATE -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case TIMESTAMP -> LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case DECIMAL -> LogicalTypes.decimal(DECIMAL_PRECISION, DECIMAL_SCALE)
                    .addToSchema(Schema.createFixed("dec", null, "dev.hardwood.benchmarks", DECIMAL_BYTES));
        };
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : BenchmarkData.dir());
        long totalValues = BenchmarkData.totalValues();
        for (LogicalElem elem : LogicalElem.values()) {
            for (NullDensity density : NullDensity.values()) {
                ensureList(dir, elem, density, totalValues);
            }
        }
    }
}
