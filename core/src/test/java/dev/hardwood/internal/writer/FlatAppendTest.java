/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.thrift.PageLimit;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A column that repeats nowhere on its path is appended a run of present values at a time
/// rather than through the shredder an entry at a time. The two must leave a chunk that encodes
/// to the same bytes and metadata, which these check by appending the same batches both ways —
/// across every physical type, the null shapes a flat column can take, and the cardinalities that
/// keep, abandon at a probe, or keep through the probes a chunk's dictionary. Each case is also held
/// to the outcome its cardinality is for; with nine tenths of the values null no verdict runs, and
/// every cardinality is decided at flush.
class FlatAppendTest {

    private static final int RECORDS_PER_BATCH = 20_000;
    private static final int BATCHES = 2;
    private static final int CHUNK_RECORDS = RECORDS_PER_BATCH * BATCHES;

    /// Where the column sits: its own repetition and the optional structs around it.
    enum Shape {
        REQUIRED,
        OPTIONAL,
        OPTIONAL_IN_STRUCT,
        REQUIRED_IN_STRUCT,
        OPTIONAL_IN_TWO_STRUCTS
    }

    /// How many distinct values the column holds, which decides what becomes of its dictionary.
    enum Cardinality {
        /// A handful: the dictionary is kept.
        FEW,
        /// Every value distinct: the dictionary is abandoned at a probe.
        ALL_DISTINCT,
        /// Distinct at first, repeating after: the dictionary is kept through the verdicts. The
        /// distinct values count from the first record of the file rather than of the chunk, so
        /// whether a verdict falls inside them, and is lost, depends on the null ratio and the row
        /// group; the case asserts the dictionary's survival, not a lost verdict.
        FRONT_LOADED,
        /// Distinct through both probes, then repeating the chunk's first values: the dictionary
        /// is given up at a probe, and the types whose dictionary wins over the chunk are interned
        /// again at flush.
        RECOVERED
    }

    static Stream<Arguments> cases() {
        List<Arguments> cases = new ArrayList<>();
        PhysicalType[] types = { PhysicalType.INT32, PhysicalType.INT64, PhysicalType.FLOAT, PhysicalType.DOUBLE,
                PhysicalType.BOOLEAN, PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY };
        for (PhysicalType type : types) {
            for (Shape shape : Shape.values()) {
                // A REQUIRED leaf has no nulls to draw and its seed leaves the ratio out, so every
                // ratio would append the same batches.
                double[] nullRatios = shape == Shape.REQUIRED ? new double[]{ 0.0 } : new double[]{ 0.0, 0.1, 0.9 };
                for (double nullRatio : nullRatios) {
                    for (Cardinality cardinality : Cardinality.values()) {
                        cases.add(Arguments.of(type, shape, nullRatio, cardinality, ColumnEncoding.AUTO));
                    }
                }
                cases.add(Arguments.of(type, shape, 0.1, Cardinality.FEW, ColumnEncoding.PLAIN));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} {1} nulls={2} {3} {4}")
    @MethodSource("cases")
    void flatAppendMatchesShreddedAppend(PhysicalType type, Shape shape, double nullRatio,
                                         Cardinality cardinality, ColumnEncoding encoding) throws IOException {
        FileSchema schema = schema(type, shape);
        // A Bloom filter on some cases, over the values each buffer ends up holding; never on
        // BOOLEAN, which cannot have one.
        double bloomFilterFpp = cardinality == Cardinality.FEW && type != PhysicalType.BOOLEAN
                ? 0.01
                : ColumnChunkBuffer.NO_BLOOM_FILTER;
        ColumnChunkBuffer flat = buffer(schema, encoding, bloomFilterFpp);
        ColumnChunkBuffer shredded = buffer(schema, encoding, bloomFilterFpp);
        RecordShredder shredder = new RecordShredder(schema);
        Random random = new Random(type.ordinal() * 1000L + shape.ordinal() * 100 + cardinality.ordinal());

        // Two row groups, the second after a reset, so the reuse of each buffer's stores is
        // covered as well.
        for (int rowGroup = 0; rowGroup < 2; rowGroup++) {
            for (int batch = 0; batch < BATCHES; batch++) {
                int firstRecord = (rowGroup * BATCHES + batch) * RECORDS_PER_BATCH;
                bind(shredder, schema, type, shape, nullRatio, cardinality, firstRecord, random);
                ColumnSource source = sourceFor(type, cardinality, firstRecord);
                assertThat(shredder.isFlat(0)).isTrue();
                for (int from = 0; from < RECORDS_PER_BATCH; ) {
                    int count = Math.min(RECORDS_PER_BATCH - from, 1 + random.nextInt(5000));
                    flat.append(shredder, source, 0, from, count);
                    shredded.appendShredded(shredder, source, 0, from, count);
                    from += count;
                    // As the row group does at a slice's end, with the chunk's own length as the
                    // projection, so both paths are judged against the same one.
                    flat.publishProjection(CHUNK_RECORDS);
                    shredded.publishProjection(CHUNK_RECORDS);
                }
            }
            assertThat(flat.retainedBytes()).isEqualTo(shredded.retainedBytes());
            // The cardinality reaches the outcome it is for, where a verdict can run at all.
            assertThat(flat.abandonedAtVerdict()).as("dictionary given up at a verdict")
                    .isEqualTo(abandonedAtVerdict(type, nullRatio, cardinality, encoding));
            assertThat(shredded.abandonedAtVerdict()).as("shredded: dictionary given up at a verdict")
                    .isEqualTo(flat.abandonedAtVerdict());
            InMemoryOutputFile flatOut = OutputFile.inMemory();
            InMemoryOutputFile shreddedOut = OutputFile.inMemory();
            flatOut.create();
            shreddedOut.create();
            ColumnChunkBuffer.Flushed flatChunk = flat.encodeTo(ChunkSink.to(flatOut), schema.getColumn(0), null);
            ColumnChunkBuffer.Flushed shreddedChunk = shredded.encodeTo(ChunkSink.to(shreddedOut), schema.getColumn(0), null);
            assertThat(bytes(flatOut)).isEqualTo(bytes(shreddedOut));
            assertThat(flatChunk).usingRecursiveComparison().isEqualTo(shreddedChunk);
            flat.reset();
            shredded.reset();
        }
    }

    /// Whether the chunk's dictionary is given up at a verdict. The two verdicts that give it up fall
    /// at 8,192 and 16,384 present values, which a chunk of 40,000 records reaches with a tenth of
    /// them null but not with nine tenths; at that ratio every cardinality is decided at flush.
    /// `BOOLEAN` is never dictionary-encoded, and a `PLAIN` policy keeps no dictionary to give up.
    private static boolean abandonedAtVerdict(PhysicalType type, double nullRatio, Cardinality cardinality,
                                              ColumnEncoding encoding) {
        boolean verdictsRun = nullRatio <= 0.1 && type != PhysicalType.BOOLEAN && encoding == ColumnEncoding.AUTO;
        return verdictsRun && (cardinality == Cardinality.ALL_DISTINCT || cardinality == Cardinality.RECOVERED);
    }

    private static ColumnChunkBuffer buffer(FileSchema schema, ColumnEncoding encoding, double bloomFilterFpp) {
        // Small pages, so a chunk has many and the page cuts are compared too.
        return new ColumnChunkBuffer(schema.getColumn(0), 4096, WriterConfig.DEFAULT_PAGE_TARGET_ROWS,
                PageLimit.MAX_PAGE_VALUES, 1 << 20, RowGroupBuffer.MAX_ROWS, encoding, 64, new CompressorFactory().getCompressor(CompressionCodec.UNCOMPRESSED),
                CompressionCodec.UNCOMPRESSED, RowGroupBuffer.MAX_STORE_CAPACITY, bloomFilterFpp);
    }

    private static FileSchema schema(PhysicalType type, Shape shape) {
        FileSchema.Builder builder = FileSchema.builder("schema");
        return switch (shape) {
            case REQUIRED -> leaf(builder, type, RepetitionType.REQUIRED).build();
            case OPTIONAL -> leaf(builder, type, RepetitionType.OPTIONAL).build();
            case OPTIONAL_IN_STRUCT -> builder.struct("a", RepetitionType.OPTIONAL,
                    a -> leaf(a, type, RepetitionType.OPTIONAL)).build();
            case REQUIRED_IN_STRUCT -> builder.struct("a", RepetitionType.OPTIONAL,
                    a -> leaf(a, type, RepetitionType.REQUIRED)).build();
            case OPTIONAL_IN_TWO_STRUCTS -> builder.struct("a", RepetitionType.OPTIONAL,
                    a -> a.struct("b", RepetitionType.OPTIONAL,
                            b -> leaf(b, type, RepetitionType.OPTIONAL))).build();
        };
    }

    private static FileSchema.Builder leaf(FileSchema.Builder builder, PhysicalType type,
                                           RepetitionType repetition) {
        return type == PhysicalType.FIXED_LEN_BYTE_ARRAY
                ? builder.addColumn("v", type, repetition, FIXED_LENGTH)
                : builder.addColumn("v", type, repetition);
    }

    private static FileSchema.StructBuilder leaf(FileSchema.StructBuilder builder, PhysicalType type,
                                                 RepetitionType repetition) {
        return type == PhysicalType.FIXED_LEN_BYTE_ARRAY
                ? builder.addColumn("v", type, repetition, FIXED_LENGTH)
                : builder.addColumn("v", type, repetition);
    }

    private static final int FIXED_LENGTH = 6;

    /// Binds `shredder` to one batch whose leaf and structs are each null at about `nullRatio`
    /// of the records.
    private static void bind(RecordShredder shredder, FileSchema schema, PhysicalType type, Shape shape,
                             double nullRatio, Cardinality cardinality, int firstRecord, Random random) {
        Validity[] leafValidities = { schema.getColumn(0).maxDefinitionLevel() > structDepth(shape)
                ? nulls(nullRatio, random)
                : null };
        Map<String, Validity> structValidities = new HashMap<>();
        if (structDepth(shape) >= 1) {
            structValidities.put("a", nulls(nullRatio, random));
        }
        if (structDepth(shape) == 2) {
            structValidities.put("a.b", nulls(nullRatio, random));
        }
        shredder.bind(new ColumnSource[]{ sourceFor(type, cardinality, firstRecord) }, leafValidities,
                structValidities, Map.of(), Map.of());
    }

    private static int structDepth(Shape shape) {
        return switch (shape) {
            case REQUIRED, OPTIONAL -> 0;
            case OPTIONAL_IN_STRUCT, REQUIRED_IN_STRUCT -> 1;
            case OPTIONAL_IN_TWO_STRUCTS -> 2;
        };
    }

    private static Validity nulls(double nullRatio, Random random) {
        boolean[] nulls = new boolean[RECORDS_PER_BATCH];
        // In runs, so present and absent runs of every length occur.
        for (int i = 0; i < nulls.length; ) {
            boolean isNull = random.nextDouble() < nullRatio;
            int run = 1 + random.nextInt(random.nextBoolean() ? 3 : 100);
            for (int k = 0; k < run && i < nulls.length; k++) {
                nulls[i++] = isNull;
            }
        }
        return Validity.ofNulls(nulls);
    }

    /// The batch's values, a function of the record number so that both appends read the same.
    private static ColumnSource sourceFor(PhysicalType type, Cardinality cardinality, int firstRecord) {
        long[] keys = new long[RECORDS_PER_BATCH];
        for (int i = 0; i < keys.length; i++) {
            int record = firstRecord + i;
            keys[i] = switch (cardinality) {
                case FEW -> record % 7;
                case ALL_DISTINCT -> record;
                case FRONT_LOADED -> record < 9_000 ? record : record % 50;
                case RECOVERED -> {
                    int chunkStart = record - record % CHUNK_RECORDS;
                    int position = record % CHUNK_RECORDS;
                    yield position < 24_000 ? record : chunkStart + position % 5_000;
                }
            };
        }
        return switch (type) {
            case INT32 -> {
                int[] values = new int[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    values[i] = (int) keys[i] * 3 - 1000;
                }
                yield new IntArrayColumnSource(values);
            }
            case INT64 -> {
                long[] values = new long[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    values[i] = keys[i] * 1_000_003L;
                }
                yield new LongArrayColumnSource(values);
            }
            case FLOAT -> {
                float[] values = new float[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    values[i] = keys[i] == 3 ? Float.NaN : keys[i] * 0.25f;
                }
                yield new FloatArrayColumnSource(values);
            }
            case DOUBLE -> {
                double[] values = new double[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    values[i] = keys[i] == 3 ? Double.NaN : keys[i] * 0.5;
                }
                yield new DoubleArrayColumnSource(values);
            }
            case BOOLEAN -> {
                boolean[] values = new boolean[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    values[i] = keys[i] % 3 == 0;
                }
                yield new BooleanArrayColumnSource(values);
            }
            case BYTE_ARRAY -> {
                byte[][] values = new byte[keys.length][];
                for (int i = 0; i < keys.length; i++) {
                    values[i] = ("value-" + keys[i]).getBytes(StandardCharsets.UTF_8);
                }
                yield new BinaryArrayColumnSource(values);
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                byte[][] values = new byte[keys.length][];
                for (int i = 0; i < keys.length; i++) {
                    values[i] = ByteBuffer.allocate(8).putLong(keys[i]).array();
                    values[i] = Arrays.copyOfRange(values[i], 2, 8);
                }
                yield new BinaryArrayColumnSource(values);
            }
            case INT96 -> throw new IllegalArgumentException("Not written");
        };
    }

    private static byte[] bytes(InMemoryOutputFile out) {
        out.close();
        ByteBuffer buffer = out.buffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
