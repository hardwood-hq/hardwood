/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryShortInBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [BinaryShortInBatchMatcher], which compares rows of at most eight bytes as one `long`, checked row
/// by row against [Arrays#equals(byte[], byte[])] — and the compiler's choice of it over the byte-wise
/// matchers.
class ShortValueEqualityTest {

    private static final int ROWS = 300;
    private static final Random RANDOM = new Random(0x5EEDL);

    @Test
    void lengthsAroundEightBytes_nullsAndTheArrayEnd_matchByteEquality() {
        for (int round = 0; round < 50; round++) {
            byte[][] values = values();
            BitSet nulls = nulls();
            // Packed exactly, so the last short values end within eight bytes of the array's end
            // and take the byte-by-byte fallback.
            BatchExchange.Batch batch = batch(nulls, values);
            byte[][] literals = {valueOfLength(values, 0), valueOfLength(values, 7), valueOfLength(values, 8),
                    valueOfLength(values, 9), values[ROWS - 1], values[ROWS - 2], "absent".getBytes()};

            for (byte[] literal : literals) {
                if (literal.length > Long.BYTES) {
                    continue;
                }
                byte[][] one = {literal};
                assertThat(run(new BinaryShortInBatchMatcher(one, Comparison.BYTE_STRING, false), batch))
                        .as("eq %s", Arrays.toString(literal))
                        .isEqualTo(expected(values, nulls, v -> Arrays.equals(v, literal)));
                assertThat(run(new BinaryShortInBatchMatcher(one, Comparison.BYTE_STRING, true), batch))
                        .as("notEq %s", Arrays.toString(literal))
                        .isEqualTo(expected(values, nulls, v -> !Arrays.equals(v, literal)));
            }
            // Members on both sides of eight bytes, so rows split between the `long` compare and
            // the byte comparison within one batch.
            assertThat(run(new BinaryShortInBatchMatcher(literals, Comparison.BYTE_STRING, false), batch))
                    .isEqualTo(expected(values, nulls, v -> contains(literals, v)));
            assertThat(run(new BinaryShortInBatchMatcher(literals, Comparison.BYTE_STRING, true), batch))
                    .isEqualTo(expected(values, nulls, v -> !contains(literals, v)));
        }
    }

    @Test
    void valuesDifferingOnlyPastTheirLength_areNotEqual() {
        // "ab" read as eight bytes picks up the next value's bytes; the mask must drop them, and
        // "ab\0" must not match "ab" even though both mask to the same leading bytes.
        BatchExchange.Batch batch = batch(null, "ab".getBytes(), "abab".getBytes(),
                new byte[]{'a', 'b', 0}, "ab".getBytes(), "padding-so-every-row-reads-eight-bytes".getBytes());
        assertThat(run(eq("ab"), batch)).containsExactly(0b01001L);
    }

    @Test
    void arrayUnderEightBytes_comparesEveryRowByteWise() {
        // Three bytes in all: no row can be read as eight, so every row takes the fallback.
        BatchExchange.Batch batch = batch(null, "ab".getBytes(), "c".getBytes());
        assertThat(run(eq("ab"), batch)).containsExactly(0b01L);
        assertThat(run(eq("c"), batch)).containsExactly(0b10L);
    }

    @Test
    void comparisonThatIsNotByteExact_isRefused() {
        assertThatThrownBy(() -> new BinaryShortInBatchMatcher(new byte[][]{{0x7F}}, Comparison.VARIABLE_DECIMAL, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Short-value equality needs a byte-exact comparison and a member of at most eight bytes,"
                        + " got VARIABLE_DECIMAL over 1 members");
    }

    @Test
    void compiler_choosesTheShortMatcherOnlyForShortByteExactLiterals() {
        FileSchema schema = stringSchema();
        byte[] eightBytes = "abcdefgh".getBytes();
        byte[] nineBytes = "abcdefghi".getBytes();

        assertThat(compile(new ResolvedPredicate.BinaryPredicate(0, Operator.EQ, eightBytes, Comparison.BYTE_STRING), schema))
                .isInstanceOf(BinaryShortInBatchMatcher.class);
        assertThat(compile(new ResolvedPredicate.BinaryPredicate(0, Operator.NOT_EQ, eightBytes, Comparison.BYTE_STRING), schema))
                .isInstanceOf(BinaryShortInBatchMatcher.class);
        assertThat(compile(new ResolvedPredicate.BinaryInPredicate(0, new byte[][]{nineBytes, eightBytes},
                Comparison.BYTE_STRING), schema))
                .isInstanceOf(BinaryShortInBatchMatcher.class);

        // A literal past eight bytes, or an equality that is not byte equality, keeps the byte-wise matcher.
        assertThat(compile(new ResolvedPredicate.BinaryPredicate(0, Operator.EQ, nineBytes, Comparison.BYTE_STRING), schema))
                .isInstanceOf(BinaryEqBatchMatcher.class);
        assertThat(compile(new ResolvedPredicate.BinaryPredicate(0, Operator.NOT_EQ, nineBytes, Comparison.BYTE_STRING), schema))
                .isInstanceOf(BinaryNotEqBatchMatcher.class);
        assertThat(compile(new ResolvedPredicate.BinaryInPredicate(0, new byte[][]{nineBytes}, Comparison.BYTE_STRING), schema))
                .isInstanceOf(BinaryInBatchMatcher.class);
        assertThat(compile(new ResolvedPredicate.BinaryPredicate(0, Operator.EQ, new byte[]{0x7F}, Comparison.VARIABLE_DECIMAL), schema))
                .isInstanceOf(BinaryEqBatchMatcher.class);
    }

    /// Packs `values` back to back into an array sized exactly to them, so the last values end at the
    /// array's end, with `nulls` absent.
    private static BatchExchange.Batch batch(BitSet nulls, byte[]... values) {
        int[] offsets = new int[values.length + 1];
        for (int i = 0; i < values.length; i++) {
            offsets[i + 1] = offsets[i] + values[i].length;
        }
        byte[] bytes = new byte[offsets[values.length]];
        for (int i = 0; i < values.length; i++) {
            System.arraycopy(values[i], 0, bytes, offsets[i], values[i].length);
        }
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = new BinaryBatchValues(bytes, offsets);
        if (nulls != null && !nulls.isEmpty()) {
            long[] validity = new long[(values.length + 63) >>> 6];
            for (int i = 0; i < values.length; i++) {
                if (!nulls.get(i)) {
                    validity[i >>> 6] |= 1L << i;
                }
            }
            batch.validity = validity;
        }
        batch.recordCount = values.length;
        return batch;
    }

    /// Runs `matcher` and clears the bits past the record count, which a matcher leaves unspecified.
    private static long[] run(ColumnBatchMatcher matcher, BatchExchange.Batch batch) {
        long[] out = new long[(batch.recordCount + 63) >>> 6];
        matcher.test(batch, out);
        int tail = batch.recordCount & 63;
        if (tail != 0) {
            out[out.length - 1] &= (1L << tail) - 1;
        }
        return out;
    }

    private static BinaryShortInBatchMatcher eq(String literal) {
        return new BinaryShortInBatchMatcher(new byte[][]{literal.getBytes()}, Comparison.BYTE_STRING, false);
    }

    private static FileSchema stringSchema() {
        return FileSchema.fromSchemaElements(List.of(SchemaElement.root("root", 1),
                SchemaElement.primitive("s", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)));
    }

    private static ColumnBatchMatcher compile(ResolvedPredicate predicate, FileSchema schema) {
        CompiledBatchFilter compiled = BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity());
        assertThat(compiled).isNotNull();
        assertThat(compiled.columnMatchers()[0]).isInstanceOf(DictionaryBinaryBatchMatcher.class);
        return ((DictionaryBinaryBatchMatcher) compiled.columnMatchers()[0]).delegate();
    }

    private interface RowTest {
        boolean test(byte[] value);
    }

    private static long[] expected(byte[][] values, BitSet nulls, RowTest test) {
        long[] words = new long[(values.length + 63) >>> 6];
        for (int i = 0; i < values.length; i++) {
            if (!nulls.get(i) && test.test(values[i])) {
                words[i >>> 6] |= 1L << i;
            }
        }
        return words;
    }

    private static boolean contains(byte[][] members, byte[] value) {
        for (byte[] member : members) {
            if (Arrays.equals(member, value)) {
                return true;
            }
        }
        return false;
    }

    /// Lengths 0 to 12 over a two-letter alphabet, so values of equal length collide often and the
    /// eight-byte boundary is crossed in both directions.
    private static byte[][] values() {
        byte[][] values = new byte[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            values[i] = new byte[RANDOM.nextInt(13)];
            for (int k = 0; k < values[i].length; k++) {
                values[i][k] = (byte) ('a' + RANDOM.nextInt(2));
            }
        }
        return values;
    }

    private static byte[] valueOfLength(byte[][] values, int length) {
        for (byte[] value : values) {
            if (value.length == length) {
                return value;
            }
        }
        return new byte[length];
    }

    private static BitSet nulls() {
        BitSet nulls = new BitSet(ROWS);
        for (int i = 0; i < ROWS; i++) {
            if (RANDOM.nextInt(8) == 0) {
                nulls.set(i);
            }
        }
        return nulls;
    }
}
