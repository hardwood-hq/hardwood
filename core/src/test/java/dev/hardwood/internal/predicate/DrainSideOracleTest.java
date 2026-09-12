/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/// Equivalence: compiled [RecordFilterCompiler] and drain-side [BatchFilterCompiler] + per-column
/// [ColumnBatchMatcher] agree on which rows survive a given predicate. Constitutes the load-bearing
/// correctness gate for the drain-side path. A single byte-array leaf is also checked against a
/// reference that does not go through [BinaryComparator] — decimals as `BigInteger`s, byte strings
/// through `Arrays.compareUnsigned` — since both paths compare through it and a shared bug would
/// otherwise leave them agreeing on the wrong rows.
///
/// The workload carries one column per supported primitive type — `id: long`, `value: double`,
/// `tag: int`, `score: float`, `flag: boolean` — plus two byte-array columns: `name` (`BYTE_ARRAY`,
/// values of differing widths on both sides of eight bytes) and `amount` (`FIXED_LEN_BYTE_ARRAY(4)`, every value padded to the
/// width). Each has its own scattered-null profile and boundary-heavy values. Tests exercise every
/// `(type, op)` pair listed in the design doc's eligibility section plus `IntIn` / `LongIn` /
/// `IsNull` / `IsNotNull`, both as single leaves and in cross-type `And` compounds.
///
/// The byte-array leaves run under each [Comparison]: `BYTE_STRING` over `name`, `FIXED_DECIMAL`
/// over the equal-width `amount`, and `VARIABLE_DECIMAL` over `name`, whose differing widths are
/// where byte order and value order disagree.
class DrainSideOracleTest {

    // 200 = 3 full 64-bit words + an 8-bit tail. Not a multiple of 64, so every
    // oracle method exercises the matcher's `tail != 0` branch and the consumer's
    // `bit < limit` / `Math.min(bit, limit)` clamps against stale trailing bits.
    private static final int N = 200;

    // Projected column indices used throughout the test.
    private static final int COL_ID = 0;     // long
    private static final int COL_VALUE = 1;  // double
    private static final int COL_TAG = 2;    // int
    private static final int COL_SCORE = 3;  // float
    private static final int COL_FLAG = 4;   // boolean
    private static final int COL_NAME = 5;   // BYTE_ARRAY, widths differ
    private static final int COL_AMOUNT = 6; // FIXED_LEN_BYTE_ARRAY(4)

    /// Width of `amount`: every value is padded to it, so a [Comparison#FIXED_DECIMAL] comparison
    /// always sees equal widths and the sign byte decides first.
    private static final int AMOUNT_WIDTH = 4;

    // ---------- Single-leaf coverage, all supported (type, op) pairs ----------

    @Test
    void singleLongLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xC0FFEE);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.LongPredicate(COL_ID, op, 100L);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleDoubleLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xBEEF);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.DoublePredicate(COL_VALUE, op, 0.5);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleIntLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xA1107A6);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.IntPredicate(COL_TAG, op, 100);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleFloatLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xF10A75);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.FloatPredicate(COL_SCORE, op, 0.5f);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleBooleanLeaf_eqAndNotEq_bothWaysAgree() {
        // A BooleanPredicate carries EQ / NOT_EQ alone: the resolver answers every ordered
        // operator on a boolean column as an equality or a constant.
        Workload w = workload(0xB001EA1);
        for (Operator op : new Operator[]{Operator.EQ, Operator.NOT_EQ}) {
            for (boolean lit : new boolean[]{true, false}) {
                ResolvedPredicate p = new ResolvedPredicate.BooleanPredicate(COL_FLAG, op, lit);
                assertSurvivorsAgree(p, w);
            }
        }
    }

    @Test
    void intIn_bothWaysAgree() {
        Workload w = workload(0x1A110);
        int[] values = new int[]{0, 100, -50, 250, Integer.MAX_VALUE, Integer.MIN_VALUE};
        ResolvedPredicate p = new ResolvedPredicate.IntInPredicate(COL_TAG, values);
        assertSurvivorsAgree(p, w);
    }

    @Test
    void longIn_bothWaysAgree() {
        Workload w = workload(0xB16601);
        long[] values = new long[]{0L, 100L, -50L, 250L, Long.MAX_VALUE, Long.MIN_VALUE};
        ResolvedPredicate p = new ResolvedPredicate.LongInPredicate(COL_ID, values);
        assertSurvivorsAgree(p, w);
    }

    @Test
    void floatingPointIn_bothWidths_bothWaysAgree() {
        Workload w = workload(0xD0AB1E);
        double customNan = Double.longBitsToDouble(0x7ff8000000000001L);
        double[] values = new double[]{0.0, -0.0, Double.NaN, customNan, 0.5, 0.1, 250.0, Float.NaN,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        ResolvedPredicate pDouble = new ResolvedPredicate.DoubleInPredicate(COL_VALUE, values, false);
        assertSurvivorsAgree(pDouble, w);
        float customFloatNan = Float.intBitsToFloat(0x7fc00001);
        float[] floatValues = new float[]{0.0f, -0.0f, Float.NaN, customFloatNan, 0.5f, 0.1f, 250.0f,
                Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};
        ResolvedPredicate pFloat = new ResolvedPredicate.FloatInPredicate(COL_SCORE, floatValues, false);
        assertSurvivorsAgree(pFloat, w);
    }

    @Test
    void floatingPointInNegation_bothWidths_bothWaysAgree() {
        Workload w = workload(0xD0ABA11);
        double customNan = Double.longBitsToDouble(0x7ff8000000000001L);
        double[] values = new double[]{0.5, Double.NaN, customNan, 0.1, Double.POSITIVE_INFINITY};
        ResolvedPredicate pDouble = ResolvedPredicate.negate(new ResolvedPredicate.DoubleInPredicate(COL_VALUE, values, false));
        assertSurvivorsAgree(pDouble, w);
        float[] floatValues = new float[]{0.5f, Float.NaN, Float.intBitsToFloat(0x7fc00001), 0.1f,
                Float.POSITIVE_INFINITY};
        ResolvedPredicate pFloat = ResolvedPredicate.negate(new ResolvedPredicate.FloatInPredicate(COL_SCORE, floatValues, false));
        assertSurvivorsAgree(pFloat, w);
    }

    @Test
    void isNull_eachColumn_bothWaysAgree() {
        Workload w = workload(0x15A11);
        for (int col = 0; col < 5; col++) {
            ResolvedPredicate p = new ResolvedPredicate.IsNullPredicate(col, 1);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void isNotNull_eachColumn_bothWaysAgree() {
        Workload w = workload(0x15A011);
        for (int col = 0; col < 5; col++) {
            ResolvedPredicate p = new ResolvedPredicate.IsNotNullPredicate(col, 1);
            assertSurvivorsAgree(p, w);
        }
    }

    // ---------- Cross-type AND coverage ----------

    @ParameterizedTest(name = "and(id {0} 100, value {1} 0.5)")
    @MethodSource("opPairs")
    void andOfLongAndDouble_bothWaysAgree(Operator opA, Operator opB) {
        Workload w = workload(0xFEED);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, opA, 100L),
                new ResolvedPredicate.DoublePredicate(COL_VALUE, opB, 0.5)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andOfIntAndBoolean_bothWaysAgree() {
        Workload w = workload(0xA10A);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(COL_TAG, Operator.GT, 0),
                new ResolvedPredicate.BooleanPredicate(COL_FLAG, Operator.EQ, true)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andOfFloatAndIsNotNull_bothWaysAgree() {
        Workload w = workload(0xF10F);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.FloatPredicate(COL_SCORE, Operator.LT, 0.5f),
                new ResolvedPredicate.IsNotNullPredicate(COL_ID, 1)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andOfLongInAndIntIn_bothWaysAgree() {
        Workload w = workload(0x10F1A);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongInPredicate(COL_ID, new long[]{0L, 100L, -50L}),
                new ResolvedPredicate.IntInPredicate(COL_TAG, new int[]{0, 100, -50})
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andSameColumnRange_bothWaysAgree() {
        // Same-column AND is now composed into an AndBatchMatcher in BatchFilterCompiler;
        // the oracle confirms the composite agrees with the compiled per-row path on
        // a range predicate.
        Workload w = workload(0x6A0E);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.LT_EQ, 200L)
        ));
        assertSurvivorsAgree(p, w);
    }

    // ---------- OR / mixed AND-OR coverage ----------

    @Test
    void orSameColumn_bothWaysAgree() {
        Workload w = workload(0x05A1ECE);
        ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.LT, -5L),
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.GT, 5L)
        ));
        assertSurvivorsAgree(p, w);
    }

    @ParameterizedTest(name = "or(id {0} 100, value {1} 0.5)")
    @MethodSource("opPairs")
    void orOfLongAndDouble_bothWaysAgree(Operator opA, Operator opB) {
        Workload w = workload(0x07ED7);
        ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, opA, 100L),
                new ResolvedPredicate.DoublePredicate(COL_VALUE, opB, 0.5)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void orOfIntAndBoolean_bothWaysAgree() {
        Workload w = workload(0x017E50);
        ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.IntPredicate(COL_TAG, Operator.GT, 0),
                new ResolvedPredicate.BooleanPredicate(COL_FLAG, Operator.EQ, true)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andOfLeafAndOrOnDistinctColumn_bothWaysAgree() {
        // `id > 0 AND (value < 0 OR value > 0.5)` — exercises mixed AND/OR with
        // the OR subtree folding into a same-column OrBatchMatcher.
        Workload w = workload(0xA10C04);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.GT, 0L),
                new ResolvedPredicate.Or(List.of(
                        new ResolvedPredicate.DoublePredicate(COL_VALUE, Operator.LT, 0.0),
                        new ResolvedPredicate.DoublePredicate(COL_VALUE, Operator.GT, 0.5)
                ))
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void orOfAndAndLeafOnDistinctColumns_bothWaysAgree() {
        // `(id > 0 AND value > 0) OR tag < 0` — cross-column AND inside an OR
        // beside a leaf on a third column; the MergePlan is Or(And(...), leaf).
        Workload w = workload(0x07AC1);
        ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.LongPredicate(COL_ID, Operator.GT, 0L),
                        new ResolvedPredicate.DoublePredicate(COL_VALUE, Operator.GT, 0.0))),
                new ResolvedPredicate.IntPredicate(COL_TAG, Operator.LT, 0)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void orWithIsNullSibling_bothWaysAgree() {
        // Mixes a null-check leaf into the OR — the matcher and MergePlan
        // both have to respect "definitely matches" semantics.
        Workload w = workload(0x05077);
        ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.EQ, 100L),
                new ResolvedPredicate.IsNullPredicate(COL_VALUE, 1)
        ));
        assertSurvivorsAgree(p, w);
    }

    // ---------- Byte-array leaves, one test per comparison ----------

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /// Big-endian two's complement in [#AMOUNT_WIDTH] bytes — how a fixed-width DECIMAL stores its
    /// unscaled value.
    private static byte[] int32(int v) {
        byte[] out = new byte[AMOUNT_WIDTH];
        for (int i = AMOUNT_WIDTH - 1; i >= 0; i--) {
            out[i] = (byte) v;
            v >>= 8;
        }
        return out;
    }

    @Test
    void singleBinaryLeaf_allOps_bothWaysAgree() {
        // Literals: a value in the pool, the empty string, a high byte whose unsigned order
        // differs from its signed one, and values past eight bytes — one exactly nine, one
        // sharing a prefix with a shorter pool value.
        Workload w = workload(0x0B1A);
        for (byte[] literal : new byte[][]{utf8("apple"), utf8(""), {(byte) 0xFF}, utf8("abcdefghi"),
                utf8("applesauce")}) {
            for (Operator op : Operator.values()) {
                assertSurvivorsAgree(
                        new ResolvedPredicate.BinaryPredicate(COL_NAME, op, literal, Comparison.BYTE_STRING), w);
            }
        }
    }

    @Test
    void singleFixedDecimalLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0x0F1D);
        for (int literal : new int[]{0, 100, -100}) {
            for (Operator op : Operator.values()) {
                assertSurvivorsAgree(
                        new ResolvedPredicate.BinaryPredicate(COL_AMOUNT, op, int32(literal),
                                Comparison.FIXED_DECIMAL), w);
            }
        }
    }

    /// `name` holds values of differing widths, so a `VARIABLE_DECIMAL` comparison over it is the
    /// sign-extending one: `0x7F` must not outrank `0x00 0x80`, and equality must accept a padded
    /// spelling of the literal's own value.
    @Test
    void singleVariableDecimalLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0x0A21);
        for (byte[] literal : new byte[][]{{0x7F}, {0x00, (byte) 0x80}, {(byte) 0x9C}}) {
            for (Operator op : Operator.values()) {
                assertSurvivorsAgree(
                        new ResolvedPredicate.BinaryPredicate(COL_NAME, op, literal,
                                Comparison.VARIABLE_DECIMAL), w);
            }
        }
    }

    /// Members include a value in the pool, the empty string, a high byte, one the pool never
    /// holds, and values past eight bytes. `name` holds values of differing widths, so it carries the two comparisons a
    /// variable-width column can have — `VARIABLE_DECIMAL` has to accept a padded spelling of a member.
    @Test
    void binaryIn_variableWidthComparisons_bothWaysAgree() {
        Workload w = workload(0x0B5);
        byte[][] values = {utf8("apple"), utf8(""), {(byte) 0xFF}, utf8("absent"), {0x7F},
                utf8("applesauce"), utf8("applesaucer")};
        for (Comparison comparison : new Comparison[]{Comparison.BYTE_STRING, Comparison.VARIABLE_DECIMAL}) {
            assertSurvivorsAgree(new ResolvedPredicate.BinaryInPredicate(COL_NAME, values, comparison), w);
        }
    }

    /// `FIXED_DECIMAL` only ever reaches a fixed-width column, so it runs over `amount`.
    @Test
    void binaryIn_fixedDecimal_bothWaysAgree() {
        Workload w = workload(0x0B6);
        byte[][] values = {int32(0), int32(100), int32(-100), int32(Integer.MIN_VALUE)};
        assertSurvivorsAgree(new ResolvedPredicate.BinaryInPredicate(COL_AMOUNT, values,
                Comparison.FIXED_DECIMAL), w);
    }

    /// `negate` expands `IN` to a conjunction of `NOT_EQ` leaves on one column, which folds into a
    /// single per-column composite rather than reaching the `IN` matcher.
    @Test
    void notBinaryIn_bothWaysAgree() {
        Workload w = workload(0x0B2);
        // A member past eight bytes compiles to the byte-wise `NOT_EQ`, the others to the
        // short-value matcher, so the conjunction chains both kinds.
        byte[][] values = {utf8("apple"), utf8("cherry"), utf8("applesauce")};
        assertSurvivorsAgree(ResolvedPredicate.negate(
                new ResolvedPredicate.BinaryInPredicate(COL_NAME, values, Comparison.BYTE_STRING)), w);
    }

    @Test
    void andOfBinaryAndFixedDecimal_bothWaysAgree() {
        Workload w = workload(0x0B3);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.BinaryPredicate(COL_NAME, Operator.GT_EQ, utf8("b"),
                        Comparison.BYTE_STRING),
                new ResolvedPredicate.BinaryPredicate(COL_AMOUNT, Operator.LT, int32(0),
                        Comparison.FIXED_DECIMAL)));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void orOfBinaryAndFixedDecimal_bothWaysAgree() {
        Workload w = workload(0x0B4);
        ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.BinaryPredicate(COL_NAME, Operator.EQ, utf8("cherry"),
                        Comparison.BYTE_STRING),
                new ResolvedPredicate.BinaryPredicate(COL_AMOUNT, Operator.LT, int32(-100),
                        Comparison.FIXED_DECIMAL)));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void orOfBinaryInAndFixedDecimal_bothWaysAgree() {
        Workload w = workload(0x07B1A7);
        ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.BinaryInPredicate(COL_NAME, new byte[][]{utf8("cherry"), utf8("")},
                        Comparison.BYTE_STRING),
                new ResolvedPredicate.BinaryPredicate(COL_AMOUNT, Operator.LT, int32(-100),
                        Comparison.FIXED_DECIMAL)));
        assertSurvivorsAgree(p, w);
    }

    private static Stream<Arguments> opPairs() {
        Operator[] ops = Operator.values();
        List<Arguments> pairs = new ArrayList<>();
        for (Operator a : ops) {
            for (Operator b : ops) {
                pairs.add(Arguments.of(a, b));
            }
        }
        return pairs.stream();
    }

    // ---------- Oracle plumbing ----------

    private static void assertSurvivorsAgree(ResolvedPredicate predicate, Workload w) {
        BitSet compiled = compiledSurvivors(predicate, w);
        BitSet drainSide = drainSideSurvivors(predicate, w);
        // Every predicate used in this file is intentionally drain-eligible. A `null`
        // here means BatchFilterCompiler.tryCompile newly refused to compile something
        // it used to handle — that's a regression we want to catch loudly, not skip.
        // Ineligibility tests live in BatchFilterCompilerTest.IneligibleShapes.
        assertNotNull(drainSide,
                () -> "BatchFilterCompiler.tryCompile returned null for an expected-eligible predicate: "
                        + describe(predicate));
        assertEquals(compiled, drainSide, () -> "compiled/drain-side diverged for " + describe(predicate));
        BitSet reference = binaryLeafReference(predicate, w);
        if (reference != null) {
            assertEquals(reference, drainSide, () -> "reference/drain-side diverged for " + describe(predicate));
        }
    }

    /// Survivors of a single byte-array leaf, computed without [BinaryComparator]: both paths above
    /// compare through it, so a bug there would leave them agreeing on the wrong rows. A decimal
    /// compares as the number its bytes spell, a byte string as unsigned bytes. `null` for any other
    /// predicate.
    private static BitSet binaryLeafReference(ResolvedPredicate predicate, Workload w) {
        return switch (predicate) {
            case ResolvedPredicate.BinaryPredicate p -> referenceSurvivors(p.columnIndex(), w,
                    value -> holds(p.op(), referenceCompare(value, p.value(), p.comparison())));
            case ResolvedPredicate.BinaryInPredicate p -> referenceSurvivors(p.columnIndex(), w, value -> {
                for (byte[] member : p.values()) {
                    if (referenceCompare(value, member, p.comparison()) == 0) {
                        return true;
                    }
                }
                return false;
            });
            default -> null;
        };
    }

    private interface ValueTest {
        boolean test(byte[] value);
    }

    private static BitSet referenceSurvivors(int column, Workload w, ValueTest test) {
        byte[][] values = column == COL_NAME ? w.names : w.amounts;
        BitSet nulls = column == COL_NAME ? w.nameNulls : w.amountNulls;
        BitSet out = new BitSet(N);
        for (int i = 0; i < N; i++) {
            if (!nulls.get(i) && test.test(values[i])) {
                out.set(i);
            }
        }
        return out;
    }

    private static int referenceCompare(byte[] value, byte[] literal, Comparison comparison) {
        return switch (comparison) {
            case BYTE_STRING, STORED_BYTES -> Arrays.compareUnsigned(value, literal);
            case FIXED_DECIMAL, VARIABLE_DECIMAL -> asNumber(value).compareTo(asNumber(literal));
            case INT96_INSTANT -> throw new IllegalArgumentException("No drain-side workload uses " + comparison);
        };
    }

    /// Big-endian two's complement, the empty array being zero.
    private static BigInteger asNumber(byte[] bytes) {
        return bytes.length == 0 ? BigInteger.ZERO : new BigInteger(bytes);
    }

    private static boolean holds(Operator op, int cmp) {
        return switch (op) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }

    /// The predicate with byte-array literals spelled out, which `toString` prints as identity hashes.
    private static String describe(ResolvedPredicate predicate) {
        return switch (predicate) {
            case ResolvedPredicate.BinaryPredicate p -> "BinaryPredicate[column=" + p.columnIndex() + ", " + p.op()
                    + " " + Arrays.toString(p.value()) + ", " + p.comparison() + "]";
            case ResolvedPredicate.BinaryInPredicate p -> "BinaryInPredicate[column=" + p.columnIndex() + ", IN "
                    + Arrays.deepToString(p.values()) + ", " + p.comparison() + "]";
            case ResolvedPredicate.And and -> "And" + and.children().stream().map(DrainSideOracleTest::describe).toList();
            case ResolvedPredicate.Or or -> "Or" + or.children().stream().map(DrainSideOracleTest::describe).toList();
            default -> predicate.toString();
        };
    }

    private static BitSet compiledSurvivors(ResolvedPredicate predicate, Workload w) {
        RowMatcher matcher = RecordFilterCompiler.compile(predicate, w.schema,
                w.projection::toProjectedIndex);
        BitSet out = new BitSet(N);
        for (int i = 0; i < N; i++) {
            if (matcher.test(w.row(i))) {
                out.set(i);
            }
        }
        return out;
    }

    private static BitSet drainSideSurvivors(ResolvedPredicate predicate, Workload w) {
        CompiledBatchFilter compiled = BatchFilterCompiler.tryCompile(predicate, w.schema,
                w.projection::toProjectedIndex);
        if (compiled == null) {
            // Predicate not eligible for drain-side compilation. Callers in this file
            // intentionally use eligible predicates, so `assertSurvivorsAgree` will
            // fail loudly on a null return rather than silently skipping.
            return null;
        }

        int wordsLen = (N + 63) >>> 6;
        long[][] perColumn = new long[compiled.columnMatchers().length][];
        for (int col = 0; col < compiled.columnMatchers().length; col++) {
            ColumnBatchMatcher m = compiled.columnMatchers()[col];
            if (m == null) {
                continue;
            }
            long[] colWords = new long[wordsLen];
            m.test(w.batch(col), colWords);
            perColumn[col] = colWords;
        }

        long[] combined = new long[wordsLen];
        MergePlan plan = compiled.mergePlan();
        if (plan instanceof MergePlan.Column c) {
            // Mirror the production single-column fast path (aliasing).
            System.arraycopy(perColumn[c.projectedIndex()], 0, combined, 0, wordsLen);
        }
        else {
            new MergePlanEvaluator(wordsLen).eval(plan, combined, wordsLen, perColumn);
        }

        BitSet out = new BitSet(N);
        for (int i = 0; i < N; i++) {
            if ((combined[i >>> 6] & (1L << i)) != 0) {
                out.set(i);
            }
        }
        return out;
    }

    // ---------- Workload construction ----------

    private static Workload workload(long seed) {
        Random r = new Random(seed);
        long[] ids = new long[N];
        double[] values = new double[N];
        int[] tags = new int[N];
        float[] scores = new float[N];
        boolean[] flags = new boolean[N];
        byte[][] names = new byte[N][];
        byte[][] amounts = new byte[N][];
        BitSet idNulls = new BitSet(N);
        BitSet valueNulls = new BitSet(N);
        BitSet tagNulls = new BitSet(N);
        BitSet scoreNulls = new BitSet(N);
        BitSet flagNulls = new BitSet(N);
        BitSet nameNulls = new BitSet(N);
        BitSet amountNulls = new BitSet(N);

        // Boundary-heavy values to cover NaN, infinities, type extremes, and
        // equal-to-literal cases. The first few rows of each column carry these.
        double[] boundaryDoubles = {0.5, -0.5, 0.0, -0.0, Double.NaN,
                Double.longBitsToDouble(0x7ff8000000000042L),
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE, Double.MAX_VALUE};
        float[] boundaryFloats = {0.5f, -0.5f, 0.0f, -0.0f, Float.NaN,
                Float.intBitsToFloat(0x7fc00001),
                Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
                Float.MIN_VALUE, Float.MAX_VALUE};
        int[] boundaryInts = {100, -100, 0, Integer.MIN_VALUE, Integer.MAX_VALUE};
        // Proper prefixes and extensions of the literals, the empty value, and high bytes whose
        // unsigned order differs from their signed one — plus the widths that make a
        // sign-extending comparison disagree with a byte-wise one.
        // Values past eight bytes take the long-value side of the short-value matcher and the
        // byte-wise matchers' long literals; `applesauce` shares its first five bytes with `apple`
        // and `abcdefghi` sits one byte past the boundary.
        byte[][] namePool = {utf8(""), utf8("a"), utf8("app"), utf8("apple"), utf8("apples"),
                utf8("b"), utf8("banana"), utf8("cherry"), {0}, {(byte) 0x7F},
                {0x00, (byte) 0x80}, {(byte) 0x9C}, {(byte) 0xFF}, {(byte) 0xFF, 0},
                utf8("abcdefghi"), utf8("applesauce"), utf8("applesauces")};
        int[] boundaryAmounts = {100, -50, 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE};

        for (int i = 0; i < N; i++) {
            ids[i] = r.nextInt(300) - 50; // straddles literal 100
            values[i] = (i < boundaryDoubles.length)
                    ? boundaryDoubles[i]
                    : (r.nextDouble() * 2.0 - 1.0); // [-1, 1) — straddles literal 0.5
            tags[i] = (i < boundaryInts.length)
                    ? boundaryInts[i]
                    : r.nextInt(300) - 50; // straddles literal 100
            scores[i] = (i < boundaryFloats.length)
                    ? boundaryFloats[i]
                    : (float) (r.nextDouble() * 2.0 - 1.0); // straddles literal 0.5
            flags[i] = r.nextBoolean();
            names[i] = namePool[r.nextInt(namePool.length)];
            amounts[i] = int32((i < boundaryAmounts.length)
                    ? boundaryAmounts[i]
                    : r.nextInt(400) - 200); // straddles literals -100, -50, 0 and 100
            if (r.nextInt(10) == 0) {
                idNulls.set(i);
            }
            if (r.nextInt(15) == 0) {
                valueNulls.set(i);
            }
            if (r.nextInt(12) == 0) {
                tagNulls.set(i);
            }
            if (r.nextInt(13) == 0) {
                scoreNulls.set(i);
            }
            if (r.nextInt(14) == 0) {
                flagNulls.set(i);
            }
            if (r.nextInt(11) == 0) {
                nameNulls.set(i);
            }
            if (r.nextInt(9) == 0) {
                amountNulls.set(i);
            }
        }
        return new Workload(ids, idNulls, values, valueNulls,
                tags, tagNulls, scores, scoreNulls, flags, flagNulls,
                names, nameNulls, amounts, amountNulls);
    }

    private static final class Workload {
        final long[] ids;
        final BitSet idNulls;
        final double[] values;
        final BitSet valueNulls;
        final int[] tags;
        final BitSet tagNulls;
        final float[] scores;
        final BitSet scoreNulls;
        final boolean[] flags;
        final BitSet flagNulls;
        final byte[][] names;
        final BitSet nameNulls;
        final byte[][] amounts;
        final BitSet amountNulls;
        final FileSchema schema;
        final ProjectedSchema projection;

        Workload(long[] ids, BitSet idNulls,
                 double[] values, BitSet valueNulls,
                 int[] tags, BitSet tagNulls,
                 float[] scores, BitSet scoreNulls,
                 boolean[] flags, BitSet flagNulls,
                 byte[][] names, BitSet nameNulls,
                 byte[][] amounts, BitSet amountNulls) {
            this.ids = ids;
            this.idNulls = idNulls;
            this.values = values;
            this.valueNulls = valueNulls;
            this.tags = tags;
            this.tagNulls = tagNulls;
            this.scores = scores;
            this.scoreNulls = scoreNulls;
            this.flags = flags;
            this.flagNulls = flagNulls;
            this.names = names;
            this.nameNulls = nameNulls;
            this.amounts = amounts;
            this.amountNulls = amountNulls;
            SchemaElement root = SchemaElement.root("root", 7);
            SchemaElement c1 = SchemaElement.primitive("id", PhysicalType.INT64, RepetitionType.OPTIONAL);
            SchemaElement c2 = SchemaElement.primitive("value", PhysicalType.DOUBLE, RepetitionType.OPTIONAL);
            SchemaElement c3 = SchemaElement.primitive("tag", PhysicalType.INT32, RepetitionType.OPTIONAL);
            SchemaElement c4 = SchemaElement.primitive("score", PhysicalType.FLOAT, RepetitionType.OPTIONAL);
            SchemaElement c5 = SchemaElement.primitive("flag", PhysicalType.BOOLEAN, RepetitionType.OPTIONAL);
            SchemaElement c6 = SchemaElement.primitive("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL);
            SchemaElement c7 = SchemaElement.fixedLengthPrimitive("amount", AMOUNT_WIDTH, RepetitionType.OPTIONAL);
            this.schema = FileSchema.fromSchemaElements(List.of(root, c1, c2, c3, c4, c5, c6, c7));
            this.projection = ProjectedSchema.create(schema, ColumnProjection.all());
        }

        RowReader row(int i) {
            return new SyntheticRow(
                    ids[i], idNulls.get(i),
                    values[i], valueNulls.get(i),
                    tags[i], tagNulls.get(i),
                    scores[i], scoreNulls.get(i),
                    flags[i], flagNulls.get(i),
                    names[i], nameNulls.get(i),
                    amounts[i], amountNulls.get(i));
        }

        BatchExchange.Batch batch(int projectedIdx) {
            BatchExchange.Batch b = new BatchExchange.Batch();
            switch (projectedIdx) {
                case COL_ID -> {
                    b.values = ids;
                    b.validity = nullsToValidity(idNulls);
                }
                case COL_VALUE -> {
                    b.values = values;
                    b.validity = nullsToValidity(valueNulls);
                }
                case COL_TAG -> {
                    b.values = tags;
                    b.validity = nullsToValidity(tagNulls);
                }
                case COL_SCORE -> {
                    b.values = scores;
                    b.validity = nullsToValidity(scoreNulls);
                }
                case COL_FLAG -> {
                    b.values = flags;
                    b.validity = nullsToValidity(flagNulls);
                }
                case COL_NAME -> {
                    b.values = binaryValues(names, nameNulls, true);
                    b.validity = nullsToValidity(nameNulls);
                }
                case COL_AMOUNT -> {
                    b.values = binaryValues(amounts, amountNulls, false);
                    b.validity = nullsToValidity(amountNulls);
                }
                default -> throw new IllegalArgumentException("col " + projectedIdx);
            }
            b.recordCount = N;
            return b;
        }

        /// Packs `values` back to back the way `FlatColumnWorker` assembles a byte-array batch.
        /// With `emptyNulls` a null row occupies an empty slice (`BYTE_ARRAY`); otherwise its bytes
        /// stay in place, the scratch a null `FIXED_LEN_BYTE_ARRAY` slot keeps — so a matcher that
        /// ignored validity would compare them and diverge from the oracle.
        private static BinaryBatchValues binaryValues(byte[][] values, BitSet nulls, boolean emptyNulls) {
            int[] offsets = new int[N + 1];
            for (int i = 0; i < N; i++) {
                int length = emptyNulls && nulls.get(i) ? 0 : values[i].length;
                offsets[i + 1] = offsets[i] + length;
            }
            byte[] bytes = new byte[offsets[N]];
            for (int i = 0; i < N; i++) {
                System.arraycopy(values[i], 0, bytes, offsets[i], offsets[i + 1] - offsets[i]);
            }
            return new BinaryBatchValues(bytes, offsets);
        }

        private static long[] nullsToValidity(BitSet nulls) {
            if (nulls.isEmpty()) {
                return null;
            }
            BitSet validity = new BitSet(N);
            validity.set(0, N);
            validity.andNot(nulls);
            int wordsLen = (N + 63) >>> 6;
            long[] words = validity.toLongArray();
            return words.length < wordsLen ? Arrays.copyOf(words, wordsLen) : words;
        }
    }

    private static final class SyntheticRow implements RowReader {
        private final long idValue;
        private final boolean idNull;
        private final double valueValue;
        private final boolean valueNull;
        private final int tagValue;
        private final boolean tagNull;
        private final float scoreValue;
        private final boolean scoreNull;
        private final boolean flagValue;
        private final boolean flagNull;
        private final byte[] nameValue;
        private final boolean nameNull;
        private final byte[] amountValue;
        private final boolean amountNull;

        SyntheticRow(long idValue, boolean idNull,
                     double valueValue, boolean valueNull,
                     int tagValue, boolean tagNull,
                     float scoreValue, boolean scoreNull,
                     boolean flagValue, boolean flagNull,
                     byte[] nameValue, boolean nameNull,
                     byte[] amountValue, boolean amountNull) {
            this.idValue = idValue;
            this.idNull = idNull;
            this.valueValue = valueValue;
            this.valueNull = valueNull;
            this.tagValue = tagValue;
            this.tagNull = tagNull;
            this.scoreValue = scoreValue;
            this.scoreNull = scoreNull;
            this.flagValue = flagValue;
            this.flagNull = flagNull;
            this.nameValue = nameValue;
            this.nameNull = nameNull;
            this.amountValue = amountValue;
            this.amountNull = amountNull;
        }

        @Override public boolean isNull(int idx) {
            return switch (idx) {
                case COL_ID -> idNull;
                case COL_VALUE -> valueNull;
                case COL_TAG -> tagNull;
                case COL_SCORE -> scoreNull;
                case COL_FLAG -> flagNull;
                case COL_NAME -> nameNull;
                case COL_AMOUNT -> amountNull;
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }

        @Override public boolean isNull(String name) {
            return switch (name) {
                case "id" -> idNull;
                case "value" -> valueNull;
                case "tag" -> tagNull;
                case "score" -> scoreNull;
                case "flag" -> flagNull;
                case "name" -> nameNull;
                case "amount" -> amountNull;
                default -> throw new IllegalArgumentException(name);
            };
        }

        @Override public long getLong(int idx) { return idValue; }
        @Override public long getLong(String name) { return idValue; }
        @Override public double getDouble(int idx) { return valueValue; }
        @Override public double getDouble(String name) { return valueValue; }
        @Override public int getInt(int idx) { return tagValue; }
        @Override public int getInt(String name) { return tagValue; }
        @Override public float getFloat(int idx) { return scoreValue; }
        @Override public float getFloat(String name) { return scoreValue; }
        @Override public boolean getBoolean(int idx) { return flagValue; }
        @Override public boolean getBoolean(String name) { return flagValue; }

        @Override public int getFieldCount() { return 7; }
        @Override public String getFieldName(int idx) {
            return switch (idx) {
                case COL_ID -> "id";
                case COL_VALUE -> "value";
                case COL_TAG -> "tag";
                case COL_SCORE -> "score";
                case COL_FLAG -> "flag";
                case COL_NAME -> "name";
                case COL_AMOUNT -> "amount";
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }

        // Defaults for everything else — the predicate paths under test should not call them.
        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public void next() { throw new UnsupportedOperationException(); }
        @Override public void close() {}
        @Override public String getString(int idx) { throw new UnsupportedOperationException(); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(int idx) {
            return switch (idx) {
                case COL_NAME -> nameValue;
                case COL_AMOUNT -> amountValue;
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }

        @Override public byte[] getBinary(String name) {
            return switch (name) {
                case "name" -> nameValue;
                case "amount" -> amountValue;
                default -> throw new IllegalArgumentException(name);
            };
        }
        @Override public LocalDate getDate(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(int idx) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalDateTime getLocalTimestamp(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalDateTime getLocalTimestamp(String name) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(int idx) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(int idx) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(); }
        @Override public PqVariant getVariant(int idx) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(int idx) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(); }
        @Override public Object getRawValue(int idx) { throw new UnsupportedOperationException(); }
        @Override public Object getRawValue(String name) { throw new UnsupportedOperationException(); }
    }
}
