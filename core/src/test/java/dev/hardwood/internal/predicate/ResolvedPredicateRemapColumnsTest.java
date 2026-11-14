/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.dictionary.MembershipIndex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [ResolvedPredicate#remapColumns] moves each `IN` leaf to the file's column and carries its
/// membership index over as the same instance, so every file of a read probes one index rather
/// than building its own; and a leaf always carries the index its dictionary check needs.
class ResolvedPredicateRemapColumnsTest {

    /// Column 0 of the predicate is column 3 of the file.
    private static final int[] MAPPING = { 3 };

    private static ResolvedPredicate remapped(ResolvedPredicate predicate) {
        return ResolvedPredicate.remapColumns(predicate, MAPPING);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void aListKeepsItsIndex(String type, Leaf leaf) {
        ResolvedPredicate remapped = remapped(leaf.predicate());
        assertThat(remapped).isInstanceOf(leaf.predicate().getClass());
        assertThat(leaf.column().applyAsInt(remapped)).isEqualTo(3);
        assertThat(leaf.membership().apply(remapped)).isNotNull().isSameAs(leaf.membership().apply(leaf.predicate()));
    }

    static Stream<Arguments> aListKeepsItsIndex() {
        return Stream.of(
                Arguments.of("INT32", Leaf.of(new ResolvedPredicate.IntInPredicate(0, new int[]{ 1, 2 }),
                        ResolvedPredicate.IntInPredicate::columnIndex, ResolvedPredicate.IntInPredicate::membership)),
                Arguments.of("INT64", Leaf.of(new ResolvedPredicate.LongInPredicate(0, new long[]{ 1L, 2L }),
                        ResolvedPredicate.LongInPredicate::columnIndex, ResolvedPredicate.LongInPredicate::membership)),
                Arguments.of("UINT32", Leaf.of(new ResolvedPredicate.UnsignedIntInPredicate(0, new int[]{ 1 }),
                        ResolvedPredicate.UnsignedIntInPredicate::columnIndex, ResolvedPredicate.UnsignedIntInPredicate::membership)),
                Arguments.of("UINT64", Leaf.of(new ResolvedPredicate.UnsignedLongInPredicate(0, new long[]{ 1L }),
                        ResolvedPredicate.UnsignedLongInPredicate::columnIndex, ResolvedPredicate.UnsignedLongInPredicate::membership)),
                Arguments.of("FLOAT", Leaf.of(new ResolvedPredicate.FloatInPredicate(0, new float[]{ 1f }),
                        ResolvedPredicate.FloatInPredicate::columnIndex, ResolvedPredicate.FloatInPredicate::membership)),
                Arguments.of("DOUBLE", Leaf.of(new ResolvedPredicate.DoubleInPredicate(0, new double[]{ 1 }),
                        ResolvedPredicate.DoubleInPredicate::columnIndex, ResolvedPredicate.DoubleInPredicate::membership)),
                Arguments.of("FLOAT16", Leaf.of(new ResolvedPredicate.Float16InPredicate(0, new float[]{ 1.5f }),
                        ResolvedPredicate.Float16InPredicate::columnIndex, ResolvedPredicate.Float16InPredicate::membership)),
                Arguments.of("BYTE_STRING", Leaf.of(new ResolvedPredicate.BinaryInPredicate(0,
                        new byte[][]{ "a".getBytes(StandardCharsets.UTF_8) }, ResolvedPredicate.BinaryPredicate.Comparison.BYTE_STRING),
                        ResolvedPredicate.BinaryInPredicate::columnIndex, ResolvedPredicate.BinaryInPredicate::membership)));
    }

    /// An `IN` leaf with its record type's column and index accessors, as the leaf types share no
    /// supertype that declares them.
    record Leaf(ResolvedPredicate predicate, ToIntFunction<ResolvedPredicate> column,
            Function<ResolvedPredicate, Object> membership) {

        @SuppressWarnings("unchecked")
        static <P extends ResolvedPredicate> Leaf of(P predicate, ToIntFunction<P> column, Function<P, ?> membership) {
            return new Leaf(predicate, leaf -> column.applyAsInt((P) leaf), leaf -> membership.apply((P) leaf));
        }
    }

    @Test
    void aBinaryListKeepsItsComparison() {
        ResolvedPredicate.BinaryInPredicate binaries = new ResolvedPredicate.BinaryInPredicate(0,
                new byte[][]{ "a".getBytes(StandardCharsets.UTF_8) }, ResolvedPredicate.BinaryPredicate.Comparison.BYTE_STRING);
        assertThat(((ResolvedPredicate.BinaryInPredicate) remapped(binaries)).comparison()).isEqualTo(binaries.comparison());
    }

    @Test
    void aListUnderAConnectiveKeepsItsIndex() {
        ResolvedPredicate.LongInPredicate longs = new ResolvedPredicate.LongInPredicate(0, new long[]{ 1L, 2L });
        ResolvedPredicate.Or or = (ResolvedPredicate.Or) remapped(
                new ResolvedPredicate.Or(List.of(new ResolvedPredicate.And(List.of(longs)))));
        ResolvedPredicate.And and = (ResolvedPredicate.And) or.children().get(0);
        ResolvedPredicate.LongInPredicate remappedLongs = (ResolvedPredicate.LongInPredicate) and.children().get(0);
        assertThat(remappedLongs.columnIndex()).isEqualTo(3);
        assertThat(remappedLongs.membership()).isSameAs(longs.membership());
    }

    @Test
    void aLeafIsNeverWithoutTheIndexItsDictionaryCheckNeeds() {
        assertThatThrownBy(() -> new ResolvedPredicate.LongInPredicate(0, new long[]{ 1L }, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("membership");
        byte[][] values = { "a".getBytes(StandardCharsets.UTF_8) };
        assertThatThrownBy(() -> new ResolvedPredicate.BinaryInPredicate(0, values,
                ResolvedPredicate.BinaryPredicate.Comparison.BYTE_STRING, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A BYTE_STRING membership needs dictionary index");
    }

    @Test
    void aBinaryListThatIsNotByteExactBuildsNoIndex() {
        // The dictionary check runs only on byte-exact comparisons, so a DECIMAL list carries none.
        ResolvedPredicate.BinaryInPredicate decimals = new ResolvedPredicate.BinaryInPredicate(0,
                new byte[][]{ { 0x01 } }, ResolvedPredicate.BinaryPredicate.Comparison.VARIABLE_DECIMAL);
        assertThat(decimals.membership()).isNull();
        assertThat(((ResolvedPredicate.BinaryInPredicate) remapped(decimals)).membership()).isNull();
        assertThatThrownBy(() -> new ResolvedPredicate.BinaryInPredicate(0, decimals.values(),
                ResolvedPredicate.BinaryPredicate.Comparison.VARIABLE_DECIMAL,
                MembershipIndex.Binaries.of(decimals.values())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A VARIABLE_DECIMAL membership takes no dictionary index");
    }
}
