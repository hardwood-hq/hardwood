/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.Statistics;

/// One unit's min/max statistics, decoded — sourced from either row-group-level [Statistics]
/// or a [ColumnIndex] entry for a single page, and asked directly what they prove about a
/// leaf predicate.
///
/// The bounds are decoded once, where the unit is sourced, and held as the primitives the
/// comparisons actually use. A leaf's physical type therefore stops mattering past that point:
/// eleven leaf types map onto six of these, because a `FLOAT16` bound is a `float` once
/// decoded and an `IN` predicate reads the same bounds as the comparison of the same width.
///
/// Bounds the filter layer cannot compare against never become one of the typed variants at
/// all. They become [NullCountOnlyStats], which keeps the one statistic that does not depend
/// on them and carries why the rest were dropped. It proves nothing about a value predicate:
/// [#canDrop] is `false` and [#decideLeaf] is [FilterDecision#MIGHT_MATCH]. Deciding that
/// where the bounds are sourced keeps every comparator free of the question, and keeps a
/// comparator added later from having to remember it.
sealed interface MinMaxStats {

    System.Logger LOG = System.getLogger(MinMaxStats.class.getName());

    /// Read from the deprecated `min`/`max` Thrift fields, which compare unsigned whatever the
    /// column's type is, so they carry the wrong order for every signed one.
    String DEPRECATED_SORT_ORDER =
            "they come from the deprecated min/max fields, which compare unsigned";

    /// The Parquet spec forbids writing `NaN` to statistics min/max, but older and buggy
    /// writers have produced such bounds (#566). `NaN` sorts above every finite value in
    /// `Double.compare`'s total order, so comparing against it would prune units holding
    /// matching finite rows.
    String NOT_A_NUMBER = "one of them is NaN, which sits outside the column's ordering";

    /// The spec requires `min <= max`. A pair the wrong way round excludes every value it
    /// should contain and contains every value it should exclude, so it is wrong in both
    /// pruning directions at once (#1172).
    String INVERTED = "the minimum sorts above the maximum";

    /// The number of null values in the unit, or `null` if unknown.
    Long nullCount();

    /// Why the file's bounds were discarded, or `null` where they were kept — or where the
    /// file wrote none to discard.
    default String discardReason() {
        return null;
    }

    /// Whether these bounds prove no row can match the leaf, so the unit can be skipped.
    boolean canDrop(ResolvedPredicate leaf);

    /// Whether these bounds prove every value in the unit satisfies the leaf. Assumes the
    /// caller has established a zero null count.
    boolean alwaysMatches(ResolvedPredicate leaf);

    /// Says at `WARNING` that these bounds were discarded, so that a read which pruned nothing
    /// can be traced back to the file that caused it. Silent when they were kept.
    ///
    /// Reporting only; the discard itself already took effect when the unit was sourced, since
    /// unusable bounds become [NullCountOnlyStats], which drops nothing.
    ///
    /// Every discard is reported, with no attempt to collapse repeats. Statistics that will
    /// not compare are rare, and a reader who finds the volume unhelpful can raise the level
    /// on this logger — neither is worth carrying state through the evaluators to pre-empt.
    ///
    /// @param logContext where these statistics were read from, for the message to name
    default void reportIfDiscarded(LogContext logContext) {
        String discardReason = discardReason();
        if (discardReason == null) {
            return;
        }
        LOG.log(System.Logger.Level.WARNING,
                "{0}Ignoring the min/max statistics for pruning: {1}. "
                        + "Rows they could have skipped are read and filtered instead.",
                logContext.prefix(), discardReason);
    }

    /// Whether the unit is proven to hold no nulls.
    default boolean nullFree() {
        Long nullCount = nullCount();
        return nullCount != null && nullCount == 0;
    }

    /// What these statistics prove about a value predicate, as a three-valued
    /// [FilterDecision].
    ///
    /// [FilterDecision#ALWAYS_MATCHES] requires the whole `[min, max]` interval to satisfy
    /// the predicate **and** a proven-zero null count — a null row satisfies no value
    /// predicate, so without it a fully-matching range still cannot promise every row. That
    /// conjunction is the reason to compose the two halves here rather than at each call
    /// site: [#alwaysMatches] answers only the interval half, and dropping the null-count
    /// half returns null rows to a caller that asked for a value.
    ///
    /// Truncated (inexact) bounds are safe by construction: they only widen the interval,
    /// and a predicate satisfied by the widened interval is satisfied by the actual values.
    ///
    /// `IS NULL` and `IS NOT NULL` are not value predicates and do not come here.
    /// [RowGroupFilterEvaluator] decides them from the null count against the row count,
    /// which lets it drop a wholly-null row group — something this cannot see, holding no
    /// row count of its own.
    default FilterDecision decideLeaf(ResolvedPredicate leaf) {
        if (canDrop(leaf)) {
            return FilterDecision.CANNOT_MATCH;
        }
        if (!nullFree()) {
            return FilterDecision.MIGHT_MATCH;
        }
        return alwaysMatches(leaf) ? FilterDecision.ALWAYS_MATCHES : FilterDecision.MIGHT_MATCH;
    }

    // ==================== Sourcing ====================

    /// The statistics of a column chunk, or of a single page where they are carried inline on
    /// the page header, decoded as the given leaf reads them.
    static MinMaxStats of(Statistics stats, ResolvedPredicate leaf) {
        if (stats.isMinMaxDeprecated()) {
            return new NullCountOnlyStats(stats.nullCount(), DEPRECATED_SORT_ORDER);
        }
        return sourced(stats.minValue(), stats.maxValue(), stats.nullCount(), leaf);
    }

    /// The [ColumnIndex] entry for one page of a column chunk.
    static MinMaxStats ofPage(ColumnIndex columnIndex, int pageIndex, ResolvedPredicate leaf) {
        long[] nullCounts = columnIndex.nullCounts();
        return sourced(columnIndex.minValues().get(pageIndex), columnIndex.maxValues().get(pageIndex),
                nullCounts != null ? Long.valueOf(nullCounts[pageIndex]) : null, leaf);
    }

    /// Decodes the pair as the leaf reads it, yielding [NullCountOnlyStats] where the file
    /// wrote no bounds, where the leaf reads none, or where the pair does not hold together.
    private static MinMaxStats sourced(byte[] min, byte[] max, Long nullCount, ResolvedPredicate leaf) {
        if (min == null || max == null) {
            // Nothing was written, so nothing was discarded; a half-present pair prunes no
            // more than an absent one.
            return new NullCountOnlyStats(nullCount, null);
        }
        return switch (leaf) {
            case ResolvedPredicate.IntPredicate ignored -> IntStats.of(
                    StatisticsDecoder.decodeInt(min), StatisticsDecoder.decodeInt(max), nullCount);
            case ResolvedPredicate.IntInPredicate ignored -> IntStats.of(
                    StatisticsDecoder.decodeInt(min), StatisticsDecoder.decodeInt(max), nullCount);
            case ResolvedPredicate.LongPredicate ignored -> LongStats.of(
                    StatisticsDecoder.decodeLong(min), StatisticsDecoder.decodeLong(max), nullCount);
            case ResolvedPredicate.LongInPredicate ignored -> LongStats.of(
                    StatisticsDecoder.decodeLong(min), StatisticsDecoder.decodeLong(max), nullCount);
            case ResolvedPredicate.BooleanPredicate ignored -> BooleanStats.of(
                    StatisticsDecoder.decodeBoolean(min), StatisticsDecoder.decodeBoolean(max), nullCount);
            case ResolvedPredicate.FloatPredicate p -> FloatStats.of(
                    StatisticsDecoder.decodeFloat(min), StatisticsDecoder.decodeFloat(max),
                    p.ieee754TotalOrder(), nullCount);
            // A binary16 bound is a float once decoded, so it needs no variant of its own.
            case ResolvedPredicate.Float16Predicate p -> FloatStats.of(
                    StatisticsDecoder.decodeFloat16(min), StatisticsDecoder.decodeFloat16(max),
                    p.ieee754TotalOrder(), nullCount);
            case ResolvedPredicate.DoublePredicate p -> DoubleStats.of(
                    StatisticsDecoder.decodeDouble(min), StatisticsDecoder.decodeDouble(max),
                    p.ieee754TotalOrder(), nullCount);
            case ResolvedPredicate.BinaryPredicate p -> BinaryStats.of(min, max, p.signed(), nullCount);
            case ResolvedPredicate.BinaryInPredicate p -> BinaryStats.of(min, max, p.signed(), nullCount);
            // An IN list reads the bounds of its column's own width, so it lands on the same
            // variant the comparison of that width does.
            case ResolvedPredicate.Float16InPredicate p -> FloatStats.of(
                    StatisticsDecoder.decodeFloat16(min), StatisticsDecoder.decodeFloat16(max),
                    p.ieee754TotalOrder(), nullCount);
            case ResolvedPredicate.DoubleInPredicate p -> p.floatColumn()
                    ? FloatStats.of(StatisticsDecoder.decodeFloat(min), StatisticsDecoder.decodeFloat(max),
                            p.ieee754TotalOrder(), nullCount)
                    : DoubleStats.of(StatisticsDecoder.decodeDouble(min), StatisticsDecoder.decodeDouble(max),
                            p.ieee754TotalOrder(), nullCount);
            // These leaves read no bounds, so there is nothing to decode and nothing to
            // validate. What the file wrote is not discarded; it is simply not their business.
            case ResolvedPredicate.IsNullPredicate ignored -> new NullCountOnlyStats(nullCount, null);
            case ResolvedPredicate.IsNotNullPredicate ignored -> new NullCountOnlyStats(nullCount, null);
            case ResolvedPredicate.GeospatialPredicate ignored -> new NullCountOnlyStats(nullCount, null);
            case ResolvedPredicate.And ignored -> new NullCountOnlyStats(nullCount, null);
            case ResolvedPredicate.Or ignored -> new NullCountOnlyStats(nullCount, null);
        };
    }

    /// Reports a leaf reaching bounds of a width that cannot decide it. A unit is decoded for
    /// the leaf it is then asked about, so this is a wiring mistake rather than anything a
    /// file can cause.
    private static IllegalArgumentException wrongWidth(String type, ResolvedPredicate leaf) {
        return new IllegalArgumentException(
                type + " statistics cannot decide a " + leaf.getClass().getSimpleName());
    }

    /// Why a floating-point pair cannot be compared against, or `null` when it can — over
    /// `double` for both widths, since a `float` widens exactly and both the `NaN` test and
    /// the ordering are the same question at either.
    private static String floatingPointReason(double min, double max, boolean ieee754TotalOrder) {
        if (Double.isNaN(min) || Double.isNaN(max)) {
            return NOT_A_NUMBER;
        }
        // The comparators widen a ±0 bound under the type-defined ordering, where the spec
        // leaves the two zeroes interchangeable; the same widening applies here so that
        // legitimate bounds of (+0, -0) are not read as inverted.
        if (!ieee754TotalOrder) {
            min = (min == 0.0) ? -0.0 : min;
            max = (max == 0.0) ? 0.0 : max;
        }
        return Double.compare(min, max) > 0 ? INVERTED : null;
    }

    // ==================== The typed bounds ====================

    /// Everything left when there are no bounds to compare against — because the file wrote
    /// none, because this leaf reads none, or because the pair it wrote was discarded as
    /// unusable, which `discardReason` is what tells apart.
    ///
    /// The null count survives all three, and decides `IS NOT NULL` on its own.
    record NullCountOnlyStats(Long nullCount, String discardReason) implements MinMaxStats {

        @Override
        public boolean canDrop(ResolvedPredicate leaf) {
            return false;
        }

        @Override
        public boolean alwaysMatches(ResolvedPredicate leaf) {
            return false;
        }
    }

    /// `INT32` bounds, including those of an `INT32`-backed `DECIMAL`.
    record IntStats(int min, int max, Long nullCount) implements MinMaxStats {

        static MinMaxStats of(int min, int max, Long nullCount) {
            return min > max ? new NullCountOnlyStats(nullCount, INVERTED) : new IntStats(min, max, nullCount);
        }

        @Override
        public boolean canDrop(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.IntPredicate p ->
                        StatisticsFilterSupport.canDrop(p.op(), p.value(), min, max);
                case ResolvedPredicate.IntInPredicate p ->
                        StatisticsFilterSupport.canDropIntIn(p.values(), min, max);
                default -> throw wrongWidth("INT32", leaf);
            };
        }

        @Override
        public boolean alwaysMatches(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.IntPredicate p ->
                        StatisticsFilterSupport.alwaysMatches(p.op(), p.value(), min, max);
                case ResolvedPredicate.IntInPredicate p ->
                        StatisticsFilterSupport.alwaysMatchesIntIn(p.values(), min, max);
                default -> throw wrongWidth("INT32", leaf);
            };
        }
    }

    /// `INT64` bounds, including those of an `INT64`-backed `DECIMAL`.
    record LongStats(long min, long max, Long nullCount) implements MinMaxStats {

        static MinMaxStats of(long min, long max, Long nullCount) {
            return min > max ? new NullCountOnlyStats(nullCount, INVERTED) : new LongStats(min, max, nullCount);
        }

        @Override
        public boolean canDrop(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.LongPredicate p ->
                        StatisticsFilterSupport.canDrop(p.op(), p.value(), min, max);
                case ResolvedPredicate.LongInPredicate p ->
                        StatisticsFilterSupport.canDropLongIn(p.values(), min, max);
                default -> throw wrongWidth("INT64", leaf);
            };
        }

        @Override
        public boolean alwaysMatches(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.LongPredicate p ->
                        StatisticsFilterSupport.alwaysMatches(p.op(), p.value(), min, max);
                case ResolvedPredicate.LongInPredicate p ->
                        StatisticsFilterSupport.alwaysMatchesLongIn(p.values(), min, max);
                default -> throw wrongWidth("INT64", leaf);
            };
        }
    }

    /// `BOOLEAN` bounds, compared as `0` and `1`.
    record BooleanStats(boolean min, boolean max, Long nullCount) implements MinMaxStats {

        static MinMaxStats of(boolean min, boolean max, Long nullCount) {
            // false sorts below true, so only a true minimum with a false maximum is inverted.
            return min && !max
                    ? new NullCountOnlyStats(nullCount, INVERTED)
                    : new BooleanStats(min, max, nullCount);
        }

        @Override
        public boolean canDrop(ResolvedPredicate leaf) {
            if (leaf instanceof ResolvedPredicate.BooleanPredicate p) {
                return StatisticsFilterSupport.canDrop(p.op(), ordinal(p.value()), ordinal(min), ordinal(max));
            }
            throw wrongWidth("BOOLEAN", leaf);
        }

        @Override
        public boolean alwaysMatches(ResolvedPredicate leaf) {
            if (leaf instanceof ResolvedPredicate.BooleanPredicate p) {
                return StatisticsFilterSupport.alwaysMatches(p.op(), ordinal(p.value()), ordinal(min), ordinal(max));
            }
            throw wrongWidth("BOOLEAN", leaf);
        }

        private static long ordinal(boolean value) {
            return value ? 1 : 0;
        }
    }

    /// `FLOAT` bounds, and the `FLOAT16` ones that widen exactly onto them.
    ///
    /// `ieee754TotalOrder` is carried rather than applied here because it changes what the
    /// comparators compare against, not whether the bounds hold together; see
    /// [StatisticsFilterSupport#canDropFloat].
    record FloatStats(float min, float max, boolean ieee754TotalOrder, Long nullCount)
            implements MinMaxStats {

        static MinMaxStats of(float min, float max, boolean ieee754TotalOrder, Long nullCount) {
            String reason = floatingPointReason(min, max, ieee754TotalOrder);
            return reason != null
                    ? new NullCountOnlyStats(nullCount, reason)
                    : new FloatStats(min, max, ieee754TotalOrder, nullCount);
        }

        @Override
        public boolean canDrop(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.FloatPredicate p -> StatisticsFilterSupport.canDropFloat(
                        p.op(), p.value(), min, max, ieee754TotalOrder);
                case ResolvedPredicate.Float16Predicate p -> StatisticsFilterSupport.canDropFloat(
                        p.op(), p.value(), min, max, ieee754TotalOrder);
                case ResolvedPredicate.DoubleInPredicate p -> StatisticsFilterSupport.canDropDoubleIn(
                        p.values(), min, max, ieee754TotalOrder);
                case ResolvedPredicate.Float16InPredicate p -> StatisticsFilterSupport.canDropDoubleIn(
                        p.values(), min, max, ieee754TotalOrder);
                default -> throw wrongWidth("FLOAT", leaf);
            };
        }

        /// NaN values sit outside the min/max ordering, so a unit whose `[min, max]` fully
        /// satisfies the predicate may still hold non-matching NaN rows. `nan_count` would
        /// settle it — the reader parses it and Hardwood's writer emits it — but nothing here
        /// consults it, so a floating-point column is never promised a full match (#898).
        @Override
        public boolean alwaysMatches(ResolvedPredicate leaf) {
            return false;
        }
    }

    /// `DOUBLE` bounds. See [FloatStats] for what `ieee754TotalOrder` is doing here.
    record DoubleStats(double min, double max, boolean ieee754TotalOrder, Long nullCount)
            implements MinMaxStats {

        static MinMaxStats of(double min, double max, boolean ieee754TotalOrder, Long nullCount) {
            String reason = floatingPointReason(min, max, ieee754TotalOrder);
            return reason != null
                    ? new NullCountOnlyStats(nullCount, reason)
                    : new DoubleStats(min, max, ieee754TotalOrder, nullCount);
        }

        @Override
        public boolean canDrop(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.DoublePredicate p -> StatisticsFilterSupport.canDropDouble(
                        p.op(), p.value(), min, max, ieee754TotalOrder);
                case ResolvedPredicate.DoubleInPredicate p -> StatisticsFilterSupport.canDropDoubleIn(
                        p.values(), min, max, ieee754TotalOrder);
                default -> throw wrongWidth("DOUBLE", leaf);
            };
        }

        /// See [FloatStats#alwaysMatches].
        @Override
        public boolean alwaysMatches(ResolvedPredicate leaf) {
            return false;
        }
    }

    /// Byte-string bounds, compared unsigned or as big-endian two's complement according to
    /// the column's order.
    record BinaryStats(byte[] min, byte[] max, boolean signed, Long nullCount) implements MinMaxStats {

        static MinMaxStats of(byte[] min, byte[] max, boolean signed, Long nullCount) {
            // -100 sorts below +100 as a two's complement number and above it as a byte
            // string, so only the column's own order tells a decimal's bounds apart from an
            // inverted pair.
            int comparison = signed
                    ? BinaryComparator.compareSigned(min, max)
                    : BinaryComparator.compareUnsigned(min, max);
            return comparison > 0
                    ? new NullCountOnlyStats(nullCount, INVERTED)
                    : new BinaryStats(min, max, signed, nullCount);
        }

        @Override
        public boolean canDrop(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.BinaryPredicate p -> StatisticsFilterSupport.canDropCompared(
                        p.op(), compare(p.value(), min), compare(p.value(), max), compare(min, max));
                case ResolvedPredicate.BinaryInPredicate p ->
                        StatisticsFilterSupport.canDropBinaryIn(p.values(), min, max, signed);
                default -> throw wrongWidth("BYTE_ARRAY", leaf);
            };
        }

        @Override
        public boolean alwaysMatches(ResolvedPredicate leaf) {
            return switch (leaf) {
                case ResolvedPredicate.BinaryPredicate p -> StatisticsFilterSupport.alwaysMatchesCompared(
                        p.op(), compare(p.value(), min), compare(p.value(), max), compare(min, max));
                case ResolvedPredicate.BinaryInPredicate p ->
                        StatisticsFilterSupport.alwaysMatchesBinaryIn(p.values(), min, max, signed);
                default -> throw wrongWidth("BYTE_ARRAY", leaf);
            };
        }

        private int compare(byte[] left, byte[] right) {
            return signed
                    ? BinaryComparator.compareSigned(left, right)
                    : BinaryComparator.compareUnsigned(left, right);
        }
    }
}
