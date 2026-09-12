/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;

import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.metadata.Statistics;

/// The statistics one unit of a column carries — a column chunk, or a single page of one — and
/// what they prove about a leaf predicate.
///
/// Each implementation resolves where the unit's statistics come from. What each statistic
/// proves is written once, in [MinMaxStats], [NullStats] and [DefinitionLevelStats], and which
/// of them answers which predicate is written once, in [#decide]. [RowGroupFilterEvaluator] and
/// [PageFilterEvaluator] both ask [#decide], so the two prove the same things from the same
/// statistics.
///
/// A unit that cannot be sourced — a column the row group does not carry, or one it carries no
/// metadata for — is a unit whose every statistic is unknown, and it proves nothing.
///
/// Bloom filters, dictionaries and geospatial statistics are not statistics of a unit: Parquet
/// carries them per column chunk only, and [RowGroupFilterEvaluator] applies them itself.
sealed interface UnitStats {

    /// The row count of a unit whose source does not carry one. Every proof that needs a row
    /// count yields [FilterDecision#MIGHT_MATCH] without one.
    long UNKNOWN_ROW_COUNT = -1;

    /// The number of rows the unit covers, or [#UNKNOWN_ROW_COUNT].
    long rowCount();

    /// The unit's min/max statistics, decoded as `leaf` reads them.
    MinMaxStats minMax(ResolvedPredicate leaf);

    /// The unit's null count.
    NullStats nulls();

    /// The unit's definition level histogram.
    DefinitionLevelStats definitionLevels();

    /// Narrows `enclosing`, the position of whatever holds this unit, to the unit itself.
    LogContext locate(LogContext enclosing);

    /// What this unit's statistics prove about one leaf predicate.
    ///
    /// A null predicate on a group that a definition level separates from the leaf — an optional
    /// or repeated node between the two — is answered from [#definitionLevels] alone. The leaf it
    /// is answered from may be repeated, and then its null count tallies absent groups and null
    /// elements alike, over more entries than there are rows, so neither null count rule holds
    /// for it.
    ///
    /// A null predicate on the leaf itself, or on a group with only required nodes down to the
    /// leaf, is answered from [#definitionLevels] where the file wrote a histogram sized for the
    /// leaf, and from [#nulls] otherwise. The leaf is then not repeated and is null exactly where
    /// the node is absent, so it writes one entry per row and the two agree wherever both are
    /// present. The size check is what keeps a predicate carrying level `0` from reading a
    /// histogram at the wrong level: only a required column's histogram is sized for it, and for
    /// that column `0` is its real level.
    ///
    /// A value predicate cannot match a unit that is null on every row, since a null satisfies
    /// none of them, `NOT_EQ` included. Otherwise it is answered from [#minMax]. Every value
    /// predicate type is named here rather than reached by a `default`, so a new one does not
    /// compile until it is placed — and a predicate a null can satisfy must not be placed with
    /// these.
    ///
    /// @param leaf a leaf predicate; `AND` and `OR` are folded by the evaluators
    /// @param logContext the position enclosing this unit, which the unit narrows to itself to
    ///        report bounds it had to discard, or `null` to decide without reporting — for a dry
    ///        run whose only purpose is to record which sources a real decision would consult,
    ///        so that the real decision raises each warning once
    default FilterDecision decide(ResolvedPredicate leaf, LogContext logContext) {
        return switch (leaf) {
            case ResolvedPredicate.IsNullPredicate p -> {
                DefinitionLevelStats levels = definitionLevels();
                yield p.group() || levels.sizedFor(p.leafDefinitionLevel())
                        ? levels.decideIsNull(p.definitionLevel(), p.leafDefinitionLevel())
                        : nulls().decideIsNull();
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                DefinitionLevelStats levels = definitionLevels();
                yield p.group() || levels.sizedFor(p.leafDefinitionLevel())
                        ? levels.decideIsNotNull(p.definitionLevel(), p.leafDefinitionLevel())
                        : nulls().decideIsNotNull();
            }
            // The constants carry no definition levels, so they read the null count alone; a
            // level-0 histogram would only repeat what it says.
            case ResolvedPredicate.EveryNonNullRowPredicate ignored -> nulls().decideIsNotNull();
            case ResolvedPredicate.NoRowPredicate ignored -> FilterDecision.CANNOT_MATCH;
            case ResolvedPredicate.IntPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.LongPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.UnsignedIntPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.UnsignedLongPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.FloatPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.Float16Predicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.DoublePredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.BooleanPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.BinaryPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.IntInPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.LongInPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.UnsignedIntInPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.UnsignedLongInPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.BinaryInPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.DoubleInPredicate ignored -> decideValue(leaf, logContext);
            case ResolvedPredicate.Float16InPredicate ignored -> decideValue(leaf, logContext);
            // Geospatial statistics sit on the column chunk's metadata alone, outside any unit.
            case ResolvedPredicate.GeospatialPredicate ignored -> FilterDecision.MIGHT_MATCH;
            case ResolvedPredicate.And ignored -> throw notALeaf(leaf);
            case ResolvedPredicate.Or ignored -> throw notALeaf(leaf);
        };
    }

    private FilterDecision decideValue(ResolvedPredicate leaf, LogContext logContext) {
        if (nulls().allNull()) {
            return FilterDecision.CANNOT_MATCH;
        }
        MinMaxStats minMax = minMax(leaf);
        if (logContext != null) {
            minMax.reportIfDiscarded(locate(logContext));
        }
        return minMax.decideLeaf(leaf);
    }

    private static IllegalArgumentException notALeaf(ResolvedPredicate predicate) {
        return new IllegalArgumentException(
                "Unit statistics decide a leaf predicate, not " + predicate.getClass().getSimpleName());
    }

    // ==================== Sourcing ====================

    /// A column chunk, sourced from its [ColumnMetaData].
    ///
    /// @param metaData the chunk's metadata, or `null` where there is none to source from
    /// @param rowCount the row group's row count
    record ChunkStats(ColumnMetaData metaData, long rowCount, BoundsReadability readability)
            implements UnitStats {

        /// The chunk of the leaf column at `columnIndex` in `rowGroup`.
        static ChunkStats of(RowGroup rowGroup, int columnIndex, BoundsReadability readability) {
            ColumnMetaData metaData = columnIndex >= 0 && columnIndex < rowGroup.columns().size()
                    ? rowGroup.columns().get(columnIndex).metaData()
                    : null;
            return new ChunkStats(metaData, rowGroup.numRows(), readability);
        }

        @Override
        public MinMaxStats minMax(ResolvedPredicate leaf) {
            Statistics statistics = statistics();
            return statistics == null
                    ? new MinMaxStats.NullCountOnlyStats(null, null)
                    : MinMaxStats.of(statistics, leaf, readability);
        }

        @Override
        public NullStats nulls() {
            Statistics statistics = statistics();
            Long nullCount = statistics == null ? null : statistics.nullCount();
            return new NullStats(nullCount == null ? NullStats.UNKNOWN_NULL_COUNT : nullCount, rowCount);
        }

        @Override
        public DefinitionLevelStats definitionLevels() {
            SizeStatistics sizeStatistics = metaData == null ? null : metaData.sizeStatistics();
            return new DefinitionLevelStats(
                    sizeStatistics == null ? null : sizeStatistics.definitionLevelHistogram(), rowCount);
        }

        @Override
        public LogContext locate(LogContext enclosing) {
            return metaData == null ? enclosing : enclosing.withColumn(metaData.pathInSchema());
        }

        private Statistics statistics() {
            return metaData == null ? null : metaData.statistics();
        }
    }

    /// One page of a column chunk, sourced from the chunk's [ColumnIndex] at the page's position.
    ///
    /// A page the index flags as null-only is null on every row, so its null count is its row
    /// count: the flag and the count state the same fact, and one rule serves both. Its
    /// `min_values` and `max_values` entries are placeholders rather than bounds, and are not
    /// decoded.
    ///
    /// @param rowCount the rows from this page's first row up to the next page's, or up to the
    ///        row group's end for the last page
    record IndexPageStats(ColumnIndex columnIndex, int pageIndex, long rowCount,
            BoundsReadability readability) implements UnitStats {

        /// Page `pageIndex` of the column chunk that `columnIndex` describes and `pages` locates.
        static IndexPageStats of(ColumnIndex columnIndex, List<PageLocation> pages, int pageIndex,
                long rowGroupRowCount, BoundsReadability readability) {
            long firstRow = pages.get(pageIndex).firstRowIndex();
            long endRow = pageIndex + 1 < pages.size()
                    ? pages.get(pageIndex + 1).firstRowIndex()
                    : rowGroupRowCount;
            return new IndexPageStats(columnIndex, pageIndex, endRow - firstRow, readability);
        }

        @Override
        public MinMaxStats minMax(ResolvedPredicate leaf) {
            return nullPage()
                    ? new MinMaxStats.NullCountOnlyStats(rowCount, null)
                    : MinMaxStats.ofPage(columnIndex, pageIndex, leaf, readability);
        }

        @Override
        public NullStats nulls() {
            if (nullPage()) {
                return new NullStats(rowCount, rowCount);
            }
            long[] nullCounts = columnIndex.nullCounts();
            return new NullStats(nullCounts == null ? NullStats.UNKNOWN_NULL_COUNT : nullCounts[pageIndex],
                    rowCount);
        }

        @Override
        public DefinitionLevelStats definitionLevels() {
            return new DefinitionLevelStats(columnIndex.definitionLevelHistogram(pageIndex), rowCount);
        }

        @Override
        public LogContext locate(LogContext enclosing) {
            return enclosing.withPageIndex(pageIndex);
        }

        private boolean nullPage() {
            return columnIndex.nullPages()[pageIndex];
        }
    }
}
