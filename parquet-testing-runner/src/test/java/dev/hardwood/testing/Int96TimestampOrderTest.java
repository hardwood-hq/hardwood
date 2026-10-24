/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.FilterDecision;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.reader.footer.FileFooter;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.reader.plan.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static dev.hardwood.internal.predicate.FilterDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.FilterDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.FilterDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

/// `int96_timestamp_order.parquet`, written by parquet-java with `INT96_TIMESTAMP_ORDER` declared
/// on its one `INT96` column. Its chunk statistics and its column index both bound the column by
/// instant, 1968-05-23 to 1970-01-02, over values whose stored bytes sort in another order.
/// The file is one row group of one page, so the column index is evaluated directly: a read
/// decides the row group from the chunk statistics before the column index is consulted.
class Int96TimestampOrderTest {

    private static final Instant EARLY = Instant.parse("1968-05-23T00:00:00.000000123Z");
    private static final Instant SAME_DAY_EARLY = Instant.parse("1970-01-01T00:00:00.000001Z");
    private static final Instant LATE_IN_DAY = Instant.parse("1970-01-01T23:59:59.999999999Z");
    private static final Instant NEXT_DAY = Instant.parse("1970-01-02T00:00:00Z");

    private static final Instant BEFORE_THE_MINIMUM = EARLY.minusNanos(1);
    private static final Instant AFTER_THE_MAXIMUM = NEXT_DAY.plusNanos(1);
    private static final Instant BETWEEN = Instant.parse("1969-07-20T20:17:00Z");


    @Test
    void declaresTheOrder() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file()))) {
            assertThat(reader.getFileMetaData().columnOrders()).containsExactly(ColumnOrder.INT96_TIMESTAMP_ORDER);
        }
    }

    @Test
    void chunkStatisticsDecideARowGroupByInstant() throws IOException {
        assertThat(rowGroupDecision(FilterPredicate.eq("ts", BEFORE_THE_MINIMUM))).isEqualTo(CANNOT_MATCH);
        assertThat(rowGroupDecision(FilterPredicate.eq("ts", AFTER_THE_MAXIMUM))).isEqualTo(CANNOT_MATCH);
        assertThat(rowGroupDecision(FilterPredicate.lt("ts", EARLY))).isEqualTo(CANNOT_MATCH);
        assertThat(rowGroupDecision(FilterPredicate.eq("ts", BETWEEN))).isEqualTo(MIGHT_MATCH);
        assertThat(rowGroupDecision(FilterPredicate.gtEq("ts", EARLY))).isEqualTo(ALWAYS_MATCHES);
    }

    @Test
    void theColumnIndexDecidesAPageByInstant() throws IOException {
        assertThat(matchingRows(FilterPredicate.eq("ts", BEFORE_THE_MINIMUM), true).isEmpty()).isTrue();
        assertThat(matchingRows(FilterPredicate.gt("ts", NEXT_DAY), true).isEmpty()).isTrue();
        assertThat(matchingRows(FilterPredicate.eq("ts", BETWEEN), true).overlapsPage(0, 4)).isTrue();
    }

    /// Without the order the file declares, the same bounds decide nothing.
    @Test
    void theBoundsAreReadOnlyUnderTheDeclaredOrder() throws IOException {
        assertThat(matchingRows(FilterPredicate.eq("ts", BEFORE_THE_MINIMUM), false).overlapsPage(0, 4)).isTrue();
    }

    @Test
    void aFilteredReadReturnsTheMatchingRows() throws IOException {
        assertThat(read(FilterPredicate.gt("ts", SAME_DAY_EARLY))).containsExactly(LATE_IN_DAY, NEXT_DAY);
        assertThat(read(FilterPredicate.lt("ts", SAME_DAY_EARLY))).containsExactly(EARLY);
        assertThat(read(FilterPredicate.eq("ts", AFTER_THE_MAXIMUM))).isEmpty();
    }

    private static Path file() throws IOException {
        return ParquetTestingRepoCloner.getTestFile("data/int96_timestamp_order.parquet");
    }

    private static FilterDecision rowGroupDecision(FilterPredicate predicate) throws IOException {
        InputFile inputFile = InputFile.of(file());
        inputFile.open();
        try {
            FileFooter footer = ParquetMetadataReader.read(inputFile);
            RowGroup rowGroup = footer.readFooter().metaData().rowGroups().get(0);
            return RowGroupFilterEvaluator.decideRowGroup(FilterPredicateResolver.resolve(predicate, footer.schema()),
                    rowGroup, null, null, new LogContext(inputFile.name(), 0),
                    BoundsReadability.of(footer.schema(), footer.readFooter()));
        }
        finally {
            inputFile.close();
        }
    }

    private static RowRanges matchingRows(FilterPredicate predicate, boolean underTheDeclaredOrder) throws IOException {
        InputFile inputFile = InputFile.of(file());
        inputFile.open();
        try {
            FileFooter footer = ParquetMetadataReader.read(inputFile);
            RowGroup rowGroup = footer.readFooter().metaData().rowGroups().get(0);
            BoundsReadability readability = underTheDeclaredOrder
                    ? BoundsReadability.of(footer.schema(), footer.readFooter())
                    : BoundsReadability.ALL;
            return PageFilterEvaluator.computeMatchingRows(FilterPredicateResolver.resolve(predicate, footer.schema()),
                    rowGroup, RowGroupIndexBuffers.fetch(inputFile, rowGroup), new LogContext(inputFile.name(), 0),
                    readability);
        }
        finally {
            inputFile.close();
        }
    }

    private static List<Instant> read(FilterPredicate predicate) throws IOException {
        List<Instant> values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file()));
                RowReader rows = reader.buildRowReader().filter(predicate).build()) {
            while (rows.hasNext()) {
                rows.next();
                values.add(rows.getTimestamp("ts"));
            }
        }
        return values;
    }
}
