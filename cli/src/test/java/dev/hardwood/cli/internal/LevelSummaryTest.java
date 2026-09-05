/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/// Pins the schema-derived level labels against the shapes that change
/// the outcome: a LIST, a MAP, a struct member, an unannotated repeated
/// field, a flat required column and a flat optional one. The labels are
/// what make a histogram readable, so a change to the walk shows up here
/// rather than in a screenshot.
///
/// Also pins the quantities derived from the histograms, the three arms
/// of the consistency check, and the element-wise combination `inspect`
/// uses to reach a file-wide block.
class LevelSummaryTest {

    /// The dive fixture covers the schema shapes; `size_statistics_test`
    /// covers the counts, since its histograms have values in the buckets
    /// below the leaf rather than the dive fixture's all-present data.
    private static final String DIVE_FIXTURE = "/dive_screenshots_fixture.parquet";
    private static final String COUNTS_FIXTURE = "/size_statistics_test.parquet";

    private static FileSchema fixtureSchema() throws Exception {
        return fixtureSchema(DIVE_FIXTURE);
    }

    private static FileSchema fixtureSchema(String resource) throws Exception {
        Path file = Path.of(LevelSummaryTest.class.getResource(resource).toURI());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return reader.getFileSchema();
        }
    }

    private static ColumnSchema column(FileSchema schema, String dottedName) {
        for (ColumnSchema candidate : schema.getColumns()) {
            if (candidate.fieldPath().matchesDottedName(dottedName)) {
                return candidate;
            }
        }
        throw new IllegalArgumentException("no such column: " + dottedName);
    }

    private static ColumnMetaData chunkMetaData(String dottedName) throws Exception {
        return chunkMetaData(DIVE_FIXTURE, dottedName);
    }

    private static ColumnMetaData chunkMetaData(String resource, String dottedName) throws Exception {
        Path file = Path.of(LevelSummaryTest.class.getResource(resource).toURI());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            for (ColumnChunk chunk : reader.getFileMetaData().rowGroups().get(0).columns()) {
                if (chunk.metaData().pathInSchema().matchesDottedName(dottedName)) {
                    return chunk.metaData();
                }
            }
        }
        throw new IllegalArgumentException("no such column: " + dottedName);
    }

    private static LevelSummary summaryOf(FileSchema schema, String dottedName) throws Exception {
        return LevelSummary.of(schema, column(schema, dottedName), chunkMetaData(dottedName));
    }

    private static LevelSummary summaryOf(String resource, String dottedName) throws Exception {
        FileSchema schema = fixtureSchema(resource);
        return LevelSummary.of(schema, column(schema, dottedName), chunkMetaData(resource, dottedName));
    }

    /// Restates a chunk's declared value count so the consistency check
    /// has something to disagree with — no writer in reach produces a
    /// chunk whose histogram contradicts its own header.
    private static ColumnMetaData withNumValues(ColumnMetaData source, long numValues) {
        return copyOf(source, numValues, source.statistics(), source.sizeStatistics());
    }

    private static ColumnMetaData withNullCount(ColumnMetaData source, long nullCount) {
        return copyOf(source, source.numValues(),
                new Statistics(null, null, nullCount, null, false), source.sizeStatistics());
    }

    private static ColumnMetaData withDefinitionHistogram(ColumnMetaData source, long... histogram) {
        SizeStatistics original = source.sizeStatistics();
        return copyOf(source, source.numValues(), source.statistics(),
                new SizeStatistics(original.unencodedByteArrayDataBytes(),
                        original.repetitionLevelHistogram(), histogram));
    }

    private static ColumnMetaData copyOf(ColumnMetaData source, long numValues, Statistics statistics,
                                         SizeStatistics sizeStatistics) {
        return new ColumnMetaData(source.type(), source.encodings(), source.pathInSchema(), source.codec(),
                numValues, source.totalUncompressedSize(), source.totalCompressedSize(),
                source.keyValueMetadata(), source.dataPageOffset(), source.dictionaryPageOffset(),
                statistics, source.geospatialStatistics(), source.bloomFilterOffset(),
                source.bloomFilterLength(), source.encodingStats(), sizeStatistics);
    }

    private static LevelSummary.LevelRow row(int level, String label, long count) {
        return new LevelSummary.LevelRow(level, label, count, 0.0);
    }

    /// A required column holds no nulls whether or not the writer said so, and
    /// the present-value count on the row beside it already asserts exactly
    /// that. Reporting "unknown" here would make the two contradict each other.
    @Test
    void nullCountFollowsFromTheColumnShapeWithoutStatistics() throws Exception {
        FileSchema schema = fixtureSchema();
        LevelSummary required = summaryOf(schema, "id");

        assertThat(required.maxDefinitionLevel()).isZero();
        assertThat(required.nullCount(null)).isZero();
    }

    /// A nullable column with no histogram and no `null_count` has no source
    /// for the figure, and inventing one from the value count would report
    /// every null as a present value.
    @Test
    void nullCountIsUnknownWhereNeitherSourceAnswers() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema optional = column(schema, "websites.list.element");
        ColumnMetaData stripped = copyOf(chunkMetaData("websites.list.element"),
                chunkMetaData("websites.list.element").numValues(), null, null);

        assertThat(LevelSummary.of(schema, optional, stripped).nullCount(null)).isNegative();
    }

    /// Where the histogram is present the count it implies is exact, so it
    /// answers even for a chunk whose statistics the writer omitted.
    @Test
    void nullCountFallsBackToTheHistogramWhereStatisticsAreAbsent() throws Exception {
        LevelSummary summary = summaryOf(DIVE_FIXTURE, "websites.list.element");

        assertThat(summary.nullCount(null))
                .isEqualTo(summary.numValues() - summary.presentValues());
    }

    /// The writer's own figure wins where it recorded one; the consistency
    /// check is what reports the two disagreeing, not this.
    @Test
    void declaredNullCountWinsOverTheImpliedOne() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema websites = column(schema, "websites.list.element");
        ColumnMetaData declared = withNullCount(chunkMetaData("websites.list.element"), 7);

        assertThat(LevelSummary.of(schema, websites, declared).nullCount(declared.statistics()))
                .isEqualTo(7);
    }

    @Test
    void listLabelsNameTheUserFacingFieldNotTheSyntheticListNode() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema websites = column(schema, "websites.list.element");

        assertThat(LevelSummary.definitionLabels(schema, websites))
                .containsExactly("websites null", "websites empty", "element null", "element present");
        assertThat(LevelSummary.repetitionLabels(schema, websites))
                .containsExactly("new record", "websites.list");
    }

    @Test
    void mapLabelsNameTheMapFieldNotKeyValue() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema value = column(schema, "names.common.key_value.value");

        assertThat(LevelSummary.definitionLabels(schema, value))
                .containsExactly("names null", "common null", "common empty", "value null", "value present");
        assertThat(LevelSummary.repetitionLabels(schema, value))
                .containsExactly("new record", "names.common.key_value");
    }

    @Test
    void flatRequiredColumnHasOnlyThePresentLevel() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema id = column(schema, "id");

        assertThat(LevelSummary.definitionLabels(schema, id)).containsExactly("id present");
        assertThat(LevelSummary.repetitionLabels(schema, id)).containsExactly("new record");
    }

    @Test
    void flatOptionalColumnHasNullAndPresent() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema confidence = column(schema, "confidence");

        assertThat(LevelSummary.definitionLabels(schema, confidence))
                .containsExactly("confidence null", "confidence present");
    }

    /// A struct member's label names the member, not the struct: the
    /// enclosing struct already has its own level below it.
    @Test
    void structMemberLabelsNameTheMember() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema xmin = column(schema, "bbox.xmin");

        assertThat(LevelSummary.definitionLabels(schema, xmin))
                .containsExactly("bbox null", "xmin null", "xmin present");
    }

    /// A repeated field with no LIST annotation has no wrapper group, so the
    /// empty-collection bucket has no enclosing field to borrow a name from
    /// and falls back to the repeated node's own.
    @Test
    void unannotatedRepeatedFieldLabelsTheEmptyBucketWithItsOwnName() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.group("root", RepetitionType.REQUIRED, 1),
                SchemaElement.primitive("tags", PhysicalType.INT32, RepetitionType.REPEATED)));
        ColumnSchema tags = column(schema, "tags");

        assertThat(LevelSummary.definitionLabels(schema, tags))
                .containsExactly("tags empty", "tags present");
        assertThat(LevelSummary.repetitionLabels(schema, tags))
                .containsExactly("new record", "tags");
    }

    @Test
    void listColumnDerivesRecordsPresentValuesAndAverages() throws Exception {
        FileSchema schema = fixtureSchema();
        LevelSummary summary = summaryOf(schema, "websites.list.element");

        // num_values 300, rep [150, 150], def [0, 0, 0, 300], unencoded 8130.
        assertThat(summary.hasRecords()).isTrue();
        assertThat(summary.records()).isEqualTo(150L);
        assertThat(summary.presentValues()).isEqualTo(300L);
        assertThat(summary.avgFanOut()).isEqualTo(2.0);
        assertThat(summary.hasAvgListLength()).isTrue();
        assertThat(summary.avgListLength()).isEqualTo(2.0);
        assertThat(summary.hasUnencoded()).isTrue();
        assertThat(summary.unencodedBytes()).isEqualTo(8130L);
        assertThat(summary.avgValueSize()).isEqualTo(27.1);
        assertThat(summary.mismatch()).isNull();
    }

    /// The dive fixture's lists are all present and all non-empty, so it
    /// cannot show that the buckets below the first repeated node are
    /// subtracted. `tags` there is `[[1, 2], [], None, [3]]`: def
    /// `[1, 1, 0, 3]` over rep `[4, 1]`, so two of the four records hold no
    /// element and the three elements sit in two lists.
    @Test
    void averagesSubtractTheRecordsThatHoldNoElement() throws Exception {
        LevelSummary summary = summaryOf(COUNTS_FIXTURE, "tags.list.element");

        assertThat(summary.records()).isEqualTo(4L);
        assertThat(summary.presentValues()).isEqualTo(3L);
        assertThat(summary.definitionTotal()).isEqualTo(5L);
        // Five level slots over four records.
        assertThat(summary.avgFanOut()).isEqualTo(1.25);
        // Three elements over the two records whose list is non-empty; the
        // absent list and the empty one are both excluded from each side.
        assertThat(summary.hasAvgListLength()).isTrue();
        assertThat(summary.avgListLength()).isEqualTo(1.5);
        assertThat(summary.mismatch()).isNull();
    }

    /// A required, non-repeated `BYTE_ARRAY` writes no histograms at all,
    /// but every value is present by definition — so the present-value
    /// count is still known, and with it the average value size.
    @Test
    void flatRequiredByteArrayStillHasAnAverageValueSize() throws Exception {
        FileSchema schema = fixtureSchema();
        LevelSummary summary = summaryOf(schema, "id");

        // Present but empty, which is how a writer records "no levels here";
        // distinct from the absent structure `metric_a` carries.
        assertThat(chunkMetaData("id").sizeStatistics().definitionLevelHistogram()).isEmpty();
        assertThat(summary.hasDefinitionHistogram()).isFalse();
        assertThat(summary.presentValues()).isEqualTo(150L);
        assertThat(summary.records()).isEqualTo(150L);
        // One value per record by definition, with no histogram to divide.
        assertThat(summary.hasAvgFanOut()).isTrue();
        assertThat(summary.avgFanOut()).isEqualTo(1.0);
        assertThat(summary.unencodedBytes()).isEqualTo(1800L);
        assertThat(summary.avgValueSize()).isEqualTo(12.0);
        assertThat(summary.hasAvgListLength()).isFalse();
        assertThat(summary.mismatch()).isNull();
    }

    /// The format records an unencoded size only for `BYTE_ARRAY`, because
    /// for a fixed width it is the value count times the width. Computing it
    /// keeps the figure available on every column. `Avg value size` stays
    /// `BYTE_ARRAY`-only: for a fixed width it would restate the width.
    @Test
    void fixedWidthUnencodedSizeIsComputedFromTheValueCount() throws Exception {
        FileSchema schema = fixtureSchema();
        LevelSummary summary = summaryOf(schema, "confidence");

        assertThat(summary.hasDefinitionHistogram()).isTrue();
        assertThat(summary.presentValues()).isEqualTo(150L);
        assertThat(summary.hasUnencoded()).isTrue();
        assertThat(summary.unencodedBytes()).isEqualTo(150L * 8);   // DOUBLE
        // No length prefixes on a fixed-width value, which is what keeps the
        // figure comparable with a recorded BYTE_ARRAY one.
        assertThat(summary.lengthPrefixBytes()).isZero();
        assertThat(summary.hasAvgValueSize()).isFalse();
    }

    /// A writer omits `SizeStatistics` entirely for a required, non-repeated,
    /// fixed-width column — the shape least able to spare the figure, since
    /// nothing else in the footer reports its size. The summary still forms
    /// and the unencoded size still follows from the value count; only the
    /// recorded-statistics flag goes false.
    @Test
    void columnWithoutSizeStatisticsStillYieldsAComputedSize() throws Exception {
        FileSchema schema = fixtureSchema();
        LevelSummary summary = summaryOf(schema, "metric_a");

        assertThat(summary.hasSizeStatistics()).isFalse();
        assertThat(summary.hasDefinitionHistogram()).isFalse();
        assertThat(summary.hasUnencoded()).isTrue();
        assertThat(summary.unencodedBytes()).isEqualTo(150L * 8);
    }

    @Test
    void levelRowsCarryLabelsCountsAndShares() throws Exception {
        FileSchema schema = fixtureSchema();
        LevelSummary summary = summaryOf(schema, "websites.list.element");

        assertThat(summary.definitionLevels())
                .extracting(LevelSummary.LevelRow::label, LevelSummary.LevelRow::count)
                .containsExactly(
                        tuple("websites null", 0L),
                        tuple("websites empty", 0L),
                        tuple("element null", 0L),
                        tuple("element present", 300L));
        assertThat(summary.definitionLevels().get(3).share()).isEqualTo(1.0);
        assertThat(summary.repetitionLevels())
                .extracting(LevelSummary.LevelRow::label, LevelSummary.LevelRow::count)
                .containsExactly(tuple("new record", 150L), tuple("websites.list", 150L));
    }

    /// A bucket holding one value in a million still has to read as
    /// present, so a non-zero share never rounds away to an empty bar.
    @Test
    void barUsesEighthBlocksForSubCellResolution() {
        assertThat(LevelSummary.bar(1.0, 4)).isEqualTo("████");
        assertThat(LevelSummary.bar(0.5, 4)).isEqualTo("██");
        assertThat(LevelSummary.bar(0.0, 4)).isEmpty();
        assertThat(LevelSummary.bar(0.03, 4)).isEqualTo("▏");
    }

    /// The bar is the first thing worth dropping on a narrow pane and the
    /// count the last, so the level and its label survive every width.
    @Test
    void narrowWidthsDropTheBarThenThePercentage() {
        List<LevelSummary.LevelRow> rows = List.of(
                new LevelSummary.LevelRow(0, "websites null", 0L, 0.0),
                new LevelSummary.LevelRow(1, "element present", 300L, 1.0));

        assertThat(LevelSummary.renderLevels(rows, 60).get(1)).contains("█").contains("100.0%");
        assertThat(LevelSummary.renderLevels(rows, 50).get(1)).doesNotContain("█").contains("100.0%");
        assertThat(LevelSummary.renderLevels(rows, 40).get(1)).doesNotContain("%").contains("300");
        assertThat(LevelSummary.renderLevels(rows, 40).get(1)).contains("element present");
    }

    /// A wide-glyph label occupies two cells per character, so padding by
    /// `String.length()` would push the count column out of line with
    /// ASCII-labelled rows; the label field pads by display width.
    @Test
    void wideGlyphLabelsKeepTheCountColumnAligned() {
        List<LevelSummary.LevelRow> rows = List.of(
                new LevelSummary.LevelRow(0, "element present", 300L, 1.0),
                new LevelSummary.LevelRow(1, "市区町村", 300L, 1.0));

        List<String> lines = LevelSummary.renderLevels(rows, 60);
        int asciiPrefix = countColumnStart(lines.get(0));
        int widePrefix = countColumnStart(lines.get(1));
        assertThat(widePrefix).isEqualTo(asciiPrefix);
    }

    private static int countColumnStart(String line) {
        return Strings.width(line.substring(0, line.indexOf("300")));
    }

    /// A full-share row is the widest a histogram produces, so it decides
    /// whether the block fits its budget. Below [LevelSummary#MINIMUM_WIDTH]
    /// the fixed level, label and count columns cannot fit at all and the
    /// caller is expected not to ask.
    @Test
    void renderedLevelRowsNeverExceedTheGivenWidth() {
        List<LevelSummary.LevelRow> rows = List.of(
                new LevelSummary.LevelRow(0, "websites null", 0L, 0.0),
                new LevelSummary.LevelRow(1, "element present", 6_291_456L, 1.0));

        for (int width = LevelSummary.MINIMUM_WIDTH; width <= 120; width++) {
            int given = width;
            assertThat(LevelSummary.renderLevels(rows, given))
                    .as("rows rendered for width %d", given)
                    .allSatisfy(line -> assertThat(line.length()).isLessThanOrEqualTo(given));
        }
    }

    /// The check is what makes the screen worth opening on a suspect
    /// file, so it has to fire on a chunk whose declared count and
    /// histogram disagree rather than rendering both without comment.
    @Test
    void mismatchedValueCountIsReported() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema websites = column(schema, "websites.list.element");
        ColumnMetaData original = chunkMetaData("websites.list.element");
        ColumnMetaData tampered = withNumValues(original, 299L);

        LevelSummary summary = LevelSummary.of(schema, websites, tampered);

        assertThat(summary.mismatch()).contains("299").contains("300");
    }

    /// The second arm: `null_count` has to agree with the values that never
    /// reached the maximum definition level, which is the same fact stated
    /// twice in the footer.
    @Test
    void mismatchedNullCountIsReported() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema websites = column(schema, "websites.list.element");
        ColumnMetaData tampered = withNullCount(chunkMetaData("websites.list.element"), 7L);

        LevelSummary summary = LevelSummary.of(schema, websites, tampered);

        assertThat(summary.mismatch()).contains("nulls 7").contains("implied by def 0");
    }

    /// A histogram with the wrong number of buckets cannot be paired with
    /// the level names, so it is dropped from the block — but dropping it
    /// silently would hide the writer defect the screen exists to surface.
    @Test
    void malformedHistogramIsReportedRatherThanSilentlyDropped() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema websites = column(schema, "websites.list.element");
        // max def 3 needs four buckets; this writer emitted three.
        ColumnMetaData tampered = withDefinitionHistogram(
                chunkMetaData("websites.list.element"), 0L, 0L, 300L);

        LevelSummary summary = LevelSummary.of(schema, websites, tampered);

        assertThat(summary.hasDefinitionHistogram()).isFalse();
        assertThat(summary.mismatch()).isEqualTo("def histogram has 3 buckets, max def 3 needs 4");
    }

    /// The fallbacks that make a required or non-repeated column's counts
    /// known are only valid for those columns. Where they do not apply the
    /// quantity does not exist, and returning `num_values` anyway would
    /// report level slots as records and nulls as present values.
    @Test
    void unknownCountsThrowRatherThanFallBackToTheValueCount() throws Exception {
        FileSchema schema = fixtureSchema();
        ColumnSchema websites = column(schema, "websites.list.element");
        ColumnMetaData noHistograms = withDefinitionHistogram(
                chunkMetaData("websites.list.element"), 0L, 0L, 300L);

        LevelSummary summary = LevelSummary.of(schema, websites, noHistograms);

        assertThat(summary.hasPresentValues()).isFalse();
        assertThatThrownBy(summary::presentValues)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("max def 3");
        assertThatThrownBy(summary::lengthPrefixBytes).isInstanceOf(IllegalStateException.class);
    }

    /// Summing beats sampling one row group: the file-wide block has to be
    /// the file's own histogram, not the first chunk's scaled up.
    @Test
    void combineLevelsAddsCountsAndRecomputesShares() {
        List<LevelSummary.LevelRow> combined = LevelSummary.combineLevels(List.of(
                List.of(row(0, "tags null", 1L), row(1, "tags present", 3L)),
                List.of(row(0, "tags null", 3L), row(1, "tags present", 13L))));

        assertThat(combined)
                .extracting(LevelSummary.LevelRow::label, LevelSummary.LevelRow::count)
                .containsExactly(tuple("tags null", 4L), tuple("tags present", 16L));
        assertThat(combined.get(0).share()).isEqualTo(0.2);
        assertThat(combined.get(1).share()).isEqualTo(0.8);
    }

    /// A chunk whose histogram the file omits contributes nothing rather
    /// than zeroing the levels the other chunks did record.
    @Test
    void combineLevelsSkipsChunksWithoutAHistogram() {
        assertThat(LevelSummary.combineLevels(List.of(
                List.of(),
                List.of(row(0, "tags null", 2L), row(1, "tags present", 6L)),
                List.of())))
                .extracting(LevelSummary.LevelRow::count)
                .containsExactly(2L, 6L);

        assertThat(LevelSummary.combineLevels(List.of(List.of(), List.of()))).isEmpty();
    }

    /// Two chunks of one column cannot legitimately disagree on how many
    /// levels it has, so summing them into a shorter array would silently
    /// drop a bucket.
    @Test
    void combineLevelsRejectsChunksThatDisagreeOnLevelCount() {
        assertThatThrownBy(() -> LevelSummary.combineLevels(List.of(
                List.of(row(0, "tags null", 1L), row(1, "tags present", 3L)),
                List.of(row(0, "tags null", 1L), row(1, "tags empty", 1L), row(2, "tags present", 3L)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2 and 3");
    }

    /// Either page-index field on its own means the pages are described. A
    /// required `BYTE_ARRAY` column has no histogram to write, so keying
    /// only on the column index would report its page index as absent.
    @Test
    void pageStatisticsAreCarriedByEitherIndex() {
        ColumnIndex withHistograms = new ColumnIndex(null, null, null, null, null, null, new long[]{1, 3}, null);
        ColumnIndex withoutHistograms = new ColumnIndex(null, null, null, null, null, null, null, null);
        OffsetIndex withSizes = new OffsetIndex(List.of(), new long[]{15});
        OffsetIndex withoutSizes = new OffsetIndex(List.of(), null);

        assertThat(LevelSummary.hasPageLevelHistograms(withHistograms)).isTrue();
        assertThat(LevelSummary.hasPageLevelHistograms(withoutHistograms)).isFalse();
        assertThat(LevelSummary.hasPageLevelHistograms(null)).isFalse();
        assertThat(LevelSummary.hasPageUnencodedSizes(withSizes)).isTrue();
        assertThat(LevelSummary.hasPageUnencodedSizes(withoutSizes)).isFalse();
        assertThat(LevelSummary.hasPageUnencodedSizes(null)).isFalse();
    }
}
