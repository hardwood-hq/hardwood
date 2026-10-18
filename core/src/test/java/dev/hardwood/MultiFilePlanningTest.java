/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.CountingInputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// What a multi-file read opens, and when.
///
/// A file is planned when the read reaches it, so a caller who stops early never
/// pays for the files beyond where they stopped. The counts here are deliberately
/// bounds rather than exact numbers: the fetchers prefetch ahead until
/// backpressure stops it, and how far it gets is a timing question.
///
/// That it stops short of the end is only a property of a read something stops — a
/// row budget or a cap. A read with neither is bounded by batch backpressure alone,
/// and these fixtures together hold fewer rows than one batch, so it legitimately
/// reaches every file; see [#anUncappedFilteredReadYieldsWithoutPlanningTheWholeRead].
class MultiFilePlanningTest {

    private static final int FILE_COUNT = 8;

    private static List<CountingInputFile> countingFiles() {
        List<CountingInputFile> files = new ArrayList<>();
        for (int i = 0; i < FILE_COUNT; i++) {
            files.add(new CountingInputFile(InputFile.of(
                    Paths.get("src/test/resources/multi_file_part" + (i % 2) + ".parquet"))));
        }
        return files;
    }

    private static long touched(List<CountingInputFile> files) {
        return files.stream().filter(f -> f.readCount() > 0).count();
    }

    @Test
    void openingTheFilesReadsOnlyTheFirstFooter() throws IOException {
        List<CountingInputFile> files = countingFiles();
        try (ParquetFileReader reader = ParquetFileReader.openAll(files)) {
            assertThat(touched(files))
                    .as("openAll reads the reference schema, which is the first file's footer")
                    .isEqualTo(1);
            assertThat(reader.getFileCount()).isEqualTo(FILE_COUNT);
        }
    }

    /// The cap is what bounds planning, not the caller walking away after one row. An
    /// abandoned loop leaves the fetchers free to run on, so without `head` this asserts
    /// only that the calling thread reached the assertion first — which on a loaded
    /// machine it does not. See [#anUncappedFilteredReadYieldsWithoutPlanningTheWholeRead],
    /// which is the same read without a cap and therefore asserts no bound at all.
    @Test
    void aReadThatStopsEarlyDoesNotOpenEveryFile() throws IOException {
        List<CountingInputFile> files = countingFiles();
        try (ParquetFileReader reader = ParquetFileReader.openAll(files);
             RowReader rows = reader.buildRowReader().head(1).build()) {
            if (rows.hasNext()) {
                rows.next();
            }
            assertThat(touched(files))
                    .as("a caller who reads one row should not have paid for all %d files", FILE_COUNT)
                    .isLessThan(FILE_COUNT);
        }
    }

    /// Statistics prove every row group of both fixtures holds only ids `>= 0`, so a
    /// capped read of them assembles exactly the rows it returns and can stop at the cap.
    @Test
    void aCappedReadUnderAProvenFilterDoesNotOpenEveryFile() throws IOException {
        List<CountingInputFile> files = countingFiles();
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.openAll(files);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gtEq("id", 0L))
                     .head(5)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        assertThat(ids).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(touched(files))
                .as("the first file alone covers head(5), so the other %d are never planned",
                        FILE_COUNT - 1)
                .isLessThan(FILE_COUNT);
    }

    /// `id > 150` splits the first surviving row group, so from there on a scanned row
    /// is not necessarily a matching one and the cap has to be counted over matches.
    @Test
    void aCappedReadUnderAnUndecidedFilterStillCountsMatches() throws IOException {
        List<CountingInputFile> files = countingFiles();
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.openAll(files);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gt("id", 150L))
                     .head(5)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        assertThat(ids).containsExactly(151L, 152L, 153L, 154L, 155L);
    }

    /// Building a filtered reader asks nothing about the read as a whole, so it yields
    /// its first row without the whole read having been planned first.
    ///
    /// No bound on files touched is asserted here, unlike the capped and unfiltered
    /// cases above. Those two stop the read — one on the row budget, one on the cap —
    /// and it is that stop, not the abandoned loop, which bounds how far planning runs.
    /// An uncapped filtered read has neither, so it is bounded only by batch
    /// backpressure, and these fixtures hold fewer rows in total than one batch: walking
    /// every file to fill the first batch is the pipeline working, not planning running
    /// away. A bound here would only pass while the calling thread beat the fetchers
    /// to the assertion.
    @Test
    void anUncappedFilteredReadYieldsWithoutPlanningTheWholeRead() throws IOException {
        List<CountingInputFile> files = countingFiles();
        try (ParquetFileReader reader = ParquetFileReader.openAll(files);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gtEq("id", 0L))
                     .build()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(0L);
        }
    }

    /// The filter is still applied exactly when statistics decide only part of the read:
    /// `id > 150` drops every `part0` file outright and splits the first `part1` group.
    @Test
    void anUncappedFilteredReadStillReturnsExactlyTheMatchingRows() throws IOException {
        List<CountingInputFile> files = countingFiles();
        long matched = 0;
        try (ParquetFileReader reader = ParquetFileReader.openAll(files);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gt("id", 150L))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id")).isGreaterThan(150L);
                matched++;
            }
        }
        // Four part1 files, ids 150-249, of which 99 exceed 150.
        assertThat(matched).isEqualTo(4 * 99);
    }

    @Test
    void readingEveryRowStillReadsEveryFile() throws IOException {
        List<CountingInputFile> files = countingFiles();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.openAll(files);
             RowReader reading = reader.buildRowReader().build()) {
            while (reading.hasNext()) {
                reading.next();
                rows++;
            }
        }
        assertThat(rows).isPositive();
        assertThat(touched(files))
                .as("planning is deferred, not skipped")
                .isEqualTo(FILE_COUNT);
    }
}
