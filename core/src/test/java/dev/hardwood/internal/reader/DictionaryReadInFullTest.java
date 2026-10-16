/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Whether the fetch plan marks a column chunk's dictionary as read in full, so dictionary-space
/// evaluation decides every entry up front.
class DictionaryReadInFullTest {

    private static final Path INDEXED = Path.of("src/test/resources/dictionary_coverage_indexed.parquet");
    private static final Path UNINDEXED = Path.of("src/test/resources/dictionary_coverage_unindexed.parquet");

    private static final FilterPredicate URL_FILTER = FilterPredicate.lt("url", "u10");

    @Test
    void anIndexedReadOfEveryPageMarksTheDictionary() throws Exception {
        assertThat(urlDictionaryReadInFull(INDEXED, URL_FILTER, 0)).isTrue();
    }

    @Test
    void anIndexedReadNarrowedByAnotherColumnDoesNotMarkTheDictionary() throws Exception {
        FilterPredicate filter = FilterPredicate.and(FilterPredicate.lt("id", 50L), URL_FILTER);

        assertThat(urlDictionaryReadInFull(INDEXED, filter, 0)).isFalse();
    }

    @Test
    void aLimitedIndexedReadDoesNotMarkTheDictionary() throws Exception {
        assertThat(urlDictionaryReadInFull(INDEXED, URL_FILTER, 10)).isFalse();
    }

    @Test
    void aSequentialReadWhoseFilterCouldDropPagesDoesNotMarkTheDictionary() throws Exception {
        assertThat(urlDictionaryReadInFull(UNINDEXED, URL_FILTER, 0)).isFalse();
    }

    @Test
    void aSequentialReadNoPageCanBeDroppedFromMarksTheDictionary() throws Exception {
        // A leaf under OR cannot drop a page, since a sibling branch may still match its rows.
        FilterPredicate filter = FilterPredicate.or(FilterPredicate.lt("id", 0L), URL_FILTER);

        assertThat(urlDictionaryReadInFull(UNINDEXED, filter, 0)).isTrue();
    }

    @Test
    void aLimitedSequentialReadDoesNotMarkTheDictionary() throws Exception {
        FilterPredicate filter = FilterPredicate.or(FilterPredicate.lt("id", 0L), URL_FILTER);

        assertThat(urlDictionaryReadInFull(UNINDEXED, filter, 10)).isFalse();
    }

    private static boolean urlDictionaryReadInFull(Path file, FilterPredicate filter, long maxRows)
            throws Exception {
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            schema = reader.getFileSchema();
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = new RowGroupIterator(List.of(InputFile.of(file)), context, maxRows)) {
            iterator.openFirst();
            iterator.initialize(ColumnProjection.columns("id", "url"),
                    FilterPredicateResolver.resolve(filter, schema));
            RowGroupIterator.WorkItem workItem = iterator.workItemAt(0);
            PageIterator pages = iterator.getColumnPlan(workItem, 1).pages();
            assertThat(pages.hasNext()).isTrue();
            return ((Dictionary.ByteArrayDictionary) pages.next().dictionary()).readInFull();
        }
    }
}
