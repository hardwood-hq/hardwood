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
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// A page-pruned read of a dictionary-encoded column fetches the dictionary page and the
/// surviving pages, not the pages between them (#1037).
class DictionaryPrefixFetchTest {

    /// One row group of 1 300 000 rows, one dictionary-encoded column of ~1.3 MB. Every value
    /// sorts below `zzzz` except the last 1 000 rows, which are `zzzz`, so the column index drops
    /// every page but the last one or two, more than 1 MiB past the dictionary page.
    private static final Path FILE = Path.of("src/test/resources/dictionary_tail_match.parquet");
    private static final long ROWS = 1_300_000;
    private static final long MATCHES = 1_000;

    @Test
    void tailMatchFetchesTheDictionaryAndTheSurvivingPagesSeparately() throws Exception {
        // A range predicate: pruning reads no dictionary for it, so the read fetches the
        // dictionary page itself.
        CountingInputFile file = new CountingInputFile(InputFile.of(FILE));
        file.open();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader()
                        .filter(FilterPredicate.gt("category", "zzz"))
                        .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                assertThat(rowReader.getString("category")).isEqualTo("zzzz");
                rows++;
            }
        }

        assertThat(rows).isEqualTo(MATCHES);
        IoBudget.of(FILE, List.of("category"), ROWS - MATCHES, ROWS).assertWithin(file);
    }
}
