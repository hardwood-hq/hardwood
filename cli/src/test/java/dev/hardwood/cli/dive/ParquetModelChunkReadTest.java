/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.metadata.ColumnMetaData;

import static org.assertj.core.api.Assertions.assertThat;

/// The byte-level chunk reads dive makes outside the read pipeline stay bounded by what they
/// need, so a column chunk larger than one `readRange` can address is still inspectable.
class ParquetModelChunkReadTest {

    /// One row group, 10 000 rows; column 1 (`category`) is dictionary-encoded with 10 entries.
    private static final Path FIXTURE = Path.of(ParquetModelChunkReadTest.class
            .getResource("/column_index_pushdown_dict.parquet").getPath());

    private static final int DICTIONARY_COLUMN = 1;

    @Test
    void loadingADictionaryReadsOnlyTheDictionaryPage() throws IOException {
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(FIXTURE));
        try (ParquetModel model = ParquetModel.open(counting, "column_index_pushdown_dict.parquet")) {
            ColumnMetaData metaData = model.chunk(0, DICTIONARY_COLUMN).metaData();
            long dictionaryPageBytes = metaData.dataPageOffset() - metaData.dictionaryPageOffset();
            long before = counting.bytesRead();

            assertThat(model.dictionaryForced(0, DICTIONARY_COLUMN).size()).isEqualTo(10);

            assertThat(counting.bytesRead() - before).isEqualTo(dictionaryPageBytes)
                    .isLessThan(metaData.totalCompressedSize());
        }
    }

    /// A window smaller than a page makes headers straddle window ends and bodies span several
    /// windows; the walk must find the same headers as one read of the whole chunk.
    @Test
    void walkingPageHeadersInWindowsFindsTheSameHeaders() throws IOException {
        try (ParquetModel model = ParquetModel.open(InputFile.of(FIXTURE), "column_index_pushdown_dict.parquet")) {
            ColumnMetaData metaData = model.chunk(0, DICTIONARY_COLUMN).metaData();
            List<PageHeader> whole = model.pageHeaders(0, DICTIONARY_COLUMN);

            List<PageHeader> windowed = ParquetModel.walkPageHeaders(model.inputFile(),
                    metaData.dictionaryPageOffset(), metaData.totalCompressedSize(), 16);

            assertThat(whole).hasSizeGreaterThan(2);
            assertThat(shapes(windowed)).isEqualTo(shapes(whole));
        }
    }

    /// Headers carry statistics as `byte[]`, which records compare by identity.
    private static List<String> shapes(List<PageHeader> headers) {
        return headers.stream()
                .map(h -> h.type() + " " + h.compressedPageSize() + "/" + h.uncompressedPageSize())
                .toList();
    }
}
