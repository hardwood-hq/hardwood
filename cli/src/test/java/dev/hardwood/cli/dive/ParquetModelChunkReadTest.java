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

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnMetaData;

import static org.assertj.core.api.Assertions.assertThat;

/// The dictionary read dive makes outside the read pipeline stays bounded by what it needs, so
/// a column chunk larger than one `readRange` can address is still inspectable.
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
}
