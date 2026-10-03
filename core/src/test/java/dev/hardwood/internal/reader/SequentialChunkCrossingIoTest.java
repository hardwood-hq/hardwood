/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Without an OffsetIndex, [SequentialFetchPlan] reads a column chunk in pieces. A page header
/// or page that starts in one piece and ends in the next is read from the piece it starts in,
/// so no byte of the column chunk is fetched twice (#1364).
///
/// Pages are about 512 bytes. With 4 KiB pieces, many pages and 1 KiB header peeks cross from
/// one piece into the next. With 300-byte pieces, a page is longer than a piece and a peek spans
/// several, so a read before the page read moves more than one piece past the page's start.
class SequentialChunkCrossingIoTest {

    private static final Path FLAT_FILE = Path.of("src/test/resources/misaligned_pages_no_index.parquet");
    private static final Path NESTED_V2_FILE = Path.of("src/test/resources/misaligned_pages_nested_v2.parquet");
    private static final long ROWS = 2_000;

    @AfterEach
    void clearPieceSize() {
        System.clearProperty(SequentialFetchPlan.CHUNK_SIZE_PROPERTY);
    }

    @ParameterizedTest
    @ValueSource(ints = { 300, 4 * 1024 })
    void fullReadFetchesEachByteOnce(int pieceSize) throws Exception {
        System.setProperty(SequentialFetchPlan.CHUNK_SIZE_PROPERTY, String.valueOf(pieceSize));
        CountingInputFile file = new CountingInputFile(InputFile.of(FLAT_FILE));
        file.open();

        long rows = readAll(file, "wide", 0);

        assertThat(rows).isEqualTo(ROWS);
        IoBudget.of(FLAT_FILE, List.of("wide"), 0, ROWS).assertWithin(file);
    }

    @ParameterizedTest
    @ValueSource(ints = { 300, 4 * 1024 })
    void maskedNestedReadFetchesEachByteOnce(int pieceSize) throws Exception {
        System.setProperty(SequentialFetchPlan.CHUNK_SIZE_PROPERTY, String.valueOf(pieceSize));
        CountingInputFile file = new CountingInputFile(InputFile.of(NESTED_V2_FILE));
        file.open();

        long rows = readAll(file, "tags", ROWS / 2);

        assertThat(rows).isEqualTo(ROWS / 2);
        // Not IoBudget: the mask gate's page-format probe reads the chunk's first page header on
        // its own, ahead of the plan, which MaskProbeIoTest pins. Only the plan's reads are checked.
        List<CountingInputFile.Read> pieces = file.reads().stream()
                .filter(read -> read.reason().contains("seqChunk"))
                .sorted(Comparator.comparingLong(CountingInputFile.Read::offset))
                .toList();
        assertThat(pieces).hasSizeGreaterThan(1);
        for (int i = 1; i < pieces.size(); i++) {
            assertThat(pieces.get(i).offset())
                    .as("data bytes fetched twice: %s and %s", pieces.get(i - 1), pieces.get(i))
                    .isGreaterThanOrEqualTo(pieces.get(i - 1).end());
        }
    }

    /// Reads `column` in full, or its last `tailRows` rows when `tailRows` is positive, and
    /// returns the number of rows read.
    private static long readAll(CountingInputFile file, String column, long tailRows) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            ParquetFileReader.RowReaderBuilder builder = reader.buildRowReader().projection(ColumnProjection.columns(column));
            if (tailRows > 0) {
                builder = builder.tail(tailRows);
            }
            try (RowReader rowReader = builder.build()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rows++;
                }
            }
        }
        return rows;
    }
}
