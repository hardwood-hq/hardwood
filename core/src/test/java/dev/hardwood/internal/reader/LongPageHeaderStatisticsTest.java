/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// A data page header longer than the initial peek is read by growing the peek.
///
/// `long_page_header_statistics.parquet` is written by pyarrow, which copies the longest value of
/// a page verbatim into the inline `statistics.max_value` of its data page header. Its one column,
/// `moves`, holds `"a"` and 1100 `b`s, so the header outruns the peek. Hardwood's own writer puts
/// no statistics in page headers, so no round-trip test reaches this path.
class LongPageHeaderStatisticsTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/long_page_header_statistics.parquet");

    private static final List<String> EXPECTED = List.of("a", "b".repeat(1100));

    @Test
    void theDataPageHeaderIsLongerThanTheInitialPeek() throws Exception {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            ColumnMetaData chunk = meta.rowGroups().get(0).columns().get(0).metaData();
            ByteBuffer bytes = file.readRange(chunk.dataPageOffset(),
                    Math.toIntExact(file.length() - chunk.dataPageOffset()));

            ThriftCompactReader reader = new ThriftCompactReader(bytes);
            PageHeader header = PageHeaderReader.read(reader);

            assertThat(header.dataPageHeader().statistics().maxValue()).hasSize(1100);
            assertThat(reader.getBytesRead()).isGreaterThan(PageFormatProbe.INITIAL_PEEK_SIZE);
        }
    }

    @Test
    void theRowReaderReadsTheColumn() throws Exception {
        List<String> values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                values.add(rows.getString("moves"));
            }
        }

        assertThat(values).isEqualTo(EXPECTED);
    }

    @Test
    void theColumnReaderReadsTheColumn() throws Exception {
        List<String> values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns("moves")).build()) {
            ColumnReader moves = cols.getColumnReader("moves");
            while (cols.nextBatch()) {
                String[] batch = moves.getStrings();
                for (int i = 0; i < moves.getRecordCount(); i++) {
                    values.add(batch[i]);
                }
            }
        }

        assertThat(values).isEqualTo(EXPECTED);
    }
}
