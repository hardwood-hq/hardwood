/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies [RowValueFormatter] renders INTERVAL fields correctly when called
/// through the live `RowReader` path — both single-line and expanded forms.
/// Complements [RowValueFormatterTest], which only tests the dictionary entry
/// path with synthetic byte arrays.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RowValueFormatterIntervalTest {

    // Row 0: 1 month, 15 days, 1 hour (3_600_000 ms)
    // Row 1: 0 months, 30 days, 0 ms
    // Row 2: null

    private SchemaNode durationField;
    private int durationIdx;
    private String row0Formatted;
    private String row1Formatted;
    private String row2Formatted;
    private String row0Expanded;
    private String row1Expanded;
    private String row2Expanded;

    @BeforeAll
    void readAll() throws IOException {
        Path file = Path.of(getClass().getResource("/interval_logical_type_test.parquet").getPath());
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            FileSchema schema = fileReader.getFileSchema();
            durationField = schema.getField("duration");
            durationIdx = schema.getColumn("duration").columnIndex();

            rowReader.next();
            row0Formatted = RowValueFormatter.format(rowReader, durationIdx, durationField);
            row0Expanded = RowValueFormatter.formatExpanded(rowReader, durationIdx, durationField, true);

            rowReader.next();
            row1Formatted = RowValueFormatter.format(rowReader, durationIdx, durationField);
            row1Expanded = RowValueFormatter.formatExpanded(rowReader, durationIdx, durationField, true);

            rowReader.next();
            row2Formatted = RowValueFormatter.format(rowReader, durationIdx, durationField);
            row2Expanded = RowValueFormatter.formatExpanded(rowReader, durationIdx, durationField, true);
        }
    }

    @Test
    void formatRendersAllComponents() {
        assertThat(row0Formatted).isEqualTo("1mo 15d 3600000ms");
    }

    @Test
    void formatOmitsZeroComponents() {
        assertThat(row1Formatted).isEqualTo("30d");
    }

    @Test
    void formatRendersNullAsNull() {
        assertThat(row2Formatted).isEqualTo("null");
    }

    @Test
    void formatExpandedMatchesFormatForPrimitiveLeaf() {
        assertThat(row0Expanded).isEqualTo(row0Formatted);
        assertThat(row1Expanded).isEqualTo(row1Formatted);
        assertThat(row2Expanded).isEqualTo(row2Formatted);
    }
}
