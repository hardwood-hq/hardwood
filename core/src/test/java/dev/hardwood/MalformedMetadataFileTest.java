/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies that a malformed metadata value rejected by the Thrift readers
/// (hardwood-hq/hardwood#604) surfaces through the public reader as a controlled,
/// **file-attributed** error. The low-level field-name coverage lives in
/// [dev.hardwood.internal.thrift.MalformedMetadataValidationTest]; this test pins
/// the end-to-end behaviour that the message also names the offending file.
///
/// No byte is named: the footer parsed, and it is the value that is wrong
/// rather than the bytes holding it, so an offset would point at bytes that are
/// exactly what the writer meant to put there.
class MalformedMetadataFileTest {

    @Test
    void negativeDataPageOffsetFailsWithFileContext() {
        // Footer is structurally valid except for data_page_offset = -1.
        Path file = Paths.get("src/test/resources/negative_data_page_offset.parquet");
        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(file)))
                .isInstanceOf(ParquetReadException.class)
                // The byte is the field's own header rather than the footer's first:
                // the reader was standing on data_page_offset when it rejected it.
                .hasMessage("[negative_data_page_offset.parquet] footer at byte 147 (0x000093)"
                        + " — ColumnMetaData.data_page_offset — must be non-negative but was -1");
    }
}
