/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

class IndexValueFormatterTest {

    @Test
    void rendersPrintableString() {
        assertThat(IndexValueFormatter.format("hello".getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo("hello");
    }

    @Test
    void rendersNonAsciiPrintableString() {
        assertThat(IndexValueFormatter.format("Última".getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo("Última");
    }

    @Test
    void truncatesLongString() {
        String longValue = "abcdefghijklmnopqrstuvwxyz";
        assertThat(IndexValueFormatter.format(longValue.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .hasSize(20)
                .endsWith("...");
    }

    @Test
    void replacesControlCharsWithPlaceholder() {
        byte[] mixed = {'A', 0x01, 'B', 0x00, 'C'};
        assertThat(IndexValueFormatter.format(mixed, stringColumn()))
                .isEqualTo("A\u00B7B\u00B7C");
    }

    @Test
    void rendersAllControlBytesAsHex() {
        byte[] allNull = new byte[19];
        String result = IndexValueFormatter.format(allNull, stringColumn());
        assertThat(result).startsWith("0x").hasSize(20);
    }

    @Test
    void decodesInt32() {
        byte[] bytes = {0x2A, 0x00, 0x00, 0x00};
        assertThat(IndexValueFormatter.format(bytes, intColumn())).isEqualTo("42");
    }

    @Test
    void rendersEmptyStringExplicitly() {
        assertThat(IndexValueFormatter.format(new byte[0], stringColumn())).isEqualTo("\"\"");
    }

    private static ColumnSchema stringColumn() {
        return new ColumnSchema(FieldPath.of("s"), PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                null, 0, 1, 0, new LogicalType.StringType());
    }

    private static ColumnSchema intColumn() {
        return new ColumnSchema(FieldPath.of("i"), PhysicalType.INT32, RepetitionType.OPTIONAL,
                null, 0, 1, 0, null);
    }
}
