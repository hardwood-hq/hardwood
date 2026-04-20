/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal.table;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

class RowTableTest {

    @Test
    void displayWidthCountsAsciiAsOne() {
        assertThat(RowTable.displayWidth("hello")).isEqualTo(5);
        assertThat(RowTable.displayWidth("")).isZero();
    }

    @Test
    void displayWidthTreatsLatinAccentsAsNarrow() {
        assertThat(RowTable.displayWidth("Última")).isEqualTo(6);
        assertThat(RowTable.displayWidth("Ñuble")).isEqualTo(5);
    }

    @Test
    void displayWidthCountsHangulAsWide() {
        // 5 Hangul syllables → 10 terminal cells
        assertThat(RowTable.displayWidth("말도나도주")).isEqualTo(10);
    }

    @Test
    void displayWidthCountsCjkIdeographsAsWide() {
        // 3 CJK ideographs → 6 terminal cells
        assertThat(RowTable.displayWidth("漢字水")).isEqualTo(6);
    }

    @Test
    void displayWidthCountsKanaAsWide() {
        assertThat(RowTable.displayWidth("コキンボ")).isEqualTo(8);
    }

    @Test
    void renderBareByteArrayAsStringWhenValidUtf8() {
        SchemaNode.PrimitiveNode schema = bareByteArray();
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

        assertThat(RowTable.renderValue(bytes, schema)).isEqualTo("hello");
    }

    @Test
    void renderBareByteArrayAsByteSummaryWhenInvalidUtf8() {
        SchemaNode.PrimitiveNode schema = bareByteArray();
        // Lone continuation byte — not a valid UTF-8 sequence.
        byte[] bytes = {(byte) 0xC3, (byte) 0x28, (byte) 0xA0, (byte) 0xA1};

        assertThat(RowTable.renderValue(bytes, schema)).isEqualTo("<4 bytes>");
    }

    @Test
    void renderAnnotatedStringAlwaysDecodes() {
        SchemaNode.PrimitiveNode schema = new SchemaNode.PrimitiveNode(
                "f", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                new LogicalType.StringType(), 0, 0, 0);
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

        assertThat(RowTable.renderValue(bytes, schema)).isEqualTo("hello");
    }

    private static SchemaNode.PrimitiveNode bareByteArray() {
        return new SchemaNode.PrimitiveNode(
                "f", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null, 0, 0, 0);
    }

    @Test
    void rendersTableWithWideCharsAligned() {
        String[] headers = {"A", "B"};
        List<String[]> rows = List.of(
                new String[]{"buenos aires", "12"},
                new String[]{"말도나도주", "3"}
        );
        String out = RowTable.renderTable(headers, rows);
        String[] lines = out.split("\n");
        // Every line must have the same display width so the borders align visually.
        int expected = RowTable.displayWidth(lines[0]);
        for (String line : lines) {
            assertThat(RowTable.displayWidth(line))
                    .as("line width: %s", line)
                    .isEqualTo(expected);
        }
    }
}
