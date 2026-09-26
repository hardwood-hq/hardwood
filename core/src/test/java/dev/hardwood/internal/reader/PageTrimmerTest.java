/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PageTrimmerTest {

    /// Records `[10, 11]`, `[20]`, `[30, 31, 32]`, `[40]`, as values with their levels.
    private static Page.IntPage fourRecords() {
        int[] values = { 10, 11, 20, 30, 31, 32, 40 };
        int[] rep = { 0, 1, 0, 0, 1, 1, 0 };
        int[] def = { 3, 3, 3, 3, 3, 3, 3 };
        return new Page.IntPage(values, def, rep, 3, values.length);
    }

    @Test
    void testKeepsRecordsOfEachInterval() {
        Page.IntPage trimmed = (Page.IntPage) PageTrimmer.trim(fourRecords(),
                PageRowMask.of(new int[]{ 0, 1, 2, 3 }));

        assertThat(trimmed.size()).isEqualTo(5);
        assertThat(Arrays.copyOf(trimmed.values(), 5)).containsExactly(10, 11, 30, 31, 32);
        assertThat(Arrays.copyOf(trimmed.repetitionLevels(), 5)).containsExactly(0, 1, 0, 1, 1);
        assertThat(Arrays.copyOf(trimmed.definitionLevels(), 5)).containsExactly(3, 3, 3, 3, 3);
    }

    @Test
    void testKeepsTrailingRecords() {
        Page.IntPage trimmed = (Page.IntPage) PageTrimmer.trim(fourRecords(),
                PageRowMask.of(new int[]{ 2, 4 }));

        assertThat(trimmed.size()).isEqualTo(4);
        assertThat(Arrays.copyOf(trimmed.values(), 4)).containsExactly(30, 31, 32, 40);
        assertThat(Arrays.copyOf(trimmed.repetitionLevels(), 4)).containsExactly(0, 1, 1, 0);
    }

    /// Byte-array values move with their dictionary indices, and a null keeps its slot.
    @Test
    void testMovesByteArrayValuesWithDictionaryIndices() {
        byte[][] values = { { 'a' }, { 'b' }, null, { 'c' } };
        int[] dictIndices = { 7, 8, 0, 9 };
        int[] rep = { 0, 0, 0, 1 };
        int[] def = { 3, 3, 1, 3 };
        Page.ByteArrayPage page = new Page.ByteArrayPage(values, def, rep, 3, values.length, null, dictIndices);

        Page.ByteArrayPage trimmed = (Page.ByteArrayPage) PageTrimmer.trim(page,
                PageRowMask.of(new int[]{ 1, 3 }));

        assertThat(trimmed.size()).isEqualTo(3);
        assertThat(trimmed.values()[0]).containsExactly('b');
        assertThat(trimmed.values()[1]).isNull();
        assertThat(trimmed.values()[2]).containsExactly('c');
        assertThat(Arrays.copyOf(trimmed.dictIndices(), 3)).containsExactly(8, 0, 9);
        assertThat(Arrays.copyOf(trimmed.definitionLevels(), 3)).containsExactly(3, 1, 3);
        assertThat(Arrays.copyOf(trimmed.repetitionLevels(), 3)).containsExactly(0, 0, 1);
    }

    /// A fixed-width page carries no levels: record `r` is values `[r * k, (r + 1) * k)`.
    @Test
    void testKeepsFixedWidthRecords() {
        double[] values = { 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5 };
        Page.DoublePage page = new Page.DoublePage(values, null, null, 2, values.length, 2);

        Page.DoublePage trimmed = (Page.DoublePage) PageTrimmer.trim(page,
                PageRowMask.of(new int[]{ 1, 2, 3, 4 }));

        assertThat(trimmed.size()).isEqualTo(4);
        assertThat(trimmed.fixedListK()).isEqualTo(2);
        assertThat(Arrays.copyOf(trimmed.values(), 4)).containsExactly(1, 1.5, 3, 3.5);
    }

    /// A column with no repeated ancestor, read by a nested worker, has one value per record.
    @Test
    void testKeepsValuesOfColumnWithoutRepetition() {
        long[] values = { 100, 101, 102, 103, 104 };
        int[] def = { 1, 0, 1, 1, 1 };
        Page.LongPage page = new Page.LongPage(values, def, null, 1, values.length);

        Page.LongPage trimmed = (Page.LongPage) PageTrimmer.trim(page,
                PageRowMask.of(new int[]{ 1, 2, 4, 5 }));

        assertThat(trimmed.size()).isEqualTo(2);
        assertThat(Arrays.copyOf(trimmed.values(), 2)).containsExactly(101, 104);
        assertThat(Arrays.copyOf(trimmed.definitionLevels(), 2)).containsExactly(0, 1);
        assertThat(trimmed.repetitionLevels()).isNull();
    }

    @Test
    void testRejectsPageStartingMidRecord() {
        int[] values = { 1, 2, 3 };
        int[] rep = { 1, 0, 1 };
        Page.IntPage page = new Page.IntPage(values, null, rep, 3, values.length);

        assertThatThrownBy(() -> PageTrimmer.trim(page, PageRowMask.of(new int[]{ 0, 1 })))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: first repetition level must be 0 but was 1");
    }

    @Test
    void testRejectsAllMask() {
        assertThatThrownBy(() -> PageTrimmer.trim(fourRecords(), PageRowMask.ALL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("PageRowMask.ALL keeps the page as is; nothing to trim");
    }
}
