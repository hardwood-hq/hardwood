/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [PageTrimmer] on pages read in place: the trimmed page holds the kept values' levels, and its
/// kept runs lead a fill to the kept values where they lie in the page.
class PageTrimmerTest {

    /// Records `[10, 11]`, `[20]`, `[30, 31, 32]`, `[40]`, as values with their levels.
    private static PageDecoder.InPlaceNestedPage fourRecords() {
        return intPage(new int[]{ 10, 11, 20, 30, 31, 32, 40 }, new int[]{ 3, 3, 3, 3, 3, 3, 3 },
                new int[]{ 0, 1, 0, 0, 1, 1, 0 }, 3, 0);
    }

    @Test
    void testKeepsRecordsOfEachInterval() {
        PageDecoder.InPlaceNestedPage trimmed = trim(fourRecords(), 0, 1, 2, 3);

        assertThat(trimmed.size()).isEqualTo(5);
        assertThat(keptInts(trimmed)).containsExactly(10, 11, 30, 31, 32);
        assertThat(Arrays.copyOf(trimmed.repetitionLevels(), 5)).containsExactly(0, 1, 0, 1, 1);
        assertThat(Arrays.copyOf(trimmed.definitionLevels(), 5)).containsExactly(3, 3, 3, 3, 3);
    }

    @Test
    void testKeepsTrailingRecords() {
        PageDecoder.InPlaceNestedPage trimmed = trim(fourRecords(), 2, 4);

        assertThat(trimmed.size()).isEqualTo(4);
        assertThat(keptInts(trimmed)).containsExactly(30, 31, 32, 40);
        assertThat(Arrays.copyOf(trimmed.repetitionLevels(), 4)).containsExactly(0, 1, 1, 0);
    }

    /// Byte-array values are read through the runs with their levels, and a null keeps its slot.
    @Test
    void testKeepsByteArrayValues() {
        int[] rep = { 0, 0, 0, 1 };
        int[] def = { 3, 3, 1, 3 };
        byte[] plain = { 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 1, 0, 0, 0, 'c' };
        BinaryPageValues values = BinaryPageValues.plain(PhysicalType.BYTE_ARRAY, 0, def, 3, 4, plain, 0,
                plain.length, new PageDecoder.LevelScratch());
        PageDecoder.InPlaceNestedPage page = new PageDecoder.InPlaceNestedPage(4, def, rep, 0, null, values, null);

        PageDecoder.InPlaceNestedPage trimmed = trim(page, 1, 3);

        assertThat(trimmed.size()).isEqualTo(3);
        assertThat(keptBytes(trimmed, 3)).isDeepEqualTo(new byte[][]{ { 'b' }, null, { 'c' } });
        assertThat(Arrays.copyOf(trimmed.definitionLevels(), 3)).containsExactly(3, 1, 3);
        assertThat(Arrays.copyOf(trimmed.repetitionLevels(), 3)).containsExactly(0, 0, 1);
    }

    /// Dictionary ids are read through the runs with their levels, and a null keeps its slot.
    @Test
    void testKeepsDictionaryIds() {
        Dictionary.ByteArrayDictionary dictionary = TestDictionaries.byteArrays(
                new byte[][]{ { 'a' }, { 'b' }, { 'c' } });
        int[] rep = { 0, 0, 0, 1 };
        int[] def = { 3, 3, 1, 3 };
        BinaryPageValues values = BinaryPageValues.ids(def, 3, 4, new int[]{ 0, 1, 2 }, 3, dictionary);
        PageDecoder.InPlaceNestedPage page = new PageDecoder.InPlaceNestedPage(4, def, rep, 0, null, values, null);

        PageDecoder.InPlaceNestedPage trimmed = trim(page, 1, 3);

        assertThat(trimmed.size()).isEqualTo(3);
        assertThat(keptBytes(trimmed, 3)).isDeepEqualTo(new byte[][]{ { 'b' }, null, { 'c' } });
        assertThat(Arrays.copyOf(trimmed.definitionLevels(), 3)).containsExactly(3, 1, 3);
    }

    /// A fixed-width page carries no levels: record `r` is values `[r * k, (r + 1) * k)`.
    @Test
    void testKeepsFixedWidthRecords() {
        PageDecoder.InPlaceNestedPage page = intPage(new int[]{ 0, 5, 10, 15, 20, 25, 30, 35 }, null, null, 2, 2);

        PageDecoder.InPlaceNestedPage trimmed = trim(page, 1, 2, 3, 4);

        assertThat(trimmed.size()).isEqualTo(4);
        assertThat(trimmed.fixedListK()).isEqualTo(2);
        assertThat(trimmed.definitionLevels()).isNull();
        assertThat(trimmed.repetitionLevels()).isNull();
        assertThat(keptInts(trimmed)).containsExactly(10, 15, 30, 35);
    }

    /// A column with no repeated ancestor, read as a nested column, has one value per record.
    @Test
    void testKeepsValuesOfColumnWithoutRepetition() {
        PageDecoder.InPlaceNestedPage page = intPage(new int[]{ 100, 0, 102, 103, 104 }, new int[]{ 1, 0, 1, 1, 1 },
                null, 1, 0);

        PageDecoder.InPlaceNestedPage trimmed = trim(page, 1, 2, 4, 5);

        assertThat(trimmed.size()).isEqualTo(2);
        assertThat(keptInts(trimmed)).containsExactly(0, 104);
        assertThat(Arrays.copyOf(trimmed.definitionLevels(), 2)).containsExactly(0, 1);
        assertThat(trimmed.repetitionLevels()).isNull();
    }

    @Test
    void testRejectsMaskPastLastRecordOfRegularPage() {
        assertThatThrownBy(() -> trim(fourRecords(), 3, 5))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: page row mask selects records up to 5 but the page holds 4 records");
    }

    @Test
    void testRejectsMaskPastLastRecordOfFixedWidthPage() {
        PageDecoder.InPlaceNestedPage page = intPage(new int[]{ 0, 5, 10, 15 }, null, null, 2, 2);

        assertThatThrownBy(() -> trim(page, 1, 3))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: page row mask selects records up to 3 but the page holds 2 records");
    }

    @Test
    void testRejectsPageStartingMidRecord() {
        PageDecoder.InPlaceNestedPage page = intPage(new int[]{ 1, 2, 3 }, null, new int[]{ 1, 0, 1 }, 3, 0);

        assertThatThrownBy(() -> trim(page, 0, 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: first repetition level must be 0 but was 1");
    }

    @Test
    void testRejectsAllMask() {
        assertThatThrownBy(() -> PageTrimmer.trim(fourRecords(), PageRowMask.ALL, new PageDecoder.LevelScratch()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("PageRowMask.ALL keeps the page as is; nothing to trim");
    }

    private static PageDecoder.InPlaceNestedPage trim(PageDecoder.InPlaceNestedPage page, int... intervals) {
        return PageTrimmer.trim(page, PageRowMask.of(intervals), new PageDecoder.LevelScratch());
    }

    /// A page of `INT32` values in place, the present ones as `PLAIN` bytes.
    private static PageDecoder.InPlaceNestedPage intPage(int[] values, int[] def, int[] rep, int maxDef, int k) {
        ByteBuffer plain = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < values.length; i++) {
            if (def == null || def[i] == maxDef) {
                plain.putInt(values[i]);
            }
        }
        FlatPageValues flat = FlatPageValues.plain(PhysicalType.INT32, def, maxDef, values.length, plain.array(), 0,
                plain.position());
        return new PageDecoder.InPlaceNestedPage(values.length, def, rep, k, flat, null, null);
    }

    /// The kept values of a trimmed numeric page, read through its runs as a fill reads them.
    private static int[] keptInts(PageDecoder.InPlaceNestedPage trimmed) {
        int[] out = new int[trimmed.size()];
        FlatPageValues values = trimmed.flat().view();
        KeptRuns runs = trimmed.runs();
        int kept = 0;
        while (kept < trimmed.size()) {
            int run = runs.runOf(kept);
            int n = runs.remaining(run, kept);
            values.copy(runs.pagePosition(run, kept), out, kept, n);
            kept += n;
        }
        return out;
    }

    /// The kept values of a trimmed binary page, `null` at a null, read through its runs.
    private static byte[][] keptBytes(PageDecoder.InPlaceNestedPage trimmed, int maxDef) {
        int size = trimmed.size();
        BinaryPageValues values = trimmed.binary().view();
        KeptRuns runs = trimmed.runs();
        int[] starts = new int[size];
        int[] ends = new int[size];
        int[] ids = new int[size];
        byte[] bytes = new byte[runs.bytes(trimmed.binary(), 0, size)];
        int kept = 0;
        int destByte = 0;
        while (kept < size) {
            int run = runs.runOf(kept);
            int n = runs.remaining(run, kept);
            if (values.dictionary() != null) {
                values.copyIds(runs.pagePosition(run, kept), n, ids, starts, ends, kept);
            }
            else {
                destByte = values.copyPlain(runs.pagePosition(run, kept), n, starts, ends, kept, bytes, destByte);
            }
            kept += n;
        }
        byte[][] out = new byte[size][];
        for (int i = 0; i < size; i++) {
            if (trimmed.definitionLevels()[i] == maxDef) {
                out[i] = values.dictionary() != null
                        ? values.dictionary().entry(ids[i])
                        : Arrays.copyOfRange(bytes, starts[i], ends[i]);
            }
        }
        return out;
    }
}
