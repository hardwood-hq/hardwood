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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// [NestedFill] of cells holding segments of masked pages: each segment reads its page's kept
/// values through the page's [KeptRuns], from wherever in the runs it starts, and a masked
/// fixed-size-list page writes the levels it omits in a cell shared with a regular page.
///
/// The column is an optional list of optional elements: definition level `3` for a present
/// element, `2` for a null element, `1` for an empty list, `0` for a null list.
class NestedFillMaskedPageTest {

    private static final int MAX_DEF = 3;

    @Test
    void aMaskedFixedSizeListPageWritesItsLevelsInACellWithARegularPage() {
        Column column = column(PhysicalType.INT32);
        // Records [1, 2], null
        NestedPageTask regular = page(intPage(new int[]{ 1, 2, 0 }, new int[]{ 3, 3, 0 }, new int[]{ 0, 1, 0 }, 0), null);
        // Records [10, 11], [20, 21], [30, 31], of which the first and the last are kept
        NestedPageTask fixed = page(intPage(new int[]{ 10, 11, 20, 21, 30, 31 }, null, null, 2), new int[]{ 0, 1, 2, 3 });
        NestedCell cell = cell(regular, 0, 2);
        add(cell, fixed, 0, 2);

        NestedBatch batch = column.fill(cell);

        assertThat(batch.fixedListK).isZero();
        assertThat(batch.recordCount).isEqualTo(4);
        assertThat(batch.valueCount).isEqualTo(7);
        assertThat(batch.definitionLevels).containsExactly(3, 3, 0, 3, 3, 3, 3);
        assertThat(batch.repetitionLevels).containsExactly(0, 1, 0, 0, 1, 0, 1);
        assertThat(batch.recordOffsets).containsExactly(0, 2, 3, 5);
        assertThat(presentInts(batch)).containsExactly(1, 2, 10, 11, 30, 31);
    }

    @Test
    void aMaskedFixedSizeListPageAloneFillsAFixedWidthBatch() {
        Column column = column(PhysicalType.INT32);
        NestedPageTask fixed = page(intPage(new int[]{ 10, 11, 20, 21, 30, 31 }, null, null, 2), new int[]{ 0, 1, 2, 3 });

        NestedBatch batch = column.fill(cell(fixed, 0, 2));

        assertThat(batch.fixedListK).isEqualTo(2);
        assertThat(batch.definitionLevels).isNull();
        assertThat(batch.repetitionLevels).isNull();
        assertThat(batch.recordOffsets).containsExactly(0, 2);
        assertThat((int[]) batch.values).containsExactly(10, 11, 30, 31);
    }

    /// The page keeps records 2, 4 and 5, in two runs; the first cell takes its first two kept
    /// records, which span both runs, the second its last, which starts inside the second run.
    @Test
    void aMaskedPageSplitAcrossTwoCellsReadsEachFromItsPlaceInTheRuns() {
        Column column = column(PhysicalType.INT32);
        // Records [10, null], [], [30, 31], [null, 41], [50], [60, 61]
        NestedPageTask page = page(intPage(new int[]{ 10, 0, 0, 30, 31, 0, 41, 50, 60, 61 },
                new int[]{ 3, 2, 1, 3, 3, 2, 3, 3, 3, 3 }, new int[]{ 0, 1, 0, 0, 1, 0, 1, 0, 0, 1 }, 0),
                new int[]{ 2, 3, 4, 6 });

        NestedBatch first = column.fill(cell(page, 0, 2));
        NestedBatch second = column.fill(cell(page, 2, 3));

        assertThat(first.recordOffsets).containsExactly(0, 2);
        assertThat(first.repetitionLevels).containsExactly(0, 1, 0);
        assertThat((int[]) first.values).containsExactly(30, 31, 50);
        assertThat(second.recordOffsets).containsExactly(0);
        assertThat(second.repetitionLevels).containsExactly(0, 1);
        assertThat((int[]) second.values).containsExactly(60, 61);
    }

    /// Plain byte-array values of a masked page split across two cells: each cell's bytes are its
    /// segment's, summed over the runs it reads.
    @Test
    void aMaskedPlainBinaryPageSplitAcrossTwoCellsHoldsEachCellsBytes() {
        Column column = column(PhysicalType.BYTE_ARRAY);
        // Records ["a", null], ["bb"], null, ["ccc", "dd"], ["e"]
        int[] def = { 3, 2, 3, 0, 3, 3, 3 };
        int[] rep = { 0, 1, 0, 0, 0, 1, 0 };
        byte[] plain = plain("a", "bb", "ccc", "dd", "e");
        PageDecoder.LevelScratch scratch = new PageDecoder.LevelScratch();
        BinaryPageValues values = BinaryPageValues.plain(PhysicalType.BYTE_ARRAY, 0, def, MAX_DEF, def.length, plain, 0,
                plain.length, scratch);
        NestedPageTask page = page(new PageDecoder.InPlaceNestedPage(def.length, def, rep, 0, null, values, null),
                new int[]{ 1, 2, 3, 5 }, scratch);

        NestedBatch first = column.fill(cell(page, 0, 2));
        NestedBatch second = column.fill(cell(page, 2, 3));

        assertThat(strings(first)).containsExactly("bb", "ccc", "dd");
        assertThat(first.recordOffsets).containsExactly(0, 1);
        assertThat(strings(second)).containsExactly("e");
    }

    // ==================== Helpers ====================

    private record Column(ColumnSchema schema, NestedLevelComputer.Layers layers) {

        NestedBatch fill(NestedCell cell) {
            return NestedFill.fill(cell, BatchExchange.detachingNested(schema.name(), 16), schema, layers,
                    NestedIndexMode.ALL_ITEMS, new BinaryBatchValues.ViewDemand());
        }
    }

    private static Column column(PhysicalType type) {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(type, RepetitionType.OPTIONAL))
                .build();
        return new Column(schema.getColumn(0), NestedLevelComputer.computeLayers(schema)[0]);
    }

    /// A page of `INT32` values in place, the present ones as `PLAIN` bytes.
    private static PageDecoder.InPlaceNestedPage intPage(int[] values, int[] def, int[] rep, int k) {
        ByteBuffer plain = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < values.length; i++) {
            if (def == null || def[i] == MAX_DEF) {
                plain.putInt(values[i]);
            }
        }
        FlatPageValues flat = FlatPageValues.plain(PhysicalType.INT32, def, MAX_DEF, values.length, plain.array(), 0,
                plain.position());
        return new PageDecoder.InPlaceNestedPage(values.length, def, rep, k, flat, null, null);
    }

    private static NestedPageTask page(PageDecoder.InPlaceNestedPage inPlace, int[] mask) {
        return page(inPlace, mask, new PageDecoder.LevelScratch());
    }

    /// A measured page, trimmed to the records of `mask` unless it is `null`.
    private static NestedPageTask page(PageDecoder.InPlaceNestedPage inPlace, int[] mask,
                                       PageDecoder.LevelScratch scratch) {
        PageInfo info = new PageInfo(null, null, null, null, PageRowMask.ALL, PageInfo.UNKNOWN_RECORDS);
        NestedPageTask page = new NestedPageTask(info, "test.parquet", 0, 0, null, null, 0, scratch);
        page.inPlace = mask == null ? inPlace : PageTrimmer.trim(inPlace, PageRowMask.of(mask), scratch);
        page.measure = PageMeasure.of(page.inPlace, PageInfo.UNKNOWN_RECORDS, scratch);
        page.measured = true;
        return page;
    }

    private static NestedCell cell(NestedPageTask page, int recordStart, int recordEnd) {
        NestedCell cell = new NestedCell("test.parquet", false, 0);
        add(cell, page, recordStart, recordEnd);
        return cell;
    }

    /// Adds the page's kept records `[recordStart, recordEnd)` to `cell`.
    private static void add(NestedCell cell, NestedPageTask page, int recordStart, int recordEnd) {
        cell.add(page, recordStart, recordEnd, page.measure.recordStart(recordStart), page.measure.recordStart(recordEnd));
    }

    /// The batch's values at its present elements.
    private static int[] presentInts(NestedBatch batch) {
        int[] values = (int[]) batch.values;
        return IntStream.range(0, batch.valueCount).filter(v -> batch.definitionLevels[v] == MAX_DEF)
                .map(v -> values[v]).toArray();
    }

    private static String[] strings(NestedBatch batch) {
        BinaryBatchValues values = (BinaryBatchValues) batch.values;
        String[] strings = new String[batch.valueCount];
        for (int i = 0; i < batch.valueCount; i++) {
            strings[i] = new String(values.byteArrayAt(i), StandardCharsets.UTF_8);
        }
        return strings;
    }

    /// `PLAIN` byte-array values, each behind its 4-byte length.
    private static byte[] plain(String... values) {
        ByteBuffer plain = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        for (String value : values) {
            plain.putInt(value.length()).put(value.getBytes(StandardCharsets.UTF_8));
        }
        return Arrays.copyOf(plain.array(), plain.position());
    }
}
