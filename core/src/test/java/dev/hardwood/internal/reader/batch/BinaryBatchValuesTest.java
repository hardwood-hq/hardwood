/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.decode.TestDictionaries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A [BinaryBatchValues] filled positionally, as a cell's segments fill it
/// ([dev.hardwood.internal.reader.pipeline.BinaryFill],
/// [dev.hardwood.internal.reader.pipeline.NestedFill]): dictionary values held as their entry ids,
/// beside plain values whose bytes and views the segments write, and the window of every value's
/// view, built as the cell completes where the column's [BinaryBatchValues.ViewDemand] was raised
/// and when asked otherwise.
class BinaryBatchValuesTest {

    private static final Dictionary.ByteArrayDictionary STATIONS = dictionary("Hamburg", "Oslo", "Abha");

    @Test
    void dictionaryValuesResolveThroughTheirEntries() {
        BinaryBatchValues values = filled(STATIONS, notRequested(), 1, 0, 1, 2, 1, 0);

        assertThat(values.hasWindow()).isFalse();
        assertThat(strings(values, 6)).containsExactly("Oslo", "Hamburg", "Oslo", "Abha", "Oslo", "Hamburg");
        for (int i = 0; i < 6; i++) {
            assertThat(values.buffer(i)).as("value %d", i).isSameAs(STATIONS.chunkBytes(0));
        }
        // No view is written: the batch neither allocates view arrays nor takes the dictionary's chunks
        assertThat(values.viewArraysAllocated()).isFalse();
        assertThat(values.bufferCount()).isEqualTo(1);
    }

    @Test
    void aRaisedDemandBuildsTheWindowAsTheCellCompletes() {
        BinaryBatchValues values = filled(STATIONS, requested(), 1, 0, 1, 2);

        assertThat(values.hasWindow()).isTrue();
        BinaryBatchValues.Window window = values.window(4);
        assertThat(windowStrings(window)).containsExactly("Oslo", "Hamburg", "Oslo", "Abha");
        assertThat(window.buffers()[window.bufferIds()[3]]).isSameAs(STATIONS.chunkBytes(0));
        assertThat(window.starts()[0]).isEqualTo(window.starts()[2]);
        // The dictionary's chunks are the window's buffers, never the batch's
        assertThat(values.bufferCount()).isEqualTo(1);
    }

    /// A value whose entry is in the second or a later chunk of a dictionary is read from that
    /// chunk, through its entry or through the window.
    @Test
    void dictionaryValuesAreReadFromTheChunkHoldingTheirEntry() {
        String[] entries = new String[3000];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = String.valueOf(i).repeat(1000).substring(0, 1000);
        }
        Dictionary.ByteArrayDictionary dictionary = dictionary(entries);
        assertThat(dictionary.chunkCount()).isGreaterThan(2);

        for (BinaryBatchValues.ViewDemand demand : new BinaryBatchValues.ViewDemand[]{ notRequested(), requested() }) {
            BinaryBatchValues values = filled(dictionary, demand, 2999, 0, -1, 1500, "Tunis", 600);

            assertThat(strings(values, 6)).containsExactly(entries[2999], entries[0], "", entries[1500], "Tunis",
                    entries[600]);
            assertThat(values.buffer(0)).isSameAs(dictionary.chunkBytes(dictionary.chunkOf(2999)));
            assertThat(values.buffer(3)).isSameAs(dictionary.chunkBytes(dictionary.chunkOf(1500)));
            assertThat(values.buffer(5)).isSameAs(dictionary.chunkBytes(dictionary.chunkOf(600)));
            assertThat(values.stringAt(0)).isEqualTo(entries[2999]);
            // The window carries each chunk under its buffer id, after the buffer of the plain value
            BinaryBatchValues.Window window = values.window(6);
            assertThat(windowStrings(window)).containsExactly(entries[2999], entries[0], "", entries[1500], "Tunis",
                    entries[600]);
            assertThat(window.buffers()[window.bufferIds()[3]]).isSameAs(values.buffer(3));
        }
    }

    @Test
    void aNullDictionaryValueIsEmpty() {
        for (BinaryBatchValues.ViewDemand demand : new BinaryBatchValues.ViewDemand[]{ notRequested(), requested() }) {
            BinaryBatchValues values = filled(STATIONS, demand, 0, -1, 2);

            assertThat(strings(values, 3)).containsExactly("Hamburg", "", "Abha");
            assertThat(values.byteArrayAt(1)).isEmpty();
        }
    }

    @Test
    void aBatchReadsItsDictionaryValuesAlongsidePlainValues() {
        for (BinaryBatchValues.ViewDemand demand : new BinaryBatchValues.ViewDemand[]{ notRequested(), requested() }) {
            BinaryBatchValues values = filled(STATIONS, demand, 0, 1, 0, "Tunis", 1, 2, 2, 2);

            assertThat(strings(values, 8)).containsExactly("Hamburg", "Oslo", "Hamburg", "Tunis", "Oslo", "Abha", "Abha",
                    "Abha");
            assertThat(values.dictIndices).containsExactly(0, 1, 0, -1, 1, 2, 2, 2);
            assertThat(values.buffer(0)).isSameAs(STATIONS.chunkBytes(0));
            assertThat(values.buffer(3)).isNotSameAs(STATIONS.chunkBytes(0));
        }
    }

    /// Taking the window of a batch with dictionary values raises the column's demand, so that its
    /// later batches have theirs built as they complete; reading values one at a time, as bytes or
    /// strings, or the window of a batch without dictionary values, raises nothing.
    @Test
    void takingAWindowRaisesTheDemand() {
        BinaryBatchValues.ViewDemand demand = notRequested();
        BinaryBatchValues plain = filled(null, demand, "Tunis");
        assertThat(windowStrings(plain.window(1))).containsExactly("Tunis");

        BinaryBatchValues entries = filled(STATIONS, demand, 0, 1, "Tunis");
        assertThat(entries.stringAt(0)).isEqualTo("Hamburg");
        assertThat(entries.byteArrayAt(1)).isEqualTo(utf8("Oslo"));
        assertThat(entries.stringAt(2)).isEqualTo("Tunis");

        assertThat(demand.requested).isFalse();

        assertThat(windowStrings(entries.window(3))).containsExactly("Hamburg", "Oslo", "Tunis");

        assertThat(demand.requested).isTrue();
        // The batch is not changed; the next one gets its window as it completes
        assertThat(entries.hasWindow()).isFalse();
        assertThat(filled(STATIONS, demand, 2).hasWindow()).isTrue();
    }

    /// A window built when asked has arrays of its own, the dictionary's chunks among its buffers,
    /// and leaves the batch as it was.
    @Test
    void aWindowBuiltWhenAskedLeavesTheBatchAsItWas() {
        BinaryBatchValues values = filled(STATIONS, notRequested(), 1, -1, "Tunis", 2, 0);

        BinaryBatchValues.Window window = values.window(4);

        assertThat(window.starts()).hasSize(4);
        assertThat(windowStrings(window)).containsExactly("Oslo", "", "Tunis", "Abha");
        assertThat(window.buffers()[window.bufferIds()[0]]).isSameAs(STATIONS.chunkBytes(0));
        assertThat(values.hasWindow()).isFalse();
        assertThat(values.bufferCount()).isEqualTo(2);
    }

    @Test
    void aWindowHasExactlyTheValuesAskedFor() {
        BinaryBatchValues built = filled(STATIONS, requested(), 1, 0, 2);
        BinaryBatchValues plain = filled(null, notRequested(), "Lima", "Tunis");

        assertThat(windowStrings(built.window(2))).containsExactly("Oslo", "Hamburg");
        assertThat(windowStrings(plain.window(1))).containsExactly("Lima");
        // A batch without dictionary values hands out its own views where they fit exactly
        assertThat(plain.window(2).starts()).isSameAs(plain.starts());
    }

    @Test
    void aBatchWithDictionaryValuesRefusesItsViewArrays() {
        for (BinaryBatchValues.ViewDemand demand : new BinaryBatchValues.ViewDemand[]{ notRequested(), requested() }) {
            BinaryBatchValues values = filled(STATIONS, demand, 0, 1);

            assertThatThrownBy(values::buffers)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("A batch's dictionary values have no views of their own");
            assertThatThrownBy(values::starts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("A batch's dictionary values have no views of their own");
        }
    }

    @Test
    void aSecondDictionaryInOneBatchIsRejected() {
        BinaryBatchValues values = new BinaryBatchValues(4);
        values.prepareForFill();
        values.installDictionary(STATIONS);

        assertThatThrownBy(() -> values.installDictionary(dictionary("Accra", "Lima")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A batch holds values of two column chunks' dictionaries");
    }

    @Test
    void aRefilledSlotTakesTheNextRowGroupsDictionary() {
        BinaryBatchValues values = filled(STATIONS, requested(), 0, 1, 2, 0);

        Dictionary.ByteArrayDictionary next = dictionary("Accra", "Lima");
        fill(values, next, notRequested(), 1, 1, 0, 1);

        assertThat(values.hasWindow()).isFalse();
        assertThat(strings(values, 4)).containsExactly("Lima", "Lima", "Accra", "Lima");
        assertThat(values.buffer(0)).isSameAs(next.chunkBytes(0));
        assertThat(values.bufferCount()).isEqualTo(1);
    }

    @Test
    void aRefilledSlotStartsWithoutTheDictionaryOfTheLast() {
        BinaryBatchValues values = filled(STATIONS, notRequested(), 0, 1);

        fill(values, null, notRequested(), "Lima");

        assertThat(values.dictionary).isNull();
        assertThat(values.hasWindow()).isFalse();
        assertThat(strings(values, 1)).containsExactly("Lima");
    }

    /// A dictionary chunk the size of a full plain chunk is still the dictionary's: a holder that
    /// keeps its full chunks for its next fill does not take it as a spare and write plain bytes
    /// over its entries.
    @Test
    void aKeptHolderDoesNotReuseADictionaryChunkAsASpare() {
        String first = "a".repeat(BinaryBatchValues.CHUNK_BYTES);
        String second = "b".repeat(BinaryBatchValues.CHUNK_BYTES);
        Dictionary.ByteArrayDictionary dictionary = dictionary(first, second);
        assertThat(dictionary.chunkCount()).isEqualTo(2);
        assertThat(dictionary.chunkBytes(0)).hasSize(BinaryBatchValues.CHUNK_BYTES);
        BinaryBatchValues values = new BinaryBatchValues(3);
        values.keepChunks();
        fill(values, dictionary, requested(), 0, 1, "Tunis");
        assertThat(strings(values, 3)).containsExactly(first, second, "Tunis");

        // Enough plain bytes that the fill takes a full chunk, a spare where the last fill kept one
        String plain = "c".repeat(BinaryBatchValues.CHUNK_BYTES / 2 + 1);
        fill(values, null, requested(), plain, "Oslo", "Abha");

        assertThat(strings(values, 3)).containsExactly(plain, "Oslo", "Abha");
        assertThat(values.buffer(0)).isNotSameAs(dictionary.chunkBytes(0)).isNotSameAs(dictionary.chunkBytes(1));
        assertThat(dictionary.entry(0)).isEqualTo(utf8(first));
        assertThat(dictionary.entry(1)).isEqualTo(utf8(second));
    }

    @Test
    void compactingKeepsTheEntriesAndTheColumnsDemand() {
        BinaryBatchValues.ViewDemand demand = notRequested();
        BinaryBatchValues raw = filled(STATIONS, demand, 0, 1, 2, 1, "Tunis");

        BinaryBatchValues kept = raw.compact(new int[]{ 1, 2, 4 }, 3);

        assertThat(kept.dictIndices).containsExactly(1, 2, -1);
        assertThat(kept.hasWindow()).isFalse();
        assertThat(strings(kept, 3)).containsExactly("Oslo", "Abha", "Tunis");
        assertThat(demand.requested).isFalse();

        // The kept batch raises the column's demand as the raw batch would
        assertThat(windowStrings(kept.window(3))).containsExactly("Oslo", "Abha", "Tunis");
        assertThat(demand.requested).isTrue();
    }

    @Test
    void compactingKeepsTheWindowAndTheBuffersTheViewsPointInto() {
        BinaryBatchValues raw = filled(STATIONS, requested(), "Tunis", 0, 1, 0, 1, 2, 0, 1);

        BinaryBatchValues kept = raw.compact(new int[]{ 0, 2, 5 }, 3);

        assertThat(kept.hasWindow()).isTrue();
        assertThat(windowStrings(kept.window(3))).containsExactly("Tunis", "Oslo", "Abha");
        assertThat(strings(kept, 3)).containsExactly("Tunis", "Oslo", "Abha");
        assertThat(kept.buffer(0)).isSameAs(raw.buffer(0));
        assertThat(kept.buffer(2)).isSameAs(STATIONS.chunkBytes(0));
        assertThat(kept.bufferCount()).isEqualTo(raw.bufferCount());
    }

    private static BinaryBatchValues.ViewDemand notRequested() {
        return new BinaryBatchValues.ViewDemand();
    }

    private static BinaryBatchValues.ViewDemand requested() {
        BinaryBatchValues.ViewDemand demand = new BinaryBatchValues.ViewDemand();
        demand.requested = true;
        return demand;
    }

    private static BinaryBatchValues filled(Dictionary.ByteArrayDictionary dictionary,
                                            BinaryBatchValues.ViewDemand demand, Object... entries) {
        BinaryBatchValues values = new BinaryBatchValues(entries.length);
        fill(values, dictionary, demand, entries);
        return values;
    }

    /// Fills `values` as one cell's segments would: an `Integer` entry is a dictionary value's
    /// entry id, `-1` a null with its empty view; a `String` entry is a plain value, its bytes and
    /// view written, its entry id `-1` in a batch with a dictionary.
    private static void fill(BinaryBatchValues values, Dictionary.ByteArrayDictionary dictionary,
                             BinaryBatchValues.ViewDemand demand, Object... entries) {
        values.prepareForFill();
        int[] ids = dictionary != null ? values.installDictionary(dictionary) : null;
        int plainBytes = 0;
        boolean views = false;
        for (Object entry : entries) {
            if (entry instanceof String text) {
                plainBytes += utf8(text).length;
                views = true;
            }
            else {
                views |= (Integer) entry < 0;
            }
        }
        PlainBytes dest = new PlainBytes(values, plainBytes);
        if (views) {
            values.installViews();
        }
        for (int i = 0; i < entries.length; i++) {
            if (entries[i] instanceof String text) {
                byte[] bytes = utf8(text);
                byte[] chunk = dest.room(bytes.length);
                System.arraycopy(bytes, 0, chunk, dest.position(), bytes.length);
                values.fillBufferIds()[i] = dest.id();
                values.fillStarts()[i] = dest.position();
                values.fillEnds()[i] = dest.position() + bytes.length;
                dest.advance(bytes.length);
                if (ids != null) {
                    ids[i] = -1;
                }
            }
            else {
                int id = (Integer) entries[i];
                ids[i] = id;
                if (id < 0) {
                    values.fillBufferIds()[i] = 0;
                    values.fillStarts()[i] = 0;
                    values.fillEnds()[i] = 0;
                }
            }
        }
        values.finishFill(entries.length, demand);
    }

    private static Dictionary.ByteArrayDictionary dictionary(String... entries) {
        byte[][] values = new byte[entries.length][];
        for (int i = 0; i < entries.length; i++) {
            values[i] = utf8(entries[i]);
        }
        return TestDictionaries.byteArrays(values);
    }

    private static String[] strings(BinaryBatchValues values, int count) {
        String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            int start = values.start(i);
            result[i] = new String(values.buffer(i), start, values.end(i) - start, StandardCharsets.UTF_8);
        }
        return result;
    }

    private static String[] windowStrings(BinaryBatchValues.Window window) {
        String[] result = new String[window.starts().length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new String(window.buffers()[window.bufferIds()[i]], window.starts()[i],
                    window.ends()[i] - window.starts()[i], StandardCharsets.UTF_8);
        }
        return result;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
