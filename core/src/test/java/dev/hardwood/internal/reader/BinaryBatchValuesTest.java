/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A [BinaryBatchValues] filled positionally, as a cell's segments fill it ([BinaryFill],
/// [NestedFill]): entry ids whose views are built when a reader first needs them, beside plain
/// values whose bytes and views the segments write.
class BinaryBatchValuesTest {

    private static final Dictionary.ByteArrayDictionary STATIONS = dictionary("Hamburg", "Oslo", "Abha");

    @Test
    void dictionaryValuesViewOneCopyOfTheDictionary() {
        BinaryBatchValues values = filled(STATIONS, null, 1, 0, 1, 2, 1, 0);

        assertThat(strings(values, 6)).containsExactly("Oslo", "Hamburg", "Oslo", "Abha", "Oslo", "Hamburg");
        assertThat(values.byteCount()).isEqualTo("HamburgOsloAbha".length());
        assertThat(values.starts()[0]).isEqualTo(values.starts()[2]).isEqualTo(values.starts()[4]);
    }

    @Test
    void aNullDictionaryValueGetsAnEmptyView() {
        BinaryBatchValues values = filled(STATIONS, null, 0, -1, 2);

        assertThat(values.ends()[1] - values.starts()[1]).isEqualTo(0);
        assertThat(values.stringAt(0)).isEqualTo("Hamburg");
        assertThat(values.stringAt(2)).isEqualTo("Abha");
    }

    /// Values drawing less from the dictionary than its size are appended, and the dictionary is
    /// not copied in.
    @Test
    void aDictionaryLargerThanTheBatchsValuesFromItIsNotCopiedIn() {
        BinaryBatchValues values = filled(STATIONS, null, 2);

        assertThat(strings(values, 1)).containsExactly("Abha");
        assertThat(values.byteCount()).isEqualTo("Abha".length());
    }

    @Test
    void aBatchViewsItsDictionaryAlongsidePlainValues() {
        BinaryBatchValues values = filled(STATIONS, null, 0, 1, 0, "Tunis", 1, 2, 2, 2);

        assertThat(strings(values, 8)).containsExactly("Hamburg", "Oslo", "Hamburg", "Tunis", "Oslo", "Abha", "Abha", "Abha");
        // The dictionary values' bytes exceed the dictionary's, so it is copied in once after the plain bytes.
        assertThat(values.byteCount()).isEqualTo("TunisHamburgOsloAbha".length());
        assertThat(values.dictIndices).containsExactly(0, 1, 0, -1, 1, 2, 2, 2);
    }

    @Test
    void aSecondDictionaryInOneBatchIsRejected() {
        BinaryBatchValues values = new BinaryBatchValues(4, 32);
        values.prepareForFill();
        values.installDictionary(STATIONS);

        assertThatThrownBy(() -> values.installDictionary(dictionary("Accra", "Lima")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A batch holds values of two column chunks' dictionaries");
    }

    @Test
    void aRefilledSlotTakesTheNextRowGroupsDictionary() {
        BinaryBatchValues values = filled(STATIONS, null, 0, 1, 2, 0);
        values.ensureViews();

        fill(values, dictionary("Accra", "Lima"), null, 1, 1, 0, 1);

        assertThat(strings(values, 4)).containsExactly("Lima", "Lima", "Accra", "Lima");
        assertThat(values.byteCount()).isEqualTo("AccraLima".length());
    }

    @Test
    void aRefilledSlotStartsWithoutTheDeferredValuesOfTheLast() {
        BinaryBatchValues values = filled(STATIONS, null, 0, 1);
        values.ensureViews();

        fill(values, null, null, "Lima");

        assertThat(values.dictionary).isNull();
        assertThat(strings(values, 1)).containsExactly("Lima");
    }

    @Test
    void deferredValuesGetTheirViewsWhenFirstRead() {
        BinaryBatchValues values = filled(STATIONS, null, 0, 1, 2, 0);

        // A batch of dictionary values only has no view arrays until a reader needs them.
        assertThat(values.viewArraysAllocated()).isFalse();

        values.ensureViews();

        assertThat(strings(values, 4)).containsExactly("Hamburg", "Oslo", "Abha", "Hamburg");
        // The deferred values' bytes exceed the dictionary's, so it is copied in once.
        assertThat(values.byteCount()).isEqualTo("HamburgOsloAbha".length());
    }

    @Test
    void aDeferredNullGetsItsEmptyViewAtOnce() {
        BinaryBatchValues values = filled(STATIONS, null, 0, -1, 2);

        assertThat(values.viewArraysAllocated()).isTrue();
        assertThat(values.hasPendingViews()).isTrue();

        values.ensureViews();

        assertThat(strings(values, 3)).containsExactly("Hamburg", "", "Abha");
    }

    @Test
    void buildingDeferredViewsRaisesTheDemand() {
        BinaryBatchValues.ViewDemand demand = new BinaryBatchValues.ViewDemand();
        BinaryBatchValues plain = filled(null, demand, "Tunis");
        plain.ensureViews();

        // Views that were never deferred raise nothing.
        assertThat(demand.requested).isFalse();

        BinaryBatchValues deferred = filled(STATIONS, demand, 0, 1, "Tunis");

        // A string read resolves dictionary values through their ids and plain ones through the
        // views their segment wrote, so it builds nothing and raises nothing.
        assertThat(deferred.stringAt(0)).isEqualTo("Hamburg");
        assertThat(deferred.stringAt(2)).isEqualTo("Tunis");
        assertThat(demand.requested).isFalse();

        deferred.ensureViews();

        assertThat(demand.requested).isTrue();
    }

    @Test
    void compactingKeepsTheKeptDictionaryValuesDeferred() {
        BinaryBatchValues.ViewDemand demand = new BinaryBatchValues.ViewDemand();
        BinaryBatchValues raw = filled(STATIONS, demand, 0, 1, 2, 1, "Tunis");

        BinaryBatchValues kept = raw.compact(new int[]{ 1, 2, 4 }, 3);

        // A filtered read of the ids builds no views and leaves the column deferring.
        assertThat(kept.dictIndices).containsExactly(1, 2, -1);
        assertThat(kept.hasPendingViews()).isTrue();
        assertThat(demand.requested).isFalse();

        assertThat(strings(kept, 3)).containsExactly("Oslo", "Abha", "Tunis");
        assertThat(demand.requested).isTrue();
        // Building the kept batch's views leaves the raw batch's bytes as they were.
        assertThat(strings(raw, 5)).containsExactly("Hamburg", "Oslo", "Abha", "Oslo", "Tunis");
    }

    @Test
    void compactingCarriesWhereTheDictionaryWasCopiedIn() {
        BinaryBatchValues raw = filled(STATIONS, null, "Tunis", 0, 1, 0, 1, 2, 0, 1);
        raw.ensureViews();

        BinaryBatchValues kept = raw.compact(new int[]{ 0, 2, 5 }, 3);

        assertThat(strings(kept, 3)).containsExactly("Tunis", "Oslo", "Abha");
        // The kept values point into the dictionary already copied in; nothing is appended.
        assertThat(kept.byteCount()).isEqualTo("TunisHamburgOsloAbha".length());
    }

    private static BinaryBatchValues filled(Dictionary.ByteArrayDictionary dictionary,
                                            BinaryBatchValues.ViewDemand demand, Object... entries) {
        BinaryBatchValues values = new BinaryBatchValues(entries.length, 32);
        fill(values, dictionary, demand, entries);
        return values;
    }

    /// Fills `values` as one cell's segments would: an `Integer` entry is a dictionary value's
    /// entry id, `-1` a null with its empty view; a `String` entry is a plain value, its bytes and
    /// view written, its entry id `-1` in a batch with a dictionary.
    private static void fill(BinaryBatchValues values, Dictionary.ByteArrayDictionary dictionary,
                             BinaryBatchValues.ViewDemand demand, Object... entries) {
        values.prepareForFill();
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
        values.prepareBytes(plainBytes);
        if (views) {
            values.installViews();
        }
        int[] ids = dictionary != null ? values.installDictionary(dictionary) : null;
        int pos = 0;
        for (int i = 0; i < entries.length; i++) {
            if (entries[i] instanceof String text) {
                byte[] bytes = utf8(text);
                System.arraycopy(bytes, 0, values.fillBytes(), pos, bytes.length);
                values.fillStarts()[i] = pos;
                pos += bytes.length;
                values.fillEnds()[i] = pos;
                if (ids != null) {
                    ids[i] = -1;
                }
            }
            else {
                int id = (Integer) entries[i];
                ids[i] = id;
                if (id < 0) {
                    values.fillStarts()[i] = 0;
                    values.fillEnds()[i] = 0;
                }
            }
        }
        values.finishFill(entries.length, demand != null ? demand : new BinaryBatchValues.ViewDemand());
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
            result[i] = new String(values.bytes(), values.starts()[i], values.ends()[i] - values.starts()[i],
                    StandardCharsets.UTF_8);
        }
        return result;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
