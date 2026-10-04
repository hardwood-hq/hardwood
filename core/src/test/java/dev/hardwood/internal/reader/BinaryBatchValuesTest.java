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

class BinaryBatchValuesTest {

    private static final Dictionary.ByteArrayDictionary STATIONS = dictionary("Hamburg", "Oslo", "Abha");

    @Test
    void dictionaryValuesViewOneCopyOfTheDictionary() {
        BinaryBatchValues values = new BinaryBatchValues(6, 32);

        values.viewDictionaryRange(page(STATIONS, 1, 0, 1, 2, 1, 0), 0, 0, 6);

        assertThat(strings(values, 6)).containsExactly("Oslo", "Hamburg", "Oslo", "Abha", "Oslo", "Hamburg");
        assertThat(values.byteCount).isEqualTo("HamburgOsloAbha".length());
        assertThat(values.starts[0]).isEqualTo(values.starts[2]).isEqualTo(values.starts[4]);
    }

    @Test
    void aNullDictionaryValueGetsAnEmptyView() {
        BinaryBatchValues values = new BinaryBatchValues(3, 32);

        values.viewDictionaryRange(page(STATIONS, 0, -1, 2), 0, 0, 3);

        assertThat(values.ends[1] - values.starts[1]).isEqualTo(0);
        assertThat(values.stringAt(0)).isEqualTo("Hamburg");
        assertThat(values.stringAt(2)).isEqualTo("Abha");
    }

    @Test
    void aDictionaryLargerThanTheBatchsValuesFromItIsNotCopiedIn() {
        BinaryBatchValues values = new BinaryBatchValues(1, 32);

        values.viewDictionaryRange(page(STATIONS, 2), 0, 0, 1);

        assertThat(strings(values, 1)).containsExactly("Abha");
        assertThat(values.byteCount).isEqualTo("Abha".length());
    }

    /// The batch takes two values of a long page whose values far outweigh the dictionary;
    /// what decides is the two values the batch takes, not the page.
    @Test
    void theBatchsShareOfAPageDecidesNotThePage() {
        Dictionary.ByteArrayDictionary wide = dictionary("a".repeat(100), "b".repeat(100), "c".repeat(100));
        int[] indices = new int[1_000];
        BinaryBatchValues values = new BinaryBatchValues(2, 32);

        values.viewDictionaryRange(page(wide, indices), 0, 0, 2);

        assertThat(strings(values, 2)).containsExactly("a".repeat(100), "a".repeat(100));
        assertThat(values.byteCount).isEqualTo(200);
    }

    /// Values are appended until those from the dictionary reach its size; the value that
    /// gets there brings the dictionary in, and later values view it.
    @Test
    void aDictionaryIsCopiedInOnceTheBatchsValuesFromItReachItsSize() {
        BinaryBatchValues values = new BinaryBatchValues(4, 32);
        Page.DictionaryByteArrayPage page = page(STATIONS, 0, 1, 2, 1);

        for (int i = 0; i < 4; i++) {
            values.viewDictionaryRange(page, i, i, 1);
        }

        assertThat(strings(values, 4)).containsExactly("Hamburg", "Oslo", "Abha", "Oslo");
        assertThat(values.byteCount).isEqualTo("HamburgOslo".length() + "HamburgOsloAbha".length());
        assertThat(values.starts[3]).isEqualTo(values.byteCount - "Abha".length() - "Oslo".length());
    }

    @Test
    void plainValuesAppendInOrderWithEmptyViewsForNulls() {
        BinaryBatchValues values = new BinaryBatchValues(3, 1);

        values.appendRange(new byte[][] { utf8("Tunis"), null, utf8("Lima") }, 0, 0, 3);

        assertThat(strings(values, 3)).containsExactly("Tunis", "", "Lima");
        assertThat(values.byteCount).isEqualTo("TunisLima".length());
    }

    @Test
    void aBatchViewsItsDictionaryAlongsidePlainValues() {
        BinaryBatchValues values = new BinaryBatchValues(8, 32);

        values.viewDictionaryRange(page(STATIONS, 0, 1, 0, 1, 0, 1), 0, 0, 3);
        values.appendAt(3, utf8("Tunis"), 0, "Tunis".length());
        values.viewDictionaryRange(page(STATIONS, 1, 2, 2, 2), 0, 4, 4);

        assertThat(strings(values, 8)).containsExactly("Hamburg", "Oslo", "Hamburg", "Tunis", "Oslo", "Abha", "Abha", "Abha");
        // The first range's bytes exceed the dictionary's, so the dictionary is copied in once.
        assertThat(values.byteCount).isEqualTo("HamburgOsloAbhaTunis".length());
    }

    @Test
    void aSecondDictionaryInOneBatchIsRejected() {
        BinaryBatchValues values = new BinaryBatchValues(4, 32);
        values.viewDictionaryRange(page(STATIONS, 0, 1), 0, 0, 1);

        assertThatThrownBy(() -> values.viewDictionaryRange(page(dictionary("Accra", "Lima"), 1, 0), 0, 1, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A batch holds values of two column chunks' dictionaries");
    }

    @Test
    void resetStartsTheNextBatchWithoutTheDictionariesOfTheLast() {
        BinaryBatchValues values = new BinaryBatchValues(4, 32);
        values.viewDictionaryRange(page(STATIONS, 0, 1, 2, 0), 0, 0, 4);

        values.reset();
        values.viewDictionaryRange(page(STATIONS, 2, 2, 1, 0), 0, 0, 4);

        assertThat(strings(values, 4)).containsExactly("Abha", "Abha", "Oslo", "Hamburg");
        assertThat(values.byteCount).isEqualTo("HamburgOsloAbha".length());
    }

    @Test
    void aResetSlotTakesTheNextRowGroupsDictionary() {
        BinaryBatchValues values = new BinaryBatchValues(4, 32);
        values.viewDictionaryRange(page(STATIONS, 0, 1, 2, 0), 0, 0, 4);

        values.reset();
        values.viewDictionaryRange(page(dictionary("Accra", "Lima"), 1, 1, 0, 1), 0, 0, 4);

        assertThat(strings(values, 4)).containsExactly("Lima", "Lima", "Accra", "Lima");
        assertThat(values.byteCount).isEqualTo("AccraLima".length());
    }

    @Test
    void singleValueViewsMatchTheRangeForm() {
        BinaryBatchValues values = new BinaryBatchValues(3, 32);
        Page.DictionaryByteArrayPage page = page(STATIONS, 2, 0, 2, 1, 2, 2);

        values.viewDictionaryValue(page, 1, 0);
        values.viewDictionaryValue(page, 0, 1);
        values.viewDictionaryValue(page, 3, 2);

        assertThat(strings(values, 3)).containsExactly("Hamburg", "Abha", "Oslo");
        assertThat(values.byteCount).isEqualTo("HamburgAbha".length() + "HamburgOsloAbha".length());
    }

    private static Dictionary.ByteArrayDictionary dictionary(String... entries) {
        byte[][] values = new byte[entries.length][];
        for (int i = 0; i < entries.length; i++) {
            values[i] = utf8(entries[i]);
        }
        return new Dictionary.ByteArrayDictionary(values);
    }

    private static Page.DictionaryByteArrayPage page(Dictionary.ByteArrayDictionary dictionary, int... indices) {
        return new Page.DictionaryByteArrayPage(dictionary, indices, null, null, 0, indices.length);
    }

    private static String[] strings(BinaryBatchValues values, int count) {
        String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            result[i] = new String(values.bytes, values.starts[i], values.ends[i] - values.starts[i],
                    StandardCharsets.UTF_8);
        }
        return result;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
