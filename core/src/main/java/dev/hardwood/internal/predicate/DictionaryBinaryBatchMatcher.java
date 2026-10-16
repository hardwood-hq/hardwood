/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.Arrays;

import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.Dictionary;

/// Evaluates a binary matcher once per referenced dictionary entry and answers
/// encoded rows from the cached outcomes.
///
/// A batch without a dictionary goes directly to the delegate's optimized
/// whole-batch loop. In a dictionary batch, a `-1` entry ID identifies a value
/// written `PLAIN` after the chunk's dictionary filled up; such a row uses the
/// delegate's per-value operation over its byte view. Rows with an entry ID never
/// read their view, so a batch of dictionary values only is decided without its
/// views being built.
public final class DictionaryBinaryBatchMatcher implements BinaryBatchMatcher {

    private static final byte UNKNOWN = 0;
    private static final byte NO_MATCH = 1;
    private static final byte MATCH = 2;

    /// SPIKE (#1258): `lazy` (default), `eager`, or `planned` (eager when the read covers the whole chunk).
    private static final String SPIKE_ENTRY_MODE = System.getProperty("hardwood.spike.entryMode", "lazy");

    private final BinaryBatchMatcher delegate;
    private Dictionary.ByteArrayDictionary cachedDictionary;
    private byte[] entryStates;
    /// SPIKE (#1258): entries decided so far for the cached dictionary, for the adaptive mode.
    private int decided;

    public DictionaryBinaryBatchMatcher(BinaryBatchMatcher delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean readsEveryValueView() {
        return false;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        BinaryBatchValues values = (BinaryBatchValues) batch.values;
        Dictionary.ByteArrayDictionary dictionary = values.dictionary;
        if (dictionary == null) {
            delegate.test(batch, outWords);
            return;
        }

        prepareDictionary(dictionary);
        writeMatches(values, values.dictIndices, batch.validity, batch.recordCount, outWords);
        if (SPIKE_ENTRY_MODE.equals("adaptive")) {
            decideRestOnceMostlyTouched();
        }
    }

    /// SPIKE (#1258): once an eighth of the entries are decided, decide the rest in dictionary order.
    private void decideRestOnceMostlyTouched() {
        int size = cachedDictionary.size();
        if (decided == size || (long) decided * 8 < size) {
            return;
        }
        byte[] entryBytes = cachedDictionary.entryBytes();
        int[] entryOffsets = cachedDictionary.entryOffsets();
        for (int i = 0; i < size; i++) {
            if (entryStates[i] == UNKNOWN) {
                entryStates[i] = delegate.testValue(entryBytes, entryOffsets[i], entryOffsets[i + 1])
                        ? MATCH : NO_MATCH;
            }
        }
        decided = size;
    }

    @Override
    public boolean testValue(byte[] bytes, int from, int to) {
        return delegate.testValue(bytes, from, to);
    }

    BinaryBatchMatcher delegate() {
        return delegate;
    }

    private void prepareDictionary(Dictionary.ByteArrayDictionary dictionary) {
        if (dictionary == cachedDictionary) {
            return;
        }
        cachedDictionary = dictionary;
        decided = 0;
        int size = dictionary.size();
        if (entryStates == null || entryStates.length < size) {
            entryStates = new byte[size];
        }
        else {
            Arrays.fill(entryStates, 0, size, UNKNOWN);
        }
        if (SPIKE_ENTRY_MODE.equals("eager")
                || (SPIKE_ENTRY_MODE.equals("planned") && dictionary.spikeWholeChunk)) {
            // SPIKE (#1258): decide every entry up front, in dictionary order.
            byte[] entryBytes = dictionary.entryBytes();
            int[] entryOffsets = dictionary.entryOffsets();
            for (int i = 0; i < size; i++) {
                entryStates[i] = delegate.testValue(entryBytes, entryOffsets[i], entryOffsets[i + 1])
                        ? MATCH : NO_MATCH;
            }
        }
    }

    private void writeMatches(BinaryBatchValues values, int[] dictionaryIndices,
                              long[] validity, int recordCount, long[] outWords) {
        byte[] entryBytes = cachedDictionary.entryBytes();
        int[] entryOffsets = cachedDictionary.entryOffsets();
        int activeWords = (recordCount + 63) >>> 6;
        for (int wordIndex = 0; wordIndex < activeWords; wordIndex++) {
            int base = wordIndex << 6;
            int rows = Math.min(64, recordCount - base);
            long present = validity != null ? validity[wordIndex] : -1L;
            long word = 0L;
            for (int bit = 0; bit < rows; bit++) {
                if ((present & (1L << bit)) == 0L) {
                    continue;
                }
                int row = base + bit;
                int dictionaryIndex = dictionaryIndices[row];
                boolean matches;
                if (dictionaryIndex < 0) {
                    matches = testView(values, row);
                }
                else {
                    byte state = entryStates[dictionaryIndex];
                    if (state == UNKNOWN) {
                        state = delegate.testValue(entryBytes, entryOffsets[dictionaryIndex],
                                entryOffsets[dictionaryIndex + 1]) ? MATCH : NO_MATCH;
                        entryStates[dictionaryIndex] = state;
                        decided++;
                    }
                    matches = state == MATCH;
                }
                if (matches) {
                    word |= 1L << bit;
                }
            }
            outWords[wordIndex] = word;
        }
    }

    /// Tests row `row` through its byte view; the first such call on a batch whose
    /// dictionary values were deferred builds their views.
    private boolean testView(BinaryBatchValues values, int row) {
        return delegate.testValue(values.bytes(), values.starts()[row], values.ends()[row]);
    }
}
