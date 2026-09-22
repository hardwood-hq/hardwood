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
/// whole-batch loop. In a dictionary batch, `-1` entry IDs identify plain rows
/// (and rows from a second chunk in a straddling batch); those use the
/// delegate's per-value operation over their packed byte slices.
public final class DictionaryBinaryBatchMatcher implements BinaryBatchMatcher {

    private static final byte UNKNOWN = 0;
    private static final byte NO_MATCH = 1;
    private static final byte MATCH = 2;

    private final BinaryBatchMatcher delegate;
    private Dictionary.ByteArrayDictionary cachedDictionary;
    private byte[] entryStates;
    private int undecidedEntries;

    public DictionaryBinaryBatchMatcher(BinaryBatchMatcher delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean requiresDictionaryIndices() {
        return true;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        BinaryBatchValues values = (BinaryBatchValues) batch.values;
        Dictionary.ByteArrayDictionary dictionary = values.dictionary;
        if (dictionary == null) {
            delegate.test(batch, outWords);
            return;
        }

        int[] dictionaryIndices = values.dictIndices;
        if (dictionaryIndices == null) {
            throw new IllegalStateException(
                    "A binary batch with a dictionary must retain its dictionary indices");
        }

        prepareDictionary(dictionary);
        if (undecidedEntries != 0) {
            decideReferencedEntries(values, dictionaryIndices, batch.validity, batch.recordCount);
        }
        writeMatches(values, dictionaryIndices, batch.validity, batch.recordCount, outWords);
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
        int size = dictionary.size();
        if (entryStates == null || entryStates.length < size) {
            entryStates = new byte[size];
        }
        else {
            Arrays.fill(entryStates, 0, size, UNKNOWN);
        }
        undecidedEntries = size;
    }

    private void decideReferencedEntries(BinaryBatchValues values, int[] dictionaryIndices,
                                         long[] validity, int recordCount) {
        byte[][] entries = cachedDictionary.values();
        for (int row = 0; row < recordCount; row++) {
            if (validity != null && (validity[row >>> 6] & (1L << row)) == 0L) {
                continue;
            }
            int dictionaryIndex = dictionaryIndices[row];
            if (dictionaryIndex >= 0 && entryStates[dictionaryIndex] == UNKNOWN) {
                byte[] entry = entries[dictionaryIndex];
                entryStates[dictionaryIndex] =
                        delegate.testValue(entry, 0, entry.length) ? MATCH : NO_MATCH;
                undecidedEntries--;
                if (undecidedEntries == 0) {
                    return;
                }
            }
        }
    }

    private void writeMatches(BinaryBatchValues values, int[] dictionaryIndices,
                              long[] validity, int recordCount, long[] outWords) {
        byte[] bytes = values.bytes;
        int[] offsets = values.offsets;
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
                boolean matches = dictionaryIndex >= 0
                        ? entryStates[dictionaryIndex] == MATCH
                        : delegate.testValue(bytes, offsets[row], offsets[row + 1]);
                if (matches) {
                    word |= 1L << bit;
                }
            }
            outWords[wordIndex] = word;
        }
    }
}
