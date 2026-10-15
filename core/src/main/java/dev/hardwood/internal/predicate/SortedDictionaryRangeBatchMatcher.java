/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.Dictionary;

/// SPIKE (#1258): evaluates an ordering predicate as an interval of dictionary ids, trusting
/// that the batch's dictionary is sorted in the delegate's comparison order.
///
/// On a sorted dictionary an ordering predicate holds on a prefix (`<`, `<=`) or a suffix
/// (`>`, `>=`) of the entries, so the boundary is found by binary search through the
/// delegate's `testValue`, and a row matches when `lo <= id < hi`.
public final class SortedDictionaryRangeBatchMatcher implements BinaryBatchMatcher {

    private final BinaryBatchMatcher delegate;
    private final boolean matchesPrefix;
    private Dictionary.ByteArrayDictionary cachedDictionary;
    private int lo;
    private int span;

    public SortedDictionaryRangeBatchMatcher(BinaryBatchMatcher delegate, boolean matchesPrefix) {
        this.delegate = delegate;
        this.matchesPrefix = matchesPrefix;
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
    }

    @Override
    public boolean testValue(byte[] bytes, int from, int to) {
        return delegate.testValue(bytes, from, to);
    }

    private void prepareDictionary(Dictionary.ByteArrayDictionary dictionary) {
        if (dictionary == cachedDictionary) {
            return;
        }
        cachedDictionary = dictionary;
        byte[] entryBytes = dictionary.entryBytes();
        int[] entryOffsets = dictionary.entryOffsets();
        int size = dictionary.size();
        // First entry whose outcome differs from the outcome of entry 0's side of the boundary:
        // for a prefix predicate the first non-match, for a suffix predicate the first match.
        int low = 0;
        int high = size;
        while (low < high) {
            int mid = (low + high) >>> 1;
            boolean matches = delegate.testValue(entryBytes, entryOffsets[mid], entryOffsets[mid + 1]);
            if (matches == matchesPrefix) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        if (matchesPrefix) {
            lo = 0;
            span = low;
        }
        else {
            lo = low;
            span = size - low;
        }
    }

    private void writeMatches(BinaryBatchValues values, int[] dictionaryIndices,
                              long[] validity, int recordCount, long[] outWords) {
        int lo = this.lo;
        int span = this.span;
        int activeWords = (recordCount + 63) >>> 6;
        for (int wordIndex = 0; wordIndex < activeWords; wordIndex++) {
            int base = wordIndex << 6;
            int rows = Math.min(64, recordCount - base);
            long present = validity != null ? validity[wordIndex] : -1L;
            long word = 0L;
            int signs = 0;
            for (int bit = 0; bit < rows; bit++) {
                int id = dictionaryIndices[base + bit];
                signs |= id;
                long inRange = Integer.compareUnsigned(id - lo, span) < 0 ? 1L : 0L;
                word |= inRange << bit;
            }
            word &= present;
            if (signs < 0) {
                word = correctPlainRows(values, dictionaryIndices, base, rows, present, word);
            }
            outWords[wordIndex] = word;
        }
    }

    /// Decides the rows of one word whose id is `-1` (a value written `PLAIN`) through their views.
    private long correctPlainRows(BinaryBatchValues values, int[] dictionaryIndices, int base, int rows,
                                  long present, long word) {
        for (int bit = 0; bit < rows; bit++) {
            int row = base + bit;
            if (dictionaryIndices[row] < 0 && (present & (1L << bit)) != 0L
                    && delegate.testValue(values.bytes(), values.starts()[row], values.ends()[row])) {
                word |= 1L << bit;
            }
        }
        return word;
    }
}
