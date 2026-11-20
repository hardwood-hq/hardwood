/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.batch.FlatBatch;

/// Evaluates a binary matcher once per referenced dictionary entry and answers
/// encoded rows from the cached outcomes.
///
/// A batch without a dictionary goes directly to the delegate's optimized
/// whole-batch loop. In a dictionary batch, a `-1` entry ID identifies a value
/// written `PLAIN` after the chunk's dictionary filled up; such a row uses the
/// delegate's per-value operation over its bytes. Rows with an entry ID are decided
/// by their entry's outcome alone.
public final class DictionaryBinaryBatchMatcher implements BinaryBatchMatcher {

    private static final byte UNKNOWN = 0;
    private static final byte NO_MATCH = 1;
    private static final byte MATCH = 2;

    private final BinaryBatchMatcher delegate;

    public DictionaryBinaryBatchMatcher(BinaryBatchMatcher delegate) {
        this.delegate = delegate;
    }

    @Override
    public void test(FlatBatch batch, long[] outWords) {
        BinaryBatchValues values = (BinaryBatchValues) batch.values;
        Dictionary.ByteArrayDictionary dictionary = values.dictionary;
        if (dictionary == null) {
            delegate.test(batch, outWords);
            return;
        }

        writeMatches(values, dictionary, dictionary.outcomes(this), batch.validity, batch.recordCount, outWords);
    }

    @Override
    public boolean testValue(byte[] bytes, int from, int to) {
        return delegate.testValue(bytes, from, to);
    }

    BinaryBatchMatcher delegate() {
        return delegate;
    }

    /// The outcome tables a column's filter fragment keeps per dictionary of the column: one for
    /// each dictionary matcher in it, `0` for a `null` fragment.
    public static int tables(ColumnBatchMatcher fragment) {
        return switch (fragment) {
            case null -> 0;
            case DictionaryBinaryBatchMatcher dictionary -> 1;
            case AndBatchMatcher and -> tables(and.first()) + tables(and.second());
            case OrBatchMatcher or -> tables(or.first()) + tables(or.second());
            default -> 0;
        };
    }

    /// Decides the batch's rows from `dictionary`'s outcome table `entryStates`, deciding and
    /// storing the outcome of each entry not yet decided.
    private void writeMatches(BinaryBatchValues values, Dictionary.ByteArrayDictionary dictionary, byte[] entryStates,
                              long[] validity, int recordCount, long[] outWords) {
        int[] dictionaryIndices = values.dictIndices;
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
                        state = dictionary.testEntry(dictionaryIndex, delegate) ? MATCH : NO_MATCH;
                        entryStates[dictionaryIndex] = state;
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

    /// Tests row `row`, which has no entry ID, through its bytes.
    private boolean testView(BinaryBatchValues values, int row) {
        return delegate.testValue(values.buffer(row), values.start(row), values.end(row));
    }
}
