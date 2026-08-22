/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.binary;

import java.util.Arrays;

import dev.hardwood.internal.predicate.BinaryBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.Dictionary;

/// Equality matcher for binary columns. Dictionary-encoded rows compare their
/// integer entry IDs; plain rows compare their packed byte ranges without
/// materializing a per-row `byte[]`.
public final class BinaryEqBatchMatcher implements BinaryBatchMatcher {

    private static final int ABSENT = -1;
    private static final int MULTIPLE = -2;

    private final byte[] literal;
    private Dictionary.ByteArrayDictionary cachedDictionary;
    private int cachedDictionaryId;
    private long[] cachedDictionaryMask;

    public BinaryEqBatchMatcher(byte[] literal) {
        this.literal = literal;
    }

    @Override
    public boolean requiresDictionaryIndices() {
        return true;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        BinaryBatchValues values = (BinaryBatchValues) batch.values;
        Dictionary.ByteArrayDictionary dictionary = values.dictionary;
        int[] dictionaryIndices = dictionary == null ? null : values.dictIndices;
        if (dictionary != null && dictionaryIndices == null) {
            throw new IllegalStateException(
                    "A binary batch with a dictionary must retain its dictionary indices");
        }

        int dictionaryId = dictionary == null ? ABSENT : dictionaryId(dictionary);
        int n = batch.recordCount;
        int fullWords = n >>> 6;
        int tail = n & 63;

        for (int w = 0; w < fullWords; w++) {
            int base = w << 6;
            long word = 0L;
            for (int b = 0; b < 64; b++) {
                int row = base + b;
                word |= (matches(values, dictionaryIndices, dictionaryId, row) ? 1L : 0L) << b;
            }
            outWords[w] = word;
        }
        if (tail != 0) {
            int base = fullWords << 6;
            long word = 0L;
            for (int b = 0; b < tail; b++) {
                int row = base + b;
                word |= (matches(values, dictionaryIndices, dictionaryId, row) ? 1L : 0L) << b;
            }
            outWords[fullWords] = word;
        }

        long[] validity = batch.validity;
        if (validity != null) {
            int activeWords = (n + 63) >>> 6;
            for (int w = 0; w < activeWords; w++) {
                outWords[w] &= validity[w];
            }
        }
    }

    private boolean matches(BinaryBatchValues values, int[] dictionaryIndices,
                            int dictionaryId, int row) {
        if (dictionaryIndices != null) {
            int rowDictionaryId = dictionaryIndices[row];
            if (rowDictionaryId >= 0) {
                if (dictionaryId >= 0) {
                    return rowDictionaryId == dictionaryId;
                }
                return dictionaryId == MULTIPLE
                        && (cachedDictionaryMask[rowDictionaryId >>> 6]
                            & (1L << rowDictionaryId)) != 0;
            }
        }
        int start = values.offsets[row];
        int end = values.offsets[row + 1];
        return Arrays.equals(values.bytes, start, end, literal, 0, literal.length);
    }

    private int dictionaryId(Dictionary.ByteArrayDictionary dictionary) {
        if (dictionary != cachedDictionary) {
            cachedDictionary = dictionary;
            resolveDictionary(dictionary.values());
        }
        return cachedDictionaryId;
    }

    private void resolveDictionary(byte[][] entries) {
        int firstMatch = ABSENT;
        long[] matches = null;
        for (int i = 0; i < entries.length; i++) {
            if (Arrays.equals(entries[i], literal)) {
                if (firstMatch == ABSENT) {
                    firstMatch = i;
                }
                else {
                    if (matches == null) {
                        matches = new long[(entries.length + 63) >>> 6];
                        matches[firstMatch >>> 6] |= 1L << firstMatch;
                    }
                    matches[i >>> 6] |= 1L << i;
                }
            }
        }
        cachedDictionaryId = matches == null ? firstMatch : MULTIPLE;
        cachedDictionaryMask = matches;
    }
}
