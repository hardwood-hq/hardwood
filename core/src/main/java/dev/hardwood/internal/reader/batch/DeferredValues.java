/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import dev.hardwood.metadata.PhysicalType;

/// The entry ids of a flat numeric column's dictionary-encoded batches, kept for a reader of
/// dictionary ids, and the values they stand for, which the batch writes only when its reader
/// reads them.
///
/// A column whose reader may ask for its entry ids has a [Demand]. While its reader has read no
/// values, a batch decoded entirely from one dictionary keeps the ids alone: its [BatchExchange.Batch#values]
/// stays `null` and [#materialize] writes them on the reader's thread when it asks. That raises
/// [Demand#values], and from then on each batch writes its values as it is filled, and its ids
/// as well only once the reader has asked for ids ([Demand#ids]). A batch a dictionary does not
/// cover entirely has values and no ids.
public final class DeferredValues {

    private DeferredValues() {
    }

    /// What the reader of one column has read, raised by the reader and read by the fills of its
    /// batches. One instance per column and read; never lowered.
    public static final class Demand {
        /// Set once the reader read a batch's values.
        public volatile boolean values;
        /// Set once the reader asked for a batch's entry ids.
        public volatile boolean ids;
    }

    /// Whether a flat column of `type` can keep its entry ids: the fixed-width numeric types.
    public static boolean holds(PhysicalType type) {
        return switch (type) {
            case INT32, INT64, FLOAT, DOUBLE -> true;
            default -> false;
        };
    }

    /// The values of the first `count` records of a batch whose values are deferred, written from
    /// its entry ids into an array of exactly `count`; a null's element is left at zero.
    public static Object materialize(BatchExchange.Batch batch, int count) {
        Object out = switch (batch.dictionary) {
            case Dictionary.IntDictionary d -> new int[count];
            case Dictionary.LongDictionary d -> new long[count];
            case Dictionary.FloatDictionary d -> new float[count];
            case Dictionary.DoubleDictionary d -> new double[count];
            default -> throw new IllegalStateException("Not a numeric dictionary: " + batch.dictionary);
        };
        write(batch.dictionary, batch.dictionaryIds, 0, count, out);
        return out;
    }

    /// Writes the values of entry ids `[from, to)` of `ids` from `dictionary` to the same
    /// positions of `out`; an id of `-1`, a null, leaves its element as it is.
    public static void write(Dictionary dictionary, int[] ids, int from, int to, Object out) {
        switch (dictionary) {
            case Dictionary.IntDictionary d -> {
                int[] values = d.values();
                int[] dest = (int[]) out;
                for (int i = from; i < to; i++) {
                    int id = ids[i];
                    if (id >= 0) {
                        dest[i] = values[id];
                    }
                }
            }
            case Dictionary.LongDictionary d -> {
                long[] values = d.values();
                long[] dest = (long[]) out;
                for (int i = from; i < to; i++) {
                    int id = ids[i];
                    if (id >= 0) {
                        dest[i] = values[id];
                    }
                }
            }
            case Dictionary.FloatDictionary d -> {
                float[] values = d.values();
                float[] dest = (float[]) out;
                for (int i = from; i < to; i++) {
                    int id = ids[i];
                    if (id >= 0) {
                        dest[i] = values[id];
                    }
                }
            }
            case Dictionary.DoubleDictionary d -> {
                double[] values = d.values();
                double[] dest = (double[]) out;
                for (int i = from; i < to; i++) {
                    int id = ids[i];
                    if (id >= 0) {
                        dest[i] = values[id];
                    }
                }
            }
            default -> throw new IllegalStateException("Not a numeric dictionary: " + dictionary);
        }
    }
}
