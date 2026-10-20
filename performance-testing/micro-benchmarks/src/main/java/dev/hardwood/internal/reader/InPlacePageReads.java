/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.schema.ColumnSchema;

/// Reads a data page through [PageDecoder]'s in-place entry points and writes its values as a fill
/// does, into arrays it reuses across pages, for benchmarks outside the reader's package.
public final class InPlacePageReads {

    private final PageDecoder.LevelScratch scratch = new PageDecoder.LevelScratch();
    private Object numbers;
    private int[] starts = new int[0];
    private int[] ends = new int[0];
    private int[] ids = new int[0];
    private int[] bufferIds = new int[0];

    /// Reads `page` of `column` and writes its values; returns the array holding them, which the
    /// next read reuses: a primitive array for a numeric column, the entry ids of a dictionary
    /// page or, for a plain one, the batch its values' bytes were copied into, in chunks as a fill
    /// copies them.
    public Object read(PageDecoder decoder, ColumnSchema column, ByteBuffer page, Dictionary dictionary,
                       DecodeBuffers buffers) {
        if (column.maxRepetitionLevel() > 0) {
            PageDecoder.InPlaceNestedPage nested = decoder.decodeNestedInPlace(page, dictionary, scratch, buffers);
            return nested.flat() != null ? write(nested.flat(), column) : write(nested.binary());
        }
        return switch (column.type()) {
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> write(decoder.decodeFlatBinary(page, dictionary, scratch, buffers));
            default -> write(decoder.decodeFlat(page, dictionary, scratch, buffers), column);
        };
    }

    private Object write(FlatPageValues values, ColumnSchema column) {
        int size = values.size();
        Object out = switch (column.type()) {
            case BOOLEAN -> numbers instanceof boolean[] a && a.length >= size ? a : new boolean[size];
            case INT32 -> numbers instanceof int[] a && a.length >= size ? a : new int[size];
            case INT64 -> numbers instanceof long[] a && a.length >= size ? a : new long[size];
            case FLOAT -> numbers instanceof float[] a && a.length >= size ? a : new float[size];
            case DOUBLE -> numbers instanceof double[] a && a.length >= size ? a : new double[size];
            default -> throw new IllegalArgumentException("Not a numeric column: " + column);
        };
        numbers = out;
        values.copy(0, out, 0, size);
        return out;
    }

    private Object write(BinaryPageValues values) {
        int size = values.size();
        if (starts.length < size) {
            starts = new int[size];
            ends = new int[size];
            ids = new int[size];
            bufferIds = new int[size];
        }
        if (values.dictionary() != null) {
            values.copyIds(0, size, ids, starts, ends, bufferIds, 0);
            return ids;
        }
        BinaryBatchValues chunks = new BinaryBatchValues(size);
        values.copyPlain(0, size, starts, ends, bufferIds, 0, new PlainBytes(chunks, values.bytes(0, size)));
        return chunks;
    }
}
