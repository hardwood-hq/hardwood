/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

/// The values of a decoded flat binary page (`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`), as
/// its fills read them, and its measure: the bytes of any range of its rows.
///
/// A page holds either plain values, whose bytes a fill copies into its cell's byte buffer, or
/// dictionary ids, which a fill writes as the batch's entry ids and which have no bytes of their
/// own. Each comes in place or from a decoded [Page]:
///
/// | Kind | From | Values |
/// |---|---|---|
/// | plain | a `V1` `PLAIN` page | length-prefixed or fixed-width bytes in the decompressed page |
/// | ids | a `V1` dictionary-encoded page | one id per non-null value |
/// | arrays | any other page ([Page.ByteArrayPage]) | one array per value, `null` for a null |
/// | row ids | any other dictionary page ([Page.DictionaryByteArrayPage]) | one id per value, `-1` for a null |
///
/// The fill writes the page's rows in page order, so the in-place kinds are read with one forward
/// cursor, as [FlatPageValues] is. The definition levels, ids and byte prefix of an in-place page
/// live in the decode thread's scratch, and its bytes in the decompressor's buffer, for the length
/// of its task.
final class BinaryPageValues {

    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private enum Kind {
        PLAIN,
        IDS,
        ARRAYS,
        ROW_IDS
    }

    private final Kind kind;
    /// The page's values, nulls included.
    private final int numValues;
    /// The page's definition levels, `null` when the page has none.
    private final int[] definitionLevels;
    private final int maxDefinitionLevel;
    private final boolean allPresent;
    /// `bytePrefix[r]` is the bytes of the rows before row `r`; `null` for ids, and for fixed-width
    /// values without nulls, whose bytes are arithmetic.
    private final int[] bytePrefix;
    /// Plain values in place: the decompressed page, where its values start, and their width, `0`
    /// for length-prefixed values.
    private final byte[] data;
    private final int plainOffset;
    private final int width;
    /// Ids: one per non-null value ([Kind#IDS]) or one per value ([Kind#ROW_IDS]).
    private final int[] ids;
    private final Dictionary.ByteArrayDictionary dictionary;
    /// A decoded page's values ([Kind#ARRAYS]).
    private final byte[][] arrays;

    /// The page row the cursor is at, the non-null values before it, and where the value of the
    /// next non-null row starts in [#data].
    private int row;
    private int nonNull;
    private int dataPos;

    private BinaryPageValues(Kind kind, int numValues, int[] definitionLevels, int maxDefinitionLevel,
                             boolean allPresent, int[] bytePrefix, byte[] data, int plainOffset, int width,
                             int[] ids, Dictionary.ByteArrayDictionary dictionary, byte[][] arrays) {
        this.kind = kind;
        this.numValues = numValues;
        this.definitionLevels = definitionLevels;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.allPresent = allPresent;
        this.bytePrefix = bytePrefix;
        this.data = data;
        this.plainOffset = plainOffset;
        this.width = width;
        this.ids = ids;
        this.dictionary = dictionary;
        this.arrays = arrays;
        this.dataPos = plainOffset;
    }

    /// `PLAIN` values of `type` from `offset` in `data`: length-prefixed when `width` is `0`,
    /// `width` bytes each otherwise. Measures them: walks the length prefixes into `scratch`'s
    /// byte prefix.
    ///
    /// @throws ParquetReadException if the value section does not hold exactly the page's values
    static BinaryPageValues plain(PhysicalType type, int width, int[] definitionLevels, int maxDefinitionLevel,
                                  int numValues, byte[] data, int offset, int limit,
                                  PageDecoder.LevelScratch scratch) {
        int nonNull = FlatPageValues.countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        boolean allPresent = nonNull == numValues;
        int[] prefix = null;
        int end;
        if (width == 0) {
            prefix = scratch.bytePrefix(numValues + 1);
            end = measureLengthPrefixed(definitionLevels, maxDefinitionLevel, numValues, data, offset, limit, prefix);
        }
        else {
            long bytes = (long) nonNull * width;
            if (bytes > limit - offset) {
                throw new ParquetReadException(type == PhysicalType.INT96
                        ? "Unexpected EOF while reading INT96"
                        : "Unexpected EOF while reading fixed-length byte array");
            }
            end = offset + (int) bytes;
            if (!allPresent) {
                prefix = scratch.bytePrefix(numValues + 1);
                prefix[0] = 0;
                for (int r = 0; r < numValues; r++) {
                    prefix[r + 1] = prefix[r] + (definitionLevels[r] == maxDefinitionLevel ? width : 0);
                }
            }
        }
        if (end != limit) {
            throw new ParquetReadException("PLAIN value section does not end with its " + nonNull + " " + type
                    + " values; leftover bytes: " + (limit - end));
        }
        return new BinaryPageValues(Kind.PLAIN, numValues, definitionLevels, maxDefinitionLevel, allPresent, prefix,
                data, offset, width, null, null, null);
    }

    /// Walks the length prefixes of the page's non-null values into `prefix`.
    ///
    /// @return where the last value ends
    private static int measureLengthPrefixed(int[] definitionLevels, int maxDefinitionLevel, int numValues,
                                             byte[] data, int offset, int limit, int[] prefix) {
        int pos = offset;
        long sum = 0;
        prefix[0] = 0;
        for (int r = 0; r < numValues; r++) {
            if (definitionLevels == null || definitionLevels[r] == maxDefinitionLevel) {
                if (pos > limit - 4) {
                    throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY length");
                }
                int length = (int) INT.get(data, pos);
                pos += 4;
                if (length < 0) {
                    throw new ParquetReadException("Invalid BYTE_ARRAY length: " + length);
                }
                if (length > limit - pos) {
                    throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY data");
                }
                pos += length;
                sum += length;
            }
            prefix[r + 1] = Math.toIntExact(sum);
        }
        return pos;
    }

    /// Dictionary ids already decoded into `ids`, one per non-null value.
    ///
    /// @throws ArrayIndexOutOfBoundsException if an id lies outside the dictionary
    static BinaryPageValues ids(int[] definitionLevels, int maxDefinitionLevel, int numValues, int[] ids,
                                int idCount, Dictionary.ByteArrayDictionary dictionary) {
        int size = dictionary.size();
        for (int i = 0; i < idCount; i++) {
            int id = ids[i];
            if (id < 0 || id >= size) {
                throw new ArrayIndexOutOfBoundsException("Dictionary index " + id
                        + " out of bounds for a dictionary of " + size + " entries");
            }
        }
        return new BinaryPageValues(Kind.IDS, numValues, definitionLevels, maxDefinitionLevel, idCount == numValues,
                null, null, 0, 0, ids, dictionary, null);
    }

    /// The values of a page decoded by [PageDecoder#decodePage], measured into `scratch`'s byte
    /// prefix when they are plain.
    ///
    /// @throws IllegalStateException if the page is not binary, or its values exceed 2 GB
    static BinaryPageValues of(Page page, PageDecoder.LevelScratch scratch) {
        return switch (page) {
            case Page.ByteArrayPage p -> {
                int size = p.size();
                byte[][] values = p.values();
                int[] prefix = scratch.bytePrefix(size + 1);
                long sum = 0;
                boolean allPresent = true;
                prefix[0] = 0;
                for (int r = 0; r < size; r++) {
                    byte[] value = values[r];
                    if (value != null) {
                        sum += value.length;
                    }
                    else {
                        allPresent = false;
                    }
                    if (sum > Integer.MAX_VALUE) {
                        throw BinaryBatchValues.bufferOverflow(r);
                    }
                    prefix[r + 1] = (int) sum;
                }
                yield new BinaryPageValues(Kind.ARRAYS, size, p.definitionLevels(), p.maxDefinitionLevel(),
                        allPresent, prefix, null, 0, 0, null, null, values);
            }
            case Page.DictionaryByteArrayPage p -> {
                int size = p.size();
                int[] rowIds = p.dictIndices();
                boolean allPresent = true;
                for (int r = 0; r < size && allPresent; r++) {
                    allPresent = rowIds[r] >= 0;
                }
                yield new BinaryPageValues(Kind.ROW_IDS, size, p.definitionLevels(), p.maxDefinitionLevel(),
                        allPresent, null, null, 0, 0, rowIds, p.dictionary(), null);
            }
            default -> throw new IllegalStateException("A binary column decoded a " + page.getClass().getSimpleName());
        };
    }

    /// The page's values, nulls included.
    int size() {
        return numValues;
    }

    int[] definitionLevels() {
        return definitionLevels;
    }

    /// Whether the page holds no null.
    boolean allPresent() {
        return allPresent;
    }

    /// The dictionary the page's ids refer to, `null` for plain values.
    Dictionary.ByteArrayDictionary dictionary() {
        return dictionary;
    }

    /// The bytes of page rows `[from, to)`: `0` for ids.
    int bytes(int from, int to) {
        if (dictionary != null) {
            return 0;
        }
        if (bytePrefix == null) {
            return (to - from) * width;
        }
        return bytePrefix[to] - bytePrefix[from];
    }

    /// Writes the plain values of page rows `[pageRow, pageRow + length)` as values `[destPos,
    /// destPos + length)`: each value's bytes to `dest` from `destByte` on, in row order, and its
    /// view `[starts, ends)` into `dest`. A null gets an empty view. Rows must be asked for in
    /// increasing order.
    ///
    /// @return the byte of `dest` after the last value written
    int copyPlain(int pageRow, int length, int[] starts, int[] ends, int destPos, byte[] dest, int destByte) {
        int pos = destByte;
        if (kind == Kind.ARRAYS) {
            for (int i = 0; i < length; i++) {
                byte[] value = arrays[pageRow + i];
                starts[destPos + i] = pos;
                if (value != null) {
                    System.arraycopy(value, 0, dest, pos, value.length);
                    pos += value.length;
                }
                ends[destPos + i] = pos;
            }
            return pos;
        }
        skipTo(pageRow);
        if (width > 0 && definitionLevels == null) {
            int bytes = length * width;
            System.arraycopy(data, dataPos, dest, pos, bytes);
            for (int i = 0; i < length; i++) {
                starts[destPos + i] = pos;
                pos += width;
                ends[destPos + i] = pos;
            }
            dataPos += bytes;
            nonNull += length;
        }
        else {
            int prefixLength = width == 0 ? 4 : 0;
            for (int i = 0; i < length; i++) {
                int r = pageRow + i;
                starts[destPos + i] = pos;
                if (definitionLevels == null || definitionLevels[r] == maxDefinitionLevel) {
                    int valueLength = width == 0 ? bytePrefix[r + 1] - bytePrefix[r] : width;
                    System.arraycopy(data, dataPos + prefixLength, dest, pos, valueLength);
                    dataPos += prefixLength + valueLength;
                    pos += valueLength;
                    nonNull++;
                }
                ends[destPos + i] = pos;
            }
        }
        row = pageRow + length;
        return pos;
    }

    /// Writes the entry ids of page rows `[pageRow, pageRow + length)` as values `[destPos, destPos
    /// + length)` of `dictIndices`; a null gets `-1` and an empty view in `starts` and `ends`,
    /// which are `null` only for a page with no null. Rows must be asked for in increasing order.
    void copyIds(int pageRow, int length, int[] dictIndices, int[] starts, int[] ends, int destPos) {
        if (kind == Kind.ROW_IDS) {
            System.arraycopy(ids, pageRow, dictIndices, destPos, length);
            if (!allPresent) {
                for (int i = 0; i < length; i++) {
                    if (ids[pageRow + i] < 0) {
                        starts[destPos + i] = 0;
                        ends[destPos + i] = 0;
                    }
                }
            }
            return;
        }
        skipTo(pageRow);
        if (definitionLevels == null) {
            System.arraycopy(ids, nonNull, dictIndices, destPos, length);
            nonNull += length;
        }
        else {
            for (int i = 0; i < length; i++) {
                int dest = destPos + i;
                if (definitionLevels[pageRow + i] == maxDefinitionLevel) {
                    dictIndices[dest] = ids[nonNull++];
                }
                else {
                    dictIndices[dest] = -1;
                    starts[dest] = 0;
                    ends[dest] = 0;
                }
            }
        }
        row = pageRow + length;
    }

    /// Moves the cursor to `pageRow`, past the values of the rows a mask drops.
    private void skipTo(int pageRow) {
        if (pageRow < row) {
            throw new IllegalStateException("Page rows read out of order: row " + pageRow + " after " + row);
        }
        int skipped = definitionLevels == null ? pageRow - row : countNonNullBetween(row, pageRow);
        if (kind == Kind.PLAIN) {
            dataPos += width == 0
                    ? 4 * skipped + bytePrefix[pageRow] - bytePrefix[row]
                    : width * skipped;
        }
        nonNull += skipped;
        row = pageRow;
    }

    private int countNonNullBetween(int from, int to) {
        int count = 0;
        for (int i = from; i < to; i++) {
            if (definitionLevels[i] == maxDefinitionLevel) {
                count++;
            }
        }
        return count;
    }
}
