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

import dev.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

/// The values of a decoded flat binary page (`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`), as
/// its fills read them, and its measure: the bytes of any range of its rows.
///
/// A page holds either plain values, whose bytes a fill copies into its cell's byte buffer, or
/// dictionary ids, which a fill writes as the batch's entry ids and which have no bytes of their
/// own, both read in place:
///
/// | Kind | From | Values |
/// |---|---|---|
/// | plain | a `PLAIN`, `BYTE_STREAM_SPLIT` or delta-encoded page | the value bytes in the decompressed page, or for `DELTA_BYTE_ARRAY` reconstructed into the page's scratch |
/// | ids | a dictionary-encoded page | one id per non-null value |
///
/// The fill writes the page's rows in page order, so the values are read with one forward cursor,
/// as [FlatPageValues] is; a fill that starts elsewhere reads through a [#view()] of its own. The
/// definition levels, ids and byte prefix live in the scratch the page is decoded with, and the
/// bytes in that scratch or the decompressor's buffer, for as long as the page's fills run.
final class BinaryPageValues {

    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private enum Kind {
        PLAIN,
        IDS
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
    /// Whether each length-prefixed plain value carries its 4-byte length before its bytes;
    /// `false` for the delta encodings, whose lengths are decoded up front.
    private final boolean prefixed;
    /// `BYTE_STREAM_SPLIT` values: the non-null values, which is the distance between two of a
    /// value's bytes; `0` for any other encoding.
    private final int stride;
    /// Ids: one per non-null value.
    private final int[] ids;
    private final Dictionary.ByteArrayDictionary dictionary;

    /// The page row the cursor is at, the non-null values before it, and where the value of the
    /// next non-null row starts in [#data].
    private int row;
    private int nonNull;
    private int dataPos;

    private BinaryPageValues(Kind kind, int numValues, int[] definitionLevels, int maxDefinitionLevel,
                             boolean allPresent, int[] bytePrefix, byte[] data, int plainOffset, int width,
                             boolean prefixed, int stride, int[] ids, Dictionary.ByteArrayDictionary dictionary) {
        this.kind = kind;
        this.numValues = numValues;
        this.definitionLevels = definitionLevels;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.allPresent = allPresent;
        this.bytePrefix = bytePrefix;
        this.data = data;
        this.plainOffset = plainOffset;
        this.width = width;
        this.prefixed = prefixed;
        this.stride = stride;
        this.ids = ids;
        this.dictionary = dictionary;
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
                data, offset, width, true, 0, null, null);
    }

    /// `BYTE_STREAM_SPLIT` values `width` bytes wide from `offset` in `data`: byte `b` of the `i`th
    /// non-null value at `offset + b * nonNull + i`.
    ///
    /// @throws ParquetReadException if the value section does not hold exactly the page's values
    static BinaryPageValues byteStreamSplit(int width, int[] definitionLevels, int maxDefinitionLevel, int numValues,
                                            byte[] data, int offset, int limit, PageDecoder.LevelScratch scratch) {
        int nonNull = FlatPageValues.countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        long bytes = (long) nonNull * width;
        if (bytes != limit - offset) {
            throw new ParquetReadException("BYTE_STREAM_SPLIT value section holds " + (limit - offset)
                    + " bytes, not the " + bytes + " of its " + nonNull + " FIXED_LEN_BYTE_ARRAY values");
        }
        boolean allPresent = nonNull == numValues;
        int[] prefix = null;
        if (!allPresent) {
            prefix = scratch.bytePrefix(numValues + 1);
            prefix[0] = 0;
            for (int r = 0; r < numValues; r++) {
                prefix[r + 1] = prefix[r] + (definitionLevels[r] == maxDefinitionLevel ? width : 0);
            }
        }
        return new BinaryPageValues(Kind.PLAIN, numValues, definitionLevels, maxDefinitionLevel, allPresent, prefix,
                data, offset, width, false, Math.max(1, nonNull), null, null);
    }

    /// `DELTA_LENGTH_BYTE_ARRAY` values from `offset` in `data`: their lengths, delta-encoded, then
    /// their bytes one after another. Decodes the lengths into `scratch`'s byte prefix; the fill
    /// copies the bytes from where they lie.
    ///
    /// @throws ParquetReadException if the value section does not hold exactly the page's values
    static BinaryPageValues deltaLength(int[] definitionLevels, int maxDefinitionLevel, int numValues, byte[] data,
                                        int offset, int limit, PageDecoder.LevelScratch scratch) {
        int nonNull = FlatPageValues.countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        int[] lengths = scratch.lengths(nonNull);
        // The lengths' header is there even when no value is
        DeltaBinaryPackedDecoder lengthDecoder = new DeltaBinaryPackedDecoder(data, offset, limit);
        lengthDecoder.readInts(lengths, 0, nonNull);
        int bytesStart = lengthDecoder.getPos();
        int[] prefix = prefixOf(definitionLevels, maxDefinitionLevel, numValues, lengths, scratch);
        long expected = prefix[numValues];
        if (expected != limit - bytesStart) {
            throw new ParquetReadException("DELTA_LENGTH_BYTE_ARRAY value section holds " + (limit - bytesStart)
                    + " value bytes, not the " + expected + " its lengths give");
        }
        return new BinaryPageValues(Kind.PLAIN, numValues, definitionLevels, maxDefinitionLevel, nonNull == numValues,
                prefix, data, bytesStart, 0, false, 0, null, null);
    }

    /// `DELTA_BYTE_ARRAY` values from `offset` in `data`: prefix lengths, then suffixes encoded as
    /// `DELTA_LENGTH_BYTE_ARRAY`. A value is the first prefix-length bytes of the value before it
    /// and its suffix, so the values exist nowhere whole: they are reconstructed one after another
    /// into `scratch`'s value buffer, each prefix copied from the previous value there, and the
    /// fill copies them from it.
    ///
    /// @param width the width of a `FIXED_LEN_BYTE_ARRAY` column's values, which every value must
    ///        have; `0` for a `BYTE_ARRAY` column
    ///
    /// @throws ParquetReadException if the value section is malformed
    static BinaryPageValues deltaByteArray(int width, int[] definitionLevels, int maxDefinitionLevel, int numValues,
                                           byte[] data, int offset, int limit, PageDecoder.LevelScratch scratch) {
        int nonNull = FlatPageValues.countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        int[] prefixLengths = scratch.ids(nonNull);
        int[] suffixLengths = scratch.lengths(nonNull);
        // Both lengths' headers are there even when no value is
        DeltaBinaryPackedDecoder prefixDecoder = new DeltaBinaryPackedDecoder(data, offset, limit);
        prefixDecoder.readInts(prefixLengths, 0, nonNull);
        DeltaBinaryPackedDecoder suffixDecoder = new DeltaBinaryPackedDecoder(data, prefixDecoder.getPos(), limit);
        suffixDecoder.readInts(suffixLengths, 0, nonNull);
        int suffixPos = suffixDecoder.getPos();
        long total = 0;
        for (int i = 0; i < nonNull; i++) {
            if (prefixLengths[i] < 0 || suffixLengths[i] < 0) {
                throw new ParquetReadException("Invalid DELTA_BYTE_ARRAY length: prefix " + prefixLengths[i]
                        + ", suffix " + suffixLengths[i]);
            }
            total += (long) prefixLengths[i] + suffixLengths[i];
        }
        if (total > Integer.MAX_VALUE) {
            throw BinaryBatchValues.bufferOverflow(nonNull);
        }
        byte[] values = scratch.valueBytes((int) total);
        int pos = 0;
        int previousStart = 0;
        int previousLength = 0;
        for (int i = 0; i < nonNull; i++) {
            int prefixLength = prefixLengths[i];
            int suffixLength = suffixLengths[i];
            if (prefixLength > previousLength) {
                throw new ParquetReadException("DELTA_BYTE_ARRAY value " + i + " takes a prefix of " + prefixLength
                        + " bytes from a previous value of " + previousLength);
            }
            if (suffixLength > limit - suffixPos) {
                throw new ParquetReadException("Unexpected EOF reading byte array: expected " + suffixLength
                        + ", got " + (limit - suffixPos));
            }
            System.arraycopy(values, previousStart, values, pos, prefixLength);
            System.arraycopy(data, suffixPos, values, pos + prefixLength, suffixLength);
            suffixPos += suffixLength;
            previousStart = pos;
            previousLength = prefixLength + suffixLength;
            if (width > 0 && previousLength != width) {
                throw new ParquetReadException("DELTA_BYTE_ARRAY value " + i + " is " + previousLength
                        + " bytes long, not the " + width + " of its FIXED_LEN_BYTE_ARRAY column");
            }
            pos += previousLength;
            suffixLengths[i] = previousLength;
        }
        if (suffixPos != limit) {
            throw new ParquetReadException("DELTA_BYTE_ARRAY value section does not end with its " + nonNull
                    + " values; leftover bytes: " + (limit - suffixPos));
        }
        int[] prefix = prefixOf(definitionLevels, maxDefinitionLevel, numValues, suffixLengths, scratch);
        return new BinaryPageValues(Kind.PLAIN, numValues, definitionLevels, maxDefinitionLevel, nonNull == numValues,
                prefix, values, 0, 0, false, 0, null, null);
    }

    /// The byte prefix of a page's rows from the lengths of its non-null values, into `scratch`.
    private static int[] prefixOf(int[] definitionLevels, int maxDefinitionLevel, int numValues, int[] lengths,
                                  PageDecoder.LevelScratch scratch) {
        int[] prefix = scratch.bytePrefix(numValues + 1);
        long sum = 0;
        int value = 0;
        prefix[0] = 0;
        for (int r = 0; r < numValues; r++) {
            if (definitionLevels == null || definitionLevels[r] == maxDefinitionLevel) {
                int length = lengths[value++];
                if (length < 0) {
                    throw new ParquetReadException("Invalid BYTE_ARRAY length: " + length);
                }
                sum += length;
            }
            prefix[r + 1] = Math.toIntExact(sum);
        }
        return prefix;
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
                null, null, 0, 0, false, 0, ids, dictionary);
    }

    /// A reader of the same values with a cursor of its own at the page's first row, so that two
    /// fills can read one page at once.
    BinaryPageValues view() {
        return new BinaryPageValues(kind, numValues, definitionLevels, maxDefinitionLevel, allPresent, bytePrefix,
                data, plainOffset, width, prefixed, stride, ids, dictionary);
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
        skipTo(pageRow);
        if (stride > 0) {
            for (int i = 0; i < length; i++) {
                int r = pageRow + i;
                starts[destPos + i] = pos;
                if (definitionLevels == null || definitionLevels[r] == maxDefinitionLevel) {
                    int at = plainOffset + nonNull++;
                    for (int b = 0; b < width; b++) {
                        dest[pos + b] = data[at + b * stride];
                    }
                    pos += width;
                }
                ends[destPos + i] = pos;
            }
            row = pageRow + length;
            return pos;
        }
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
            int prefixLength = width == 0 && prefixed ? 4 : 0;
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
        int skipped = FlatPageValues.countNonNull(definitionLevels, maxDefinitionLevel, row, pageRow);
        if (kind == Kind.PLAIN) {
            dataPos += width == 0
                    ? (prefixed ? 4 * skipped : 0) + bytePrefix[pageRow] - bytePrefix[row]
                    : width * skipped;
        }
        nonNull += skipped;
        row = pageRow;
    }
}
