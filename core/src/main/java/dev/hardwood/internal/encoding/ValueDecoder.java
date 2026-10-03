/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// Interface for decoders that read values directly into an output array,
/// placing them at positions indicated by definition levels.
public interface ValueDecoder {

    /// Initialize the decoder with the number of non-null values to read.
    /// Some decoders (e.g., delta encodings) need to read header information
    /// before decoding individual values.
    ///
    /// @param numNonNullValues the number of actual values in the encoded data
    default void initialize(int numNonNullValues) {
        // No-op by default - most decoders don't need initialization
    }

    /// Read long values directly into a primitive array.
    ///
    /// @param output the output array to populate
    /// @param definitionLevels definition levels indicating which positions have values (null for required columns)
    /// @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
    default void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) {
        throw new UnsupportedOperationException("readLongs not supported by this decoder");
    }

    /// Read double values directly into a primitive array.
    ///
    /// @param output the output array to populate
    /// @param definitionLevels definition levels indicating which positions have values (null for required columns)
    /// @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
    default void readDoubles(double[] output, int[] definitionLevels, int maxDefLevel) {
        throw new UnsupportedOperationException("readDoubles not supported by this decoder");
    }

    /// Read int values directly into a primitive array.
    ///
    /// @param output the output array to populate
    /// @param definitionLevels definition levels indicating which positions have values (null for required columns)
    /// @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
    default void readInts(int[] output, int[] definitionLevels, int maxDefLevel) {
        throw new UnsupportedOperationException("readInts not supported by this decoder");
    }

    /// Read float values directly into a primitive array.
    ///
    /// @param output the output array to populate
    /// @param definitionLevels definition levels indicating which positions have values (null for required columns)
    /// @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
    default void readFloats(float[] output, int[] definitionLevels, int maxDefLevel) {
        throw new UnsupportedOperationException("readFloats not supported by this decoder");
    }

    /// Read boolean values directly into a primitive array.
    ///
    /// @param output the output array to populate
    /// @param definitionLevels definition levels indicating which positions have values (null for required columns)
    /// @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
    default void readBooleans(boolean[] output, int[] definitionLevels, int maxDefLevel) {
        throw new UnsupportedOperationException("readBooleans not supported by this decoder");
    }

    /// Read byte array values directly into a byte[][] array.
    ///
    /// @param output the output array to populate
    /// @param definitionLevels definition levels indicating which positions have values (null for required columns)
    /// @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
    default void readByteArrays(byte[][] output, int[] definitionLevels, int maxDefLevel) {
        throw new UnsupportedOperationException("readByteArrays not supported by this decoder");
    }

    // -----------------------------------------------------------------------
    // Unified direct-into-batch overloads
    //
    // Process `count` slots and place decoded values at dest[destOffset+i].
    //
    //   defLevels == null   →  all-present (required column or page):  decode
    //                          `count` values densely; return value == count.
    //   defLevels != null   →  nullable page:  iterate `count` def-level slots
    //                          starting at defLevels[defLevelOffset]; read a
    //                          value from the byte stream only when the slot
    //                          equals maxDefLevel; leave null positions in dest
    //                          untouched.  Return value == non-null count.
    //
    // The decoder manages its own byte-stream position internally; the caller
    // must advance cursor.srcPos / cursor.bssCurrentIndex by the returned
    // non-null count after each call.
    //
    // Default: UnsupportedOperationException — callers fall back to the
    // existing decodePage + arraycopy path.
    // -----------------------------------------------------------------------

    /// Decode up to `count` DOUBLE values into dest[destOffset+i].
    ///
    /// @param dest           batch values array
    /// @param destOffset     first index to write into {@code dest}
    /// @param count          def-level slots to process (== values when all-present)
    /// @param defLevels      definition levels for the page, or {@code null} when all-present
    /// @param defLevelOffset index into {@code defLevels} of the first slot (ignored when null)
    /// @param maxDefLevel    the level that indicates a present value
    /// @return number of non-null values decoded
    default int readDoubles(double[] dest, int destOffset, int count,
                            int[] defLevels, int defLevelOffset, int maxDefLevel) {
        throw new UnsupportedOperationException("direct readDoubles not supported by this decoder");
    }

    /// Decode up to `count` INT64 values into dest[destOffset+i].
    /// {@code defLevels == null} means all-present; returns {@code count}.
    default int readLongs(long[] dest, int destOffset, int count,
                          int[] defLevels, int defLevelOffset, int maxDefLevel) {
        throw new UnsupportedOperationException("direct readLongs not supported by this decoder");
    }

    /// Decode up to `count` INT32 values into dest[destOffset+i].
    /// {@code defLevels == null} means all-present; returns {@code count}.
    default int readInts(int[] dest, int destOffset, int count,
                         int[] defLevels, int defLevelOffset, int maxDefLevel) {
        throw new UnsupportedOperationException("direct readInts not supported by this decoder");
    }

    /// Decode up to `count` FLOAT values into dest[destOffset+i].
    /// {@code defLevels == null} means all-present; returns {@code count}.
    default int readFloats(float[] dest, int destOffset, int count,
                           int[] defLevels, int defLevelOffset, int maxDefLevel) {
        throw new UnsupportedOperationException("direct readFloats not supported by this decoder");
    }
}
