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
    // Direct-into-batch overloads
    //
    // These decode exactly `count` non-null values from the raw byte slice
    // [srcPos, srcPos + count*elementBytes) and place them at dest[destOffset].
    // The caller guarantees:
    //   - definitionLevels is null  (all-present path only)
    //   - dest[destOffset .. destOffset+count) is within bounds
    //   - srcPos + count*elementBytes <= srcLimit
    //
    // Default: UnsupportedOperationException — callers must fall back to the
    // existing decodePage + arraycopy path when this is thrown.
    // -----------------------------------------------------------------------

    /// Decode `count` DOUBLE values from `src[srcPos..)` into `dest[destOffset..)`.
    ///
    /// @param dest the destination array (batch values array)
    /// @param destOffset first index to write into `dest`
    /// @param count number of values to decode
    /// @param src raw decompressed page bytes
    /// @param srcPos byte offset in `src` of the first value
    /// @param srcLimit exclusive byte bound of the value region in `src`
    default void readDoubles(double[] dest, int destOffset, int count,
                             byte[] src, int srcPos, int srcLimit) {
        throw new UnsupportedOperationException("direct readDoubles not supported by this decoder");
    }

    /// Decode `count` INT64 values from `src[srcPos..)` into `dest[destOffset..)`.
    default void readLongs(long[] dest, int destOffset, int count,
                           byte[] src, int srcPos, int srcLimit) {
        throw new UnsupportedOperationException("direct readLongs not supported by this decoder");
    }

    /// Decode `count` INT32 values from `src[srcPos..)` into `dest[destOffset..)`.
    default void readInts(int[] dest, int destOffset, int count,
                          byte[] src, int srcPos, int srcLimit) {
        throw new UnsupportedOperationException("direct readInts not supported by this decoder");
    }

    /// Decode `count` FLOAT values from `src[srcPos..)` into `dest[destOffset..)`.
    default void readFloats(float[] dest, int destOffset, int count,
                            byte[] src, int srcPos, int srcLimit) {
        throw new UnsupportedOperationException("direct readFloats not supported by this decoder");
    }
}
