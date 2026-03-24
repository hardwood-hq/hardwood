/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.nio.ByteBuffer;

/// Decoder for DELTA_BYTE_ARRAY encoding.
///
/// This encoding is also known as incremental encoding or front compression.
/// For each element in a sequence of byte arrays, it stores the prefix length
/// (how many bytes to copy from the previous value) plus the suffix (remaining bytes).
///
/// Format:
/// ```text
/// <Delta Encoded Prefix Lengths> <Delta Length Byte Array Encoded Suffixes>
/// ```
///
/// Example: For ["apple", "application", "apply"]
/// - Prefix lengths: 0, 4, 4 (each shares prefix with previous)
/// - Suffixes: "apple", "ication", "y"
///
/// Reconstruction:
/// - `value[0] = suffix[0]` = "apple"
/// - `value[1] = value[0][0:4] + suffix[1]` = "appl" + "ication" = "application"
/// - `value[2] = value[1][0:4] + suffix[2]` = "appl" + "y" = "apply"
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md">Parquet Encodings</a>
public class DeltaByteArrayDecoder implements ValueDecoder {

    private final byte[] data;
    private final int offset;

    // All prefix lengths read from the delta-encoded header
    private int[] prefixLengths;
    private int currentIndex;
    private int totalValues;

    // Suffix decoder (uses DELTA_LENGTH_BYTE_ARRAY)
    private DeltaLengthByteArrayDecoder suffixDecoder;
    private boolean initialized;

    // Previous value for prefix reconstruction
    private byte[] previousValue;

    public DeltaByteArrayDecoder(byte[] data, int offset) {
        this.data = data;
        this.offset = offset;
        this.currentIndex = 0;
        this.prefixLengths = null;
        this.initialized = false;
        this.previousValue = new byte[0];
    }

    /// Initialize the decoder by reading all prefix lengths and preparing the suffix decoder.
    /// Must be called before reading values, with the total number of non-null values expected.
    @Override
    public void initialize(int numNonNullValues) throws IOException {
        this.totalValues = numNonNullValues;
        this.prefixLengths = new int[numNonNullValues];

        if (numNonNullValues == 0) {
            initialized = true;
            return;
        }

        // Read all prefix lengths using DELTA_BINARY_PACKED
        // Prefix lengths are always encoded as INT32 per the spec
        DeltaBinaryPackedDecoder prefixDecoder = new DeltaBinaryPackedDecoder(data, offset);
        for (int i = 0; i < numNonNullValues; i++) {
            prefixLengths[i] = prefixDecoder.readInt();
        }

        // Create the suffix decoder (uses DELTA_LENGTH_BYTE_ARRAY)
        // Continue reading from where the prefix decoder stopped
        suffixDecoder = new DeltaLengthByteArrayDecoder(data, prefixDecoder.getPos());
        suffixDecoder.initialize(numNonNullValues);

        initialized = true;
    }

    /// Read a single byte array value.
    public byte[] readValue() throws IOException {
        if (!initialized) {
            throw new IOException("Must call initialize() before reading values");
        }

        if (currentIndex >= totalValues) {
            throw new IOException("No more values to read");
        }

        int prefixLength = prefixLengths[currentIndex];
        ByteBuffer suffix = suffixDecoder.readValue();
        currentIndex++;

        // Reconstruct the full value: prefix from previous + suffix
        int suffixLength = suffix.remaining();
        byte[] value = new byte[prefixLength + suffixLength];
        if (prefixLength > 0) {
            System.arraycopy(previousValue, 0, value, 0, prefixLength);
        }
        if (suffixLength > 0) {
            suffix.get(value, prefixLength, suffixLength);
        }

        previousValue = value;
        return value;
    }

    @Override
    public void readByteArrays(byte[][] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (!initialized) {
            throw new IOException("Must call initialize() before reading values");
        }

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = readValue();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = readValue();
                }
            }
        }
    }
}
