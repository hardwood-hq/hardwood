/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.encoding;

import java.io.IOException;
import java.io.InputStream;

import dev.morling.hardwood.metadata.PhysicalType;

/**
 * Decoder for DELTA_BYTE_ARRAY encoding.
 * <p>
 * This encoding is also known as incremental encoding or front compression.
 * For each element in a sequence of byte arrays, it stores the prefix length
 * (how many bytes to copy from the previous value) plus the suffix (remaining bytes).
 * <p>
 * Format:
 * <pre>
 * &lt;Delta Encoded Prefix Lengths&gt; &lt;Delta Length Byte Array Encoded Suffixes&gt;
 * </pre>
 * <p>
 * Example: For ["apple", "application", "apply"]
 * - Prefix lengths: 0, 4, 4 (each shares prefix with previous)
 * - Suffixes: "apple", "ication", "y"
 * <p>
 * Reconstruction:
 * - value[0] = suffix[0] = "apple"
 * - value[1] = value[0][0:4] + suffix[1] = "appl" + "ication" = "application"
 * - value[2] = value[1][0:4] + suffix[2] = "appl" + "y" = "apply"
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md">Parquet Encodings</a>
 */
public class DeltaByteArrayDecoder {

    private final InputStream input;

    // All prefix lengths read from the delta-encoded header
    private int[] prefixLengths;
    private int currentIndex;
    private int totalValues;

    // Suffix decoder (uses DELTA_LENGTH_BYTE_ARRAY)
    private DeltaLengthByteArrayDecoder suffixDecoder;
    private boolean initialized;

    // Previous value for prefix reconstruction
    private byte[] previousValue;

    public DeltaByteArrayDecoder(InputStream input) {
        this.input = input;
        this.currentIndex = 0;
        this.prefixLengths = null;
        this.initialized = false;
        this.previousValue = new byte[0];
    }

    /**
     * Initialize the decoder by reading all prefix lengths and preparing the suffix decoder.
     * Must be called before reading values, with the total number of values expected.
     */
    public void initialize(int numValues) throws IOException {
        this.totalValues = numValues;
        this.prefixLengths = new int[numValues];

        if (numValues == 0) {
            initialized = true;
            return;
        }

        // Read all prefix lengths using DELTA_BINARY_PACKED
        // Prefix lengths are always encoded as INT32 per the spec
        DeltaBinaryPackedDecoder prefixDecoder = new DeltaBinaryPackedDecoder(input, PhysicalType.INT32);
        for (int i = 0; i < numValues; i++) {
            Object value = prefixDecoder.readValue();
            prefixLengths[i] = ((Number) value).intValue();
        }

        // Create the suffix decoder (uses DELTA_LENGTH_BYTE_ARRAY)
        suffixDecoder = new DeltaLengthByteArrayDecoder(input);
        suffixDecoder.readLengths(numValues);

        initialized = true;
    }

    /**
     * Read a single byte array value.
     */
    public byte[] readValue() throws IOException {
        if (!initialized) {
            throw new IOException("Must call initialize() before reading values");
        }

        if (currentIndex >= totalValues) {
            throw new IOException("No more values to read");
        }

        int prefixLength = prefixLengths[currentIndex];
        byte[] suffix = suffixDecoder.readValue();
        currentIndex++;

        // Reconstruct the full value: prefix from previous + suffix
        byte[] value = new byte[prefixLength + suffix.length];
        if (prefixLength > 0) {
            System.arraycopy(previousValue, 0, value, 0, prefixLength);
        }
        if (suffix.length > 0) {
            System.arraycopy(suffix, 0, value, prefixLength, suffix.length);
        }

        previousValue = value;
        return value;
    }

    /**
     * Read multiple values into a buffer.
     */
    public void readValues(Object[] buffer, int offset, int count) throws IOException {
        // If not initialized yet, initialize now
        if (!initialized) {
            initialize(count);
        }

        for (int i = 0; i < count; i++) {
            buffer[offset + i] = readValue();
        }
    }
}
