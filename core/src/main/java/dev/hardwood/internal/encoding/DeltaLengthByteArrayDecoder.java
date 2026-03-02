/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.util.Arrays;

/**
 * Decoder for DELTA_LENGTH_BYTE_ARRAY encoding.
 * <p>
 * This encoding stores byte arrays by first delta-encoding all lengths using
 * DELTA_BINARY_PACKED, then concatenating all byte data together.
 * <p>
 * Format:
 * <pre>
 * &lt;Delta Encoded Lengths&gt; &lt;Concatenated Byte Array Data&gt;
 * </pre>
 * <p>
 * Example: For ["Hello", "World", "Foobar"]
 * - Lengths: DeltaEncoding(5, 5, 6)
 * - Data: "HelloWorldFoobar"
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md">Parquet Encodings</a>
 */
public class DeltaLengthByteArrayDecoder implements ValueDecoder {

    private final byte[] data;
    private int pos;

    // All lengths read from the delta-encoded header
    private int[] lengths;
    private int currentIndex;
    private int totalValues;

    public DeltaLengthByteArrayDecoder(byte[] data, int offset) {
        this.data = data;
        this.pos = offset;
        this.currentIndex = 0;
        this.lengths = null;
    }

    @Override
    public void initialize(int numNonNullValues) throws IOException {
        this.totalValues = numNonNullValues;
        this.lengths = new int[numNonNullValues];

        if (numNonNullValues == 0) {
            return;
        }

        // Read all lengths using DELTA_BINARY_PACKED
        // Lengths are always encoded as INT32 per the spec
        DeltaBinaryPackedDecoder lengthDecoder = new DeltaBinaryPackedDecoder(data, pos);
        for (int i = 0; i < numNonNullValues; i++) {
            lengths[i] = lengthDecoder.readInt();
        }
        pos = lengthDecoder.getPos();
    }

    /**
     * Read a single byte array value.
     */
    public byte[] readValue() throws IOException {
        if (lengths == null) {
            throw new IOException("Must call initialize() before reading values");
        }

        if (currentIndex >= totalValues) {
            throw new IOException("No more values to read");
        }

        int length = lengths[currentIndex++];

        if (length == 0) {
            return new byte[0];
        }

        if (pos + length > data.length) {
            throw new IOException("Unexpected EOF reading byte array: expected " + length
                    + ", got " + (data.length - pos));
        }
        byte[] result = Arrays.copyOfRange(data, pos, pos + length);
        pos += length;
        return result;
    }

    @Override
    public void readByteArrays(byte[][] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (lengths == null) {
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
