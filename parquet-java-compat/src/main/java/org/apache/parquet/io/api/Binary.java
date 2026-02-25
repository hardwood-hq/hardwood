/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.io.api;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Parquet Binary wrapper for byte arrays.
 * <p>
 * This class wraps byte array data and provides utility methods for
 * conversion to strings, byte buffers, etc. It is compatible with
 * parquet-java's Binary class.
 * </p>
 */
public class Binary implements Comparable<Binary> {

    private final byte[] bytes;

    private Binary(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Create a Binary from a byte array without copying.
     * The array should not be modified after this call.
     *
     * @param value the byte array
     * @return the Binary
     */
    public static Binary fromConstantByteArray(byte[] value) {
        return new Binary(value);
    }

    /**
     * Create a Binary by copying the byte array.
     *
     * @param value the byte array to copy
     * @return the Binary
     */
    public static Binary fromReusedByteArray(byte[] value) {
        return new Binary(Arrays.copyOf(value, value.length));
    }

    /**
     * Create a Binary by copying a portion of a byte array.
     *
     * @param value the byte array
     * @param offset the starting offset
     * @param length the number of bytes to copy
     * @return the Binary
     */
    public static Binary fromReusedByteArray(byte[] value, int offset, int length) {
        byte[] copy = new byte[length];
        System.arraycopy(value, offset, copy, 0, length);
        return new Binary(copy);
    }

    /**
     * Create a Binary from a string (UTF-8 encoded).
     *
     * @param value the string
     * @return the Binary
     */
    public static Binary fromString(String value) {
        return new Binary(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Create a Binary from a ByteBuffer.
     *
     * @param buffer the ByteBuffer
     * @return the Binary
     */
    public static Binary fromByteBuffer(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new Binary(bytes);
    }

    /**
     * Get a copy of the bytes.
     *
     * @return a copy of the byte array
     */
    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * Get the underlying bytes without copying.
     * The returned array should not be modified.
     *
     * @return the byte array
     */
    public byte[] getBytesUnsafe() {
        return bytes;
    }

    /**
     * Get the length in bytes.
     *
     * @return the length
     */
    public int length() {
        return bytes.length;
    }

    /**
     * Convert to a string using UTF-8 encoding.
     *
     * @return the string
     */
    public String toStringUsingUTF8() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Convert to a ByteBuffer.
     *
     * @return a ByteBuffer wrapping the bytes
     */
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Write the bytes to an output stream.
     *
     * @param out the output stream
     * @throws IOException if writing fails
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(bytes);
    }

    @Override
    public int compareTo(Binary other) {
        return Arrays.compare(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Binary))
            return false;
        return Arrays.equals(bytes, ((Binary) o).bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "Binary{length=" + bytes.length + "}";
    }
}
