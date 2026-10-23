/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// What a parse over the bytes a [ByteSource] holds came to: its value, or the range it needs
/// that the source does not hold. The caller fetches the range and parses again; a parse never
/// reads the file itself.
///
/// @param value the parse's value, which may be `null`; meaningless while a range is needed
/// @param needOffset the first byte of the range needed
/// @param needLength the length of the range needed, `-1` once the parse has its value
public record Parsed<T>(T value, long needOffset, int needLength) {

    /// A parse that has its value.
    public static <T> Parsed<T> of(T value) {
        return new Parsed<>(value, 0, -1);
    }

    /// A parse that needs the `length` bytes at `offset`.
    public static <T> Parsed<T> need(long offset, int length) {
        return new Parsed<>(null, offset, length);
    }

    /// Whether the parse needs a range the source does not hold.
    public boolean needsBytes() {
        return needLength >= 0;
    }

    /// This parse's need, as a parse of another value.
    public <U> Parsed<U> asNeed() {
        return need(needOffset, needLength);
    }

    /// The value of a parse over a source that holds every range, such as a file read in place.
    ///
    /// @throws IllegalStateException if the parse needs bytes
    public T get() {
        if (needsBytes()) {
            throw new IllegalStateException("Parse needs bytes [" + needOffset + ", "
                    + (needOffset + needLength) + ") its source did not hold");
        }
        return value;
    }
}
