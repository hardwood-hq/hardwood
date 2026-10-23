/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/// The body of a range `GET`, as [RangeBodyHandler] received it.
///
/// @param bytes    the bytes received, ready to read; at most the capacity of the buffer they
///                 were received into
/// @param overflow whether the body was longer than that buffer; the response was then cancelled
///                 once the buffer was full
public record RangeBody(ByteBuffer bytes, boolean overflow) {

    /// The bytes received, decoded as UTF-8, such as an error document.
    public String text() {
        return StandardCharsets.UTF_8.decode(bytes.duplicate()).toString();
    }
}
