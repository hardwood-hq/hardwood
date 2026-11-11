/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

/// A range of the file a read requests as one: `length` bytes at `start`, under `reason`, which
/// names what the bytes are for in the request's log line and JFR event.
public record FileRange(long start, int length, String reason) {

    /// The offset just past the range.
    public long end() {
        return start + length;
    }
}
