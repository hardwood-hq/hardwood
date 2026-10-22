/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// The one bound every ULEB128 varint read into a `long` is held to, whichever buffer it is read
/// from.
public final class Uleb128 {

    private Uleb128() {
    }

    /// Whether byte `b`, read at bit offset `shift` of a varint, carries bits past a `long`. Its
    /// seven payload bits would fold onto the low ones or be lost: past bit 63 all of them, and at
    /// bit 63, the tenth byte, all but its lowest. A varint with such a byte is refused, since
    /// what it reads as is not the value it encodes.
    public static boolean overflowsLong(int shift, int b) {
        return shift > Long.SIZE - 1 || (shift == Long.SIZE - 1 && b > 1);
    }
}
