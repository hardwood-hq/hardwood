/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

/// The kinds of buffer a compressed page can arrive in. Each wraps the page's bytes from position
/// to limit; the ones with room around the page fill it with bytes no codec accepts, so a
/// decompressor that reads outside position and limit fails or decodes wrongly.
public enum PageBuffer {
    HEAP(ByteBuffer::wrap),
    READ_ONLY_HEAP(bytes -> ByteBuffer.wrap(bytes).asReadOnlyBuffer()),
    DIRECT(bytes -> ByteBuffer.allocateDirect(bytes.length).put(bytes).flip()),
    /// Position past the start of its array, array offset 0
    POSITIONED_HEAP(bytes -> ByteBuffer.wrap(surrounded(bytes, 7), 7, bytes.length)),
    /// A slice, as pages cut from a chunk are: non-zero array offset, then positioned within it
    SLICED_HEAP(bytes -> ByteBuffer.wrap(surrounded(bytes, 7)).position(3).slice()
            .position(4).limit(4 + bytes.length)),
    POSITIONED_DIRECT(bytes -> {
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length + 20);
        direct.put(surrounded(bytes, 9));
        return direct.position(9).limit(9 + bytes.length);
    });

    private static final byte FILL = 0x55;

    private final Function<byte[], ByteBuffer> wrap;

    PageBuffer(Function<byte[], ByteBuffer> wrap) {
        this.wrap = wrap;
    }

    public ByteBuffer wrap(byte[] page) {
        return wrap.apply(page);
    }

    private static byte[] surrounded(byte[] page, int before) {
        byte[] backing = new byte[page.length + 20];
        Arrays.fill(backing, FILL);
        System.arraycopy(page, 0, backing, before, page.length);
        return backing;
    }
}
