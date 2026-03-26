/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

/// Utility for ensuring a [ByteBuffer] is direct, as required by JNI-based
/// compression libraries (Snappy, ZSTD). If the source buffer is already
/// direct, it is returned as-is. Otherwise, its contents are copied into
/// a freshly allocated direct buffer.
final class DirectBuffers {

    private DirectBuffers() {
    }

    static ByteBuffer ensureDirect(ByteBuffer src) {
        if (src.isDirect()) {
            return src;
        }
        ByteBuffer direct = ByteBuffer.allocateDirect(src.remaining());
        direct.put(src);
        direct.flip();
        return direct;
    }
}
