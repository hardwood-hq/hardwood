/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;

/// Compresses a page body on the write path, the inverse of [Decompressor].
///
/// A page's body — level streams and values — is compressed as one unit before its
/// header is framed, so the header can record both the uncompressed and compressed sizes.
public interface Compressor {

    /// Compress `length` bytes of `data` starting at `offset`, returning a newly allocated
    /// array sized exactly to the compressed output.
    ///
    /// @param data the buffer holding the bytes to compress
    /// @param offset the first byte to compress
    /// @param length the number of bytes to compress
    /// @return the compressed bytes, in an array whose length is the compressed size
    /// @throws IOException if compression fails
    byte[] compress(byte[] data, int offset, int length) throws IOException;

    /// The name of this compressor.
    String getName();
}
