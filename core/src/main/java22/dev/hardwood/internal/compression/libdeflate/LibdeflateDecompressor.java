/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.libdeflate;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.MappedByteBuffer;

import dev.hardwood.internal.compression.Decompressor;

/**
 * High-performance GZIP decompressor using libdeflate via FFM API.
 * <p>
 * libdeflate decompressor instances are NOT thread-safe, so this implementation
 * uses a pool to manage decompressor instances across threads.
 * <p>
 * Performance: libdeflate is typically 2-4x faster than zlib for decompression.
 */
public final class LibdeflateDecompressor implements Decompressor {

    private final LibdeflatePool pool;

    public LibdeflateDecompressor(LibdeflatePool pool) {
        this.pool = pool;
    }

    @Override
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException {
        LibdeflatePool.DecompressorHandle decompressor = pool.acquire();
        try {
            LibdeflateBindings bindings = LibdeflateBindings.get();

            try (Arena arena = Arena.ofConfined()) {
                int compressedSize = compressed.remaining();

                MemorySegment input = MemorySegment.ofBuffer(compressed);

                MemorySegment output = arena.allocate(uncompressedSize);
                MemorySegment actualInSizePtr = arena.allocate(ValueLayout.JAVA_LONG);
                MemorySegment actualOutSizePtr = arena.allocate(ValueLayout.JAVA_LONG);

                long inputOffset = 0;
                long outputOffset = 0;

                // Handle concatenated GZIP members
                while (outputOffset < uncompressedSize && inputOffset < compressedSize) {
                    int result;
                    try {
                        result = (int) bindings.gzipDecompressEx.invokeExact(
                                decompressor.handle(),
                                input.asSlice(inputOffset),
                                compressedSize - inputOffset,
                                output.asSlice(outputOffset),
                                uncompressedSize - outputOffset,
                                actualInSizePtr,
                                actualOutSizePtr);
                    }
                    catch (Throwable t) {
                        throw new IOException("libdeflate invocation failed", t);
                    }

                    if (result != LibdeflateBindings.LIBDEFLATE_SUCCESS) {
                        throw new IOException("libdeflate decompression failed: " +
                                LibdeflateBindings.errorMessage(result));
                    }

                    long consumedInput = actualInSizePtr.get(ValueLayout.JAVA_LONG, 0);
                    long producedOutput = actualOutSizePtr.get(ValueLayout.JAVA_LONG, 0);

                    inputOffset += consumedInput;
                    outputOffset += producedOutput;
                }

                if (outputOffset != uncompressedSize) {
                    throw new IOException(String.format(
                            "Decompressed size mismatch: expected %d, got %d",
                            uncompressedSize, outputOffset));
                }

                return output.toArray(ValueLayout.JAVA_BYTE);
            }
        }
        finally {
            pool.release(decompressor);
        }
    }

    @Override
    public String getName() {
        return "libdeflate (FFM)";
    }
}
