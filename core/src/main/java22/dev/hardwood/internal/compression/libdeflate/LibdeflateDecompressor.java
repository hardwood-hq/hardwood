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
import java.nio.ByteBuffer;

import dev.hardwood.internal.compression.Decompressor;

/// High-performance GZIP decompressor using libdeflate via FFM API.
///
/// libdeflate decompressor instances are NOT thread-safe, so this implementation
/// uses a pool to manage decompressor instances across threads.
///
/// Performance: libdeflate is typically 2-4x faster than zlib for decompression.
public final class LibdeflateDecompressor implements Decompressor {

    private static final ThreadLocal<MemorySegment> NATIVE_INPUT = new ThreadLocal<>();
    private static final ThreadLocal<MemorySegment> NATIVE_OUTPUT = new ThreadLocal<>();
    private static final ThreadLocal<MemorySegment> IN_SIZE_PTR = new ThreadLocal<>();
    private static final ThreadLocal<MemorySegment> OUT_SIZE_PTR = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    private final LibdeflatePool pool;

    public LibdeflateDecompressor(LibdeflatePool pool) {
        this.pool = pool;
    }

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
        LibdeflatePool.DecompressorHandle decompressor = pool.acquire();
        try {
            LibdeflateBindings bindings = LibdeflateBindings.get();

            int compressedSize = compressed.remaining();

            // The FFM downcall requires a native MemorySegment for its pointer
            // arguments. A direct buffer already maps to native memory, but a
            // heap-backed buffer (e.g. ByteBuffer.allocate) yields a heap
            // segment that the downcall rejects, so copy it into a reusable
            // native scratch segment first.
            MemorySegment input = MemorySegment.ofBuffer(compressed);
            if (!compressed.isDirect()) {
                MemorySegment scratch = borrowNative(NATIVE_INPUT, compressedSize);
                MemorySegment.copy(input, 0, scratch, 0, compressedSize);
                input = scratch;
            }
            MemorySegment output = borrowNative(NATIVE_OUTPUT, uncompressedSize);
            MemorySegment actualInSizePtr = borrowSizePtr(IN_SIZE_PTR);
            MemorySegment actualOutSizePtr = borrowSizePtr(OUT_SIZE_PTR);

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

            byte[] result = borrowOutputBuffer(uncompressedSize);
            MemorySegment.copy(output, ValueLayout.JAVA_BYTE, 0, result, 0, uncompressedSize);
            return result;
        }
        finally {
            pool.release(decompressor);
        }
    }

    private static MemorySegment borrowNative(ThreadLocal<MemorySegment> tl, int minSize) {
        MemorySegment seg = tl.get();
        if (seg == null || seg.byteSize() < minSize) {
            seg = Arena.ofAuto().allocate(minSize);
            tl.set(seg);
        }
        return seg;
    }

    private static byte[] borrowOutputBuffer(int minSize) {
        byte[] buf = OUTPUT_BUFFER.get();
        if (buf == null || buf.length < minSize) {
            buf = new byte[minSize];
            OUTPUT_BUFFER.set(buf);
        }
        return buf;
    }

    private static MemorySegment borrowSizePtr(ThreadLocal<MemorySegment> tl) {
        MemorySegment seg = tl.get();
        if (seg == null) {
            seg = Arena.ofAuto().allocate(ValueLayout.JAVA_LONG);
            tl.set(seg);
        }
        return seg;
    }

    @Override
    public String getName() {
        return "libdeflate (FFM)";
    }
}
