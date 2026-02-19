/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.libdeflate;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bindings for libdeflate native functions.
 *
 * @see <a href="https://github.com/ebiggers/libdeflate/blob/master/libdeflate.h">libdeflate.h</a>
 */
final class LibdeflateBindings {

    static final int LIBDEFLATE_SUCCESS = 0;
    static final int LIBDEFLATE_BAD_DATA = 1;
    static final int LIBDEFLATE_SHORT_OUTPUT = 2;
    static final int LIBDEFLATE_INSUFFICIENT_SPACE = 3;

    private static final Linker LINKER = Linker.nativeLinker();

    private static class Holder {
        static final LibdeflateBindings INSTANCE = new LibdeflateBindings();
    }

    final MethodHandle allocDecompressor;
    final MethodHandle freeDecompressor;
    final MethodHandle gzipDecompressEx;
    final MethodHandle deflateDecompressEx;
    final MethodHandle zlibDecompressEx;

    private LibdeflateBindings() {
        SymbolLookup lookup = LibdeflateLoader.getSymbolLookup();

        // struct libdeflate_decompressor *libdeflate_alloc_decompressor(void)
        this.allocDecompressor = LINKER.downcallHandle(
                lookup.find("libdeflate_alloc_decompressor").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS));

        // void libdeflate_free_decompressor(struct libdeflate_decompressor *d)
        this.freeDecompressor = LINKER.downcallHandle(
                lookup.find("libdeflate_free_decompressor").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // enum libdeflate_result libdeflate_gzip_decompress_ex(
        // struct libdeflate_decompressor *d,
        // const void *in, size_t in_nbytes,
        // void *out, size_t out_nbytes_avail,
        // size_t *actual_in_nbytes_ret,
        // size_t *actual_out_nbytes_ret)
        FunctionDescriptor decompressExDescriptor = FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS);

        this.gzipDecompressEx = LINKER.downcallHandle(
                lookup.find("libdeflate_gzip_decompress_ex").orElseThrow(),
                decompressExDescriptor);

        this.deflateDecompressEx = LINKER.downcallHandle(
                lookup.find("libdeflate_deflate_decompress_ex").orElseThrow(),
                decompressExDescriptor);

        this.zlibDecompressEx = LINKER.downcallHandle(
                lookup.find("libdeflate_zlib_decompress_ex").orElseThrow(),
                decompressExDescriptor);
    }

    static LibdeflateBindings get() {
        return Holder.INSTANCE;
    }

    static String errorMessage(int result) {
        return switch (result) {
            case LIBDEFLATE_SUCCESS -> "Success";
            case LIBDEFLATE_BAD_DATA -> "Bad data (invalid or corrupt compressed data)";
            case LIBDEFLATE_SHORT_OUTPUT -> "Short output (decompressed size less than expected)";
            case LIBDEFLATE_INSUFFICIENT_SPACE -> "Insufficient space (output buffer too small)";
            default -> "Unknown error code: " + result;
        };
    }
}
