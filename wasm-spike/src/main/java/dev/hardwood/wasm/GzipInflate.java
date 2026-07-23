/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import java.io.IOException;

import com.jcraft.jzlib.GZIPException;
import com.jcraft.jzlib.Inflater;
import com.jcraft.jzlib.JZlib;

/// GZIP decode for Parquet's GZIP codec using **jzlib** — a pure-Java zlib with no native
/// code (unlike `java.util.zip.Inflater`, whose native `Inflater.init` is unavailable under
/// GraalVM Web Image) and no `sun.misc.Unsafe`.
///
/// Mirrors the built-in decoder: parse each gzip member's header, inflate its raw-deflate
/// body, skip the 8-byte trailer, and repeat for concatenated members.
final class GzipInflate {

    private static final int GZIP_MAGIC = 0x8b1f;
    private static final int FHCRC = 2;
    private static final int FEXTRA = 4;
    private static final int FNAME = 8;
    private static final int FCOMMENT = 16;

    private GzipInflate() {
    }

    /// Decode `input` into `output`. Returns the number of bytes written.
    static int decompress(byte[] input, byte[] output) throws IOException {
        int ip = 0;
        int op = 0;
        while (op < output.length && ip < input.length) {
            int deflateStart = ip + gzipHeaderLength(input, ip);
            Inflater inflater;
            try {
                inflater = new Inflater(true); // nowrap = raw deflate
            }
            catch (GZIPException e) {
                throw new IOException("jzlib inflater init failed", e);
            }
            inflater.next_in = input;
            inflater.next_in_index = deflateStart;
            inflater.avail_in = input.length - deflateStart;
            inflater.next_out = output;
            inflater.next_out_index = op;
            inflater.avail_out = output.length - op;

            int status = inflater.inflate(JZlib.Z_FINISH);
            if (status != JZlib.Z_STREAM_END) {
                String msg = inflater.msg;
                inflater.end();
                throw new IOException("jzlib inflate failed (status " + status
                        + (msg != null ? ": " + msg : "") + ")");
            }
            int consumedEnd = inflater.next_in_index;
            op = inflater.next_out_index;
            inflater.end();
            ip = consumedEnd + 8; // past the gzip trailer (CRC32 + ISIZE) to the next member
        }
        return op;
    }

    private static int gzipHeaderLength(byte[] b, int start) throws IOException {
        if (b.length - start < 10) {
            throw new IOException("GZIP data too short for header");
        }
        int magic = (b[start] & 0xff) | ((b[start + 1] & 0xff) << 8);
        if (magic != GZIP_MAGIC) {
            throw new IOException("Not in GZIP format");
        }
        if ((b[start + 2] & 0xff) != 8) {
            throw new IOException("Unsupported GZIP compression method: " + (b[start + 2] & 0xff));
        }
        int flags = b[start + 3] & 0xff;
        int offset = 10;
        if ((flags & FEXTRA) != 0) {
            int xlen = (b[start + offset] & 0xff) | ((b[start + offset + 1] & 0xff) << 8);
            offset += 2 + xlen;
        }
        if ((flags & FNAME) != 0) {
            while (b[start + offset] != 0) {
                offset++;
            }
            offset++;
        }
        if ((flags & FCOMMENT) != 0) {
            while (b[start + offset] != 0) {
                offset++;
            }
            offset++;
        }
        if ((flags & FHCRC) != 0) {
            offset += 2;
        }
        return offset;
    }
}
