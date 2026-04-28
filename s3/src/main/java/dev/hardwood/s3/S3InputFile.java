/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.s3.internal.S3Api;

/// [InputFile] backed by an object in Amazon S3 (or an S3-compatible service).
///
/// Each [#readRange] call issues a signed HTTP `GET` request with a
/// byte-range header, so only the requested bytes are transferred.
///
/// [#open()] uses a suffix-range GET instead of a HEAD request. This
/// discovers the file length from the `Content-Range` response header
/// and pre-fetches the Parquet footer (which sits at the end of the file) in
/// the same round-trip — eliminating a separate HEAD request per file.
///
/// Thread-safe once [#open()] has been called.
public class S3InputFile implements InputFile {

    private static final System.Logger LOG = System.getLogger(S3InputFile.class.getName());

    /// Number of bytes to fetch from the tail of the file during [#open()].
    /// 64 KB is large enough to cover the Parquet footer in virtually all files
    /// (footer + footer length + magic is typically a few KB).
    static final int TAIL_SIZE = 64 * 1024;

    private final S3Api api;
    private final String bucket;
    private final String key;
    private long fileLength = -1;
    private ByteBuffer tailCache;
    private long tailCacheOffset;
    private final AtomicLong networkRequestCount = new AtomicLong();
    private final AtomicLong networkBytesFetched = new AtomicLong();

    S3InputFile(S3Source source, String bucket, String key) {
        this.api = source.api();
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public void open() throws IOException {
        if (fileLength >= 0) {
            return;
        }
        String suffixRange = "bytes=-" + TAIL_SIZE;
        HttpResponse<byte[]> response = api.getBytes(bucket, key, suffixRange);
        int status = response.statusCode();
        if (status != 206 && status != 200) {
            throw new IOException("Failed to open " + name()
                    + ": HTTP " + status + " " + new String(response.body()));
        }
        fileLength = parseFileLength(response);
        byte[] tail = response.body();
        long requestNo = networkRequestCount.incrementAndGet();
        long totalBytes = networkBytesFetched.addAndGet(tail.length);
        // Use a direct buffer so slices are usable from FFM-based decompressors
        // (e.g. libdeflate), which require native MemorySegments.
        tailCache = ByteBuffer.allocateDirect(tail.length);
        tailCache.put(tail);
        tailCache.flip();
        tailCacheOffset = fileLength - tail.length;
        logFetch("open-tail", tailCacheOffset, tail.length, requestNo, totalBytes);
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        // Serve from the tail cache if the requested range falls within it
        if (tailCache != null && offset >= tailCacheOffset
                && offset + length <= tailCacheOffset + tailCache.capacity()) {
            int relOffset = Math.toIntExact(offset - tailCacheOffset);
            LOG.log(System.Logger.Level.DEBUG,
                    "[{0}] readRange offset={1} length={2} reason={3} (tail cache hit)",
                    name(), offset, length, FetchReason.current());
            return tailCache.slice(relOffset, length);
        }

        long requestNo = networkRequestCount.incrementAndGet();
        long totalBytes = networkBytesFetched.addAndGet(length);
        logFetch(FetchReason.current(), offset, length, requestNo, totalBytes);
        String range = "bytes=" + offset + "-" + (offset + length - 1);
        HttpResponse<InputStream> response = api.getStream(bucket, key, range);
        int status = response.statusCode();
        if (status != 206 && status != 200) {
            try (InputStream body = response.body()) {
                throw new IOException("Failed to read range [" + offset + ", " + (offset + length)
                        + ") from " + name() + ": HTTP " + status + " " + new String(body.readAllBytes()));
            }
        }
        try (InputStream stream = response.body()) {
            ByteBuffer buf = ByteBuffer.allocateDirect(length);
            byte[] tmp = new byte[Math.min(8192, length)];
            while (buf.hasRemaining()) {
                int toRead = Math.min(tmp.length, buf.remaining());
                int read = stream.read(tmp, 0, toRead);
                if (read < 0) {
                    break;
                }
                buf.put(tmp, 0, read);
            }
            if (buf.position() != length) {
                throw new IOException("Short read from " + name() + ": expected " + length
                        + " bytes but received " + buf.position());
            }
            buf.flip();
            return buf;
        }
    }

    @Override
    public long length() {
        if (fileLength < 0) {
            throw new IllegalStateException("File not opened: " + name());
        }
        return fileLength;
    }

    @Override
    public String name() {
        return "s3://" + bucket + "/" + key;
    }

    @Override
    public void close() {
        // S3Source owns the HttpClient — nothing to close here
    }

    /// Number of HTTP requests issued against the object since [#open()].
    /// Counts the suffix-range tail fetch from `open` plus every
    /// network-fetch [#readRange] call. Tail-cache hits do not count.
    public long networkRequestCount() {
        return networkRequestCount.get();
    }

    /// Number of bytes fetched from the network since [#open()]. The tail
    /// fetch from `open` contributes its actual response size; each
    /// network-fetch [#readRange] contributes the requested length.
    /// Tail-cache hits do not count.
    public long networkBytesFetched() {
        return networkBytesFetched.get();
    }

    /// Emits a single FINE log line per network fetch carrying the byte
    /// range, the [FetchReason] tag attributed to the caller, and the
    /// running per-file totals. Designed to be greppable: one line per
    /// fetch, no nested structure.
    private void logFetch(String reason, long offset, int length, long requestNo, long totalBytes) {
        LOG.log(System.Logger.Level.DEBUG,
                "[{0}] fetch #{1} reason={2} offset={3} length={4} range=[{3},{5}) totalBytes={6}",
                name(), requestNo, reason, offset, length, offset + length, totalBytes);
    }

    /// Extracts the total file length from the HTTP response.
    ///
    /// For suffix-range requests, S3 returns a `Content-Range` header like
    /// `bytes 1000-1999/2000` where the number after `/` is the total
    /// object size. If the header is absent (e.g. file smaller than TAIL_SIZE),
    /// falls back to `Content-Length`.
    private static long parseFileLength(HttpResponse<?> response) throws IOException {
        String contentRange = response.headers().firstValue("Content-Range").orElse(null);
        if (contentRange != null) {
            int slashIdx = contentRange.lastIndexOf('/');
            if (slashIdx >= 0) {
                try {
                    return Long.parseLong(contentRange.substring(slashIdx + 1));
                }
                catch (NumberFormatException e) {
                    throw new IOException("Failed to parse Content-Range header: " + contentRange, e);
                }
            }
        }
        return response.headers().firstValueAsLong("Content-Length")
                .orElseThrow(() -> new IOException("Response missing both Content-Range and Content-Length headers"));
    }
}
