/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.reader.ReadRanges;
import dev.hardwood.internal.reader.TailedInputFile;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.s3.internal.RangeBody;
import dev.hardwood.s3.internal.S3Api;

/// The network path behind [S3InputFile]: every non-empty [#readRange] call issues
/// an HTTP `GET` with a byte-range header against one S3 object.
///
/// [#open()] uses a suffix-range GET instead of a HEAD request. This
/// discovers the file length from the `Content-Range` response header
/// and pre-fetches the Parquet footer (which sits at the end of the file)
/// in the same round-trip — eliminating a separate HEAD request per file.
/// The tail's size is the one a read takes ([TailedInputFile#tailBytes()]), so a
/// file up to that size arrives whole. The fetched tail is retained and serves
/// any subsequent read that falls entirely within it, until a read of the whole
/// tail takes it: the reader holds the tail from then on and releases it once
/// it is done with the file, so the fetcher holds none for the file's lifetime.
///
/// This class is unaware of [RangeBacking]. Under
/// [RangeBacking#SPARSE_TEMPFILE] the mmap-backed range cache wraps an
/// instance of this class, so cache hits never reach here and the
/// counters keep reporting network traffic only.
///
/// Thread-safe once [#open()] has been called.
final class S3Fetcher implements InputFile {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(S3Fetcher.class.getName()));


    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int HTTP_PRECONDITION_FAILED = 412;

    private final S3Api api;
    private final String bucket;
    private final String key;
    private long fileLength = -1;
    /// The tail fetched by [#open()], until a read of all of it takes it.
    private volatile ByteBuffer tailCache;
    private long tailCacheOffset;
    /// The object's `ETag` from the response to the suffix-range `GET` in [#open()].
    private Optional<String> identity;
    /// The `If-Match` every range request carries: the identity when it is a strong entity tag,
    /// otherwise `null`. `If-Match` compares strongly, so a weak tag would never match.
    private String ifMatch;
    private final AtomicLong networkRequestCount = new AtomicLong();
    private final AtomicLong networkBytesFetched = new AtomicLong();

    S3Fetcher(S3Api api, String bucket, String key) {
        this.api = api;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public void open() throws IOException {
        if (fileLength >= 0) {
            return;
        }
        int suffixLength = TailedInputFile.tailBytes();
        HttpResponse<RangeBody> response = api.getSuffix(bucket, key, suffixLength);
        int status = response.statusCode();
        RangeBody body = response.body();
        if (status != 206 && status != 200) {
            throw new IOException("Failed to open " + name()
                    + ": HTTP " + status + " " + body.text() + unsignedHint(status));
        }
        if (body.overflow()) {
            // A body past the suffix is the whole object of an endpoint that ignored the Range header
            throw new IOException("Failed to open " + name() + ": HTTP " + status + " with a body longer than the "
                    + suffixLength + " bytes requested; the endpoint ignored the Range header");
        }
        fileLength = parseFileLength(response);
        identity = response.headers().firstValue("ETag");
        ifMatch = identity.filter(etag -> !etag.startsWith("W/")).orElse(null);
        // A direct buffer, so slices are usable from FFM-based decompressors
        // (e.g. libdeflate), which require native MemorySegments.
        // Sliced to the bytes received: a body without a Content-Length has a buffer of the suffix
        ByteBuffer tail = body.bytes().slice();
        long requestNo = networkRequestCount.incrementAndGet();
        long totalBytes = networkBytesFetched.addAndGet(tail.remaining());
        tailCache = tail;
        tailCacheOffset = fileLength - tail.remaining();
        logFetch("open-tail", tailCacheOffset, tail.remaining(), requestNo, totalBytes);
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        if (fileLength < 0) {
            throw new IllegalStateException("File not opened: " + name());
        }
        // Checked before the tail cache and before any request, so a bad range fails the same
        // way wherever it lands.
        ReadRanges.checkBounds(name(), offset, length, fileLength);
        // An empty range has no HTTP Range form: "bytes=o-(o-1)" is unsatisfiable
        if (length == 0) {
            return ByteBuffer.allocateDirect(0);
        }
        // Serve from the tail cache if the requested range falls within it
        ByteBuffer tail = tailCache;
        if (tail != null && offset >= tailCacheOffset
                && offset + length <= tailCacheOffset + tail.capacity()) {
            int relOffset = Math.toIntExact(offset - tailCacheOffset);
            LOG.get().log(System.Logger.Level.DEBUG,
                    "[{0}] readRange offset={1} length={2} reason={3} (tail cache hit)",
                    name(), offset, length, FetchReason.current());
            if (length == tail.capacity()) {
                // The reader takes the whole tail and holds it from now on
                tailCache = null;
            }
            return tail.slice(relOffset, length);
        }

        long requestNo = networkRequestCount.incrementAndGet();
        long totalBytes = networkBytesFetched.addAndGet(length);
        logFetch(FetchReason.current(), offset, length, requestNo, totalBytes);
        String range = "bytes=" + offset + "-" + (offset + length - 1);
        HttpResponse<RangeBody> response = api.getRange(bucket, key, range, length, ifMatch);
        int status = response.statusCode();
        RangeBody body = response.body();
        // A 412 to a request without If-Match is an error of the endpoint, reported as such below
        if (status == HTTP_PRECONDITION_FAILED && ifMatch != null) {
            throw new ParquetReadException("Failed to read range [" + offset + ", " + (offset + length)
                    + ") from " + name() + ": the file changed since it was opened (HTTP 412 for If-Match "
                    + ifMatch + ")");
        }
        if (status != 206) {
            // A 200 carries the whole object from byte 0: the endpoint ignored the Range header
            String detail = status == 200
                    ? ", expected 206; the endpoint ignored the Range header"
                    : " " + body.text() + unsignedHint(status);
            throw new IOException("Failed to read range [" + offset + ", " + (offset + length)
                    + ") from " + name() + ": HTTP " + status + detail);
        }
        ByteBuffer buf = body.bytes();
        if (body.overflow()) {
            throw new IOException("Long read from " + name() + ": expected " + length
                    + " bytes but received more");
        }
        if (buf.remaining() != length) {
            throw new IOException("Short read from " + name() + ": expected " + length
                    + " bytes but received " + buf.remaining());
        }
        return buf;
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

    /// The object's `ETag`, as returned with the suffix-range `GET` in [#open()]; empty when the
    /// endpoint sent none.
    @Override
    public Optional<String> identity() {
        if (fileLength < 0) {
            throw new IllegalStateException("File not opened: " + name());
        }
        return identity;
    }

    /// No-op: the fetcher holds no per-file resources. The [S3Api] and its
    /// [java.net.http.HttpClient] are owned by the [S3Source] and outlive
    /// individual files.
    @Override
    public void close() {
    }

    /// Points out that a request the object store denied carried no credentials.
    private String unsignedHint(int status) {
        boolean denied = status == HTTP_UNAUTHORIZED || status == HTTP_FORBIDDEN;
        return denied && api.anonymous()
                ? " (the request was sent unsigned, without credentials)"
                : "";
    }

    /// Number of HTTP requests issued against the object since [#open()].
    long networkRequestCount() {
        return networkRequestCount.get();
    }

    /// Number of bytes fetched from the network since [#open()].
    long networkBytesFetched() {
        return networkBytesFetched.get();
    }

    /// Emits a single FINE log line per network fetch carrying the byte
    /// range, the [FetchReason] tag attributed to the caller, and the
    /// running per-file totals. Designed to be greppable: one line per
    /// fetch, no nested structure.
    ///
    /// Logged at `System.Logger.Level.DEBUG`, which the platform logger
    /// finder maps to JUL `Level.FINE` — the CLI's `--log-file` handler
    /// takes records from `Level.FINE`, so a refactor that lowers the level
    /// here would silently drop the fetches from the log file.
    private void logFetch(String reason, long offset, int length, long requestNo, long totalBytes) {
        LOG.get().log(System.Logger.Level.DEBUG,
                "[{0}] fetch #{1} reason={2} offset={3} length={4} range=[{3},{5}) totalBytes={6}",
                name(), requestNo, reason, offset, length, offset + length, totalBytes);
    }

    /// Extracts the total file length from the HTTP response.
    ///
    /// For suffix-range requests, S3 returns a `Content-Range` header like
    /// `bytes 1000-1999/2000` where the number after `/` is the total
    /// object size. If the header is absent (e.g. file smaller than the tail),
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
