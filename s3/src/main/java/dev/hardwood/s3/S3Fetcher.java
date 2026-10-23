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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.reader.ReadRanges;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.s3.internal.S3Api;

/// The network path behind [S3InputFile]: every non-empty [#readRange] call issues
/// an HTTP `GET` with a byte-range header against one S3 object.
///
/// [#open()] uses a suffix-range GET instead of a HEAD request. This
/// discovers the file length from the `Content-Range` response header
/// and pre-fetches the Parquet footer (which sits at the end of the file)
/// in the same round-trip — eliminating a separate HEAD request per file.
/// A file no longer than the suffix comes back whole. The fetched tail is
/// retained until [#releaseTail()] or [#close()] and serves any subsequent
/// read that falls entirely within it; a read that ends inside it fetches
/// only the bytes before it.
///
/// This class is unaware of [RangeBacking]. Under
/// [RangeBacking#SPARSE_TEMPFILE] the mmap-backed range cache wraps an
/// instance of this class, so cache hits never reach here and the
/// counters keep reporting network traffic only.
///
/// Thread-safe once [#open()] has been called.
final class S3Fetcher implements InputFile {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(S3Fetcher.class.getName()));

    static final String TAIL_BYTES_PROPERTY = "hardwood.internal.s3TailBytes";

    /// Number of bytes to fetch from the tail of the file during [#open()]
    /// without the override. The request is sent before the file's length
    /// is known, so this is also the threshold up to which a file is fetched
    /// whole; every larger file has its footer, and usually its page index,
    /// in the same round-trip.
    ///
    /// A cost heuristic, not an assumption about file layout. Every open
    /// fetches this many bytes even when only the footer is used. A footer
    /// or range that does not fit is read by a request of its own:
    /// [#readRange] serves only reads fully contained in the fetched tail
    /// and goes to the network otherwise.
    static final int DEFAULT_TAIL_SIZE = 256 * 1024;

    /// Number of bytes [#open(boolean)] fetches for a read whose footers a
    /// [dev.hardwood.MetadataSource] supplies, when the tail is larger: the
    /// trailer, and a footer the source reads on a miss, without a whole file.
    static final int SUPPLIED_FOOTER_TAIL_SIZE = 64 * 1024;

    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;

    private final S3Api api;
    private final String bucket;
    private final String key;
    private final int tailSize;
    private long fileLength = -1;
    /// `null` before the open and once released; written after [#tailCacheOffset], which it
    /// publishes to the threads reading it.
    private volatile ByteBuffer tailCache;
    private long tailCacheOffset;
    /// The object's `ETag` from the response to the suffix-range `GET` in [#open()].
    private Optional<String> identity;
    private final AtomicLong networkRequestCount = new AtomicLong();
    private final AtomicLong networkBytesFetched = new AtomicLong();

    S3Fetcher(S3Api api, String bucket, String key) {
        this.api = api;
        this.bucket = bucket;
        this.key = key;
        this.tailSize = tailSize();
    }

    /// The tail an open fetches: [#DEFAULT_TAIL_SIZE] unless overridden through
    /// [#TAIL_BYTES_PROPERTY], read for each file. A value that is not a positive byte count is
    /// rejected rather than replaced by the default.
    static int tailSize() {
        String override = System.getProperty(TAIL_BYTES_PROPERTY);
        if (override == null) {
            return DEFAULT_TAIL_SIZE;
        }
        int size;
        try {
            size = Integer.parseInt(override);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(invalidTailSize(override), e);
        }
        if (size < 1) {
            throw new IllegalArgumentException(invalidTailSize(override));
        }
        return size;
    }

    private static String invalidTailSize(String value) {
        return TAIL_BYTES_PROPERTY + " must be a positive byte count of at most " + Integer.MAX_VALUE
                + ", but was: " + value;
    }

    /// The most bytes an open holds, for a read whose footers are read from the file or, when
    /// `footerSupplied`, supplied by a [dev.hardwood.MetadataSource].
    int tailReservation(boolean footerSupplied) {
        return footerSupplied ? Math.min(tailSize, SUPPLIED_FOOTER_TAIL_SIZE) : tailSize;
    }

    @Override
    public void open() throws IOException {
        open(false);
    }

    /// Opens the file, fetching the tail sized by [#tailReservation]. Idempotent.
    void open(boolean footerSupplied) throws IOException {
        if (fileLength >= 0) {
            return;
        }
        String suffixRange = "bytes=-" + tailReservation(footerSupplied);
        HttpResponse<byte[]> response = api.getBytes(bucket, key, suffixRange);
        int status = response.statusCode();
        if (status != 206 && status != 200) {
            throw new IOException("Failed to open " + name()
                    + ": HTTP " + status + " " + new String(response.body()) + unsignedHint(status));
        }
        fileLength = parseFileLength(response);
        identity = response.headers().firstValue("ETag");
        byte[] tail = response.body();
        long requestNo = networkRequestCount.incrementAndGet();
        long totalBytes = networkBytesFetched.addAndGet(tail.length);
        // Use a direct buffer so slices are usable from FFM-based decompressors
        // (e.g. libdeflate), which require native MemorySegments.
        ByteBuffer held = ByteBuffer.allocateDirect(tail.length);
        held.put(tail);
        held.flip();
        tailCacheOffset = fileLength - tail.length;
        tailCache = held;
        logFetch("open-tail", tailCacheOffset, tail.length, requestNo, totalBytes);
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
        if (holds(tail, offset, length)) {
            int relOffset = Math.toIntExact(offset - tailCacheOffset);
            LOG.get().log(System.Logger.Level.DEBUG,
                    "[{0}] readRange offset={1} length={2} reason={3} (tail cache hit)",
                    name(), offset, length, FetchReason.current());
            return tail.slice(relOffset, length);
        }
        // A range ending inside the tail requests only the bytes before it, and takes the rest
        // from the tail, so no byte the open fetched is fetched again
        int fetched = endsInTail(tail, offset, length) ? Math.toIntExact(tailCacheOffset - offset) : length;

        long requestNo = networkRequestCount.incrementAndGet();
        long totalBytes = networkBytesFetched.addAndGet(fetched);
        logFetch(FetchReason.current(), offset, fetched, requestNo, totalBytes);
        String range = "bytes=" + offset + "-" + (offset + fetched - 1);
        HttpResponse<InputStream> response = api.getStream(bucket, key, range);
        int status = response.statusCode();
        if (status != 206) {
            // A 200 carries the whole object from byte 0: the endpoint ignored the Range header
            InputStream body = response.body();
            String detail;
            if (status == 200) {
                S3Api.discard(body);
                detail = ", expected 206; the endpoint ignored the Range header";
            }
            else {
                try (body) {
                    detail = " " + new String(body.readAllBytes()) + unsignedHint(status);
                }
            }
            throw new IOException("Failed to read range [" + offset + ", " + (offset + fetched)
                    + ") from " + name() + ": HTTP " + status + detail);
        }
        try (InputStream stream = response.body()) {
            ByteBuffer buf = ByteBuffer.allocateDirect(length);
            byte[] tmp = new byte[Math.min(8192, fetched)];
            while (buf.position() < fetched) {
                int toRead = Math.min(tmp.length, fetched - buf.position());
                int read = stream.read(tmp, 0, toRead);
                if (read < 0) {
                    break;
                }
                buf.put(tmp, 0, read);
            }
            if (buf.position() != fetched) {
                throw new IOException("Short read from " + name() + ": expected " + fetched
                        + " bytes but received " + buf.position());
            }
            // Read to the end of the response before closing it. Closed earlier, the response is
            // cancelled, and the client closes its connection, even after handing it back to the
            // pool for another request.
            if (stream.read() != -1) {
                throw new IOException("Long read from " + name() + ": expected " + fetched
                        + " bytes but received more");
            }
            if (fetched < length) {
                buf.put(tail.slice(0, length - fetched));
            }
            buf.flip();
            return buf;
        }
    }

    /// Whether `[offset, offset + length)` starts before `tail` and ends inside it.
    private boolean endsInTail(ByteBuffer tail, long offset, int length) {
        return tail != null && offset < tailCacheOffset && offset + length > tailCacheOffset;
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

    /// The bytes the tail holds; `0` before the open and once released.
    long tailBytes() {
        ByteBuffer tail = tailCache;
        return tail == null ? 0 : tail.capacity();
    }

    private boolean holds(ByteBuffer tail, long offset, int length) {
        return tail != null && offset >= tailCacheOffset && offset + length <= tailCacheOffset + tail.capacity();
    }

    /// Drops the tail, so later reads inside it issue requests. Slices handed out stay valid.
    void releaseTail() {
        tailCache = null;
    }

    /// Releases the tail. The [S3Api] and its [java.net.http.HttpClient] are
    /// owned by the [S3Source] and outlive individual files.
    @Override
    public void close() {
        releaseTail();
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
    /// finder maps to JUL `Level.FINE` — `DiveCommand` configures the
    /// JUL handler at `Level.FINE`, so a refactor that lowers the level
    /// here would silently drop dive's `--log-file` output.
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
