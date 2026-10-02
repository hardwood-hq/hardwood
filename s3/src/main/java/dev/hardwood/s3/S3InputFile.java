/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import dev.hardwood.Experimental;
import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.RangeBackedInputFile;

/// [InputFile] backed by an object in Amazon S3 (or an S3-compatible service).
///
/// A [#readRange] call that cannot be answered locally issues a signed
/// HTTP `GET` request with a byte-range header, so only the requested
/// bytes are transferred.
///
/// [#open()] uses a suffix-range GET instead of a HEAD request. This
/// discovers the file length from the `Content-Range` response header
/// and pre-fetches the Parquet footer (which sits at the end of the file) in
/// the same round-trip — eliminating a separate HEAD request per file.
/// Those tail bytes are retained for as long as the file is open, and a
/// read falling entirely inside that window is answered from them
/// without a request.
///
/// When the owning [S3Source] is configured with
/// [RangeBacking#SPARSE_TEMPFILE], an internal mmap-backed range cache
/// serves repeat reads of the same byte ranges without re-issuing HTTP
/// requests. The cache is invisible at the API surface — the same
/// counters ([#networkRequestCount], [#networkBytesFetched]) reflect
/// only network traffic in either mode.
///
/// Thread-safe once [#open()] has been called.
public class S3InputFile implements InputFile {

    /// The S3 network path, retained so the counters report network
    /// traffic only: under [RangeBacking#SPARSE_TEMPFILE] a cache hit is
    /// served from the mapping and never reaches the fetcher.
    private final S3Fetcher fetcher;

    /// What the [InputFile] methods delegate to: either [#fetcher]
    /// itself, or the range cache wrapping it. Chosen once at
    /// construction, so the read path carries no per-call dispatch.
    private final InputFile impl;

    S3InputFile(S3Source source, String bucket, String key) {
        this.fetcher = new S3Fetcher(source.api(), bucket, key);
        this.impl = source.rangeBacking() == RangeBacking.SPARSE_TEMPFILE
                ? new RangeBackedInputFile(fetcher, source.tempDir())
                : fetcher;
    }

    @Override
    public void open() throws IOException {
        impl.open();
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        return impl.readRange(offset, length);
    }

    @Override
    public long length() throws IOException {
        return impl.length();
    }

    @Override
    public String name() {
        return impl.name();
    }

    /// The object's `ETag`, taken from the response to the request [#open()] issues; empty when
    /// the endpoint sent none.
    @Override
    @Experimental
    public Optional<String> identity() {
        return fetcher.identity();
    }

    @Override
    public void close() throws IOException {
        impl.close();
    }

    /// Number of HTTP requests issued against the object since [#open()].
    /// Counts the suffix-range tail fetch from `open` plus every
    /// network-fetch [#readRange] call. Tail-cache and range-cache hits
    /// do not count.
    public long networkRequestCount() {
        return fetcher.networkRequestCount();
    }

    /// Number of bytes fetched from the network since [#open()]. The tail
    /// fetch from `open` contributes its actual response size; each
    /// network-fetch [#readRange] contributes the requested length.
    /// Tail-cache and range-cache hits do not count.
    public long networkBytesFetched() {
        return fetcher.networkBytesFetched();
    }
}
