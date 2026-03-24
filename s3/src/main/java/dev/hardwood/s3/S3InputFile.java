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

import dev.hardwood.InputFile;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/// [InputFile] backed by an object in Amazon S3.
///
/// Each [#readRange] call issues an S3 `GetObject` request with a
/// byte-range header, so only the requested bytes are transferred.
///
/// [#open()] uses a suffix-range GET instead of a HEAD request. This
/// discovers the file length from the `Content-Range` response header
/// and pre-fetches the Parquet footer (which sits at the end of the file) in
/// the same round-trip — eliminating a separate HEAD request per file.
///
/// Thread-safe once [#open()] has been called: the underlying
/// [S3Client] is thread-safe and the file length and tail cache are set
/// exactly once.
public class S3InputFile implements InputFile {

    /// Number of bytes to fetch from the tail of the file during [#open()].
    /// 64 KB is large enough to cover the Parquet footer in virtually all files
    /// (footer + footer length + magic is typically a few KB).
    static final int TAIL_SIZE = 64 * 1024;

    private final S3Client s3;
    private final String bucket;
    private final String key;
    private final boolean ownsClient;
    private long fileLength = -1;
    private ByteBuffer tailCache;
    private long tailCacheOffset;

    private S3InputFile(S3Client s3, String bucket, String key, boolean ownsClient) {
        this.s3 = s3;
        this.bucket = bucket;
        this.key = key;
        this.ownsClient = ownsClient;
    }

    /// Creates an `S3InputFile` using a caller-provided [S3Client].
    ///
    /// The caller retains ownership of the client and is responsible for closing it.
    /// This is the preferred factory when reading multiple files, as it allows
    /// connection pooling and credential reuse.
    ///
    /// @param s3     the S3 client to use
    /// @param bucket the S3 bucket name
    /// @param key    the S3 object key
    /// @return a new unopened S3InputFile
    public static S3InputFile of(S3Client s3, String bucket, String key) {
        return new S3InputFile(s3, bucket, key, false);
    }

    /// Creates an `S3InputFile` that owns a default [S3Client].
    ///
    /// The client is created using the default credential provider chain and
    /// will be closed when this `InputFile` is closed.
    ///
    /// @param bucket the S3 bucket name
    /// @param key    the S3 object key
    /// @return a new unopened S3InputFile that owns its client
    public static S3InputFile of(String bucket, String key) {
        return new S3InputFile(S3Client.create(), bucket, key, true);
    }

    /// Creates an `S3InputFile` with explicit client ownership control.
    ///
    /// Use this factory for custom endpoints such as LocalStack or MinIO, where
    /// the caller builds the [S3Client] with a custom endpoint override
    /// but wants this `InputFile` to close it.
    ///
    /// @param s3         the S3 client to use
    /// @param bucket     the S3 bucket name
    /// @param key        the S3 object key
    /// @param ownsClient if `true`, the client will be closed when this InputFile is closed
    /// @return a new unopened S3InputFile
    public static S3InputFile of(S3Client s3, String bucket, String key, boolean ownsClient) {
        return new S3InputFile(s3, bucket, key, ownsClient);
    }

    @Override
    public void open() throws IOException {
        if (fileLength >= 0) {
            return;
        }
        try {
            // Suffix-range GET: fetches the last TAIL_SIZE bytes and discovers
            // the file length from the Content-Range response header. This
            // eliminates the separate HEAD request and pre-fetches the Parquet
            // footer (which sits at the end of the file) in the same round-trip.
            String suffixRange = "bytes=-" + TAIL_SIZE;
            ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                    b -> b.bucket(bucket).key(key).range(suffixRange));

            GetObjectResponse response = resp.response();
            fileLength = parseFileLength(response);

            byte[] tail = resp.asByteArray();
            tailCache = ByteBuffer.wrap(tail);
            tailCacheOffset = fileLength - tail.length;
        }
        catch (S3Exception e) {
            throw new IOException("Failed to open " + name() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        // Serve from the tail cache if the requested range falls within it
        if (tailCache != null && offset >= tailCacheOffset
                && offset + length <= tailCacheOffset + tailCache.capacity()) {
            int relOffset = Math.toIntExact(offset - tailCacheOffset);
            return tailCache.slice(relOffset, length);
        }

        String range = "bytes=" + offset + "-" + (offset + length - 1);
        try {
            ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                    b -> b.bucket(bucket).key(key).range(range));
            return ByteBuffer.wrap(resp.asByteArray());
        }
        catch (S3Exception e) {
            throw new IOException("Failed to read range [" + offset + ", " + (offset + length)
                    + ") from " + name() + ": " + e.getMessage(), e);
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
        if (ownsClient) {
            s3.close();
        }
    }

    /// Extracts the total file length from the S3 response.
    ///
    /// For suffix-range requests, S3 returns a `Content-Range` header like
    /// `bytes 1000-1999/2000` where the number after `/` is the total
    /// object size. If the header is absent (e.g. file smaller than TAIL_SIZE),
    /// falls back to `Content-Length`.
    private static long parseFileLength(GetObjectResponse response) throws IOException {
        String contentRange = response.contentRange();
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
        return response.contentLength();
    }
}
