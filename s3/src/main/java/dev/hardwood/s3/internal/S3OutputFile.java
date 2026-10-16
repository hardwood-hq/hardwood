/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import dev.hardwood.OutputFile;

/// An [OutputFile] that writes one S3 object through a buffer of one part.
///
/// Bytes collect in the buffer; each time it fills, it is uploaded as the next part of a
/// multipart upload, which the first full part starts. [#close()] uploads the remainder and
/// completes the upload, or, for an output that never filled a part, sends the whole object in
/// one `PUT`. Until then nothing is visible at the key. [#discard()] aborts the upload, releasing
/// the parts already sent.
///
/// Part uploads run on the writing thread, so the writer waits for each one.
public final class S3OutputFile implements OutputFile {

    /// The smallest part S3 accepts, except for the last.
    public static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    /// The largest buffer a Java array can hold.
    public static final int MAX_PART_SIZE = Integer.MAX_VALUE - 8;

    /// The most parts one multipart upload may have.
    static final int MAX_PARTS = 10_000;

    private final S3Api api;
    private final String bucket;
    private final String key;
    private final int partSize;
    private final List<S3Xml.Part> parts = new ArrayList<>();

    private State state = State.NEW;
    private byte[] buffer;
    private int buffered;
    private long position;
    private String uploadId;

    public S3OutputFile(S3Api api, String bucket, String key, int partSize) {
        this.api = Objects.requireNonNull(api, "api");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.key = Objects.requireNonNull(key, "key");
        validatePartSize(partSize);
        this.partSize = partSize;
    }

    /// Rejects a part size outside [#MIN_PART_SIZE] to [#MAX_PART_SIZE].
    public static void validatePartSize(int partSize) {
        if (partSize < MIN_PART_SIZE || partSize > MAX_PART_SIZE) {
            throw new IllegalArgumentException("Upload part size must be between " + MIN_PART_SIZE + " and "
                    + MAX_PART_SIZE + " bytes: " + partSize);
        }
    }

    @Override
    public void create() {
        if (state != State.NEW) {
            throw new IllegalStateException("Output for " + target() + " was already created");
        }
        buffer = new byte[partSize];
        state = State.OPEN;
    }

    @Override
    public void write(ByteBuffer data) throws IOException {
        requireOpen();
        requireCapacity(target(), position, data.remaining(), partSize);
        while (data.hasRemaining()) {
            int length = Math.min(data.remaining(), partSize - buffered);
            data.get(buffer, buffered, length);
            buffered += length;
            position += length;
            if (buffered == partSize) {
                uploadPart();
            }
        }
    }

    @Override
    public long position() {
        requireOpen();
        return position;
    }

    /// Publishes the object. A failure aborts the upload before it is thrown, with an abort
    /// failure attached as suppressed. Does nothing once the output is closed or discarded.
    @Override
    public void close() throws IOException {
        if (state != State.OPEN) {
            return;
        }
        try {
            if (uploadId == null) {
                api.putObject(bucket, key, buffer, buffered);
            }
            else {
                if (buffered > 0) {
                    uploadPart();
                }
                api.completeMultipartUpload(bucket, key, uploadId, parts);
                uploadId = null;
            }
        }
        catch (IOException | RuntimeException e) {
            try {
                discard();
            }
            catch (IOException | RuntimeException abortFailure) {
                e.addSuppressed(abortFailure);
            }
            throw e;
        }
        finish();
    }

    /// Aborts the multipart upload, if one was started. Does nothing once the output is closed.
    @Override
    public void discard() throws IOException {
        if (state == State.CLOSED) {
            return;
        }
        finish();
        if (uploadId != null) {
            String abandoned = uploadId;
            uploadId = null;
            api.abortMultipartUpload(bucket, key, abandoned);
        }
    }

    /// Rejects a write of `length` bytes at `position` that would need more than [#MAX_PARTS]
    /// parts, before any of it is buffered.
    static void requireCapacity(String target, long position, int length, int partSize) throws IOException {
        long maximum = (long) partSize * MAX_PARTS;
        if (length > maximum - position) {
            throw new IOException("Output for " + target + " cannot exceed " + maximum + " bytes ("
                    + MAX_PARTS + " parts of " + partSize + " bytes)");
        }
    }

    private void uploadPart() throws IOException {
        if (uploadId == null) {
            uploadId = api.createMultipartUpload(bucket, key);
        }
        int partNumber = parts.size() + 1;
        String etag = api.uploadPart(bucket, key, uploadId, partNumber, buffer, buffered);
        parts.add(new S3Xml.Part(partNumber, etag));
        buffered = 0;
    }

    private void finish() {
        state = State.CLOSED;
        buffer = null;
        buffered = 0;
        parts.clear();
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException("Output for " + target() + " is not open");
        }
    }

    private String target() {
        return "s3://" + bucket + "/" + key;
    }

    private enum State {
        NEW, OPEN, CLOSED
    }
}
