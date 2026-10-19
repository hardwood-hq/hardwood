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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import dev.hardwood.OutputFile;
import dev.hardwood.s3.internal.S3Api;
import dev.hardwood.s3.internal.S3Xml;

/// An [OutputFile] that writes one S3 object, created by [S3Source#outputFile(String, String)].
///
/// The object is sent as a multipart upload, or in a single `PUT` when it is smaller than one
/// part, and appears at its key only once [#close()] succeeds. [#discard()] aborts the upload.
/// See [S3Source#outputFile(String, String)] for buffering, size limits and failure handling.
public final class S3OutputFile implements OutputFile {

    /// The smallest part S3 accepts, except for the last.
    static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    /// The largest buffer a Java array can hold.
    static final int MAX_PART_SIZE = Integer.MAX_VALUE - 8;

    /// The most parts of one output uploading at once.
    static final int MAX_UPLOADS_IN_FLIGHT = 4;

    private final S3Api api;
    private final String bucket;
    private final String key;
    private final int partSize;
    /// The parts uploaded, in part-number order.
    private final List<S3Xml.Part> parts = new ArrayList<>();
    /// The parts uploading, oldest first.
    private final ArrayDeque<PendingPart> inFlight = new ArrayDeque<>();
    /// Buffers whose upload finished, for reuse.
    private final ArrayDeque<byte[]> spareBuffers = new ArrayDeque<>();

    private State state = State.NEW;
    private byte[] buffer;
    private int buffered;
    private long position;
    private String uploadId;
    private ExecutorService uploads;

    S3OutputFile(S3Api api, String bucket, String key, int partSize) {
        this.api = Objects.requireNonNull(api, "api");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.key = Objects.requireNonNull(key, "key");
        validatePartSize(partSize);
        this.partSize = partSize;
    }

    /// Rejects a part size outside [#MIN_PART_SIZE] to [#MAX_PART_SIZE].
    static void validatePartSize(int partSize) {
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
                startPartUpload(false);
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
                    startPartUpload(true);
                }
                while (!inFlight.isEmpty()) {
                    finishOldestPart();
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

    /// Waits for the part uploads in flight to end and aborts the multipart upload, if one was
    /// started. Does nothing once the output is closed.
    ///
    /// The wait and the abort run with the thread's interrupt status cleared, and the status
    /// restored afterwards, so that an interrupted writer still releases its parts.
    @Override
    public void discard() throws IOException {
        if (state == State.CLOSED) {
            return;
        }
        boolean interrupted = Thread.interrupted();
        try {
            // S3 keeps a part whose upload completes after the abort, so the uploads end first.
            finish();
            if (uploadId != null) {
                String abandoned = uploadId;
                uploadId = null;
                api.abortMultipartUpload(bucket, key, abandoned);
            }
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Rejects a write of `length` bytes at `position` that would need more than [S3Xml#MAX_PARTS]
    /// parts, before any of it is buffered.
    static void requireCapacity(String target, long position, int length, int partSize) throws IOException {
        long maximum = (long) partSize * S3Xml.MAX_PARTS;
        if (length > maximum - position) {
            throw new IOException("Output for " + target + " cannot exceed " + maximum + " bytes ("
                    + S3Xml.MAX_PARTS + " parts of " + partSize + " bytes)");
        }
    }

    /// Hands the buffer to an upload of the next part, after taking a free upload slot, and
    /// continues in a spare buffer unless this is the last part. A part that failed in the
    /// meantime is raised here, so a failure stops the writer within one part.
    private void startPartUpload(boolean last) throws IOException {
        if (uploadId == null) {
            uploadId = api.createMultipartUpload(bucket, key);
            uploads = Executors.newVirtualThreadPerTaskExecutor();
        }
        raiseFailedPart();
        if (inFlight.size() == MAX_UPLOADS_IN_FLIGHT) {
            finishOldestPart();
        }
        int partNumber = parts.size() + inFlight.size() + 1;
        byte[] part = buffer;
        int length = buffered;
        String id = uploadId;
        inFlight.add(new PendingPart(partNumber, part,
                uploads.submit(() -> api.uploadPart(bucket, key, id, partNumber, part, length))));
        buffer = last ? null : nextBuffer();
        buffered = 0;
    }

    private byte[] nextBuffer() {
        byte[] spare = spareBuffers.poll();
        return spare != null ? spare : new byte[partSize];
    }

    /// Waits for the oldest part in flight and records its `ETag`.
    private void finishOldestPart() throws IOException {
        PendingPart oldest = inFlight.peek();
        String etag = await(oldest.etag());
        inFlight.poll();
        parts.add(new S3Xml.Part(oldest.partNumber(), etag));
        spareBuffers.push(oldest.buffer());
    }

    private void raiseFailedPart() throws IOException {
        for (PendingPart part : inFlight) {
            if (part.etag().state() == Future.State.FAILED) {
                await(part.etag());
            }
        }
    }

    private static String await(Future<String> etag) throws IOException {
        try {
            return etag.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for a part upload", e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException("Part upload failed", cause);
        }
    }

    /// Ends the output: waits for the part uploads still running to end, then releases the
    /// buffers.
    private void finish() {
        state = State.CLOSED;
        if (uploads != null) {
            uploads.close();
            uploads = null;
        }
        inFlight.clear();
        spareBuffers.clear();
        parts.clear();
        buffer = null;
        buffered = 0;
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException("Output for " + target() + " is not open");
        }
    }

    private String target() {
        return "s3://" + bucket + "/" + key;
    }

    private record PendingPart(int partNumber, byte[] buffer, Future<String> etag) {
    }

    private enum State {
        NEW, OPEN, CLOSED
    }
}
