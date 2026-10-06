/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import dev.hardwood.OutputFile;

/// Sequential, single-use S3 output with one reusable multipart payload buffer.
/// The shared transport remains owned by its source.
public final class S3OutputFile implements OutputFile {

    private static final int MIN_PART_SIZE = 5 * 1024 * 1024;
    private static final int MAX_PART_SIZE = Integer.MAX_VALUE - 8;
    private static final int MAX_PARTS = 10_000;

    private final S3Api api;
    private final String bucket;
    private final String key;
    private final int partSize;
    private final String target;
    private final List<S3Xml.Part> parts = new ArrayList<>();
    private State state = State.NEW;
    private byte[] buffer;
    private int buffered;
    private String writeId;
    private String uploadId;
    private boolean uncertainPart;

    private long position;

    public S3OutputFile(S3Api api, String bucket, String key, int partSize) {
        this.api = Objects.requireNonNull(api, "api");
        this.bucket = requireNonEmpty(bucket, "bucket");
        this.key = requireNonEmpty(key, "key");
        validatePartSize(partSize);
        this.partSize = partSize;
        target = "s3://" + bucket + "/" + key;
    }

    @Override
    public void create() {
        if (state != State.NEW) {
            throw new IllegalStateException("S3 output cannot be recreated: " + target());
        }
        try {
            buffer = new byte[partSize];
            writeId = UUID.randomUUID().toString();
            state = State.OPEN;
        }
        catch (RuntimeException | Error failure) {
            state = State.FAILED;
            releaseLocalState();
            throw failure;
        }
    }

    @Override
    public void write(ByteBuffer data) throws IOException {
        requireOpen();
        try {
            Objects.requireNonNull(data, "data");
            requireCapacity(position, data.remaining(), partSize, target());
            while (data.hasRemaining()) {
                int length = Math.min(data.remaining(), partSize - buffered);
                data.get(buffer, buffered, length);
                buffered += length;
                position += length;
                if (buffered == partSize) {
                    uploadBufferedPart();
                }
            }
        }
        catch (IOException | RuntimeException | Error failure) {
            fail(failure, false);
            throw failure;
        }
    }

    @Override
    public long position() {
        requireOpen();
        return position;
    }

    @Override
    public void close() throws IOException {
        if (state == State.FAILED) {
            discard();
            return;
        }
        if (state != State.OPEN) {
            return;
        }
        boolean publicationAttempted = false;
        try {
            if (uploadId == null) {
                publicationAttempted = true;
                api.putObject(bucket, key, buffer, buffered, writeId);
            }
            else {
                if (buffered != 0) {
                    uploadBufferedPart();
                }
                publicationAttempted = true;
                api.completeMultipartUpload(bucket, key, uploadId, parts);
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new IOException("Interrupted during publication of " + target());
            }
            commit();
        }
        catch (IOException | RuntimeException | Error failure) {
            if (publicationAttempted && uncertainPublication(failure)) {
                Verification verification = verifyPublication(failure);
                if (verification.confirmed() && !Thread.currentThread().isInterrupted()) {
                    commit();
                    return;
                }
                String detail = Thread.currentThread().isInterrupted() ? "stopped due to interruption" : verification.detail();
                suppress(failure, new IOException("Publication of " + target()
                        + " could not be confirmed; the destination may contain a completed object with write ID " + writeId
                        + " and expected length " + position + "; verification " + detail + " after " + verification.checks() + " HEAD attempts"));
            }
            fail(failure, publicationAttempted);
            throw failure;
        }
    }

    private void commit() {
        state = State.COMMITTED;
        uploadId = null;
        releaseLocalState();
    }

    private static boolean uncertainPublication(Throwable failure) {
        if (failure instanceof S3Api.HttpException response) {
            if (response.statusCode() != 408 && response.statusCode() < 500) {
                return false;
            }
            return !(response.getCause() instanceof S3Xml.ServiceException error) || uncertainServiceError(error);
        }
        if (failure instanceof S3Xml.ServiceException error) {
            return uncertainServiceError(error);
        }
        return failure instanceof IOException;
    }

    private static boolean uncertainServiceError(S3Xml.ServiceException error) {
        return switch (error.code()) {
            case "InternalError", "SlowDown", "ServiceUnavailable", "RequestTimeout" -> true;
            default -> false;
        };
    }

    private Verification verifyPublication(Throwable failure) {
        state = State.VERIFYING;
        long checks = 0;
        for (int attempt = 0;; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                return new Verification(false, false, "skipped due to interruption", checks);
            }
            if (attempt != 0) {
                try {
                    S3Api.sleepBeforeRetry(attempt);
                }
                catch (IOException backoffFailure) {
                    suppress(failure, backoffFailure);
                    return new Verification(false, false, "stopped due to interruption", checks);
                }
            }
            checks++;
            Verification check = checkPublication(failure, checks);
            if (!check.retryable()) {
                return check;
            }
            if (attempt >= api.maxRetries()) {
                return new Verification(false, false, "exhausted its budget", checks);
            }
        }
    }

    private Verification checkPublication(Throwable failure, long checks) {
        try {
            HttpResponse<Void> response = api.headObject(bucket, key);
            if (Thread.currentThread().isInterrupted()) {
                return new Verification(false, false, "stopped due to interruption", checks);
            }
            int status = response.statusCode();
            if (status == 200 && metadataMatches(response.headers())) {
                return new Verification(true, false, "matched", checks);
            }
            suppress(failure, new IOException("HEAD verification of " + target() + " returned HTTP " + status
                    + (status == 200 ? " without matching write ID and length" : "")));
            if (status != 200 && status != 404 && status != 500 && status != 503) {
                return new Verification(false, false, (status == 403 ? "denied" : "stopped") + " at HTTP " + status, checks);
            }
            return new Verification(false, true, "pending", checks);
        }
        catch (IOException verificationFailure) {
            suppress(failure, verificationFailure);
            if (Thread.currentThread().isInterrupted()) {
                return new Verification(false, false, "stopped due to interruption", checks);
            }
            return new Verification(false, true, "transport failure", checks);
        }
        catch (RuntimeException | Error verificationFailure) {
            suppress(failure, verificationFailure);
            return new Verification(false, false, "failed before confirmation", checks);
        }
    }

    private boolean metadataMatches(HttpHeaders headers) {
        List<String> ids = headers.allValues(S3Api.WRITE_ID_HEADER);
        List<String> lengths = headers.allValues("Content-Length");
        if (ids.size() != 1 || !writeId.equals(ids.getFirst()) || lengths.size() != 1) {
            return false;
        }
        String length = lengths.getFirst();
        if (length.isEmpty()) {
            return false;
        }
        for (int i = 0; i < length.length(); i++) {
            if (length.charAt(i) < '0' || length.charAt(i) > '9') {
                return false;
            }
        }
        try {
            return Long.parseLong(length) == position;
        }
        catch (NumberFormatException ignored) {
            return false;
        }
    }

    private static void suppress(Throwable failure, Throwable secondary) {
        if (failure != secondary) {
            failure.addSuppressed(secondary);
        }
    }

    @Override
    public void discard() throws IOException {
        if (state == State.COMMITTED) {
            return;
        }
        if (state != State.PUBLISH_UNKNOWN) {
            state = State.DISCARDED;
        }
        releaseLocalState();
        cleanupPreservingInterrupt();
    }

    static void validatePartSize(int partSize) {
        if (partSize < MIN_PART_SIZE || partSize > MAX_PART_SIZE) {
            throw new IllegalArgumentException("S3 upload part size must be between " + MIN_PART_SIZE + " and " + MAX_PART_SIZE + " bytes");
        }
    }

    static void requireCapacity(long position, int length, int partSize) throws IOException {
        requireCapacity(position, length, partSize, "S3 output");
    }

    private static void requireCapacity(long position, int length, int partSize, String target) throws IOException {
        long maximum = (long) partSize * MAX_PARTS;
        if (length > maximum - position) {
            throw new IOException(target + " cannot exceed " + maximum + " bytes; position is " + position + " and write length is " + length);
        }
    }

    private void uploadBufferedPart() throws IOException {
        if (uploadId == null) {
            initiateUpload();
        }
        int number = parts.size() + 1;
        uncertainPart = true;
        String etag = api.uploadPart(bucket, key, uploadId, number, buffer, buffered);
        parts.add(new S3Xml.Part(number, etag));
        buffered = 0;
        uncertainPart = false;
    }

    private void initiateUpload() throws IOException {
        try {
            uploadId = api.initiateMultipartUpload(bucket, key, writeId);
        }
        catch (IOException failure) {
            if (!(failure instanceof S3Api.HttpException) && !(failure instanceof S3Xml.ServiceException)) {
                failure.addSuppressed(new IOException("Initialization of " + target()
                        + " did not acknowledge an upload ID; an unfinished upload may remain and cannot be addressed for cleanup"));
            }
            throw failure;
        }
    }

    private void fail(Throwable failure, boolean publicationAttempted) {
        state = publicationAttempted ? State.PUBLISH_UNKNOWN : State.FAILED;
        releaseLocalState();
        try {
            cleanupPreservingInterrupt();
        }
        catch (IOException | RuntimeException | Error cleanupFailure) {
            suppress(failure, cleanupFailure);
        }
    }

    private void cleanupPreservingInterrupt() throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            cleanupUpload();
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void cleanupUpload() throws IOException {
        if (uploadId == null) {
            return;
        }
        if (!uncertainPart) {
            api.abortMultipartUpload(bucket, key, uploadId);
            uploadId = null;
            return;
        }
        IOException unresolved = new IOException("Could not confirm cleanup of multipart upload " + uploadId + " for " + target());
        for (int attempt = 0;; attempt++) {
            boolean retryable = abortOnce(unresolved);
            try {
                S3Api.UploadState uploadState = api.inspectMultipartUpload(bucket, key, uploadId);
                if (uploadState == S3Api.UploadState.MISSING) {
                    uploadId = null;
                    uncertainPart = false;
                    return;
                }
                unresolved.addSuppressed(new IOException("Multipart upload " + uploadId + " still exists: " + uploadState));
            }
            catch (IOException failure) {
                unresolved.addSuppressed(failure);
                retryable &= retryableCleanup(failure);
            }
            if (!retryable || attempt >= api.maxRetries() || Thread.currentThread().isInterrupted()) {
                throw unresolved;
            }
            try {
                S3Api.sleepBeforeRetry(attempt + 1);
            }
            catch (IOException failure) {
                unresolved.addSuppressed(failure);
                throw unresolved;
            }
        }
    }

    private boolean abortOnce(IOException unresolved) {
        try {
            api.abortMultipartUploadOnce(bucket, key, uploadId);
            return true;
        }
        catch (IOException failure) {
            unresolved.addSuppressed(failure);
            return retryableCleanup(failure);
        }
    }

    private static boolean retryableCleanup(IOException failure) {
        if (failure instanceof S3Api.HttpException response) {
            return response.statusCode() == 500 || response.statusCode() == 503;
        }
        return !(failure instanceof S3Xml.ServiceException) && !(failure instanceof BoundedBodySubscriber.BodyLimitException);
    }

    private void releaseLocalState() {
        buffer = null;
        buffered = 0;
        parts.clear();
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException("S3 output is not open (" + state + "): " + target());
        }
    }

    private String target() {
        return target;
    }

    private static String requireNonEmpty(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private enum State {
        NEW, OPEN, FAILED, VERIFYING, COMMITTED, DISCARDED, PUBLISH_UNKNOWN
    }

    private record Verification(boolean confirmed, boolean retryable, String detail, long checks) {
    }
}
