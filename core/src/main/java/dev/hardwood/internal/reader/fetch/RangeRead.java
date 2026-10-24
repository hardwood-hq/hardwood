/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.io.ByteBufferInputFile;
import dev.hardwood.internal.io.MappedInputFile;
import dev.hardwood.internal.util.Lazy;

/// One read of a byte range of a file not in memory, the request of a [FetchSequence] entry, and
/// the future of its bytes.
///
/// The read runs on a task of the read's [PrefetchTasks], a virtual thread of the context's I/O
/// executor, which completes it; the read's owner waits for those tasks before it closes the file.
/// A step of the pool never waits for a read: it yields on it ([#whenDone]), and the completion
/// queues the step again. The read is created and then submitted ([#submit]), so that its entry
/// holds it before any thread can complete it.
///
/// A read that fails completes with its failure and throws nothing. An `Error` it fails with is
/// thrown by the close of its [PrefetchTasks] ([PrefetchTasks#awaitAll()]) unless a step has
/// taken it from the read ([#bytes]): a speculative read's always, since no step takes the
/// failure of a read started ahead of need, and a demand read's when its step stopped before it
/// took it. An `Error` therefore reaches the caller once, through the step or through the close.
public final class RangeRead implements PrefetchTasks.Untaken {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(RangeRead.class.getName()));

    private final InputFile inputFile;
    private final long offset;
    private final int length;
    /// The [FetchReason] the read runs under, its entry's own rather than the submitting thread's.
    private final String reason;
    private final boolean startedSpeculative;
    private final PrefetchTasks tasks;
    private final CompletableFuture<ByteBuffer> bytes = new CompletableFuture<>();
    /// The `Error` the read failed with, until a step takes it ([#bytes]).
    private volatile Error untakenError;

    /// A read of `length` bytes at `offset` of `inputFile` under `reason`, to run on a task of
    /// `tasks` once submitted.
    RangeRead(InputFile inputFile, long offset, int length, String reason, boolean speculative, PrefetchTasks tasks) {
        this.inputFile = inputFile;
        this.offset = offset;
        this.length = length;
        this.reason = reason;
        this.startedSpeculative = speculative;
        this.tasks = Objects.requireNonNull(tasks, "tasks");
    }

    /// Runs this read on a task of its [PrefetchTasks], or fails it if they no longer admit it, as
    /// once its owner closes.
    void submit() {
        tasks.submit(this::run, () -> bytes.completeExceptionally(
                new IOException(ExceptionContext.filePrefix(inputFile.name())
                        + "The read was closed before the range at offset " + offset + " (length " + length
                        + ") was fetched")));
    }

    /// Whether `inputFile` holds its bytes in memory, so a read of it slices them without waiting.
    public static boolean inMemory(InputFile inputFile) {
        return inputFile instanceof MappedInputFile || inputFile instanceof ByteBufferInputFile;
    }

    private void run() {
        try (FetchReason.Scope ignored = FetchReason.set(reason)) {
            bytes.complete(inputFile.readRange(offset, length));
        }
        catch (Throwable e) {
            // Any throwable, checked ones an InputFile throws undeclared included: a read that
            // never completes leaves the step waiting for it parked, and the close waiting for
            // that step with it
            if (startedSpeculative) {
                // Speculative: a step that needs the bytes reads them again and reports what
                // fails there. DEBUG rather than WARN so a sustained backend outage does not emit
                // one line per range for failures that are about to be reported properly.
                LOG.get().log(System.Logger.Level.DEBUG, "Prefetch failed for range at offset {0} (length {1}) in {2}",
                        offset, length, inputFile.name(), e);
            }
            if (e instanceof Error err) {
                // Before the completion, so a step that takes the failure finds it here to clear
                untakenError = err;
                tasks.recordUntaken(this);
            }
            bytes.completeExceptionally(e);
        }
    }

    /// The `Error` the read failed with if no step has taken it, `null` otherwise.
    @Override
    public Error untakenError() {
        return untakenError;
    }

    /// Whether the read has completed, with its bytes or with a failure.
    boolean isDone() {
        return bytes.isDone();
    }

    /// Whether the read was started speculatively and failed, so a step that needs its bytes
    /// reads them again and reports a failure of that read.
    boolean failedSpeculatively() {
        return startedSpeculative && bytes.isCompletedExceptionally();
    }

    /// The bytes of a completed read.
    ///
    /// @throws IOException the read's failure, as "Failed to fetch data at offset … (length …)"
    /// @throws IllegalStateException if the read has not completed
    ByteBuffer bytes() throws IOException {
        if (!bytes.isDone()) {
            throw new IllegalStateException("Read at offset " + offset + " of " + inputFile.name()
                    + " has not completed");
        }
        try {
            return bytes.join();
        }
        catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException r) {
                throw r;
            }
            if (cause instanceof Error err) {
                untakenError = null;
                throw err;
            }
            // An IOException, or a checked exception the InputFile threw undeclared
            throw new IOException(ExceptionContext.filePrefix(inputFile.name())
                    + "Failed to fetch data at offset " + offset + " (length " + length + ")", cause);
        }
    }

    /// Waits until the read has completed, its failure included. For a thread off the pool only.
    void await() {
        try {
            bytes.join();
        }
        catch (CompletionException e) {
            // Reported by bytes(), which the caller reads next
        }
    }

    /// Runs `action` once the read has completed, on the thread that completes it, or at once on
    /// the caller's when it already has.
    void whenDone(Runnable action) {
        bytes.whenComplete((ignored, failure) -> action.run());
    }
}
