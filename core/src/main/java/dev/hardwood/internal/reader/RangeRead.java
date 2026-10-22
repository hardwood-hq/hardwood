/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.util.Lazy;

/// One read of a byte range of an input file, and the future of its bytes.
///
/// A file whose bytes are in memory ([#inMemory]) is read in place, and the read is complete when
/// [#start] returns. Any other file is read on a task of the read's [PrefetchTasks], a virtual
/// thread of the context's I/O executor, which completes the read; the read's owner waits for
/// those tasks before it closes the file. A step of the pool never waits for a read: it yields on
/// it ([#whenDone]), and the completion queues the step again.
///
/// A read that fails completes with its failure and throws nothing. An `Error` it fails with is
/// thrown by the close of its [PrefetchTasks] ([PrefetchTasks#awaitAll()]) unless a step has
/// taken it from the read ([#bytes]): a speculative read's always, since no step takes the
/// failure of a read started ahead of need, and a demand read's when its step stopped before it
/// took it. An `Error` therefore reaches the caller once, through the step or through the close.
///
/// A read has a priority. A read a step waits for is [Priority#DEMAND]; a read started ahead of
/// need, such as a one-ahead prefetch, is [Priority#SPECULATIVE] until a step waits for it
/// ([#raiseToDemand]).
public final class RangeRead implements PrefetchTasks.Untaken {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(RangeRead.class.getName()));

    /// The priority #1397's scheduler gives a read.
    public enum Priority {
        /// A step waits for the bytes.
        DEMAND,
        /// Nothing waits for the bytes yet.
        SPECULATIVE
    }

    private final InputFile inputFile;
    private final long offset;
    private final int length;
    private final boolean startedSpeculative;
    private final PrefetchTasks tasks;
    private final CompletableFuture<ByteBuffer> bytes = new CompletableFuture<>();
    private volatile Priority priority;
    /// The `Error` the read failed with, until a step takes it ([#bytes]).
    private volatile Error untakenError;

    private RangeRead(InputFile inputFile, long offset, int length, Priority priority, PrefetchTasks tasks) {
        this.inputFile = inputFile;
        this.offset = offset;
        this.length = length;
        this.priority = priority;
        this.startedSpeculative = priority == Priority.SPECULATIVE;
        this.tasks = tasks;
    }

    /// Starts the read of `length` bytes at `offset` of `inputFile`: in place for a file whose
    /// bytes are in memory, on a task of `tasks` otherwise. The read carries `reason`, composed
    /// with the caller's [FetchReason]. A read `tasks` no longer admits, as once its owner closes,
    /// fails.
    static RangeRead start(InputFile inputFile, long offset, int length, String reason, Priority priority,
                           PrefetchTasks tasks) {
        RangeRead read = new RangeRead(inputFile, offset, length, priority,
                Objects.requireNonNull(tasks, "tasks"));
        String outer = FetchReason.current();
        String composed = "unattributed".equals(outer) ? reason : outer + " | " + reason;
        if (inMemory(inputFile)) {
            read.run(composed);
        }
        else {
            tasks.submit(() -> read.run(composed), () -> read.bytes.completeExceptionally(
                    new IOException(ExceptionContext.filePrefix(inputFile.name())
                            + "The read was closed before the range at offset " + offset + " (length " + length
                            + ") was fetched")));
        }
        return read;
    }

    /// Whether `inputFile` holds its bytes in memory, so a read of it slices them without waiting.
    static boolean inMemory(InputFile inputFile) {
        return inputFile instanceof MappedInputFile || inputFile instanceof ByteBufferInputFile;
    }

    private void run(String reason) {
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
    /// @param what names what was read in the failure's message, such as `chunk`; the message is
    ///        built only on failure, as this runs for every page a walk takes
    /// @throws IOException the read's failure, as "Failed to fetch `what` at offset … (length …)"
    /// @throws IllegalStateException if the read has not completed
    ByteBuffer bytes(String what) throws IOException {
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
                    + "Failed to fetch " + what + " at offset " + offset + " (length " + length + ")", cause);
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

    /// Marks the read as one a step waits for.
    void raiseToDemand() {
        priority = Priority.DEMAND;
    }

    Priority priority() {
        return priority;
    }

    long offset() {
        return offset;
    }

    int length() {
        return length;
    }
}
