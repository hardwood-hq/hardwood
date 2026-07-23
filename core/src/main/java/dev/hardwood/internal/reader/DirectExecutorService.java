/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/// An [java.util.concurrent.ExecutorService] that runs every task inline on the calling
/// thread. Used by the synchronous read mode: with this executor,
/// `CompletableFuture.runAsync(task, exec)` completes on the caller, so page decoding spawns
/// no threads — required on a runtime without threads (a GraalVM Web Image / WebAssembly
/// build). Its presence as the decode executor is also how a [ColumnWorker] detects that it
/// must run in pull-based synchronous mode.
public final class DirectExecutorService extends AbstractExecutorService {

    private volatile boolean shutdown;

    @Override
    public void execute(Runnable command) {
        command.run();
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }
}
