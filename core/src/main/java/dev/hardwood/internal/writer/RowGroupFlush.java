/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import dev.hardwood.OutputFile;
import dev.hardwood.schema.FileSchema;

/// Encodes the column chunks of one row group, on worker threads and the caller's alike, and
/// writes them to the file in schema order.
///
/// Every chunk is claimed exactly once, in schema order, from a shared counter. Workers encode
/// what they claim into a [ChunkSink.Buffered]. The caller drains: it writes chunk `d` once
/// encoded, and until then claims and encodes chunks itself. A chunk the caller claims while it is
/// the one being drained is streamed straight to the file, so with no worker free the caller
/// encodes and streams every chunk in turn, as a single-threaded writer would. Which thread encodes
/// a chunk does not change its bytes, and the drain order fixes their place, so the file is the
/// same whatever the pool does.
///
/// The caller never waits for a worker task to start, only for a chunk a worker has claimed to
/// finish, so a pool busy with other work slows the flush down to the caller's own pace but does
/// not stall it. A task that starts after the flush finds nothing left to claim.
///
/// On a failure, claiming stops, every chunk already claimed is waited for, so no thread is still
/// reading the row group's buffers when the exception reaches the writer, and the first failure in
/// schema order is rethrown as it was raised.
final class RowGroupFlush {

    private final ColumnChunkBuffer[] columns;
    private final FileSchema schema;
    /// The next chunk to claim; any value from `columns.length` up means none is left.
    private final AtomicInteger next = new AtomicInteger();
    /// Each chunk's encoding, completed by whichever thread claimed it, normally or not.
    private final List<CompletableFuture<Encoded>> encoded;

    /// A chunk encoded ahead of its turn: its metadata with chunk-relative offsets, and its bytes.
    private record Encoded(ColumnChunkBuffer.Flushed flushed, ChunkSink.Buffered bytes) {
    }

    RowGroupFlush(ColumnChunkBuffer[] columns, FileSchema schema) {
        this.columns = columns;
        this.schema = schema;
        this.encoded = new ArrayList<>(columns.length);
        for (int c = 0; c < columns.length; c++) {
            encoded.add(new CompletableFuture<>());
        }
    }

    /// Encodes every chunk with up to `workers` tasks on `executor` helping the calling thread, and
    /// writes them to `out` in schema order.
    ///
    /// @param executor runs the worker tasks; unused where `workers` is `0`
    /// @return each chunk as written, in schema order
    List<ColumnChunkBuffer.Flushed> writeTo(OutputFile out, ExecutorService executor, int workers)
            throws IOException {
        List<Future<?>> tasks = new ArrayList<>(workers);
        try {
            for (int w = 0; w < workers; w++) {
                tasks.add(executor.submit(this::work));
            }
            List<ColumnChunkBuffer.Flushed> written = new ArrayList<>(columns.length);
            for (int d = 0; d < columns.length; d++) {
                written.add(drain(d, out));
            }
            return written;
        }
        catch (Throwable t) {
            stop();
            throw t;
        }
        finally {
            // Tasks still queued have nothing left to claim; dropping them keeps them from holding
            // a pool thread, however briefly.
            for (Future<?> task : tasks) {
                task.cancel(false);
            }
        }
    }

    /// A worker task: claims and buffers chunks until none is left or the flush has failed.
    private void work() {
        for (int c = next.getAndIncrement(); c < columns.length; c = next.getAndIncrement()) {
            try {
                encode(c, new ChunkSink.Buffered());
            }
            catch (Throwable t) {
                // Recorded against the chunk, where the caller meets it in schema order.
                return;
            }
        }
    }

    /// Writes chunk `d` to `out`, encoding chunks on the calling thread while `d` is not ready.
    private ColumnChunkBuffer.Flushed drain(int d, OutputFile out) throws IOException {
        CompletableFuture<Encoded> chunk = encoded.get(d);
        while (!chunk.isDone()) {
            int c = next.getAndIncrement();
            if (c == d) {
                // Nobody has claimed the chunk being drained: stream it.
                long start = out.position();
                return encode(d, ChunkSink.to(out)).flushed().at(start);
            }
            if (c >= columns.length) {
                break;
            }
            try {
                encode(c, new ChunkSink.Buffered());
            }
            catch (Throwable t) {
                // Recorded against chunk c; chunk d, which may be failing too, is reported first.
                break;
            }
        }
        Encoded done = join(chunk);
        long start = out.position();
        done.bytes().writeTo(out);
        return done.flushed().at(start);
    }

    /// Encodes chunk `c` into `sink` and completes its future either way, so a failure is waited
    /// for like a success.
    private Encoded encode(int c, ChunkSink sink) throws IOException {
        try {
            Encoded result = new Encoded(columns[c].encodeTo(sink, schema.getColumn(c)),
                    sink instanceof ChunkSink.Buffered buffered ? buffered : null);
            encoded.get(c).complete(result);
            return result;
        }
        catch (Throwable t) {
            encoded.get(c).completeExceptionally(t);
            throw t;
        }
    }

    /// Stops claiming and waits for every chunk already claimed, whatever its outcome. A claim
    /// either precedes the `getAndSet` and returns an index below what it returns, or follows it
    /// and returns one past the last chunk, so the chunks below it are exactly those claimed.
    private void stop() {
        int claimed = Math.min(next.getAndSet(columns.length), columns.length);
        for (int c = 0; c < claimed; c++) {
            encoded.get(c).exceptionally(t -> null).join();
        }
    }

    /// The encoded chunk, or its failure rethrown as raised.
    private static Encoded join(CompletableFuture<Encoded> chunk) throws IOException {
        try {
            return chunk.join();
        }
        catch (CompletionException e) {
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
            throw e;
        }
    }
}
