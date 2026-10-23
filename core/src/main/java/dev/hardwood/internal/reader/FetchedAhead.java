/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.hardwood.InputFile;

/// The data a read fetched for the files it opened ahead ([FileOpenAhead]): per file at most one
/// span, a read of its first row group's column chunks started before the read's columns reached
/// the row group. A read of a range the span covers takes its bytes from the span
/// ([RangeRead#start]) instead of reading them again.
///
/// A span is charged to the context's [ReadAheadBuffer] from its start until the first read takes
/// from it, when its bytes become the row group's, which its fetch plans hold as they hold every
/// read of the row group. It is dropped once every column has released the row group, or when the
/// read closes, releasing its charge if no read took from it.
final class FetchedAhead {

    private final ReadAheadBuffer buffer;
    /// Keyed by identity: the list a read opens may hold the same file twice, and the first of the
    /// two that is opened ahead holds the span.
    private final Map<InputFile, Span> spans = new IdentityHashMap<>();

    FetchedAhead(ReadAheadBuffer buffer) {
        this.buffer = buffer;
    }

    /// A span: the read of `length` bytes at `offset` of its file, the work item whose row group
    /// it covers, and whether its charge is still held.
    private record Span(RangeRead read, long offset, int length, int workItemIndex, AtomicBoolean charged) {

        boolean covers(long rangeOffset, int rangeLength) {
            return rangeOffset >= offset && rangeOffset + rangeLength <= offset + length;
        }
    }

    /// Holds `read`, charged to the buffer under its length, as the span of `inputFile`'s work item
    /// `workItemIndex`.
    ///
    /// @return `false` when the file already has a span, in which case the caller releases the
    ///         charge
    synchronized boolean add(InputFile inputFile, int workItemIndex, RangeRead read) {
        if (spans.containsKey(inputFile)) {
            return false;
        }
        spans.put(inputFile, new Span(read, read.offset(), read.length(), workItemIndex, new AtomicBoolean(true)));
        return true;
    }

    /// A read of the range that `inputFile`'s span serves, or `null` when it has none covering the
    /// range. The first read the span serves releases its charge.
    RangeRead serve(InputFile inputFile, long offset, int length, String composedReason,
                    RangeRead.Priority priority, PrefetchTasks tasks) {
        Span span;
        synchronized (this) {
            span = spans.get(inputFile);
        }
        if (span == null || !span.covers(offset, length)) {
            return null;
        }
        release(span);
        return RangeRead.servedBy(span.read(), inputFile, offset, length, composedReason, priority, tasks);
    }

    /// Drops the span of the work item `workItemIndex`, if one is held.
    void drop(int workItemIndex) {
        Span dropped = null;
        synchronized (this) {
            for (Map.Entry<InputFile, Span> entry : spans.entrySet()) {
                if (entry.getValue().workItemIndex() == workItemIndex) {
                    dropped = entry.getValue();
                    spans.remove(entry.getKey());
                    break;
                }
            }
        }
        if (dropped != null) {
            release(dropped);
        }
    }

    /// Drops every span, releasing the charges no read took.
    void close() {
        List<Span> dropped;
        synchronized (this) {
            dropped = new ArrayList<>(spans.values());
            spans.clear();
        }
        for (Span span : dropped) {
            release(span);
        }
    }

    private void release(Span span) {
        if (span.charged().compareAndSet(true, false)) {
            buffer.release(span.length());
        }
    }
}
