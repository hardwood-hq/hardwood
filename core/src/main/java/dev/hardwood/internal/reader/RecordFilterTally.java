/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.jfr.RecordFilterEvent;

/// Accumulates the outcome of record-level predicate evaluation and emits one
/// [RecordFilterEvent] per file.
///
/// A file is the finest granularity the read pipeline maintains below the whole
/// read: batches are flushed at file boundaries, so a batch — and every record in
/// it — belongs to exactly one file, while a batch may span row groups. The
/// readers call [#switchFile] as they advance onto a batch from the next file,
/// which closes out the counts gathered so far, so no counting has to happen on
/// the per-record path beyond the two increments themselves.
///
/// Counts are closed out on each *change* of file, so a read that lists the same
/// path twice in a row reports one event covering both passes over it.
///
/// Not thread-safe, and does not need to be: every path that feeds it — the
/// batch-matcher batch counts, the per-record wrapper, and the column-reader selection
/// — runs on the consumer thread. Moving any of that onto a decode thread would need
/// this revisited.
public final class RecordFilterTally {

    private String file;
    private long evaluated;
    private long kept;

    /// Attributes subsequent counts to `fileName`, emitting the counts gathered
    /// for the previous file. Idempotent for repeated calls naming the same file,
    /// which is the common case: readers call this on every batch.
    public void switchFile(String fileName) {
        if (file != null && !file.equals(fileName)) {
            emit();
        }
        file = fileName;
    }

    /// Records one record-level predicate evaluation and whether it matched.
    public void record(boolean matched) {
        evaluated++;
        if (matched) {
            kept++;
        }
    }

    /// Records a batch the predicate decided on in one go, as the batch-matcher and
    /// column-reader paths do.
    public void recordBatch(int evaluatedRecords, int matchedRecords) {
        evaluated += evaluatedRecords;
        kept += matchedRecords;
    }

    /// Emits the counts of the file the read ended on. Safe to call more than once
    /// — the second call has nothing left to report.
    public void close() {
        emit();
    }

    private void emit() {
        if (evaluated == 0) {
            return;
        }
        RecordFilterEvent event = new RecordFilterEvent();
        event.file = file;
        event.totalRecords = evaluated;
        event.recordsKept = kept;
        event.recordsSkipped = evaluated - kept;
        event.commit();
        evaluated = 0;
        kept = 0;
    }
}
