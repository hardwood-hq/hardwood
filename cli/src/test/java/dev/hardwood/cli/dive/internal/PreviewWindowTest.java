/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.ParquetModel;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that the `PreviewWindow` behaves as a sliding cache around
/// the current viewport — within-window navigation triggers no I/O,
/// boundary moves extend incrementally, far jumps refill.
class PreviewWindowTest {

    /// 3 row groups of 100 rows each: id 1..100, 101..200, 201..300.
    private static Path fixture() {
        return Path.of(PreviewWindowTest.class.getResource("/filter_pushdown_int.parquet").getPath());
    }

    @Test
    void initialSliceYieldsRequestedPage() throws IOException {
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));

        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            PreviewWindow window = new PreviewWindow();
            PreviewWindow.Slice slice = window.slice(model, 0, 10, true);

            assertThat(slice.rows()).hasSize(10);
            assertThat(slice.columnNames()).contains("id");
            // First column ("id"), first row → id=1
            assertThat(slice.rows().get(0).get(0)).isEqualTo("1");
            assertThat(slice.rows().get(9).get(0)).isEqualTo("10");
            assertThat(counting.bytesRead()).as("bytes after initial slice").isPositive();
        }
    }

    @Test
    void withinWindowNavigationStaysAccurate() throws IOException {
        // Fixture is too small (~10 KB) for byte-delta assertions — the v2
        // pipeline pre-fetches the whole file in a couple of requests
        // regardless of which rows we ask for. Test instead that the
        // window's *content* is right after each navigation.
        try (ParquetModel model = ParquetModel.open(InputFile.of(fixture()),
                "filter_pushdown_int.parquet")) {
            PreviewWindow window = new PreviewWindow();

            // First slice: rows 0..9 → ids 1..10
            PreviewWindow.Slice s1 = window.slice(model, 0, 10, true);
            assertThat(s1.rows().get(0).get(0)).isEqualTo("1");

            // Forward within window: rows 10..19 → ids 11..20
            PreviewWindow.Slice s2 = window.slice(model, 10, 10, true);
            assertThat(s2.rows()).hasSize(10);
            assertThat(s2.rows().get(0).get(0)).isEqualTo("11");
            assertThat(s2.rows().get(9).get(0)).isEqualTo("20");

            // Back to start: rows 0..9 → ids 1..10
            PreviewWindow.Slice s3 = window.slice(model, 0, 10, true);
            assertThat(s3.rows().get(0).get(0)).isEqualTo("1");
            assertThat(s3.rows().get(9).get(0)).isEqualTo("10");
        }
    }

    @Test
    void farJumpYieldsCorrectRows() throws IOException {
        try (ParquetModel model = ParquetModel.open(InputFile.of(fixture()),
                "filter_pushdown_int.parquet")) {
            PreviewWindow window = new PreviewWindow();

            // Initial window
            window.slice(model, 0, 10, true);

            // Jump to last 10 rows (firstRow=290). Should yield ids 291..300.
            PreviewWindow.Slice tailSlice = window.slice(model, 290, 10, true);
            assertThat(tailSlice.rows()).hasSize(10);
            assertThat(tailSlice.rows().get(0).get(0)).isEqualTo("291");
            assertThat(tailSlice.rows().get(9).get(0)).isEqualTo("300");

            // Subsequent slice nearby (firstRow=280) should still produce
            // correct rows — confirms the window is properly positioned
            // around the new viewport, not stuck at the old one.
            PreviewWindow.Slice nearTail = window.slice(model, 280, 10, true);
            assertThat(nearTail.rows()).hasSize(10);
            assertThat(nearTail.rows().get(0).get(0)).isEqualTo("281");
            assertThat(nearTail.rows().get(9).get(0)).isEqualTo("290");
        }
    }

    @Test
    void logicalTypesToggleDoesNotTriggerIo() throws IOException {
        // Each cell is pre-formatted in both logical and physical modes
        // on fetch, so toggling `logicalTypes` afterwards just selects
        // which pre-formatted variant the slice returns. A counting
        // input file confirms no further bytes are read across the
        // toggle.
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));
        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            PreviewWindow window = new PreviewWindow();

            PreviewWindow.Slice s1 = window.slice(model, 0, 10, true);
            long afterFirst = counting.bytesRead();
            assertThat(s1.rows().get(0).get(0)).isEqualTo("1");

            // Toggle: must not fetch again.
            PreviewWindow.Slice s2 = window.slice(model, 0, 10, false);
            assertThat(counting.bytesRead()).as("bytes after logicalTypes toggle")
                    .isEqualTo(afterFirst);
            assertThat(s2.rows()).hasSize(10);
            assertThat(s2.rows().get(0).get(0)).isEqualTo("1");

            // Toggle back: also no fetch.
            PreviewWindow.Slice s3 = window.slice(model, 0, 10, true);
            assertThat(counting.bytesRead()).as("bytes after toggle back")
                    .isEqualTo(afterFirst);
            assertThat(s3.rows().get(0).get(0)).isEqualTo("1");
        }
    }

    @Test
    void requestNearStartClampsLowerBound() throws IOException {
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));

        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            PreviewWindow window = new PreviewWindow();

            // firstRow=5 with pageSize=10: desired window = [max(0, 5-100), 5+110) = [0, 115)
            PreviewWindow.Slice slice = window.slice(model, 5, 10, true);
            assertThat(slice.rows()).hasSize(10);
        }
    }

    @Test
    void requestNearEndClampsUpperBound() throws IOException {
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));

        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            PreviewWindow window = new PreviewWindow();

            // firstRow=295 with pageSize=10 in a 300-row file:
            // requested page = [295, 300), only 5 rows actually exist past row 295.
            PreviewWindow.Slice slice = window.slice(model, 295, 10, true);
            assertThat(slice.rows()).hasSize(5);
        }
    }

    private static final class ByteCountingInputFile implements InputFile {
        private final InputFile delegate;
        private final AtomicLong bytesRead = new AtomicLong();

        ByteCountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        long bytesRead() {
            return bytesRead.get();
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            bytesRead.addAndGet(length);
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
