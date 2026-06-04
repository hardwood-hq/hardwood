/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testdata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.testdata.TaxiDataDownloader.Outcome;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaxiDataDownloaderTest {

    private static final Path TARGET = Path.of("target/does-not-matter.parquet");

    /// Records the requested sleep durations instead of actually waiting.
    private static final class RecordingSleeper implements TaxiDataDownloader.Sleeper {
        private final List<Long> delays = new ArrayList<>();

        @Override
        public void sleep(long millis) {
            delays.add(millis);
        }
    }

    /// Replays a queued sequence of outcomes, one per attempt.
    private static TaxiDataDownloader.Fetcher fetcherReturning(Outcome... outcomes) {
        Deque<Outcome> queue = new ArrayDeque<>(List.of(outcomes));
        return (url, target) -> queue.poll();
    }

    @Test
    void succeedsOnFirstAttemptWithoutSleeping() throws IOException {
        RecordingSleeper sleeper = new RecordingSleeper();

        TaxiDataDownloader.downloadWithRetry(
                fetcherReturning(new Outcome(200, 1234, null)), sleeper, "http://x", TARGET, 5);

        assertThat(sleeper.delays).isEmpty();
    }

    @Test
    void retriesRetryableStatusThenSucceeds() throws IOException {
        RecordingSleeper sleeper = new RecordingSleeper();

        TaxiDataDownloader.downloadWithRetry(
                fetcherReturning(
                        new Outcome(503, 0, "server=AmazonS3"),
                        new Outcome(403, 0, "x-amzn-waf-action=BLOCK"),
                        new Outcome(200, 999, null)),
                sleeper, "http://x", TARGET, 5);

        // Two failures before success => two back-offs, doubling each time.
        assertThat(sleeper.delays).containsExactly(2_000L, 4_000L);
    }

    @Test
    void retriesEmptyBodyResponses() throws IOException {
        RecordingSleeper sleeper = new RecordingSleeper();

        TaxiDataDownloader.downloadWithRetry(
                fetcherReturning(new Outcome(200, 0, null), new Outcome(200, 42, null)),
                sleeper, "http://x", TARGET, 5);

        assertThat(sleeper.delays).containsExactly(2_000L);
    }

    @Test
    void failsFastOnNonRetryableStatus() {
        RecordingSleeper sleeper = new RecordingSleeper();

        assertThatThrownBy(() -> TaxiDataDownloader.downloadWithRetry(
                fetcherReturning(new Outcome(404, 0, "server=AmazonS3; body=NoSuchKey")),
                sleeper, "http://x", TARGET, 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("404")
                .hasMessageContaining("after 1 attempt");

        assertThat(sleeper.delays).isEmpty();
    }

    @Test
    void givesUpAfterMaxAttemptsOnPersistentFailure() {
        RecordingSleeper sleeper = new RecordingSleeper();

        assertThatThrownBy(() -> TaxiDataDownloader.downloadWithRetry(
                fetcherReturning(
                        new Outcome(403, 0, "x-amzn-waf-action=BLOCK"),
                        new Outcome(403, 0, "x-amzn-waf-action=BLOCK"),
                        new Outcome(403, 0, "x-amzn-waf-action=BLOCK")),
                sleeper, "http://x", TARGET, 3))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("after 3 attempt")
                .hasMessageContaining("x-amzn-waf-action=BLOCK");

        // Slept between attempts 1->2 and 2->3, but not after the final failure.
        assertThat(sleeper.delays).containsExactly(2_000L, 4_000L);
    }

    @Test
    void backoffGrowsExponentiallyAndIsCapped() {
        assertThat(TaxiDataDownloader.backoffMillis(1)).isEqualTo(2_000L);
        assertThat(TaxiDataDownloader.backoffMillis(2)).isEqualTo(4_000L);
        assertThat(TaxiDataDownloader.backoffMillis(3)).isEqualTo(8_000L);
        assertThat(TaxiDataDownloader.backoffMillis(10)).isEqualTo(60_000L);
        assertThat(TaxiDataDownloader.backoffMillis(40)).isEqualTo(60_000L);
    }

    @Test
    void classifiesStatusesForRetry() {
        assertThat(TaxiDataDownloader.isRetryable(new Outcome(403, 0, null))).isTrue();
        assertThat(TaxiDataDownloader.isRetryable(new Outcome(429, 0, null))).isTrue();
        assertThat(TaxiDataDownloader.isRetryable(new Outcome(503, 0, null))).isTrue();
        assertThat(TaxiDataDownloader.isRetryable(new Outcome(404, 0, null))).isFalse();
        assertThat(TaxiDataDownloader.isRetryable(new Outcome(200, 0, null))).isTrue();
        assertThat(TaxiDataDownloader.isRetryable(new Outcome(200, 5, null))).isFalse();
    }
}
