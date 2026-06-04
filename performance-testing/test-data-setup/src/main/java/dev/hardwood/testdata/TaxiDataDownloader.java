/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testdata;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/// Downloads NYC Yellow Taxi Trip Records for performance testing.
///
/// The trip-data files are served from a CloudFront distribution that rate-limits and, for
/// shared cloud egress IPs, intermittently blocks bulk fetches with `403`/`429` responses. To
/// survive that, every download is retried with exponential back-off, requests are throttled to
/// avoid tripping the limiter, and a non-recoverable failure throws with the response status,
/// the distinguishing headers (`server`, `x-cache`, `x-amzn-waf-action`, …) and a snippet of the
/// error body so the cause (WAF block vs. missing object vs. throttling) is visible in the log.
public final class TaxiDataDownloader {

    private static final String BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/";

    public static final YearMonth DEFAULT_START = YearMonth.of(2016, 1);
    public static final YearMonth DEFAULT_END = YearMonth.of(2025, 12);
    public static final Path DATA_DIR = Path.of("target/tlc-trip-record-data");

    /// Total attempts per file before giving up (1 initial try + retries).
    private static final int MAX_ATTEMPTS = 5;
    /// Base back-off between retries; doubled each attempt and capped at [#MAX_BACKOFF_MILLIS].
    private static final long BASE_BACKOFF_MILLIS = 2_000L;
    private static final long MAX_BACKOFF_MILLIS = 60_000L;
    /// Polite delay inserted between consecutive file downloads to stay under the rate limit.
    private static final long THROTTLE_MILLIS = 1_000L;
    /// Cap on the error-body snippet captured for diagnostics.
    private static final int MAX_LOGGED_BODY = 2_000;

    /// HTTP statuses worth retrying: throttling/WAF (`403`, `429`) and transient server errors.
    /// A `404` is a genuinely absent object and is not retried.
    private static final Set<Integer> RETRYABLE_STATUSES = Set.of(403, 429, 500, 502, 503, 504);

    /// Performs a single HTTP GET, writing the body to `target`, and reports the [Outcome].
    @FunctionalInterface
    interface Fetcher {
        Outcome fetch(String url, Path target) throws IOException;
    }

    /// Suspends the current thread; abstracted so retry/back-off timing is testable without waiting.
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /// Result of one download attempt: the HTTP status, bytes written on success, and a
    /// human-readable `detail` (headers + error-body snippet) populated on failure.
    record Outcome(int status, long bytes, String detail) {
    }

    public static void main(String[] args) throws IOException {
        Path dataDir = getDataDirFromProperty();
        YearMonth start = getStartMonth();
        YearMonth end = getEndMonth();
        System.out.println("Downloading taxi data from " + start + " to " + end);
        downloadRange(dataDir, start, end);
        System.out.println("Download complete. Files in: " + dataDir.toAbsolutePath());
    }

    public static String formatFilename(YearMonth ym) {
        return String.format("yellow_tripdata_%d-%02d.parquet", ym.getYear(), ym.getMonthValue());
    }

    public static List<Path> getAvailableFiles(YearMonth start, YearMonth end) throws IOException {
        List<Path> files = new ArrayList<>();
        for (YearMonth ym = start; !ym.isAfter(end); ym = ym.plusMonths(1)) {
            Path file = DATA_DIR.resolve(formatFilename(ym));
            if (Files.exists(file) && Files.size(file) > 0) {
                files.add(file);
            }
        }
        return files;
    }

    private static Path getDataDirFromProperty() {
        String property = System.getProperty("data.dir");
        if (property == null || property.isBlank()) {
            return Path.of("target/tlc-trip-record-data");
        }
        return Path.of(property);
    }

    private static YearMonth getStartMonth() {
        String property = System.getProperty("perf.start");
        if (property == null || property.isBlank()) {
            return DEFAULT_START;
        }
        return YearMonth.parse(property);
    }

    private static YearMonth getEndMonth() {
        String property = System.getProperty("perf.end");
        if (property == null || property.isBlank()) {
            return DEFAULT_END;
        }
        return YearMonth.parse(property);
    }

    private static void downloadRange(Path dataDir, YearMonth start, YearMonth end) throws IOException {
        Files.createDirectories(dataDir);
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        Fetcher fetcher = (url, target) -> httpGet(client, url, target);
        Sleeper sleeper = Thread::sleep;

        boolean firstDownload = true;
        for (YearMonth ym = start; !ym.isAfter(end); ym = ym.plusMonths(1)) {
            String filename = formatFilename(ym);
            Path target = dataDir.resolve(filename);
            if (Files.exists(target)) {
                continue;
            }
            if (!firstDownload) {
                throttle(sleeper);
            }
            firstDownload = false;
            System.out.println("Downloading: " + BASE_URL + filename);
            downloadWithRetry(fetcher, sleeper, BASE_URL + filename, target, MAX_ATTEMPTS);
        }
    }

    /// Downloads `url` into `target`, retrying retryable failures with exponential back-off.
    /// Throws once a non-retryable status is seen or the attempts are exhausted, including the
    /// last response's diagnostics in the message.
    static void downloadWithRetry(Fetcher fetcher, Sleeper sleeper, String url, Path target, int maxAttempts)
            throws IOException {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Outcome outcome = fetcher.fetch(url, target);
            if (outcome.status() == 200 && outcome.bytes() > 0) {
                System.out.println("  Downloaded: " + outcome.bytes() + " bytes");
                return;
            }
            deleteQuietly(target);
            System.out.println("  Attempt " + attempt + "/" + maxAttempts + " failed: status "
                    + outcome.status() + describe(outcome));
            if (!isRetryable(outcome) || attempt == maxAttempts) {
                throw new IOException("Download failed for " + url + " after " + attempt
                        + " attempt(s); last status " + outcome.status() + describe(outcome));
            }
            long delay = backoffMillis(attempt);
            System.out.println("  Retrying in " + delay + " ms");
            try {
                sleeper.sleep(delay);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Download interrupted while backing off for " + url, e);
            }
        }
        throw new IOException("Download failed for " + url + ": exhausted " + maxAttempts + " attempts");
    }

    /// A `200` that produced no bytes is treated as a transient hiccup and retried; otherwise the
    /// status code decides.
    static boolean isRetryable(Outcome outcome) {
        if (outcome.status() == 200) {
            return outcome.bytes() == 0;
        }
        return RETRYABLE_STATUSES.contains(outcome.status());
    }

    static long backoffMillis(int attempt) {
        long shifted = BASE_BACKOFF_MILLIS << (attempt - 1);
        if (shifted <= 0 || shifted > MAX_BACKOFF_MILLIS) {
            return MAX_BACKOFF_MILLIS;
        }
        return shifted;
    }

    private static Outcome httpGet(HttpClient client, String url, Path target) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", "Wget")
                .GET()
                .build();
        try {
            HttpResponse<Path> response = client.send(request, HttpResponse.BodyHandlers.ofFile(target));
            int status = response.statusCode();
            if (status == 200) {
                return new Outcome(200, Files.size(target), null);
            }
            return new Outcome(status, 0, describeFailure(response, target));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Download interrupted for " + url, e);
        }
    }

    /// Builds a diagnostic string from the response headers that distinguish a WAF/edge block
    /// from an origin (S3) denial, plus a snippet of the error body written to `target`.
    private static String describeFailure(HttpResponse<Path> response, Path target) throws IOException {
        StringBuilder sb = new StringBuilder();
        appendHeader(sb, response, "server");
        appendHeader(sb, response, "x-cache");
        appendHeader(sb, response, "x-amz-cf-id");
        appendHeader(sb, response, "x-amzn-waf-action");
        appendHeader(sb, response, "retry-after");
        String body = readBodySnippet(target);
        if (!body.isBlank()) {
            sb.append("body=").append(body);
        }
        return sb.toString().strip();
    }

    private static void appendHeader(StringBuilder sb, HttpResponse<?> response, String name) {
        response.headers().firstValue(name).ifPresent(value -> sb.append(name).append('=').append(value).append("; "));
    }

    private static String readBodySnippet(Path target) throws IOException {
        if (!Files.exists(target)) {
            return "";
        }
        byte[] bytes = Files.readAllBytes(target);
        int length = Math.min(bytes.length, MAX_LOGGED_BODY);
        return new String(bytes, 0, length, StandardCharsets.UTF_8).replaceAll("\\s+", " ").strip();
    }

    private static String describe(Outcome outcome) {
        if (outcome.detail() == null || outcome.detail().isBlank()) {
            return "";
        }
        return " (" + outcome.detail() + ")";
    }

    private static void throttle(Sleeper sleeper) throws IOException {
        try {
            sleeper.sleep(THROTTLE_MILLIS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while throttling between downloads", e);
        }
    }

    private static void deleteQuietly(Path target) {
        try {
            Files.deleteIfExists(target);
        }
        catch (IOException e) {
            System.out.println("  Warning: could not delete partial file " + target + ": " + e.getMessage());
        }
    }

    private TaxiDataDownloader() {
    }
}
