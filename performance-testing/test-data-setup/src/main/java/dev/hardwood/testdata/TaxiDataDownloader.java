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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

/**
 * Downloads NYC Yellow Taxi Trip Records for performance testing.
 */
public final class TaxiDataDownloader {

    private static final String BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/";

    public static final YearMonth DEFAULT_START = YearMonth.of(2016, 1);
    public static final YearMonth DEFAULT_END = YearMonth.of(2025, 11);
    public static final Path DATA_DIR = Path.of("target/tlc-trip-record-data");

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
        YearMonth requested = YearMonth.parse(property);
        return requested.isAfter(DEFAULT_END) ? DEFAULT_END : requested;
    }

    private static void downloadRange(Path dataDir, YearMonth start, YearMonth end) throws IOException {
        Files.createDirectories(dataDir);
        for (YearMonth ym = start; !ym.isAfter(end); ym = ym.plusMonths(1)) {
            String filename = formatFilename(ym);
            Path target = dataDir.resolve(filename);
            if (!Files.exists(target)) {
                downloadFile(BASE_URL + filename, target);
            }
        }
    }

    private static void downloadFile(String url, Path target) {
        System.out.println("Downloading: " + url);
        try {
            HttpClient client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<Path> response = client.send(request,
                    HttpResponse.BodyHandlers.ofFile(target));
            if (response.statusCode() != 200) {
                Files.deleteIfExists(target);
                System.out.println("  Failed (status " + response.statusCode() + ") - skipping");
            }
            else {
                System.out.println("  Downloaded: " + Files.size(target) + " bytes");
            }
        }
        catch (Exception e) {
            System.out.println("  Failed: " + e.getMessage() + " - skipping");
            try {
                Files.deleteIfExists(target);
            }
            catch (IOException ignored) {
            }
        }
    }

    private TaxiDataDownloader() {
    }
}
