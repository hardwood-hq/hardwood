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

/**
 * Downloads an Overture Maps places Parquet file for nested schema performance testing.
 */
public final class OvertureMapsDownloader {

    private static final String FILE_URL =
            "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-02-18.0/"
            + "theme=places/type=place/part-00000-308cb36d-c529-4dc2-83bb-fe6b282a2b1a-c000.zstd.parquet";

    private static final String TARGET_FILENAME = "overture_places.zstd.parquet";

    public static void main(String[] args) throws IOException {
        Path dataDir = getDataDirFromProperty();
        Files.createDirectories(dataDir);
        Path target = dataDir.resolve(TARGET_FILENAME);

        if (Files.exists(target) && Files.size(target) > 0) {
            System.out.println("Overture Maps file already exists: " + target.toAbsolutePath()
                    + " (" + Files.size(target) + " bytes)");
            return;
        }

        downloadFile(FILE_URL, target);
        System.out.println("Download complete. File at: " + target.toAbsolutePath());
    }

    private static Path getDataDirFromProperty() {
        String property = System.getProperty("data.dir");
        if (property == null || property.isBlank()) {
            return Path.of("target/overture-maps-data");
        }
        return Path.of(property);
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

    private OvertureMapsDownloader() {
    }
}
