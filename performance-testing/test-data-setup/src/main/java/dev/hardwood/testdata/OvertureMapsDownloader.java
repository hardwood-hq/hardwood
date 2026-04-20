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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Downloads an Overture Maps places Parquet file for nested schema performance testing.
///
/// The download URL is resolved at runtime from the public STAC catalog at
/// `https://stac.overturemaps.org/`, so the downloader always fetches the current
/// release rather than relying on a hard-coded identifier that may have been rotated
/// out of the S3 bucket.
public final class OvertureMapsDownloader {

    private static final String STAC_ROOT = "https://stac.overturemaps.org/catalog.json";
    private static final String STAC_ITEM_TEMPLATE =
            "https://stac.overturemaps.org/%s/places/place/00000/00000.json";

    private static final Pattern LATEST_RELEASE_PATTERN =
            Pattern.compile("\"latest\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern AWS_HREF_PATTERN =
            Pattern.compile("\"aws\"\\s*:\\s*\\{[^}]*?\"href\"\\s*:\\s*\"(https://[^\"]+\\.parquet)\"");

    private static final String TARGET_FILENAME = "overture_places.zstd.parquet";

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    public static void main(String[] args) throws IOException, InterruptedException {
        Path dataDir = getDataDirFromProperty();
        Files.createDirectories(dataDir);
        Path target = dataDir.resolve(TARGET_FILENAME);

        if (Files.exists(target) && Files.size(target) > 0) {
            System.out.println("Overture Maps file already exists: " + target.toAbsolutePath()
                    + " (" + Files.size(target) + " bytes)");
            return;
        }

        String url = resolveLatestPlacesUrl();
        downloadFile(url, target);
        System.out.println("Download complete. File at: " + target.toAbsolutePath()
                + " (" + Files.size(target) + " bytes)");
    }

    private static Path getDataDirFromProperty() {
        String property = System.getProperty("data.dir");
        if (property == null || property.isBlank()) {
            return Path.of("target/overture-maps-data");
        }
        return Path.of(property);
    }

    private static String resolveLatestPlacesUrl() throws IOException, InterruptedException {
        String root = fetchString(STAC_ROOT);
        String release = extract(LATEST_RELEASE_PATTERN, root, STAC_ROOT, "latest");
        System.out.println("Resolved latest Overture release: " + release);

        String itemUrl = String.format(STAC_ITEM_TEMPLATE, release);
        String item = fetchString(itemUrl);
        String href = extract(AWS_HREF_PATTERN, item, itemUrl, "assets.aws.href");
        System.out.println("Resolved places Parquet URL: " + href);
        return href;
    }

    private static String fetchString(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Unexpected status " + response.statusCode() + " from " + url);
        }
        return response.body();
    }

    private static String extract(Pattern pattern, String body, String sourceUrl, String field)
            throws IOException {
        Matcher matcher = pattern.matcher(body);
        if (!matcher.find()) {
            String excerpt = body.length() > 200 ? body.substring(0, 200) + "..." : body;
            throw new IOException("Could not find '" + field + "' in STAC document at "
                    + sourceUrl + "; body starts with: " + excerpt);
        }
        return matcher.group(1);
    }

    private static void downloadFile(String url, Path target) throws IOException, InterruptedException {
        System.out.println("Downloading: " + url);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        try {
            HttpResponse<Path> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofFile(target));
            if (response.statusCode() != 200) {
                Files.deleteIfExists(target);
                throw new IOException("Download failed with status " + response.statusCode() + " for " + url);
            }
            if (Files.size(target) == 0) {
                Files.deleteIfExists(target);
                throw new IOException("Download produced an empty file for " + url);
            }
        }
        catch (IOException | InterruptedException e) {
            Files.deleteIfExists(target);
            throw e;
        }
    }

    private OvertureMapsDownloader() {
    }
}
