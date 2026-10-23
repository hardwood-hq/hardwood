/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.MetadataSource;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// A footer supplied by a [MetadataSource] is checked against an S3 object without a request
/// beyond the one that opens it. [S3InputFileIdentityTest] covers the `ETag` identity, which
/// the s3proxy file-system backend does not send.
class S3MetadataSourceIT {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    /// Its footer lies inside the tail [S3InputFile#open()] pre-fetches.
    private static final String SMALL_FOOTER = "plain_uncompressed.parquet";
    /// 400 row groups of 8 columns: the footer is larger than the pre-fetched tail.
    private static final String LARGE_FOOTER = "large_footer.parquet";

    static TestBucket bucket = S3Proxy.get().bucketFor(S3MetadataSourceIT.class)
            .withObject(SMALL_FOOTER, TEST_RESOURCES.resolve(SMALL_FOOTER))
            .withObject(LARGE_FOOTER, TestParquetGenerator.generate(400, 10, 8));

    static S3Source source;

    @BeforeAll
    static void setup() {
        bucket.create();
        source = S3Source.builder()
                .endpoint(bucket.endpoint())
                .pathStyle(true)
                .credentials(S3Credentials.of(S3Proxy.ACCESS_KEY, S3Proxy.SECRET_KEY))
                .build();
    }

    @AfterAll
    static void tearDown() {
        source.close();
        bucket.delete();
    }

    @AfterEach
    void clearTailSize() {
        System.clearProperty(S3Fetcher.TAIL_BYTES_PROPERTY);
    }

    @Test
    void aSuppliedFooterInsideThePrefetchedTailCostsNoRequest() throws Exception {
        assertThat(footerOf(SMALL_FOOTER).footerLength()).isLessThan(S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE - 8);

        assertThat(open(SMALL_FOOTER, null).requests()).isEqualTo(1);
        assertThat(open(SMALL_FOOTER, cachingSource()).requests()).isEqualTo(1);
    }

    @Test
    void aSuppliedFooterOutsideThePrefetchedTailSavesItsRequest() throws Exception {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE));
        assertThat(footerOf(LARGE_FOOTER).footerLength()).isGreaterThan(S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE);

        assertThat(open(LARGE_FOOTER, null).requests()).isEqualTo(2);
        assertThat(open(LARGE_FOOTER, cachingSource()).requests()).isEqualTo(1);
    }

    /// A tail larger than the file: an open whose footer is read from the file fetches the file
    /// whole, one whose footer is supplied no more than it would with the smaller tail.
    @Test
    void aSuppliedFooterKeepsTheOpenRequestAtItsSize() throws Exception {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(4 * 1024 * 1024));
        long length = lengthOf(LARGE_FOOTER);
        assertThat(length).isGreaterThan(S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE).isLessThan(4 * 1024 * 1024);

        assertThat(open(LARGE_FOOTER, null)).isEqualTo(new Opened(1, length));
        assertThat(open(LARGE_FOOTER, cachingSource())).isEqualTo(new Opened(1, S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE));
    }

    /// The requests an open issued and the bytes they fetched.
    private record Opened(long requests, long bytes) {
    }

    /// Opens `key`, with `metadataSource` installed when not `null`.
    private static Opened open(String key, MetadataSource metadataSource) throws IOException {
        HardwoodContext.Builder builder = HardwoodContext.builder();
        if (metadataSource != null) {
            builder.metadataSource(metadataSource);
            // Warm the source, so the open measured below is served from it.
            try (HardwoodContext context = builder.build();
                    ParquetFileReader reader = ParquetFileReader.open(source.inputFile(bucket.name(), key), context)) {
                assertThat(reader.getFileCount()).isEqualTo(1);
            }
        }
        S3InputFile file = source.inputFile(bucket.name(), key);
        try (HardwoodContext context = builder.build();
                ParquetFileReader reader = ParquetFileReader.open(file, context)) {
            return new Opened(file.networkRequestCount(), file.networkBytesFetched());
        }
    }

    private static MetadataSource cachingSource() {
        Map<String, ParsedFooter> cache = new ConcurrentHashMap<>();
        return file -> {
            String key = file.name() + "|" + file.identity().orElse("");
            ParsedFooter footer = cache.get(key);
            if (footer == null) {
                footer = ParsedFooter.readFrom(file);
                cache.put(key, footer);
            }
            return footer;
        };
    }

    private static long lengthOf(String key) throws IOException {
        try (S3InputFile file = source.inputFile(bucket.name(), key)) {
            file.open();
            return file.length();
        }
    }

    private static ParsedFooter footerOf(String key) throws IOException {
        try (S3InputFile file = source.inputFile(bucket.name(), key)) {
            file.open();
            return ParsedFooter.readFrom(file);
        }
    }
}
