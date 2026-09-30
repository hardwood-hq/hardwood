/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.Hardwood;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

class S3MultiFileIT {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    static TestBucket bucket = S3Proxy.get().bucketFor(S3MultiFileIT.class)
            .withObject("plain_uncompressed.parquet", TEST_RESOURCES.resolve("plain_uncompressed.parquet"));

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

    @Test
    void readMultipleFiles() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
                ParquetFileReader reader = hardwood.openAll(
                        source.inputFilesInBucket(bucket.name(),
                                "plain_uncompressed.parquet",
                                "plain_uncompressed.parquet"))) {
            try (RowReader rows = reader.rowReader()) {
                int count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(6); // 3 rows x 2 files
            }
        }
    }
}
