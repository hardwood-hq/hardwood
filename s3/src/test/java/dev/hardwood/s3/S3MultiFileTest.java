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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import dev.hardwood.Hardwood;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class S3MultiFileTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static GenericContainer<?> s3 = S3ProxyContainers.filesystemBacked()
            .withCopyFileToContainer(
                    MountableFile.forHostPath(TEST_RESOURCES.resolve("plain_uncompressed.parquet")),
                    S3ProxyContainers.objectPath("plain_uncompressed.parquet"));

    static S3Source source;

    @BeforeAll
    static void setup() {
        source = S3Source.builder()
                .endpoint(S3ProxyContainers.endpoint(s3))
                .pathStyle(true)
                .credentials(S3Credentials.of(S3ProxyContainers.ACCESS_KEY, S3ProxyContainers.SECRET_KEY))
                .build();
    }

    @AfterAll
    static void tearDown() {
        source.close();
    }

    @Test
    void readMultipleFiles() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
                ParquetFileReader reader = hardwood.openAll(
                        source.inputFilesInBucket("test-bucket",
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
