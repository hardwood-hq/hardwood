/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import dev.hardwood.Hardwood;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class S3MultiFileTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static S3MockContainer s3Mock = new S3MockContainer("latest");

    static S3Source source;

    @BeforeAll
    static void setup() throws Exception {
        source = S3Source.builder()
                .endpoint(s3Mock.getHttpEndpoint())
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"))
                .build();

        source.api().createBucket("test-bucket");
        source.api().putObject("test-bucket", "plain_uncompressed.parquet", Files.readAllBytes(
                TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
    }

    @AfterAll
    static void tearDown() {
        source.close();
    }

    @Test
    void readMultipleFiles() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
                MultiFileParquetReader reader = hardwood.openAll(
                        source.inputFilesInBucket("test-bucket",
                                "plain_uncompressed.parquet",
                                "plain_uncompressed.parquet"))) {
            try (RowReader rows = reader.createRowReader()) {
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
