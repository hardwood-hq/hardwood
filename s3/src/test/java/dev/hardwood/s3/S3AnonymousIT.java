/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads through an anonymous [S3Source]. s3proxy's filesystem backend serves an object
/// to unsigned requests when its file is readable by others, and denies them otherwise.
class S3AnonymousIT {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    static TestBucket bucket;
    static S3Source source;

    @BeforeAll
    static void setup() throws IOException {
        byte[] content = Files.readAllBytes(TEST_RESOURCES.resolve("plain_uncompressed.parquet"));
        bucket = S3Proxy.get().bucketFor(S3AnonymousIT.class)
                .withObject("public.parquet", content)
                .withObject("private.parquet", content);
        bucket.create();
        Files.setPosixFilePermissions(bucket.objectPath("public.parquet"),
                PosixFilePermissions.fromString("rw-r--r--"));
        Files.setPosixFilePermissions(bucket.objectPath("private.parquet"),
                PosixFilePermissions.fromString("rw-r-----"));
        source = S3Source.builder()
                .endpoint(bucket.endpoint())
                .pathStyle(true)
                .anonymous()
                .build();
    }

    @AfterAll
    static void tearDown() {
        source.close();
        bucket.delete();
    }

    @Test
    void readsPublicObject() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(bucket.uri("public.parquet")));
                RowReader rows = reader.rowReader()) {
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertThat(count).isEqualTo(3);
        }
    }

    @Test
    void deniedObjectNamesUnsignedRequest() {
        assertThatThrownBy(() -> ParquetFileReader.open(source.inputFile(bucket.uri("private.parquet"))))
                .isInstanceOf(IOException.class)
                .hasMessage("""
                        Failed to open %s: HTTP 403 \
                        <?xml version='1.0' encoding='UTF-8'?><Error><Code>AccessDenied</Code>\
                        <Message>Forbidden</Message><RequestId>4442587FB7D0A2F9</RequestId></Error> \
                        (the request was sent unsigned, without credentials)""".formatted(bucket.uri("private.parquet")));
    }
}
