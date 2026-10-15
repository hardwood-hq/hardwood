/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import dev.hardwood.s3.S3Proxy;
import dev.hardwood.s3.TestBucket;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke test for the native CLI binary against S3. Proves the AWS SDK baked
/// into the native image can reach an S3 endpoint, authenticate, and read an
/// object, and that the binary falls back to unsigned requests when no
/// credentials are configured. AWS connection properties are passed as
/// environment variables to the native binary subprocess.
@Tag("native")
class NativeBinaryS3SmokeIT {

    private static final String nativeBinary = System.getProperty("native.image.path");
    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    private static TestBucket bucket;
    private static String emptyFile;

    @BeforeAll
    static void createBucket() throws IOException {
        bucket = S3Proxy.get().bucketFor(NativeBinaryS3SmokeIT.class)
                .withObject("plain_uncompressed.parquet",
                        Files.readAllBytes(TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
        bucket.create();
        // s3proxy serves unsigned requests for files readable by others
        Files.setPosixFilePermissions(bucket.objectPath("plain_uncompressed.parquet"),
                PosixFilePermissions.fromString("rw-r--r--"));
        emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();
    }

    @AfterAll
    static void deleteBucket() {
        if (bucket != null) {
            bucket.delete();
        }
    }

    @Test
    void readsFileFromS3() throws IOException, InterruptedException {
        Map<String, String> environment = connection();
        environment.put("AWS_ACCESS_KEY_ID", S3Proxy.ACCESS_KEY);
        environment.put("AWS_SECRET_ACCESS_KEY", S3Proxy.SECRET_KEY);

        assertReadsSchema(environment);
    }

    @Test
    void readsPublicFileUnsigned() throws IOException, InterruptedException {
        Map<String, String> environment = connection();
        environment.put("AWS_EC2_METADATA_DISABLED", "true");

        assertReadsSchema(environment);
    }

    private static Map<String, String> connection() {
        Map<String, String> environment = new HashMap<>();
        environment.put("AWS_REGION", "us-east-1");
        environment.put("AWS_ENDPOINT_URL", bucket.endpoint());
        environment.put("AWS_PATH_STYLE", "true");
        environment.put("AWS_CONFIG_FILE", emptyFile);
        environment.put("AWS_SHARED_CREDENTIALS_FILE", emptyFile);
        return environment;
    }

    private static void assertReadsSchema(Map<String, String> environment) throws IOException, InterruptedException {
        Cli.Result result = CliProcess.runNative(nativeBinary, environment,
                "schema", "-f", bucket.uri("plain_uncompressed.parquet"));

        assertThat(result.exitCode())
                .withFailMessage("S3 read failed: stdout=%s stderr=%s", result.output(), result.errorOutput())
                .isZero();
        assertThat(result.output()).contains("message schema");
    }
}
