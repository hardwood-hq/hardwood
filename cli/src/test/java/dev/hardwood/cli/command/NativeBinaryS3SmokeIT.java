/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import dev.hardwood.s3.S3ProxyContainers;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke test for the native CLI binary against S3. Proves the AWS SDK baked
/// into the native image can reach an S3 endpoint, authenticate, and read an
/// object. AWS connection properties are passed as environment variables to
/// the native binary subprocess.
class NativeBinaryS3SmokeIT {

    private static final String nativeBinary = System.getProperty("native.image.path");
    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    private static GenericContainer<?> s3;
    private static String emptyFile;

    @BeforeAll
    static void startS3Proxy() throws IOException {
        s3 = S3ProxyContainers.filesystemBacked()
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_RESOURCES.resolve("plain_uncompressed.parquet")),
                        S3ProxyContainers.objectPath("plain_uncompressed.parquet"));
        s3.start();
        emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();
    }

    @AfterAll
    static void stopS3Proxy() {
        if (s3 != null) {
            s3.stop();
        }
    }

    @Test
    void readsFileFromS3() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(
                nativeBinary,
                "schema", "-f", "s3://test-bucket/plain_uncompressed.parquet")
                .redirectErrorStream(false);

        // Pass AWS connection properties as environment variables
        pb.environment().put("AWS_ACCESS_KEY_ID", S3ProxyContainers.ACCESS_KEY);
        pb.environment().put("AWS_SECRET_ACCESS_KEY", S3ProxyContainers.SECRET_KEY);
        pb.environment().put("AWS_REGION", "us-east-1");
        pb.environment().put("AWS_ENDPOINT_URL", S3ProxyContainers.endpoint(s3));
        pb.environment().put("AWS_PATH_STYLE", "true");
        pb.environment().put("AWS_CONFIG_FILE", emptyFile);
        pb.environment().put("AWS_SHARED_CREDENTIALS_FILE", emptyFile);

        Process process = pb.start();
        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        assertThat(finished).withFailMessage("Process timed out after 30s").isTrue();

        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).strip();

        assertThat(process.exitValue())
                .withFailMessage("S3 read failed: stdout=%s stderr=%s", stdout, stderr)
                .isZero();
        assertThat(stdout).contains("message schema");
    }
}
