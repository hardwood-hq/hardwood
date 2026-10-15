/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.RangeBacking;
import dev.hardwood.s3.S3Proxy;
import dev.hardwood.s3.TestBucket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// Runs commands against s3proxy with no AWS credential source configured unless a test sets
/// one, and checks how the CLI resolves credentials and region. s3proxy's filesystem backend
/// serves an object to unsigned requests when its file is readable by others, and denies them
/// otherwise, so reading `private.parquet` succeeds only with a signed request.
class S3CredentialResolutionCommandIT {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    private static final String[] PROPERTIES = {
            "aws.accessKeyId", "aws.secretAccessKey", "aws.sessionToken", "aws.profile",
            "aws.configFile", "aws.sharedCredentialsFile", "aws.disableEc2Metadata",
            "aws.ec2MetadataServiceEndpoint", "aws.region", "aws.endpointUrl", "aws.pathStyle" };

    private static final String INSTANCE_ROLE = "test-role";

    static TestBucket bucket;

    private final Map<String, String> savedProperties = new HashMap<>();

    @BeforeAll
    static void createBucket() throws IOException {
        byte[] content = Files.readAllBytes(TEST_RESOURCES.resolve("plain_uncompressed.parquet"));
        bucket = S3Proxy.get().bucketFor(S3CredentialResolutionCommandIT.class)
                .withObject("public.parquet", content)
                .withObject("private.parquet", content);
        bucket.create();
        Files.setPosixFilePermissions(bucket.objectPath("public.parquet"),
                PosixFilePermissions.fromString("rw-r--r--"));
        Files.setPosixFilePermissions(bucket.objectPath("private.parquet"),
                PosixFilePermissions.fromString("rw-r-----"));
    }

    @AfterAll
    static void deleteBucket() {
        bucket.delete();
    }

    /// The environment-variable counterpart of the system properties [#configureWithoutCredentials] sets.
    private static Map<String, String> environment() {
        Map<String, String> environment = new HashMap<>();
        environment.put("AWS_CONFIG_FILE", System.getProperty("aws.configFile"));
        environment.put("AWS_SHARED_CREDENTIALS_FILE", System.getProperty("aws.sharedCredentialsFile"));
        environment.put("AWS_EC2_METADATA_DISABLED", "true");
        environment.put("AWS_REGION", "us-east-1");
        environment.put("AWS_ENDPOINT_URL", bucket.endpoint());
        environment.put("AWS_PATH_STYLE", "true");
        return environment;
    }

    /// An instance metadata service (IMDSv2) whose instance role carries the s3proxy credentials.
    private static HttpServer startMetadataService() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/latest/api/token", exchange -> respond(exchange, "test-token"));
        server.createContext("/latest/meta-data/iam/security-credentials/", exchange -> {
            if (exchange.getRequestURI().getPath().endsWith("/" + INSTANCE_ROLE)) {
                respond(exchange, """
                        {"Code": "Success", "AccessKeyId": "%s", "SecretAccessKey": "%s", "Expiration": "%s"}"""
                        .formatted(S3Proxy.ACCESS_KEY, S3Proxy.SECRET_KEY, Instant.now().plus(Duration.ofHours(1))));
            }
            else {
                respond(exchange, INSTANCE_ROLE);
            }
        });
        server.start();
        return server;
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @BeforeEach
    void configureWithoutCredentials() throws IOException {
        // The SDK falls back to environment variables for unset system properties
        assumeTrue(System.getenv().keySet().stream().noneMatch(name -> name.startsWith("AWS_")),
                "AWS_ environment variables are set");
        for (String property : PROPERTIES) {
            String value = System.getProperty(property);
            if (value != null) {
                savedProperties.put(property, value);
            }
            System.clearProperty(property);
        }
        String emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();
        System.setProperty("aws.configFile", emptyFile);
        System.setProperty("aws.sharedCredentialsFile", emptyFile);
        // The chain would otherwise wait for an instance metadata endpoint that is not there
        System.setProperty("aws.disableEc2Metadata", "true");
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.endpointUrl", bucket.endpoint());
        System.setProperty("aws.pathStyle", "true");
    }

    @AfterEach
    void restoreProperties() {
        for (String property : PROPERTIES) {
            System.clearProperty(property);
        }
        savedProperties.forEach(System::setProperty);
    }

    @Test
    void readsPublicObjectUnsigned() {
        Cli.Result result = Cli.launch("info", "-f", bucket.uri("public.parquet"));

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).startsWith("""
                Format Version:    2
                Created By:        parquet-cpp-arrow version 24.0.0
                Row Groups:        1
                Total Rows:        3""");
    }

    @Test
    void deniedObjectNamesUnsignedRequest() {
        Cli.Result result = Cli.launch("info", "-f", bucket.uri("private.parquet"));

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo("""
                Error reading file: Failed to open %s: HTTP 403 \
                <?xml version='1.0' encoding='UTF-8'?><Error><Code>AccessDenied</Code>\
                <Message>Forbidden</Message><RequestId>4442587FB7D0A2F9</RequestId></Error> \
                (the request was sent unsigned, without credentials)""".formatted(bucket.uri("private.parquet")));
    }

    @Test
    void signsWithInstanceRoleWhenNothingLocalIsConfigured() throws IOException {
        HttpServer metadataService = startMetadataService();
        try {
            System.clearProperty("aws.disableEc2Metadata");
            System.setProperty("aws.ec2MetadataServiceEndpoint",
                    "http://localhost:" + metadataService.getAddress().getPort());

            Cli.Result result = Cli.launch("info", "-f", bucket.uri("private.parquet"));

            assertThat(result.errorOutput()).isEmpty();
            assertThat(result.exitCode()).isZero();
            assertThat(result.output()).startsWith("""
                    Format Version:    2
                    Created By:        parquet-cpp-arrow version 24.0.0
                    Row Groups:        1
                    Total Rows:        3""");
        }
        finally {
            metadataService.stop(0);
        }
    }

    @Test
    void missingRegionFails() {
        System.clearProperty("aws.region");
        System.clearProperty("aws.endpointUrl");

        Cli.Result result = Cli.launch("info", "-f", bucket.uri("public.parquet"));

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo(
                "Unable to determine AWS region. Set AWS_REGION or configure a default region in ~/.aws/config.");
    }

    @Test
    void sourceIsSharedAcrossOpensWithTheSameRangeBacking() {
        FileMixin fileMixin = new FileMixin();
        fileMixin.file = bucket.uri("public.parquet");

        assertThat(fileMixin.toInputFile()).isNotNull();
        assertThat(fileMixin.toInputFile()).isNotNull();
        assertThatThrownBy(() -> fileMixin.toInputFile(RangeBacking.SPARSE_TEMPFILE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("S3 source opened with NONE, requested SPARSE_TEMPFILE");
    }

    @Test
    void readsUnsignedWhenEnvironmentConfiguresNoCredentials() throws Exception {
        Cli.Result result = CliProcess.runJvm(environment(), "info", "-f", bucket.uri("public.parquet"));

        assertThat(result.errorOutput()).isEmpty();
        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).startsWith("Format Version:    2");
    }

    @Test
    void profileFromEnvironmentIsAConfiguredSource() throws Exception {
        Map<String, String> environment = environment();
        environment.put("AWS_PROFILE", "missing");

        Cli.Result result = CliProcess.runJvm(environment, "info", "-f", bucket.uri("public.parquet"));

        assertThat(result.exitCode()).isNotZero();
        // Further lines carry the AWS SDK's account of every provider in its chain
        assertThat(result.errorOutput().lines().findFirst()).hasValue("Unable to load AWS credentials:");
    }

    @Test
    void brokenProfileFailsWithCredentialError() {
        System.setProperty("aws.profile", "missing");

        Cli.Result result = Cli.launch("info", "-f", bucket.uri("public.parquet"));

        assertThat(result.exitCode()).isNotZero();
        // Further lines carry the AWS SDK's account of every provider in its chain
        assertThat(result.errorOutput().lines().findFirst()).hasValue("Unable to load AWS credentials:");
    }
}
