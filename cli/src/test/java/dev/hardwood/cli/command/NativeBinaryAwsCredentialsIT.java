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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;

/// Proves the native CLI binary resolves web-identity, SSO and assumed-role credentials.
/// The SDK loads these providers reflectively, which the native image only supports
/// when the reflection metadata for them is in place.
///
/// One stub server plays STS, the SSO portal and S3. It answers every S3 request with
/// 403, so the command fails; what the test checks is that the S3 request was signed
/// with the credentials the stubbed STS or SSO endpoint handed out.
@Tag("native")
class NativeBinaryAwsCredentialsIT {

    private static final String S3_FILE = "s3://test-bucket/plain_uncompressed.parquet";

    private final String nativeBinary = System.getProperty("native.image.path");
    private final List<String> stsRequests = new CopyOnWriteArrayList<>();
    private final List<String> ssoRequests = new CopyOnWriteArrayList<>();
    private final List<String> s3Requests = new CopyOnWriteArrayList<>();

    private HttpServer server;

    @TempDir
    Path home;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void resolvesWebIdentityCredentials() throws IOException, InterruptedException {
        Path tokenFile = Files.writeString(home.resolve("token"), "the-web-identity-token");

        ProcessBuilder pb = cli();
        pb.environment().put("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile.toString());
        pb.environment().put("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/hardwood");
        pb.environment().put("AWS_ROLE_SESSION_NAME", "hardwood-test");

        String stderr = run(pb);

        assertThat(stsRequests).as(stderr).singleElement().satisfies(body -> assertThat(body)
                .contains("Action=AssumeRoleWithWebIdentity")
                .contains("WebIdentityToken=the-web-identity-token"));
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request)
                .contains("Credential=AKID_STS/")
                .endsWith(" token=token_sts"));
    }

    @Test
    void resolvesSsoSessionCredentials() throws IOException, InterruptedException {
        Path aws = Files.createDirectories(home.resolve(".aws"));
        Files.writeString(aws.resolve("config"), """
                [profile hardwood-sso]
                sso_session = hardwood
                sso_account_id = 123456789012
                sso_role_name = ReadOnly

                [sso-session hardwood]
                sso_start_url = https://hardwood.awsapps.com/start
                sso_region = us-east-1
                """);
        Path cache = Files.createDirectories(aws.resolve("sso").resolve("cache"));
        Files.writeString(cache.resolve(sha1Hex("hardwood") + ".json"), """
                {"accessToken": "the-sso-access-token", "expiresAt": "2099-01-01T00:00:00Z",
                 "region": "us-east-1", "startUrl": "https://hardwood.awsapps.com/start"}
                """);

        ProcessBuilder pb = cli();
        pb.environment().put("AWS_CONFIG_FILE", aws.resolve("config").toString());
        pb.environment().put("AWS_PROFILE", "hardwood-sso");

        String stderr = run(pb);

        assertThat(ssoRequests).as(stderr).containsExactly("/federation/credentials token=the-sso-access-token");
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request)
                .contains("Credential=AKID_SSO/")
                .endsWith(" token=token_sso"));
    }

    @Test
    void resolvesAssumedRoleCredentials() throws IOException, InterruptedException {
        Path config = Files.writeString(home.resolve("config"), """
                [profile hardwood-role]
                role_arn = arn:aws:iam::123456789012:role/hardwood
                role_session_name = hardwood-test
                source_profile = base

                [profile base]
                aws_access_key_id = AKID_BASE
                aws_secret_access_key = secret_base
                """);

        ProcessBuilder pb = cli();
        pb.environment().put("AWS_CONFIG_FILE", config.toString());
        pb.environment().put("AWS_PROFILE", "hardwood-role");

        String stderr = run(pb);

        assertThat(stsRequests).as(stderr).singleElement().satisfies(body -> assertThat(body)
                .startsWith("Action=AssumeRole&")
                .contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fhardwood"));
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request)
                .contains("Credential=AKID_STS/")
                .endsWith(" token=token_sts"));
    }

    /// A CLI process whose AWS environment is the stub server and an empty `HOME`.
    private ProcessBuilder cli() {
        ProcessBuilder pb = new ProcessBuilder(nativeBinary, "schema", "-f", S3_FILE);
        Map<String, String> env = pb.environment();
        env.keySet().removeIf(name -> name.startsWith("AWS_"));
        String endpoint = "http://localhost:" + server.getAddress().getPort();
        env.put("HOME", home.toString());
        env.put("AWS_REGION", "us-east-1");
        env.put("AWS_ENDPOINT_URL", endpoint);
        env.put("AWS_ENDPOINT_URL_STS", endpoint);
        env.put("AWS_ENDPOINT_URL_SSO", endpoint);
        env.put("AWS_PATH_STYLE", "true");
        env.put("AWS_CONFIG_FILE", home.resolve("no-config").toString());
        env.put("AWS_SHARED_CREDENTIALS_FILE", home.resolve("no-credentials").toString());
        env.put("AWS_EC2_METADATA_DISABLED", "true");
        return pb;
    }

    /// Runs the CLI and returns its standard error, for the assertions to report.
    private String run(ProcessBuilder pb) throws IOException, InterruptedException {
        Path stderr = home.resolve("stderr.txt");
        Process process = pb.redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(stderr.toFile())
                .start();
        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        assertThat(finished).withFailMessage("Process timed out after 30s").isTrue();
        return Files.readString(stderr);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (exchange.getRequestMethod().equals("POST") && path.equals("/")) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            stsRequests.add(body);
            respond(exchange, 200, "text/xml", stsResponse(queryParameter(body, "Action")));
        }
        else if (path.equals("/federation/credentials")) {
            ssoRequests.add(path + " token=" + exchange.getRequestHeaders().getFirst("x-amz-sso_bearer_token"));
            respond(exchange, 200, "application/json", """
                    {"roleCredentials": {"accessKeyId": "AKID_SSO", "secretAccessKey": "secret_sso",
                     "sessionToken": "token_sso", "expiration": 4070908800000}}
                    """);
        }
        else {
            s3Requests.add(exchange.getRequestHeaders().getFirst("Authorization")
                    + " token=" + exchange.getRequestHeaders().getFirst("x-amz-security-token"));
            respond(exchange, 403, "application/xml", """
                    <Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>
                    """);
        }
    }

    private static String queryParameter(String query, String name) {
        int start = query.indexOf(name + "=") + name.length() + 1;
        int end = query.indexOf('&', start);
        return end < 0 ? query.substring(start) : query.substring(start, end);
    }

    /// The STS response for `action`, handing out the `AKID_STS` credentials.
    private static String stsResponse(String action) {
        return """
                <%1$sResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                  <%1$sResult>
                    <Credentials>
                      <AccessKeyId>AKID_STS</AccessKeyId>
                      <SecretAccessKey>secret_sts</SecretAccessKey>
                      <SessionToken>token_sts</SessionToken>
                      <Expiration>2099-01-01T00:00:00Z</Expiration>
                    </Credentials>
                  </%1$sResult>
                </%1$sResponse>
                """.formatted(action);
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String sha1Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
