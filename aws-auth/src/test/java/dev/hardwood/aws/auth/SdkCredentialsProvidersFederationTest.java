/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.aws.auth;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.S3Credentials;

import static org.assertj.core.api.Assertions.assertThat;

/// Resolves SSO, web-identity and assumed-role credentials through the default chain
/// against a stubbed SSO portal and STS endpoint. The SDK loads these providers
/// reflectively, so these tests fail when the `sso`, `ssooidc` or `sts` module is missing.
///
/// The surefire configuration points `HOME` at a directory under `target/`, which is
/// where the SDK looks for the SSO token cache.
class SdkCredentialsProvidersFederationTest {

    private static final List<String> SYSTEM_PROPERTIES = List.of(
            "aws.configFile", "aws.sharedCredentialsFile", "aws.profile", "aws.region",
            "aws.webIdentityTokenFile", "aws.roleArn", "aws.roleSessionName",
            "aws.endpointUrlSts", "aws.endpointUrlSso");

    private final List<String> requests = new CopyOnWriteArrayList<>();

    private HttpServer server;

    @TempDir
    Path tempDir;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", this::handle);
        server.start();

        // Keep the developer's own AWS configuration out of the chain
        System.setProperty("aws.configFile", tempDir.resolve("config").toString());
        System.setProperty("aws.sharedCredentialsFile", tempDir.resolve("credentials").toString());
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        SYSTEM_PROPERTIES.forEach(System::clearProperty);
    }

    @Test
    void defaultChainResolvesWebIdentityToken() throws IOException {
        Path tokenFile = Files.writeString(tempDir.resolve("token"), "the-web-identity-token");
        System.setProperty("aws.webIdentityTokenFile", tokenFile.toString());
        System.setProperty("aws.roleArn", "arn:aws:iam::123456789012:role/hardwood");
        System.setProperty("aws.roleSessionName", "hardwood-test");
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.endpointUrlSts", endpoint());

        S3Credentials creds = SdkCredentialsProviders.defaultChain().credentials();

        assertThat(creds.accessKeyId()).isEqualTo("AKID_STS");
        assertThat(creds.secretAccessKey()).isEqualTo("secret_sts");
        assertThat(creds.sessionToken()).isEqualTo("token_sts");
        assertThat(requests).singleElement().satisfies(body -> assertThat(body)
                .contains("Action=AssumeRoleWithWebIdentity")
                .contains("WebIdentityToken=the-web-identity-token"));
    }

    @Test
    void defaultChainResolvesSsoSessionProfile() throws IOException {
        Files.writeString(tempDir.resolve("config"), """
                [profile hardwood-sso]
                sso_session = hardwood
                sso_account_id = 123456789012
                sso_role_name = ReadOnly
                region = us-east-1

                [sso-session hardwood]
                sso_start_url = https://hardwood.awsapps.com/start
                sso_region = us-east-1
                """);
        writeSsoTokenCache("hardwood");
        System.setProperty("aws.profile", "hardwood-sso");
        System.setProperty("aws.endpointUrlSso", endpoint());

        S3Credentials creds = SdkCredentialsProviders.defaultChain().credentials();

        assertThat(creds.accessKeyId()).isEqualTo("AKID_SSO");
        assertThat(creds.secretAccessKey()).isEqualTo("secret_sso");
        assertThat(creds.sessionToken()).isEqualTo("token_sso");
        assertThat(requests).singleElement().satisfies(request -> assertThat(request)
                .startsWith("GET /federation/credentials token=the-sso-access-token query=")
                .contains("account_id=123456789012")
                .contains("role_name=ReadOnly"));
    }

    @Test
    void defaultChainResolvesAssumedRoleProfile() throws IOException {
        Files.writeString(tempDir.resolve("config"), """
                [profile hardwood-role]
                role_arn = arn:aws:iam::123456789012:role/hardwood
                role_session_name = hardwood-test
                source_profile = base
                region = us-east-1

                [profile base]
                aws_access_key_id = AKID_BASE
                aws_secret_access_key = secret_base
                """);
        System.setProperty("aws.profile", "hardwood-role");
        System.setProperty("aws.endpointUrlSts", endpoint());

        S3Credentials creds = SdkCredentialsProviders.defaultChain().credentials();

        assertThat(creds.accessKeyId()).isEqualTo("AKID_STS");
        assertThat(creds.secretAccessKey()).isEqualTo("secret_sts");
        assertThat(creds.sessionToken()).isEqualTo("token_sts");
        assertThat(requests).singleElement().satisfies(body -> assertThat(body)
                .startsWith("Action=AssumeRole&")
                .contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fhardwood"));
    }

    private String endpoint() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    /// Writes the token `aws sso login` leaves behind for the given SSO session.
    private static void writeSsoTokenCache(String sessionName) throws IOException {
        Path cacheDir = Paths.get(System.getenv("HOME"), ".aws", "sso", "cache");
        Files.createDirectories(cacheDir);
        Files.writeString(cacheDir.resolve(sha1Hex(sessionName) + ".json"), """
                {"accessToken": "the-sso-access-token", "expiresAt": "2099-01-01T00:00:00Z",
                 "region": "us-east-1", "startUrl": "https://hardwood.awsapps.com/start"}
                """);
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

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String response;
        String contentType;
        if (exchange.getRequestMethod().equals("GET")) {
            requests.add("GET " + exchange.getRequestURI().getPath()
                    + " token=" + exchange.getRequestHeaders().getFirst("x-amz-sso_bearer_token")
                    + " query=" + exchange.getRequestURI().getQuery());
            response = """
                    {"roleCredentials": {"accessKeyId": "AKID_SSO", "secretAccessKey": "secret_sso",
                     "sessionToken": "token_sso", "expiration": 4070908800000}}
                    """;
            contentType = "application/json";
        }
        else {
            requests.add(body);
            response = stsResponse(queryParameter(body, "Action"));
            contentType = "text/xml";
        }
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
