/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/// Validates [Aws4Signer] against the official AWS SigV4 test vectors from
/// [awslabs/aws-c-auth](https://github.com/awslabs/aws-c-auth).
class Aws4SignerTest {

    static Stream<Path> testCases() throws IOException {
        Path testSuite = SigningTestSuite.ensureCloned();
        return Files.list(testSuite)
                .filter(Files::isDirectory)
                .sorted();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    void canonicalRequestMatchesExpected(Path testCaseDir) throws Exception {
        TestCase tc = TestCase.parse(testCaseDir);
        String expected = readFile(testCaseDir.resolve("header-canonical-request.txt"));

        TreeMap<String, String> headers = tc.buildCanonicalHeaders();
        String actual = Aws4Signer.canonicalRequest(
                tc.method, tc.uri, headers, tc.payloadHash(), tc.normalize);

        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    void stringToSignMatchesExpected(Path testCaseDir) throws Exception {
        TestCase tc = TestCase.parse(testCaseDir);
        String expected = readFile(testCaseDir.resolve("header-string-to-sign.txt"));

        TreeMap<String, String> headers = tc.buildCanonicalHeaders();
        String canonicalRequest = Aws4Signer.canonicalRequest(
                tc.method, tc.uri, headers, tc.payloadHash(), tc.normalize);
        String scope = tc.date + "/" + tc.region + "/" + tc.service + "/aws4_request";
        String actual = Aws4Signer.stringToSign(tc.timestamp, scope, canonicalRequest);

        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    void signatureMatchesExpected(Path testCaseDir) throws Exception {
        TestCase tc = TestCase.parse(testCaseDir);
        String expected = readFile(testCaseDir.resolve("header-signature.txt"));

        // When omitSessionToken=true, the token is added after signing, not signed
        String tokenForSigning = tc.omitSessionToken ? null : tc.sessionToken;
        Aws4Signer.SignResult result = Aws4Signer.sign(
                tc.method, tc.uri, tc.requestHeaders, tc.payloadHash(),
                tc.accessKeyId, tc.secretAccessKey, tokenForSigning,
                tc.region, tc.service, tc.dateTime, tc.normalize);

        String auth = result.authorizationHeader();
        String actual = auth.substring(auth.indexOf("Signature=") + "Signature=".length());

        assertThat(actual).isEqualTo(expected);
    }

    /// Regression test for the SigV4 canonical-header lowercasing under a Turkish
    /// JVM default locale: without `Locale.ROOT`, `I` lowercases to dotless `ı`,
    /// producing a canonical request — and therefore a signature — that AWS will
    /// reject. Both signings must produce byte-identical authorization headers.
    @Test
    void signatureIsIndependentOfDefaultLocale() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Host", "example.amazonaws.com");
        headers.put("X-Custom-ID", "value");
        URI uri = URI.create("https://example.amazonaws.com/");
        ZonedDateTime now = ZonedDateTime.parse(
                "2024-01-01T00:00:00Z", DateTimeFormatter.ISO_DATE_TIME);

        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.ROOT);
            Aws4Signer.SignResult rootResult = Aws4Signer.sign(
                    "GET", uri, headers, Aws4Signer.sha256Empty(),
                    "AKID", "SECRET", null, "us-east-1", "s3", now);

            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            Aws4Signer.SignResult turkishResult = Aws4Signer.sign(
                    "GET", uri, headers, Aws4Signer.sha256Empty(),
                    "AKID", "SECRET", null, "us-east-1", "s3", now);

            assertThat(turkishResult.authorizationHeader())
                    .isEqualTo(rootResult.authorizationHeader());
        }
        finally {
            Locale.setDefault(original);
        }
    }

    // ==================== Test case parser ====================

    record TestCase(
            String method,
            URI uri,
            Map<String, String> requestHeaders,
            String body,
            String accessKeyId,
            String secretAccessKey,
            String sessionToken,
            String region,
            String service,
            String timestamp,
            String date,
            ZonedDateTime dateTime,
            boolean normalize,
            boolean signBody,
            boolean omitSessionToken) {

        String payloadHash() {
            if (signBody && body != null && !body.isEmpty()) {
                return Aws4Signer.hexEncode(Aws4Signer.sha256(body.getBytes(StandardCharsets.UTF_8)));
            }
            return Aws4Signer.hexEncode(Aws4Signer.sha256(new byte[0]));
        }

        /// Build the canonical headers map as the signer would see them.
        /// The signer will add x-amz-date and x-amz-security-token itself,
        /// so we provide the other headers plus x-amz-content-sha256 when signing body.
        TreeMap<String, String> buildCanonicalHeaders() {
            TreeMap<String, String> headers = new TreeMap<>();
            for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
                headers.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
            }
            headers.put("x-amz-date", timestamp);
            if (signBody) {
                headers.put("x-amz-content-sha256", payloadHash());
            }
            if (sessionToken != null && !omitSessionToken) {
                headers.put("x-amz-security-token", sessionToken);
            }
            return headers;
        }

        static TestCase parse(Path dir) throws Exception {
            String contextJson = readFile(dir.resolve("context.json"));
            String requestTxt = readFile(dir.resolve("request.txt"));

            // Parse context.json
            String accessKeyId = jsonString(contextJson, "access_key_id");
            String secretAccessKey = jsonString(contextJson, "secret_access_key");
            String sessionToken = jsonStringOrNull(contextJson, "token");
            String region = jsonString(contextJson, "region");
            String service = jsonString(contextJson, "service");
            String timestampIso = jsonString(contextJson, "timestamp");
            boolean normalize = jsonBoolean(contextJson, "normalize");
            boolean signBody = jsonBoolean(contextJson, "sign_body");
            boolean omitSessionToken = jsonBoolean(contextJson, "omit_session_token");

            ZonedDateTime dateTime = ZonedDateTime.parse(timestampIso, DateTimeFormatter.ISO_DATE_TIME);
            String timestamp = Aws4Signer.TIMESTAMP_FORMAT.format(dateTime);
            String date = Aws4Signer.DATE_FORMAT.format(dateTime);

            // Parse request.txt — handle multiline headers (continuation lines start with whitespace)
            String[] lines = requestTxt.split("\n");
            // Request line: METHOD /path HTTP/1.1
            // The path may contain spaces, so extract between first space and last space
            String reqLine = lines[0];
            int firstSpace = reqLine.indexOf(' ');
            int lastSpace = reqLine.lastIndexOf(' ');
            String method = reqLine.substring(0, firstSpace);
            String pathAndQuery = reqLine.substring(firstSpace + 1, lastSpace);

            // Parse headers
            Map<String, String> headers = new LinkedHashMap<>();
            int bodyStart = -1;
            String host = null;
            String lastHeaderName = null;
            for (int i = 1; i < lines.length; i++) {
                if (lines[i].isEmpty()) {
                    bodyStart = i + 1;
                    break;
                }
                // Continuation line (starts with whitespace)
                if (lines[i].charAt(0) == ' ' || lines[i].charAt(0) == '\t') {
                    if (lastHeaderName != null) {
                        headers.merge(lastHeaderName, lines[i], (old, cont) -> old + "\n" + cont);
                    }
                    continue;
                }
                int colon = lines[i].indexOf(':');
                String name = lines[i].substring(0, colon);
                String value = lines[i].substring(colon + 1);
                lastHeaderName = name;
                // Handle duplicate headers by comma-joining
                headers.merge(name, value, (old, dup) -> old + "," + dup);
                if (name.equalsIgnoreCase("host")) {
                    host = value.strip();
                }
            }

            // Build URI with correct host — encode spaces and other chars that
            // URI.create() rejects but are valid in raw HTTP request lines
            String encodedPathAndQuery = pathAndQuery.replace(" ", "%20");
            URI uri;
            if (host != null) {
                uri = URI.create("https://" + host + encodedPathAndQuery);
            }
            else {
                uri = URI.create("https://example.amazonaws.com" + encodedPathAndQuery);
            }

            // Parse body
            String body = null;
            if (bodyStart > 0 && bodyStart < lines.length) {
                StringBuilder sb = new StringBuilder();
                for (int i = bodyStart; i < lines.length; i++) {
                    if (i > bodyStart) {
                        sb.append('\n');
                    }
                    sb.append(lines[i]);
                }
                body = sb.toString();
            }

            // Build the headers map for the signer (without x-amz-date, x-amz-security-token,
            // and x-amz-content-sha256 — the signer or test code adds those)
            Map<String, String> signerHeaders = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                signerHeaders.put(entry.getKey(), entry.getValue());
            }
            if (signBody) {
                String payloadHashVal;
                if (body != null && !body.isEmpty()) {
                    payloadHashVal = Aws4Signer.hexEncode(Aws4Signer.sha256(body.getBytes(StandardCharsets.UTF_8)));
                }
                else {
                    payloadHashVal = Aws4Signer.hexEncode(Aws4Signer.sha256(new byte[0]));
                }
                signerHeaders.put("X-Amz-Content-Sha256", payloadHashVal);
            }

            return new TestCase(method, uri, signerHeaders, body,
                    accessKeyId, secretAccessKey, sessionToken,
                    region, service, timestamp, date, dateTime,
                    normalize, signBody, omitSessionToken);
        }
    }

    // ==================== Helpers ====================

    private static String readFile(Path path) throws IOException {
        String content = Files.readString(path, StandardCharsets.UTF_8);
        if (content.endsWith("\n")) {
            content = content.substring(0, content.length() - 1);
        }
        return content;
    }

    private static String jsonString(String json, String key) {
        String value = jsonStringOrNull(json, key);
        if (value == null) {
            throw new IllegalArgumentException("Missing key: " + key);
        }
        return value;
    }

    private static String jsonStringOrNull(String json, String key) {
        String search = "\"" + key + "\"";
        int idx = json.indexOf(search);
        if (idx < 0) {
            return null;
        }
        int colon = json.indexOf(':', idx + search.length());
        int quote1 = json.indexOf('"', colon + 1);
        int quote2 = json.indexOf('"', quote1 + 1);
        return json.substring(quote1 + 1, quote2);
    }

    private static boolean jsonBoolean(String json, String key) {
        String search = "\"" + key + "\"";
        int idx = json.indexOf(search);
        if (idx < 0) {
            return false;
        }
        int colon = json.indexOf(':', idx + search.length());
        String rest = json.substring(colon + 1).strip();
        return rest.startsWith("true");
    }
}
