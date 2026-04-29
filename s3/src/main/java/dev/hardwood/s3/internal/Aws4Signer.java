/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/// AWS Signature Version 4 request signer.
///
/// Implements the [SigV4 algorithm](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
/// using only JDK crypto APIs. No AWS SDK types.
public final class Aws4Signer {

    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String TERMINATOR = "aws4_request";
    static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private Aws4Signer() {
    }

    /// Result of a signing operation.
    ///
    /// @param authorizationHeader the full Authorization header value
    /// @param timestamp           the x-amz-date timestamp used
    /// @param headers             all headers that should be sent (original + added by signer)
    public record SignResult(
            String authorizationHeader,
            String timestamp,
            Map<String, String> headers) {
    }

    /// Sign an HTTP request.
    ///
    /// @param method      HTTP method (GET, PUT, etc.)
    /// @param uri         the full request URI
    /// @param headers     request headers to sign (will be copied; originals not modified)
    /// @param payloadHash hex-encoded SHA-256 of the request body (use [#sha256Empty()] for no body)
    /// @param accessKeyId     AWS access key ID
    /// @param secretAccessKey AWS secret access key
    /// @param sessionToken    session token, or `null` for long-term credentials
    /// @param region      AWS region (e.g. "us-east-1")
    /// @param service     AWS service (e.g. "s3")
    /// @param now         current time
    /// @return the signing result
    public static SignResult sign(String method, URI uri,
            Map<String, String> headers, String payloadHash,
            String accessKeyId, String secretAccessKey, String sessionToken,
            String region, String service, ZonedDateTime now) {
        return sign(method, uri, headers, payloadHash,
                accessKeyId, secretAccessKey, sessionToken,
                region, service, now, false);
    }

    /// Sign an HTTP request with path normalization control.
    ///
    /// @param normalize if `true`, normalize the URI path (resolve `.`, `..`, collapse `//`).
    ///                  S3 requires `false`.
    public static SignResult sign(String method, URI uri,
            Map<String, String> headers, String payloadHash,
            String accessKeyId, String secretAccessKey, String sessionToken,
            String region, String service, ZonedDateTime now,
            boolean normalize) {

        String timestamp = TIMESTAMP_FORMAT.format(now);
        String date = DATE_FORMAT.format(now);

        // Build the canonical headers map (lowercase keys, sorted)
        TreeMap<String, String> canonHeaders = lowercaseSorted(headers);
        canonHeaders.put("x-amz-date", timestamp);
        if (sessionToken != null) {
            canonHeaders.put("x-amz-security-token", sessionToken);
        }

        String canonicalRequest = canonicalRequest(method, uri, canonHeaders, payloadHash, normalize);
        String scope = date + "/" + region + "/" + service + "/" + TERMINATOR;
        String stringToSign = stringToSign(timestamp, scope, canonicalRequest);

        byte[] signingKey = signingKey(secretAccessKey, date, region, service);
        String signature = hexEncode(hmacSha256(signingKey, stringToSign.getBytes(StandardCharsets.UTF_8)));

        String signedHeaderNames = signedHeaderNames(canonHeaders);
        String authorization = ALGORITHM
                + " Credential=" + accessKeyId + "/" + scope
                + ", SignedHeaders=" + signedHeaderNames
                + ", Signature=" + signature;

        return new SignResult(authorization, timestamp, canonHeaders);
    }

    // ==================== Visible for testing ====================

    static String canonicalRequest(String method, URI uri,
            TreeMap<String, String> headers, String payloadHash,
            boolean normalize) {
        StringBuilder sb = new StringBuilder();
        sb.append(method).append('\n');
        sb.append(canonicalUri(uri, normalize)).append('\n');
        sb.append(canonicalQueryString(uri)).append('\n');
        sb.append(canonicalHeaders(headers)).append('\n');
        sb.append(signedHeaderNames(headers)).append('\n');
        sb.append(payloadHash);
        return sb.toString();
    }

    static String stringToSign(String timestamp, String scope, String canonicalRequest) {
        return ALGORITHM + '\n'
                + timestamp + '\n'
                + scope + '\n'
                + hexEncode(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));
    }

    // ==================== Internal ====================

    private static TreeMap<String, String> lowercaseSorted(Map<String, String> headers) {
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            sorted.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
        }
        return sorted;
    }

    private static String canonicalUri(URI uri, boolean normalize) {
        String path = uri.getRawPath();
        if (path == null || path.isEmpty()) {
            return "/";
        }
        if (normalize) {
            path = URI.create("x://h" + path).normalize().getRawPath();
            if (path == null || path.isEmpty()) {
                return "/";
            }
        }
        // Re-encode path segments to match SigV4 rules
        String[] segments = path.split("/", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(uriEncode(percentDecode(segments[i])));
        }
        String result = sb.toString();
        return result.isEmpty() ? "/" : result;
    }

    private static String canonicalQueryString(URI uri) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return "";
        }
        // Use a TreeMap to sort, but allow duplicate keys by appending to value
        TreeMap<String, java.util.List<String>> params = new TreeMap<>();
        for (String param : query.split("&")) {
            int eq = param.indexOf('=');
            String key;
            String value;
            if (eq >= 0) {
                key = uriEncode(percentDecode(param.substring(0, eq)));
                value = uriEncode(percentDecode(param.substring(eq + 1)));
            }
            else {
                key = uriEncode(percentDecode(param));
                value = "";
            }
            params.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(value);
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, java.util.List<String>> entry : params.entrySet()) {
            java.util.List<String> values = entry.getValue();
            java.util.Collections.sort(values);
            for (String value : values) {
                if (!sb.isEmpty()) {
                    sb.append('&');
                }
                sb.append(entry.getKey()).append('=').append(value);
            }
        }
        return sb.toString();
    }

    private static String canonicalHeaders(TreeMap<String, String> headers) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            sb.append(entry.getKey()).append(':')
                    .append(trimHeaderValue(entry.getValue())).append('\n');
        }
        return sb.toString();
    }

    private static String signedHeaderNames(TreeMap<String, String> headers) {
        StringBuilder sb = new StringBuilder();
        for (String key : headers.keySet()) {
            if (!sb.isEmpty()) {
                sb.append(';');
            }
            sb.append(key);
        }
        return sb.toString();
    }

    private static String trimHeaderValue(String value) {
        return value.strip().replaceAll("\\s+", " ");
    }

    static byte[] signingKey(String secretAccessKey, String date, String region, String service) {
        byte[] dateKey = hmacSha256(
                ("AWS4" + secretAccessKey).getBytes(StandardCharsets.UTF_8),
                date.getBytes(StandardCharsets.UTF_8));
        byte[] dateRegionKey = hmacSha256(dateKey, region.getBytes(StandardCharsets.UTF_8));
        byte[] dateRegionServiceKey = hmacSha256(dateRegionKey, service.getBytes(StandardCharsets.UTF_8));
        return hmacSha256(dateRegionServiceKey, TERMINATOR.getBytes(StandardCharsets.UTF_8));
    }

    // ==================== Crypto primitives ====================

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 not available", e);
        }
    }

    /// Returns the hex-encoded SHA-256 of an empty byte array.
    static String sha256Empty() {
        return hexEncode(sha256(new byte[0]));
    }

    static byte[] hmacSha256(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        }
        catch (Exception e) {
            throw new AssertionError("HmacSHA256 not available", e);
        }
    }

    static String hexEncode(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    /// Encodes a string per SigV4 rules: encode everything except `A-Za-z0-9-._~`.
    static String uriEncode(String input) {
        StringBuilder sb = new StringBuilder(input.length() * 2);
        for (byte b : input.getBytes(StandardCharsets.UTF_8)) {
            char c = (char) (b & 0xFF);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.' || c == '~') {
                sb.append(c);
            }
            else {
                sb.append(String.format("%%%02X", b & 0xFF));
            }
        }
        return sb.toString();
    }

    private static String percentDecode(String encoded) {
        if (encoded.indexOf('%') < 0) {
            return encoded;
        }
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }
}
