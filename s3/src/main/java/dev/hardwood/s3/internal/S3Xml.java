/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/// The XML bodies of the S3 multipart-upload requests and responses: the completion request,
/// the upload ID and completion results, and error documents. Responses are parsed with DTDs
/// and external entities disabled, reading only the root element's direct children.
public final class S3Xml {

    /// The most parts one multipart upload may have.
    public static final int MAX_PARTS = 10_000;

    /// The key [#rootFields] files the root element's name under; no child has an empty name.
    private static final String ROOT = "";

    private S3Xml() {
    }

    /// One uploaded part, as listed in a completion request.
    public record Part(int partNumber, String etag) {

        public Part {
            if (partNumber < 1 || partNumber > MAX_PARTS) {
                throw new IllegalArgumentException("Part number must be between 1 and " + MAX_PARTS + ": " + partNumber);
            }
            Objects.requireNonNull(etag, "etag");
        }
    }

    /// The upload ID of an `InitiateMultipartUploadResult`.
    static String uploadId(byte[] body, String description) throws IOException {
        Map<String, String> fields = rootFields(body, description);
        String uploadId = fields.get("UploadId");
        if (!"InitiateMultipartUploadResult".equals(fields.get(ROOT)) || uploadId == null || uploadId.isBlank()) {
            throw new IOException(description + " failed: response has no upload ID");
        }
        return uploadId;
    }

    /// A failed completion reported in the body of a `200` response, with the S3 error code.
    static final class CompletionFailure extends IOException {

        private final String code;

        CompletionFailure(String message, String code) {
            super(message);
            this.code = code;
        }

        /// Whether S3 asks for the completion to be retried: an internal error or throttling.
        boolean transientFailure() {
            return "InternalError".equals(code) || "SlowDown".equals(code) || "ServiceUnavailable".equals(code);
        }
    }

    /// Checks that a `200` completion response reports success. S3 sends the status before the
    /// completion is done, so a failure arrives as an `Error` document in a `200` response, raised
    /// as a [CompletionFailure].
    static void checkCompletion(byte[] body, String description) throws IOException {
        Map<String, String> fields = rootFields(body, description);
        if ("Error".equals(fields.get(ROOT))) {
            throw new CompletionFailure(description + " failed:" + summary(fields), fields.get("Code"));
        }
        if (!"CompleteMultipartUploadResult".equals(fields.get(ROOT))) {
            throw new IOException(description + " failed: unexpected response " + fields.get(ROOT));
        }
    }

    /// The code and message of an `Error` document, prefixed with a space, or an empty string
    /// when the body is not one.
    static String errorSummary(byte[] body) {
        if (body == null || body.length == 0) {
            return "";
        }
        try {
            Map<String, String> fields = rootFields(body, "error response");
            return "Error".equals(fields.get(ROOT)) ? summary(fields) : "";
        }
        catch (IOException e) {
            return "";
        }
    }

    /// The `CompleteMultipartUpload` request body listing `parts`, which must be in ascending
    /// part-number order.
    static byte[] completionBody(List<Part> parts) {
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Completion requires at least one part");
        }
        StringBuilder xml = new StringBuilder("<CompleteMultipartUpload>");
        int previous = 0;
        for (Part part : parts) {
            if (part.partNumber() <= previous) {
                throw new IllegalArgumentException("Completion parts must have ascending part numbers");
            }
            xml.append("<Part><PartNumber>").append(part.partNumber()).append("</PartNumber><ETag>");
            appendEscaped(xml, part.etag());
            xml.append("</ETag></Part>");
            previous = part.partNumber();
        }
        return xml.append("</CompleteMultipartUpload>").toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String summary(Map<String, String> fields) {
        String code = fields.get("Code");
        String message = fields.get("Message");
        return " " + (code != null ? code : "unknown error") + (message != null ? ": " + message : "");
    }

    /// The text of each direct child of the root element, keyed by local name, with the root's
    /// own name under [#ROOT].
    private static Map<String, String> rootFields(byte[] body, String description) throws IOException {
        XMLInputFactory factory = XMLInputFactory.newDefaultFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        Map<String, String> fields = new HashMap<>();
        try {
            XMLStreamReader reader = factory.createXMLStreamReader(new ByteArrayInputStream(body));
            try {
                int depth = 0;
                StringBuilder text = new StringBuilder();
                while (reader.hasNext()) {
                    int event = reader.next();
                    if (event == XMLStreamConstants.START_ELEMENT) {
                        depth++;
                        if (depth == 1) {
                            fields.put(ROOT, reader.getLocalName());
                        }
                        text.setLength(0);
                    }
                    else if (event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA) {
                        if (depth == 2) {
                            text.append(reader.getText());
                        }
                    }
                    else if (event == XMLStreamConstants.END_ELEMENT) {
                        if (depth == 2) {
                            fields.putIfAbsent(reader.getLocalName(), text.toString());
                        }
                        depth--;
                    }
                }
            }
            finally {
                reader.close();
            }
        }
        catch (XMLStreamException e) {
            throw new IOException(description + " failed: malformed XML response", e);
        }
        if (!fields.containsKey(ROOT)) {
            throw new IOException(description + " failed: empty XML response");
        }
        return fields;
    }

    private static void appendEscaped(StringBuilder xml, String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '&' -> xml.append("&amp;");
                case '<' -> xml.append("&lt;");
                case '>' -> xml.append("&gt;");
                case '"' -> xml.append("&quot;");
                default -> xml.append(c);
            }
        }
    }
}
