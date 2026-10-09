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
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

final class S3Xml {

    static final int MAX_RESPONSE_SIZE = 256 * 1024;

    private static final int MAX_DEPTH = 64;
    private static final Set<String> ERROR_FIELDS = Set.of("Code", "Message", "RequestId", "HostId");

    private S3Xml() {
    }

    static String uploadId(byte[] body) throws IOException {
        return requiredResponseValue(body, "InitiateMultipartUploadResult", "UploadId");
    }

    static String completedETag(byte[] body) throws IOException {
        return requiredResponseValue(body, "CompleteMultipartUploadResult", "ETag");
    }

    static ServiceException error(byte[] body) throws IOException {
        return serviceException(parse(body, "Error", ERROR_FIELDS).fields());
    }

    static PartsPage partsPage(byte[] body) throws IOException {
        Document document = parse(body, "ListPartsResult", Set.of("IsTruncated", "NextPartNumberMarker"));
        if ("Error".equals(document.root())) {
            throw serviceException(document.fields());
        }
        String truncated = requiredValue(document.fields(), "IsTruncated").strip();
        if (!"true".equals(truncated) && !"false".equals(truncated)) {
            throw new IOException("Invalid IsTruncated in S3 list-parts response");
        }
        int nextMarker = 0;
        if ("true".equals(truncated)) {
            nextMarker = partNumber(requiredValue(document.fields(), "NextPartNumberMarker"));
            if (nextMarker < document.lastPartNumber()) {
                throw new IOException("S3 list-parts marker precedes its last part");
            }
        }
        return new PartsPage(document.lastPartNumber() != 0, nextMarker);
    }

    record PartsPage(boolean hasParts, int nextMarker) {
    }

    static byte[] completionBody(List<Part> parts) {
        Objects.requireNonNull(parts, "parts");
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Completion requires at least one part");
        }
        StringBuilder xml = new StringBuilder("<CompleteMultipartUpload>");
        int previousPart = 0;
        for (Part part : parts) {
            if (part.partNumber() <= previousPart) {
                throw new IllegalArgumentException("Completion parts must have increasing part numbers");
            }
            xml.append("<Part><PartNumber>").append(part.partNumber()).append("</PartNumber><ETag>");
            appendXmlText(xml, part.etag());
            xml.append("</ETag></Part>");
            previousPart = part.partNumber();
        }
        xml.append("</CompleteMultipartUpload>");
        return xml.toString().getBytes(StandardCharsets.UTF_8);
    }

    record Part(int partNumber, String etag) {

        Part {
            validatePartNumber(partNumber);
            Objects.requireNonNull(etag, "etag");
            if (etag.isBlank()) {
                throw new IllegalArgumentException("Part ETag must not be blank");
            }
            validateXmlText(etag);
        }
    }

    private static String requiredResponseValue(byte[] body, String root, String field) throws IOException {
        Document document = parse(body, root, Set.of(field));
        if ("Error".equals(document.root())) {
            throw serviceException(document.fields());
        }
        return requiredValue(document.fields(), field);
    }

    private static ServiceException serviceException(Map<String, String> fields) throws IOException {
        return new ServiceException(requiredValue(fields, "Code"), fields.get("Message"),
                fields.get("RequestId"), fields.get("HostId"));
    }

    private static String requiredValue(Map<String, String> fields, String name) throws IOException {
        String value = fields.get(name);
        if (value == null || value.isBlank()) {
            throw new IOException("S3 XML response is missing " + name);
        }
        return value;
    }

    private static Document parse(byte[] body, String expectedRoot, Set<String> expectedFields) throws IOException {
        Objects.requireNonNull(body, "body");
        if (body.length > MAX_RESPONSE_SIZE) {
            throw new IOException("S3 XML response exceeds " + MAX_RESPONSE_SIZE + " bytes");
        }
        XMLInputFactory factory = XMLInputFactory.newDefaultFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false);
        factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setXMLResolver((publicId, systemId, baseUri, namespace) -> {
            throw new XMLStreamException("External XML resources are not permitted");
        });
        try (XmlReader xml = new XmlReader(factory.createXMLStreamReader(new ByteArrayInputStream(body)))) {
            return readDocument(xml.reader(), expectedRoot, expectedFields);
        }
        catch (XMLStreamException e) {
            throw new IOException("Invalid S3 XML response for " + expectedRoot, e);
        }
    }

    private static Document readDocument(XMLStreamReader reader, String expectedRoot, Set<String> expectedFields)
            throws XMLStreamException, IOException {
        boolean elementOnly = reader.getEventType() == XMLStreamConstants.START_ELEMENT;
        String root = elementOnly ? reader.getLocalName() : null;
        Set<String> fields = expectedFields;
        Map<String, String> values = new HashMap<>();
        int depth = elementOnly ? 1 : 0;
        int lastPartNumber = 0;
        while (reader.hasNext()) {
            int event = reader.next();
            rejectUnsafeEvent(event);
            if (event == XMLStreamConstants.START_ELEMENT) {
                depth++;
                if (depth > MAX_DEPTH) {
                    throw new IOException("S3 XML response exceeds maximum element depth");
                }
                String name = reader.getLocalName();
                if (depth == 1) {
                    if (!expectedRoot.equals(name) && !"Error".equals(name)) {
                        throw new IOException("Unexpected S3 XML response root: " + diagnostic(name, 128));
                    }
                    root = name;
                    fields = "Error".equals(name) ? ERROR_FIELDS : expectedFields;
                }
                else if (depth == 2 && "ListPartsResult".equals(root) && "Part".equals(name)) {
                    int number = readPart(reader);
                    if (number <= lastPartNumber) {
                        throw new IOException("S3 list-parts response has unordered or duplicate parts");
                    }
                    lastPartNumber = number;
                    depth--;
                }
                else if (depth == 2 && fields.contains(name)) {
                    String value = readText(reader);
                    if (values.putIfAbsent(name, value) != null) {
                        throw new IOException("Duplicate S3 XML response field: " + name);
                    }
                    depth--;
                }
            }
            else if (event == XMLStreamConstants.END_ELEMENT) {
                depth--;
                if (depth == 0 && elementOnly) {
                    break;
                }
            }
        }
        if (root == null || depth != 0) {
            throw new IOException("Incomplete S3 XML response for " + expectedRoot);
        }
        return new Document(root, values, lastPartNumber);
    }

    private static int readPart(XMLStreamReader reader) throws XMLStreamException, IOException {
        Document part = readDocument(reader, "Part", Set.of("PartNumber"));
        return partNumber(requiredValue(part.fields(), "PartNumber"));
    }

    private static int partNumber(String value) throws IOException {
        try {
            int number = Integer.parseInt(value.strip());
            validatePartNumber(number);
            return number;
        }
        catch (IllegalArgumentException e) {
            throw new IOException("Invalid S3 list-parts number or marker", e);
        }
    }

    static void validatePartNumber(int partNumber) {
        if (partNumber < 1 || partNumber > 10_000) {
            throw new IllegalArgumentException("Part number must be between 1 and 10000");
        }
    }

    private static String readText(XMLStreamReader reader) throws XMLStreamException, IOException {
        StringBuilder text = new StringBuilder();
        while (reader.hasNext()) {
            int event = reader.next();
            rejectUnsafeEvent(event);
            switch (event) {
                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE -> text.append(reader.getText());
                case XMLStreamConstants.END_ELEMENT -> {
                    return text.toString();
                }
                case XMLStreamConstants.START_ELEMENT -> throw new IOException("Nested S3 XML response field: " + reader.getLocalName());
                default -> {
                }
            }
        }
        throw new IOException("Incomplete S3 XML response field");
    }

    private static void rejectUnsafeEvent(int event) throws IOException {
        if (event == XMLStreamConstants.DTD || event == XMLStreamConstants.ENTITY_REFERENCE) {
            throw new IOException("S3 XML response must not contain a DTD or entity reference");
        }
    }

    private static void validateXmlText(String text) {
        for (int offset = 0; offset < text.length();) {
            int codePoint = text.codePointAt(offset);
            boolean valid = codePoint == '\t' || codePoint == '\n' || codePoint == '\r'
                    || (codePoint >= 0x20 && codePoint <= 0xD7FF)
                    || (codePoint >= 0xE000 && codePoint <= 0xFFFD)
                    || (codePoint >= 0x10000 && codePoint <= 0x10FFFF);
            if (!valid) {
                throw new IllegalArgumentException("ETag contains an invalid XML character");
            }
            offset += Character.charCount(codePoint);
        }
    }

    private static void appendXmlText(StringBuilder xml, String text) {
        for (int i = 0; i < text.length(); i++) {
            char character = text.charAt(i);
            switch (character) {
                case '&' -> xml.append("&amp;");
                case '<' -> xml.append("&lt;");
                case '>' -> xml.append("&gt;");
                case '\r' -> xml.append("&#13;");
                default -> xml.append(character);
            }
        }
    }

    private static String diagnostic(String value, int limit) {
        if (value == null) {
            return "";
        }
        return value.length() <= limit ? value : value.substring(0, limit) + "...";
    }

    private record Document(String root, Map<String, String> fields, int lastPartNumber) {
    }

    private record XmlReader(XMLStreamReader reader) implements AutoCloseable {

        @Override
        public void close() throws XMLStreamException {
            reader.close();
        }
    }

    static final class ServiceException extends IOException {

        private final String code;
        private final String requestId;
        private final String hostId;

        private ServiceException(String code, String message, String requestId, String hostId) {
            super("S3 error " + diagnostic(code, 128) + ": " + diagnostic(message, 512)
                    + " [requestId=" + diagnostic(requestId, 128) + ", hostId=" + diagnostic(hostId, 128) + "]");
            this.code = code;
            this.requestId = requestId;
            this.hostId = hostId;
        }

        String code() {
            return code;
        }

        String requestId() {
            return requestId;
        }

        String hostId() {
            return hostId;
        }
    }
}
