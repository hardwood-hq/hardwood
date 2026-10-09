/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3XmlTest {

    @ParameterizedTest
    @ValueSource(strings = { "", " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"" })
    void readsUploadIdWithoutChangingOpaqueCharacters(String namespace) throws IOException {
        byte[] body = bytes("""
                <InitiateMultipartUploadResult%s>
                    <Bucket>bucket</Bucket>
                    <Key>file.parquet</Key>
                    <UploadId> +/=%%&amp;&lt;雪 </UploadId>
                    <Optional><UploadId>ignored</UploadId></Optional>
                </InitiateMultipartUploadResult>
                """.formatted(namespace));

        assertThat(S3Xml.uploadId(body)).isEqualTo(" +/=%&<雪 ");
    }

    @Test
    void readsPrefixedNamespaceAndCompletionETag() throws IOException {
        byte[] body = bytes("""
                <s3:CompleteMultipartUploadResult xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/">
                    <s3:Location>https://bucket.s3.amazonaws.com/file.parquet</s3:Location>
                    <s3:ETag>&quot;opaque&amp;&lt;etag&gt;-2&quot;</s3:ETag>
                </s3:CompleteMultipartUploadResult>
                """);

        assertThat(S3Xml.completedETag(body)).isEqualTo("\"opaque&<etag>-2\"");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "<InitiateMultipartUploadResult/>",
            "<InitiateMultipartUploadResult><UploadId/></InitiateMultipartUploadResult>",
            "<InitiateMultipartUploadResult><UploadId> </UploadId></InitiateMultipartUploadResult>",
            "<InitiateMultipartUploadResult><UploadId>one</UploadId><UploadId>two</UploadId></InitiateMultipartUploadResult>",
            "<InitiateMultipartUploadResult><UploadId><Nested>one</Nested></UploadId></InitiateMultipartUploadResult>",
            "<InitiateMultipartUploadResult><UploadId>one</UploadId>",
            "<Other><UploadId>one</UploadId></Other>",
            "<InitiateMultipartUploadResult><UploadId>one</UploadId></InitiateMultipartUploadResult><Other/>",
            "<InitiateMultipartUploadResult><UploadId>one</UploadId></InitiateMultipartUploadResult>junk"
    })
    void rejectsInvalidInitiationResponse(String body) {
        assertThatThrownBy(() -> S3Xml.uploadId(bytes(body)))
                .isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<CompleteMultipartUploadResult/>",
            "<CompleteMultipartUploadResult><ETag/></CompleteMultipartUploadResult>",
            "<CompleteMultipartUploadResult><ETag> </ETag></CompleteMultipartUploadResult>",
            "<CompleteMultipartUploadResult><ETag>one</ETag><ETag>two</ETag></CompleteMultipartUploadResult>",
            "<CompleteMultipartUploadResult><Optional><ETag>nested</ETag></Optional></CompleteMultipartUploadResult>",
            "<CompleteMultipartUploadResult><ETag>one</ETag>",
            "<InitiateMultipartUploadResult><ETag>one</ETag></InitiateMultipartUploadResult>"
    })
    void rejectsInvalidCompletionResponse(String body) {
        assertThatThrownBy(() -> S3Xml.completedETag(bytes(body)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void rejectsErrorDocumentInsteadOfAcknowledgingCompletion() {
        byte[] body = bytes("""

                <Error><Code>InvalidPart</Code><Message>Invalid &amp; missing part</Message>
                    <RequestId>request-1</RequestId><HostId>host-1</HostId></Error>
                """);

        assertThatThrownBy(() -> S3Xml.completedETag(body))
                .isInstanceOf(S3Xml.ServiceException.class)
                .hasMessageContaining("InvalidPart")
                .hasMessageContaining("Invalid & missing part")
                .hasMessageContaining("request-1");
    }

    @Test
    void preservesServiceErrorCodeAndRequestIdentifiers() throws IOException {
        S3Xml.ServiceException error = S3Xml.error(bytes("""
                <Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <Code>NoSuchUpload</Code><Message>No upload</Message>
                    <RequestId>request-1</RequestId><HostId>host-1</HostId>
                    <Optional>ignored</Optional>
                </Error>
                """));

        assertThat(error.code()).isEqualTo("NoSuchUpload");
        assertThat(error.requestId()).isEqualTo("request-1");
        assertThat(error.hostId()).isEqualTo("host-1");
    }

    @Test
    void errorMessageIsOptional() throws IOException {
        assertThat(S3Xml.error(bytes("<Error><Code>NoSuchUpload</Code></Error>")))
                .hasMessageContaining("NoSuchUpload");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<Error/>",
            "<Error><Code/></Error>",
            "<Error><Code>one</Code><Code>two</Code></Error>",
            "<Error><Code>NoSuchUpload</Code><RequestId>one</RequestId><RequestId>two</RequestId></Error>",
            "<Other><Code>NoSuchUpload</Code></Other>"
    })
    void doesNotInferServiceErrorFromMalformedResponse(String body) {
        assertThatThrownBy(() -> S3Xml.error(bytes(body)))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(S3Xml.ServiceException.class);
    }

    @Test
    void limitsErrorDiagnostics() throws IOException {
        S3Xml.ServiceException error = S3Xml.error(bytes(
                "<Error><Code>InternalError</Code><Message>" + "x".repeat(10_000) + "</Message></Error>"));

        assertThat(error.code()).isEqualTo("InternalError");
        assertThat(error.getMessage()).hasSizeLessThan(1024);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<!DOCTYPE InitiateMultipartUploadResult [<!ENTITY id 'injected'>]><InitiateMultipartUploadResult><UploadId>&id;</UploadId></InitiateMultipartUploadResult>",
            "<!DOCTYPE InitiateMultipartUploadResult SYSTEM 'file:///nonexistent-s3-xml-test'><InitiateMultipartUploadResult><UploadId>one</UploadId></InitiateMultipartUploadResult>",
            "<!DOCTYPE InitiateMultipartUploadResult [<!ENTITY id SYSTEM 'file:///nonexistent-s3-xml-test'>]><InitiateMultipartUploadResult><UploadId>&id;</UploadId></InitiateMultipartUploadResult>",
            "<InitiateMultipartUploadResult><UploadId>&undefined;</UploadId></InitiateMultipartUploadResult>"
    })
    void rejectsDtdAndEntityReferences(String body) {
        assertThatThrownBy(() -> S3Xml.uploadId(bytes(body)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void boundsResponseSizeAndNesting() {
        byte[] largeBody = bytes("<InitiateMultipartUploadResult><UploadId>"
                + "x".repeat(256 * 1024) + "</UploadId></InitiateMultipartUploadResult>");
        byte[] deeplyNested = bytes("<InitiateMultipartUploadResult><UploadId>one</UploadId>"
                + "<Optional>".repeat(100) + "</Optional>".repeat(100) + "</InitiateMultipartUploadResult>");

        assertThatThrownBy(() -> S3Xml.uploadId(largeBody)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> S3Xml.uploadId(deeplyNested)).isInstanceOf(IOException.class);
    }

    @Test
    void serializesOrderedPartsAndPreservesOpaqueETags() throws IOException {
        byte[] body = S3Xml.completionBody(List.of(
                new S3Xml.Part(1, "\"etag&<one>\""),
                new S3Xml.Part(10_000, "\"etag雪\"")));

        assertThat(new String(body, StandardCharsets.UTF_8)).isEqualTo("""
                <CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"etag&amp;&lt;one&gt;"</ETag></Part><Part><PartNumber>10000</PartNumber><ETag>"etag雪"</ETag></Part></CompleteMultipartUpload>""");
    }

    @Test
    void preservesCarriageReturnInXmlText() {
        byte[] body = S3Xml.completionBody(List.of(new S3Xml.Part(1, "etag\rvalue")));

        assertThat(new String(body, StandardCharsets.UTF_8)).contains("<ETag>etag&#13;value</ETag>");
    }

    @Test
    void rejectsEmptyUnorderedAndDuplicateCompletionReceipts() {
        assertThatThrownBy(() -> S3Xml.completionBody(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> S3Xml.completionBody(List.of(new S3Xml.Part(2, "two"), new S3Xml.Part(1, "one"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> S3Xml.completionBody(List.of(new S3Xml.Part(1, "one"), new S3Xml.Part(1, "one"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 10_001 })
    void rejectsInvalidPartNumbers(int partNumber) {
        assertThatThrownBy(() -> new S3Xml.Part(partNumber, "etag"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "etag\u0000", "etag\uD800" })
    void rejectsMissingOrInvalidETags(String etag) {
        assertThatThrownBy(() -> new S3Xml.Part(1, etag))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullReceipts() {
        assertThatThrownBy(() -> new S3Xml.Part(1, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> S3Xml.completionBody(null)).isInstanceOf(NullPointerException.class);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
