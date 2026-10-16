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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3XmlTest {

    @Test
    void readsTheUploadId() throws IOException {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <Bucket>b</Bucket>
                  <Key>k</Key>
                  <UploadId>a+b/c=</UploadId>
                </InitiateMultipartUploadResult>
                """;

        assertThat(S3Xml.uploadId(bytes(xml), "Create")).isEqualTo("a+b/c=");
    }

    @Test
    void rejectsAnInitiationResponseWithoutUploadId() {
        assertThatThrownBy(() -> S3Xml.uploadId(bytes("<InitiateMultipartUploadResult><Key>k</Key></InitiateMultipartUploadResult>"), "Create"))
                .isInstanceOf(IOException.class)
                .hasMessage("Create failed: response has no upload ID");
    }

    @Test
    void acceptsACompletionResult() {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <Location>http://bucket.s3.amazonaws.com/key</Location>
                  <ETag>"x-2"</ETag>
                </CompleteMultipartUploadResult>
                """;

        assertThatCode(() -> S3Xml.checkCompletion(bytes(xml), "Complete")).doesNotThrowAnyException();
    }

    @Test
    void rejectsAnErrorInACompletionResponse() {
        assertThatThrownBy(() -> S3Xml.checkCompletion(bytes("<Error><Code>InternalError</Code><Message>Try again.</Message></Error>"), "Complete"))
                .isInstanceOf(IOException.class)
                .hasMessage("Complete failed: InternalError: Try again.");
    }

    @Test
    void rejectsAMalformedResponse() {
        assertThatThrownBy(() -> S3Xml.checkCompletion(bytes("<CompleteMultipartUploadResult>"), "Complete"))
                .isInstanceOf(IOException.class)
                .hasMessage("Complete failed: malformed XML response");
    }

    @Test
    void rejectsADocumentTypeDeclaration() {
        String xml = "<!DOCTYPE r [<!ENTITY e SYSTEM \"file:///etc/passwd\">]><CompleteMultipartUploadResult>&e;</CompleteMultipartUploadResult>";

        assertThatThrownBy(() -> S3Xml.checkCompletion(bytes(xml), "Complete"))
                .isInstanceOf(IOException.class)
                .hasMessage("Complete failed: malformed XML response");
    }

    @Test
    void summarizesAnErrorDocument() {
        assertThat(S3Xml.errorSummary(bytes("<Error><Code>NoSuchBucket</Code><Message>Gone</Message><RequestId>1</RequestId></Error>")))
                .isEqualTo(" NoSuchBucket: Gone");
        assertThat(S3Xml.errorSummary(bytes("not xml"))).isEmpty();
        assertThat(S3Xml.errorSummary(new byte[0])).isEmpty();
    }

    @Test
    void listsPartsInTheCompletionBodyWithEscapedETags() {
        byte[] body = S3Xml.completionBody(List.of(new S3Xml.Part(1, "\"a&b\""), new S3Xml.Part(2, "\"c\"")));

        assertThat(new String(body, StandardCharsets.UTF_8)).isEqualTo("""
                <CompleteMultipartUpload>\
                <Part><PartNumber>1</PartNumber><ETag>&quot;a&amp;b&quot;</ETag></Part>\
                <Part><PartNumber>2</PartNumber><ETag>&quot;c&quot;</ETag></Part>\
                </CompleteMultipartUpload>""");
    }

    @Test
    void rejectsPartsOutOfOrder() {
        assertThatThrownBy(() -> S3Xml.completionBody(List.of(new S3Xml.Part(2, "a"), new S3Xml.Part(1, "b"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Completion parts must have ascending part numbers");
    }

    @Test
    void rejectsAPartNumberOutOfRange() {
        assertThatThrownBy(() -> new S3Xml.Part(10_001, "a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Part number must be between 1 and 10000: 10001");
    }

    private static byte[] bytes(String xml) {
        return xml.getBytes(StandardCharsets.UTF_8);
    }
}
