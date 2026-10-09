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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ListPartsXmlTest {

    @Test
    void readsPartPresenceAndPagination() throws IOException {
        S3Xml.PartsPage page = S3Xml.partsPage(bytes("""
                <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <IsTruncated>true</IsTruncated><NextPartNumberMarker>7</NextPartNumberMarker>
                    <Part><PartNumber>1</PartNumber><ETag>etag-one</ETag><Size>100</Size></Part>
                    <Part><PartNumber>7</PartNumber><ETag>etag-seven</ETag><Size>200</Size></Part>
                </ListPartsResult>
                """));

        assertThat(page.hasParts()).isTrue();
        assertThat(page.nextMarker()).isEqualTo(7);
    }

    @Test
    void emptyFinalPageHasNoNextMarker() throws IOException {
        S3Xml.PartsPage page = S3Xml.partsPage(bytes("<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>"));
        assertThat(page.hasParts()).isFalse();
        assertThat(page.nextMarker()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<ListPartsResult/>",
            "<ListPartsResult><IsTruncated>maybe</IsTruncated></ListPartsResult>",
            "<ListPartsResult><IsTruncated>true</IsTruncated></ListPartsResult>",
            "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>0</NextPartNumberMarker></ListPartsResult>",
            "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>10001</NextPartNumberMarker></ListPartsResult>",
            "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>9999999999999999999999</NextPartNumberMarker></ListPartsResult>",
            "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>4</NextPartNumberMarker><Part><PartNumber>7</PartNumber></Part></ListPartsResult>",
            "<ListPartsResult><IsTruncated>false</IsTruncated><Part/></ListPartsResult>",
            "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>0</PartNumber></Part></ListPartsResult>",
            "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><PartNumber>2</PartNumber></Part></ListPartsResult>",
            "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>2</PartNumber></Part><Part><PartNumber>1</PartNumber></Part></ListPartsResult>",
            "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber></Part><Part><PartNumber>1</PartNumber></Part></ListPartsResult>"
    })
    void rejectsMalformedOrMisleadingListing(String body) {
        assertThatThrownBy(() -> S3Xml.partsPage(bytes(body))).isInstanceOf(IOException.class);
    }

    @Test
    void preservesNoSuchUploadCodeInsteadOfReportingEmptyPage() {
        assertThatThrownBy(() -> S3Xml.partsPage(bytes("<Error><Code>NoSuchUpload</Code></Error>")))
                .isInstanceOf(S3Xml.ServiceException.class).hasMessageContaining("NoSuchUpload");
    }

    private static byte[] bytes(String body) {
        return body.getBytes(StandardCharsets.UTF_8);
    }
}
