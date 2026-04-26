/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KvMetadataFormatterTest {

    @Test
    void jsonIsPrettyPrinted() {
        String json = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";

        String out = KvMetadataFormatter.format("org.apache.spark.sql.parquet.row.metadata", json);

        assertThat(out).contains("\n");
        assertThat(out).contains("  \"type\": \"struct\"");
        assertThat(out).contains("  \"fields\":");
    }

    @Test
    void jsonEscapedQuotesDontBreakParsing() {
        String json = "{\"say\": \"he said \\\"hi\\\"\"}";

        String out = KvMetadataFormatter.format("k", json);

        assertThat(out).contains("\"say\": \"he said \\\"hi\\\"\"");
    }

    @Test
    void arrowSchemaBase64IsRecognized() {
        // A short base64 string starting with "/////" — the Arrow IPC continuation marker.
        String fakeArrow = "/////8gEAAAQAAAA";

        String out = KvMetadataFormatter.format("ARROW:schema", fakeArrow);

        assertThat(out).contains("Arrow IPC schema");
        assertThat(out).contains("Hex dump");
    }

    @Test
    void unknownValuePassesThrough() {
        String plain = "spark 3.5.0";

        String out = KvMetadataFormatter.format("writer.version", plain);

        assertThat(out).isEqualTo(plain);
    }
}
