/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaNamesTest {

    private static final Pattern AVRO_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    @Test
    void leavesLegalNamesUnchanged() {
        assertThat(SchemaNames.sanitize("name")).isEqualTo("name");
        assertThat(SchemaNames.sanitize("_Name_42")).isEqualTo("_Name_42");
    }

    @Test
    void replacesIllegalCharacters() {
        assertThat(SchemaNames.sanitize("say \"hi\"")).isEqualTo("say__hi_");
        assertThat(SchemaNames.sanitize("total (usd)")).isEqualTo("total__usd_");
        assertThat(SchemaNames.sanitize("a.b")).isEqualTo("a_b");
        assertThat(SchemaNames.sanitize("café")).isEqualTo("caf_");
        assertThat(SchemaNames.sanitize("bell\u0007field")).isEqualTo("bell_field");
    }

    @Test
    void prefixesLeadingDigit() {
        assertThat(SchemaNames.sanitize("1foo")).isEqualTo("_1foo");
        assertThat(SchemaNames.sanitize("9")).isEqualTo("_9");
    }

    @Test
    void mapsEmptyNameToUnderscore() {
        assertThat(SchemaNames.sanitize("")).isEqualTo("_");
    }

    @ParameterizedTest
    @ValueSource(strings = { "name", "_Name_42", "a0" })
    void recognizesLegalNames(String name) {
        assertThat(SchemaNames.isLegal(name)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "1", "-", "a.b", "café" })
    void rejectsIllegalNames(String name) {
        assertThat(SchemaNames.isLegal(name)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "1", "-", "say \"hi\"", "a.b.c", "bell\u0007field", "café", "___", "9lives", "ä€" })
    void alwaysProducesALegalAvroName(String name) {
        assertThat(SchemaNames.sanitize(name)).matches(AVRO_NAME);
    }
}
