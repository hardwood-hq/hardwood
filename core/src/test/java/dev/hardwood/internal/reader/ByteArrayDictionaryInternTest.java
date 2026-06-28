/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The per-chunk interned-`String` cache decodes each dictionary entry once and
/// hands back the same instance on every request.
class ByteArrayDictionaryInternTest {

    @Test
    void internedStringDecodesEachEntryOnceAndReusesIt() {
        Dictionary.ByteArrayDictionary dict = new Dictionary.ByteArrayDictionary(new byte[][] {
            "alpha".getBytes(StandardCharsets.UTF_8),
            "beta".getBytes(StandardCharsets.UTF_8),
        });

        String alpha = dict.internedString(0);
        String beta = dict.internedString(1);

        assertThat(alpha).isEqualTo("alpha");
        assertThat(beta).isEqualTo("beta");
        // Same entry -> the same cached instance (decoded once per chunk).
        assertThat(dict.internedString(0)).isSameAs(alpha);
        assertThat(dict.internedString(1)).isSameAs(beta);
        // Different entries -> different instances.
        assertThat(beta).isNotSameAs(alpha);
    }
}
