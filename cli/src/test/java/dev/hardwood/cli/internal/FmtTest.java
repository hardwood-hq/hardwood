/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.util.Locale;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FmtTest {

    @Test
    void usesRootLocaleForGroupingSeparator() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            assertThat(Fmt.fmt("%,d", 1_000_000)).isEqualTo("1,000,000");
        }
        finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void usesRootLocaleForDecimalSeparator() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            assertThat(Fmt.fmt("%.2f", 1.5)).isEqualTo("1.50");
        }
        finally {
            Locale.setDefault(previous);
        }
    }
}
