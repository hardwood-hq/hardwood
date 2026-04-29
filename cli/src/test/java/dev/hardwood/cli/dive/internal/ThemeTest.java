/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import org.junit.jupiter.api.Test;

import dev.tamboui.style.Color;
import dev.tamboui.style.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

/// Pins each tone to its current implementation so an accidental
/// rebinding shows up in review. The roles ([Theme#primary],
/// [Theme#dim], [Theme#accent]) define the visual hierarchy; the
/// concrete style/colour each returns is an implementation choice
/// that this test guards.
class ThemeTest {

    @Test
    void primaryIsBoldDefaultFg() {
        assertThat(Theme.primary().effectiveModifiers()).contains(Modifier.BOLD);
        assertThat(Theme.primary().fg()).isEmpty();
    }

    @Test
    void dimIsFaintDefaultFg() {
        assertThat(Theme.dim().effectiveModifiers()).contains(Modifier.DIM);
        assertThat(Theme.dim().fg()).isEmpty();
    }

    /// Accent's foreground depends on the host terminal's truecolor
    /// capability (probed via `$COLORTERM`). Truecolor terminals get
    /// Solarized blue pinned via RGB; others fall back to named
    /// `Color.BLUE`.
    @Test
    void accentIsBlue() {
        assertThat(Theme.accent().effectiveModifiers()).doesNotContain(Modifier.BOLD);
        Color fg = Theme.accent().fg().orElseThrow();
        if (Theme.supportsTruecolor(System.getenv("COLORTERM"))) {
            assertThat(fg).isEqualTo(Color.rgb(38, 139, 210));
        }
        else {
            assertThat(fg).isSameAs(Color.BLUE);
        }
    }

    /// Pins both branches of the `$COLORTERM` probe deterministically.
    /// [#accentIsBlue] can only cover whichever branch matches the
    /// test process's actual environment, so it would not have caught
    /// #394 (truecolor frozen to false at native-image build time) on
    /// a build runner that has `COLORTERM` unset.
    @Test
    void truecolorProbeRecognisesBothAdvertisedValues() {
        assertThat(Theme.supportsTruecolor("truecolor")).isTrue();
        assertThat(Theme.supportsTruecolor("24bit")).isTrue();
        assertThat(Theme.supportsTruecolor("256color")).isFalse();
        assertThat(Theme.supportsTruecolor("")).isFalse();
        assertThat(Theme.supportsTruecolor(null)).isFalse();
    }
}
