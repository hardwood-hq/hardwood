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

    /// Error is the only tone that marks content as wrong rather than
    /// as structure, so it carries a hue but no bold — bold would make
    /// a mismatch row read as the selected row.
    @Test
    void errorIsRed() {
        assertThat(Theme.error().effectiveModifiers()).doesNotContain(Modifier.BOLD);
        Color fg = Theme.error().fg().orElseThrow();
        if (Theme.supportsTruecolor(System.getenv("COLORTERM"))) {
            assertThat(fg).isEqualTo(Color.rgb(220, 50, 47));
        }
        else {
            assertThat(fg).isSameAs(Color.RED);
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

    /// The `screenshots` Maven profile sets this property so the
    /// checked-in `dive` SVGs keep the Solarized palette no matter
    /// which terminal the regeneration ran in. Without the override
    /// a recording made without `$COLORTERM` rewrites every asset to
    /// the named-ANSI fallback.
    @Test
    void forcedTruecolorHoldsWithoutColorterm() {
        System.setProperty(Theme.FORCE_TRUECOLOR_PROPERTY, "true");
        try {
            assertThat(Theme.accent().fg().orElseThrow()).isEqualTo(Color.rgb(38, 139, 210));
            assertThat(Theme.selection().fg().orElseThrow()).isEqualTo(Color.rgb(181, 137, 0));
            assertThat(Theme.error().fg().orElseThrow()).isEqualTo(Color.rgb(220, 50, 47));
        }
        finally {
            System.clearProperty(Theme.FORCE_TRUECOLOR_PROPERTY);
        }
    }
}
