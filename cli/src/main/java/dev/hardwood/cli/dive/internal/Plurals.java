/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

/// Renders `count + noun` strings consistently across the `dive` TUI: picks
/// singular vs plural form based on the count, and formats the number with
/// the locale-independent grouping separator (comma). Handles irregular
/// plurals ("entry / entries", "leaf / leaves") by requiring both forms from
/// the caller. Zero takes the plural form (standard English convention).
public final class Plurals {

    private Plurals() {
    }

    public static String format(long count, String singular, String plural) {
        return String.format("%,d", count) + " " + (count == 1 ? singular : plural);
    }
}
