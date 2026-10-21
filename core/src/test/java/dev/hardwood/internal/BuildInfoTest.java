/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The build identity baked into `hardwood-core`. Its components end up verbatim inside the
/// `created_by` of every file the writer produces, so a component the build resolved wrongly
/// ships in the files themselves.
///
/// [BuildInfo#resolve] maps a missing, blank or unsubstituted value to [BuildInfo#UNKNOWN],
/// so asserting that a component is non-blank and free of `${` holds however badly the build
/// went. Each assertion here is instead one the placeholder itself would fail.
class BuildInfoTest {

    /// The version comes from `${project.version}`, which Maven always supplies, so resolving
    /// it to the placeholder means the resource was never filtered.
    @Test
    void resolvesTheVersionToARealValueNotThePlaceholder() {
        assertThat(BuildInfo.version()).isNotEqualTo(BuildInfo.UNKNOWN);
    }

    /// The revision comes from `git rev-parse --short=7`, so where it resolves at all it is
    /// at least seven lowercase hex digits, more where seven are ambiguous among the clone's
    /// objects, and short of a full SHA's forty. [BuildInfo#UNKNOWN] is admitted because a build
    /// outside a checkout legitimately reports it — but nothing else is, which is what catches a
    /// revision that came through as a full SHA, an error message, or a stray placeholder.
    @Test
    void resolvesTheRevisionToAShortCommitHashOrThePlaceholder() {
        assertThat(BuildInfo.revision()).matches("[0-9a-f]{7,39}|" + BuildInfo.UNKNOWN);
    }

    @Test
    void marksTheRevisionDirtyOnlyWhenTheWorkingTreeWas() {
        assertThat(BuildInfo.revisionWithDirtyMark())
                .isEqualTo(BuildInfo.dirty() ? BuildInfo.revision() + "-dirty" : BuildInfo.revision());
    }
}
