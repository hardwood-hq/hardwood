/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests of each outcome, run by [ForkedJvmExtensionTest].
@EnabledIfSystemProperty(named = ForkedJvmExtensionTest.FIXTURE_PROPERTY, matches = "true")
@ForkedJvm(jvmArgs = "-Xmx64m")
class OutcomesFixture {

    @Test
    void passes() {
        assertTrue(Boolean.getBoolean(ForkedJvmExtension.CHILD_PROPERTY));
    }

    @Test
    void fails() {
        throw new AssertionError("failed in the child");
    }

    @Test
    void aborts() {
        Assumptions.abort("aborted in the child");
    }

    @Test
    @DisabledIfSystemProperty(named = ForkedJvmExtension.CHILD_PROPERTY, matches = "true", disabledReason = "skipped in the child")
    void skippedInChild() {
    }
}
