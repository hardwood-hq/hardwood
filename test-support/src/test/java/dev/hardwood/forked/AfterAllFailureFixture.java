/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/// A class whose `@AfterAll` method fails in the child after its test passed, run by
/// [ForkedJvmExtensionTest].
@EnabledIfSystemProperty(named = ForkedJvmExtensionTest.FIXTURE_PROPERTY, matches = "true")
@ForkedJvm(jvmArgs = "-Xmx64m")
class AfterAllFailureFixture {

    @AfterAll
    static void failInChild() {
        throw new IllegalStateException("after all failed in the child");
    }

    @Test
    void passes() {
    }
}
