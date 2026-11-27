/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/// A test that leaves a non-daemon thread running and throws an `OutOfMemoryError`, which JUnit
/// rethrows out of the child's launcher, run by [ForkedJvmExtensionTest].
@EnabledIfSystemProperty(named = ForkedJvmExtensionTest.FIXTURE_PROPERTY, matches = "true")
@ForkedJvm(jvmArgs = "-Xmx64m", timeoutSeconds = 60)
class RethrownErrorFixture {

    @Test
    void throwsOutOfMemoryError() {
        Thread.ofPlatform().daemon(false).start(() -> {
            try {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        throw new OutOfMemoryError("thrown by the fixture");
    }
}
