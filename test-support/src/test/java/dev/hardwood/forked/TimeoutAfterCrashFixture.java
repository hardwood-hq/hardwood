/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// A parameterized test whose first invocation ends the child JVM and whose second hangs the child
/// it then runs in, run by [ForkedJvmExtensionTest].
@EnabledIfSystemProperty(named = ForkedJvmExtensionTest.FIXTURE_PROPERTY, matches = "true")
@ForkedJvm(jvmArgs = "-Xmx64m", timeoutSeconds = 3)
class TimeoutAfterCrashFixture {

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3 })
    void crashesThenHangs(int invocation) throws InterruptedException {
        if (invocation == 1) {
            Runtime.getRuntime().halt(3);
        }
        if (invocation == 2) {
            new CountDownLatch(1).await();
        }
    }
}
