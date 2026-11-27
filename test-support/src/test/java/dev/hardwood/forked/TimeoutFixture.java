/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/// A class whose `@BeforeAll` method hangs the child JVM past its timeout, before any of its
/// tests, run by [ForkedJvmExtensionTest].
@EnabledIfSystemProperty(named = ForkedJvmExtensionTest.FIXTURE_PROPERTY, matches = "true")
@ForkedJvm(jvmArgs = "-Xmx64m", timeoutSeconds = 1)
class TimeoutFixture {

    @BeforeAll
    static void hang() throws InterruptedException {
        new CountDownLatch(1).await();
    }

    @Test
    void passes() {
    }

    @Test
    void passesToo() {
    }
}
