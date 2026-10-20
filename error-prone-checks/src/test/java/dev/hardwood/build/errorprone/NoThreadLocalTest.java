/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.build.errorprone;

import org.junit.jupiter.api.Test;

import com.google.errorprone.CompilationTestHelper;

final class NoThreadLocalTest {

    private final CompilationTestHelper compilationHelper =
            CompilationTestHelper.newInstance(NoThreadLocal.class, getClass());

    @Test
    void rejectsThreadLocalsInMainSources() {
        compilationHelper
                .addSourceLines(
                        "src/main/java/dev/hardwood/Test.java",
                        """
                        package dev.hardwood;
                        final class Test {
                          // BUG: Diagnostic contains: Do not create a ThreadLocal
                          private static final ThreadLocal<byte[]> BYTES = new ThreadLocal<>();
                          // BUG: Diagnostic contains: Do not create a ThreadLocal
                          private static final ThreadLocal<int[]> INTS = ThreadLocal.withInitial(() -> new int[8]);
                          // BUG: Diagnostic contains: Do not create a ThreadLocal
                          private static final ThreadLocal<String> NAME = new InheritableThreadLocal<>();
                          // BUG: Diagnostic contains: Do not create a ThreadLocal
                          private static final ThreadLocal<Long> ANONYMOUS = new ThreadLocal<>() {
                            @Override
                            protected Long initialValue() {
                              return 0L;
                            }
                          };
                        }
                        """)
                .doTest();
    }

    @Test
    void rejectsThreadLocalsInMultiReleaseSourceRoot() {
        compilationHelper
                .addSourceLines(
                        "src/main/java22/dev/hardwood/Test.java",
                        """
                        package dev.hardwood;
                        final class Test {
                          // BUG: Diagnostic contains: Do not create a ThreadLocal
                          private static final ThreadLocal<byte[]> BYTES = new ThreadLocal<>();
                        }
                        """)
                .doTest();
    }

    @Test
    void allowsSuppressedThreadLocalsAndTestSources() {
        compilationHelper
                .addSourceLines(
                        "src/main/java/dev/hardwood/Scoped.java",
                        """
                        package dev.hardwood;
                        final class Scoped {
                          @SuppressWarnings("NoThreadLocal")
                          private static final ThreadLocal<String> TAG = new ThreadLocal<>();
                        }
                        """)
                .addSourceLines(
                        "src/test/java/dev/hardwood/TestHelper.java",
                        """
                        package dev.hardwood;
                        final class TestHelper {
                          private static final ThreadLocal<String> TAG = ThreadLocal.withInitial(() -> "");
                        }
                        """)
                .doTest();
    }
}
