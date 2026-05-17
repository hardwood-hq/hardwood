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

final class NoLegacyJavadocTest {

    private final CompilationTestHelper compilationHelper =
            CompilationTestHelper.newInstance(NoLegacyJavadoc.class, getClass());

    @Test
    void rejectsLegacyJavadocBlockComments() {
        compilationHelper
                .addSourceLines(
                        "src/main/java/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "// BUG: Diagnostic contains: CLAUDE.md:47.",
                        "/**",
                        " * Legacy JavaDoc.",
                        " */",
                        "final class Test {}")
                .doTest();
    }

    @Test
    void allowsMarkdownJavadocAndRegularBlockComments() {
        compilationHelper
                .addSourceLines(
                        "src/test/java/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "/// Markdown JavaDoc.",
                        "final class Test {",
                        "  /* Regular block comments are allowed. */",
                        "}")
                .doTest();
    }

    @Test
    void ignoresSourcesOutsideConventionalJavaRoots() {
        compilationHelper
                .addSourceLines(
                        "src/main/java22/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "/**",
                        " * Legacy JavaDoc.",
                        " */",
                        "final class Test {}")
                .doTest();
    }
}
