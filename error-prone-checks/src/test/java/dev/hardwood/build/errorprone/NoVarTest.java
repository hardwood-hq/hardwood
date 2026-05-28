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

final class NoVarTest {

    private final CompilationTestHelper compilationHelper = CompilationTestHelper.newInstance(NoVar.class, getClass());

    @Test
    void rejectsLocalVariableTypeInference() {
        compilationHelper
                .addSourceLines(
                        "src/main/java/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "final class Test {",
                        "  void test() {",
                        "    // BUG: Diagnostic contains: Do not use var; see CLAUDE.md:40.",
                        "    var value = \"value\";",
                        "  }",
                        "}")
                .doTest();
    }

    @Test
    void rejectsEnhancedForVariableTypeInference() {
        compilationHelper
                .addSourceLines(
                        "src/test/java/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "import java.util.List;",
                        "final class Test {",
                        "  void test(List<String> values) {",
                        "    // BUG: Diagnostic contains: Do not use var; see CLAUDE.md:40.",
                        "    for (var value : values) {",
                        "      value.length();",
                        "    }",
                        "  }",
                        "}")
                .doTest();
    }

    @Test
    void rejectsTryWithResourcesVariableTypeInference() {
        compilationHelper
                .addSourceLines(
                        "src/test/java/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "import java.io.StringReader;",
                        "final class Test {",
                        "  void test() throws Exception {",
                        "    // BUG: Diagnostic contains: Do not use var; see CLAUDE.md:40.",
                        "    try (var reader = new StringReader(\"value\")) {",
                        "      reader.read();",
                        "    }",
                        "  }",
                        "}")
                .doTest();
    }

    @Test
    void allowsExplicitTypesAndNonCodeVarText() {
        compilationHelper
                .addSourceLines(
                        "src/main/java/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "final class Test {",
                        "  void test() {",
                        "    String value = \"var\";",
                        "    // var is allowed in comments.",
                        "    String var = value;",
                        "    var.length();",
                        "  }",
                        "}")
                .doTest();
    }

    @Test
    void ignoresSourcesOutsideConventionalJavaRoots() {
        compilationHelper
                .addSourceLines(
                        "src/main/java22/dev/hardwood/Test.java",
                        "package dev.hardwood;",
                        "final class Test {",
                        "  void test() {",
                        "    var value = \"value\";",
                        "  }",
                        "}")
                .doTest();
    }
}
