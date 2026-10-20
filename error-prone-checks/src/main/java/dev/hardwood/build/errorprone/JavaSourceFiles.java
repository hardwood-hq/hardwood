/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.build.errorprone;

import java.util.regex.Pattern;

import com.google.errorprone.VisitorState;

final class JavaSourceFiles {

    private static final Pattern CONVENTIONAL_SOURCE_ROOT = Pattern.compile("/src/(main|test)/java\\d*/");
    private static final Pattern MAIN_SOURCE_ROOT = Pattern.compile("/src/main/java\\d*/");

    private JavaSourceFiles() {}

    static boolean isConventionalJavaSource(VisitorState state) {
        return CONVENTIONAL_SOURCE_ROOT.matcher(path(state)).find();
    }

    /// Whether the file is under a main source root, `src/main/java` or a multi-release one.
    static boolean isMainJavaSource(VisitorState state) {
        return MAIN_SOURCE_ROOT.matcher(path(state)).find();
    }

    private static String path(VisitorState state) {
        return state.getPath().getCompilationUnit().getSourceFile().toUri().getPath().replace('\\', '/');
    }
}
