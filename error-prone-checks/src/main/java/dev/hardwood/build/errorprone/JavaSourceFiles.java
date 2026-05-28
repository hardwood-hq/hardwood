/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.build.errorprone;

import com.google.errorprone.VisitorState;

final class JavaSourceFiles {

    private static final String MAIN_JAVA = "/src/main/java/";
    private static final String TEST_JAVA = "/src/test/java/";

    private JavaSourceFiles() {}

    static boolean isConventionalJavaSource(VisitorState state) {
        String path = state.getPath().getCompilationUnit().getSourceFile().toUri().getPath().replace('\\', '/');
        return path.contains(MAIN_JAVA) || path.contains(TEST_JAVA);
    }
}
