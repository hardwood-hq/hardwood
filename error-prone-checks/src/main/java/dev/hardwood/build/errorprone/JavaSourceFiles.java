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

    private JavaSourceFiles() {}

    static boolean isConventionalJavaSource(VisitorState state) {
        String path = state.getPath().getCompilationUnit().getSourceFile().toUri().getPath().replace('\\', '/');
        return CONVENTIONAL_SOURCE_ROOT.matcher(path).find();
    }
}
