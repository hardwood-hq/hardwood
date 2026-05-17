/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.build.errorprone;

import com.google.auto.service.AutoService;
import com.google.errorprone.BugPattern;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.fixes.ErrorProneEndPosTable;
import com.google.errorprone.fixes.ErrorPronePosition;
import com.google.errorprone.matchers.Description;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.tools.javac.tree.JCTree;

/// Enforces Hardwood's policy requiring Markdown JavaDoc comments.
@AutoService(BugChecker.class)
@BugPattern(
        name = "NoLegacyJavadoc",
        summary = "JavaDoc must use Markdown /// syntax, not /** */; see CLAUDE.md:47.",
        severity = BugPattern.SeverityLevel.ERROR)
public final class NoLegacyJavadoc extends BugChecker implements BugChecker.CompilationUnitTreeMatcher {

    private static final String LEGACY_JAVADOC_START = "/**";

    @Override
    public Description matchCompilationUnit(CompilationUnitTree tree, VisitorState state) {
        if (!JavaSourceFiles.isConventionalJavaSource(state)) {
            return Description.NO_MATCH;
        }

        int start = state.getSourceCode().toString().indexOf(LEGACY_JAVADOC_START);
        if (start < 0) {
            return Description.NO_MATCH;
        }
        return describeMatch(new SourceOffsetPosition((JCTree) tree, start));
    }

    private static final class SourceOffsetPosition implements ErrorPronePosition {

        private final JCTree tree;
        private final int start;

        private SourceOffsetPosition(JCTree tree, int start) {
            this.tree = tree;
            this.start = start;
        }

        @Override
        public int getStartPosition() {
            return start;
        }

        @Override
        public int getPreferredPosition() {
            return start;
        }

        @Override
        public JCTree getTree() {
            return tree;
        }

        @Override
        public int getEndPosition(ErrorProneEndPosTable endPositions) {
            return start + LEGACY_JAVADOC_START.length();
        }
    }
}
