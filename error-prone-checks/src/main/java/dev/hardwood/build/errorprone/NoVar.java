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
import com.google.errorprone.matchers.Description;
import com.sun.source.tree.VariableTree;
import com.sun.tools.javac.tree.JCTree.JCVariableDecl;

/// Enforces Hardwood's policy against Java local-variable type inference.
@AutoService(BugChecker.class)
@BugPattern(
        name = "NoVar",
        summary = "Do not use var; see CLAUDE.md:40.",
        severity = BugPattern.SeverityLevel.ERROR)
public final class NoVar extends BugChecker implements BugChecker.VariableTreeMatcher {

    @Override
    public Description matchVariable(VariableTree tree, VisitorState state) {
        if (!JavaSourceFiles.isConventionalJavaSource(state) || !usesVarType(tree)) {
            return Description.NO_MATCH;
        }
        return describeMatch(tree);
    }

    private static boolean usesVarType(VariableTree tree) {
        return tree instanceof JCVariableDecl && ((JCVariableDecl) tree).declaredUsingVar();
    }
}
