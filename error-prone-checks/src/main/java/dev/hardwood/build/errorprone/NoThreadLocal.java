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
import com.google.errorprone.matchers.Matcher;
import com.google.errorprone.matchers.method.MethodMatchers;
import com.google.errorprone.util.ASTHelpers;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.NewClassTree;
import com.sun.tools.javac.code.Type;

/// Enforces that library code keeps no state on the threads that run it. A thread-local is held on
/// whichever thread reaches it, the application's included, for that thread's life; decode buffers
/// belong in the `DecodeBuffers` a context's pool hands its tasks.
@AutoService(BugChecker.class)
@BugPattern(
        name = "NoThreadLocal",
        summary = "Do not create a ThreadLocal; it leaves state on threads Hardwood does not own. "
                + "Keep decode buffers in DecodeBuffers.",
        severity = BugPattern.SeverityLevel.ERROR)
public final class NoThreadLocal extends BugChecker
        implements BugChecker.NewClassTreeMatcher, BugChecker.MethodInvocationTreeMatcher {

    private static final Matcher<ExpressionTree> WITH_INITIAL =
            MethodMatchers.staticMethod().onClass("java.lang.ThreadLocal").named("withInitial");

    @Override
    public Description matchNewClass(NewClassTree tree, VisitorState state) {
        Type threadLocal = state.getTypeFromString("java.lang.ThreadLocal");
        if (!JavaSourceFiles.isMainJavaSource(state) || threadLocal == null
                || !ASTHelpers.isSubtype(ASTHelpers.getType(tree), threadLocal, state)) {
            return Description.NO_MATCH;
        }
        return describeMatch(tree);
    }

    @Override
    public Description matchMethodInvocation(MethodInvocationTree tree, VisitorState state) {
        if (!JavaSourceFiles.isMainJavaSource(state) || !WITH_INITIAL.matches(tree, state)) {
            return Description.NO_MATCH;
        }
        return describeMatch(tree);
    }
}
