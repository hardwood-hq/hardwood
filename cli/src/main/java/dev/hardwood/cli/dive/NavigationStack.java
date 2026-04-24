/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.util.ArrayList;
import java.util.List;

/// Mutable stack of [ScreenState] representing the user's drill-down path.
///
/// The bottom of the stack is always the [ScreenState.Overview] root; it is never
/// popped. `push` descends into a child screen, `pop` returns to the parent,
/// `replaceTop` updates the current screen's state in place (e.g. after moving
/// the cursor). `clearToRoot` collapses back to Overview.
public final class NavigationStack {

    private final List<ScreenState> stack = new ArrayList<>();

    public NavigationStack(ScreenState root) {
        if (!(root instanceof ScreenState.Overview)) {
            throw new IllegalArgumentException("Navigation stack root must be Overview, got " + root);
        }
        stack.add(root);
    }

    public ScreenState top() {
        return stack.get(stack.size() - 1);
    }

    public void push(ScreenState state) {
        stack.add(state);
    }

    public void pop() {
        if (stack.size() == 1) {
            return;
        }
        stack.remove(stack.size() - 1);
    }

    public void replaceTop(ScreenState state) {
        stack.set(stack.size() - 1, state);
    }

    public void clearToRoot() {
        while (stack.size() > 1) {
            stack.remove(stack.size() - 1);
        }
    }

    public int depth() {
        return stack.size();
    }

    public List<ScreenState> frames() {
        return List.copyOf(stack);
    }
}
