/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import dev.hardwood.cli.dive.ScreenState;

/// Result of resolving the `:` prompt's typed number against the current
/// screen's list: either the state to jump to, or the reason `n` was refused.
public record JumpOutcome(ScreenState state, String error) {

    public static JumpOutcome to(ScreenState state) {
        return new JumpOutcome(state, null);
    }

    public static JumpOutcome refuse(String error) {
        return new JumpOutcome(null, error);
    }
}
