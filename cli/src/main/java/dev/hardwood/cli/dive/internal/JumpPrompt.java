/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

/// The `:` prompt's state, owned by [DiveApp] while the prompt is open.
/// `input` is what has been typed so far; `error` says why the last `Enter`
/// did not move anywhere, and is `null` until one does not.
public record JumpPrompt(String input, String error) {
}
