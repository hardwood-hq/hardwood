/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro.runtime;

/// How a [Driver] runs a machine's commands: where each runs, what running it takes, and the fact
/// it results in.
///
/// @param <C> the commands
/// @param <F> the facts they result in
public interface Interpreter<C, F> {

    /// What a command occupies while it runs.
    enum Resource {
        /// waiting: runs on a virtual thread of its own
        IO,
        /// computing: runs on the driver's pool, or the consumer's thread without one
        CPU
    }

    Resource resource(C command);

    /// Runs `command`, outside the driver's lock; returns its result.
    F execute(C command) throws Exception;

    /// The fact of `command` failing with `cause`.
    F failed(C command, Throwable cause);
}
