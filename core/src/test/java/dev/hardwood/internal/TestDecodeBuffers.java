/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.util.function.Function;

/// Sets of [DecodeBuffers] for tests that call decode code directly, outside a context's pool.
public final class TestDecodeBuffers {

    private TestDecodeBuffers() {
    }

    /// A new, empty set.
    public static DecodeBuffers create() {
        return new DecodeBuffers();
    }

    /// Runs `work` on the calling thread with a new set.
    public static <T> T decode(Function<DecodeBuffers, T> work) {
        return work.apply(create());
    }
}
