/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/// Closes the input files a reader or iterator owns, or the child readers a reader closes with
/// it, attempting every one even when an earlier one fails to close.
public final class InputFileCloser {

    private InputFileCloser() {
    }

    /// Throws `failure`, an [IOException], a [RuntimeException] or an [Error], unless it is `null`.
    public static void rethrow(Throwable failure) throws IOException {
        switch (failure) {
            case null -> {
            }
            case IOException e -> throw e;
            case RuntimeException e -> throw e;
            case Error e -> throw e;
            default -> throw new IllegalStateException("Unexpected close failure", failure);
        }
    }

    /// Attempts to close everything in `files`, skipping `null` entries. Each close failure, an
    /// [IOException], a [RuntimeException] or an [Error], is attached as suppressed to
    /// `failure`, or becomes the failure when `failure` is `null`.
    ///
    /// @return `failure`, or the first close failure when `failure` is `null`
    public static Throwable closeAll(Collection<? extends Closeable> files, Throwable failure) {
        Throwable first = failure;
        for (Closeable file : files) {
            if (file == null) {
                continue;
            }
            try {
                file.close();
            }
            catch (IOException | RuntimeException | Error closeException) {
                if (first == null) {
                    first = closeException;
                }
                else {
                    first.addSuppressed(closeException);
                }
            }
        }
        return first;
    }
}
