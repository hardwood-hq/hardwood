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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [InputFileCloser] closes every input file or child reader it is given, whatever an earlier one
/// throws, an `Error` (which a child's speculative work can raise) included.
class InputFileCloserTest {

    @Test
    void everyCloseableIsClosedWhateverAnEarlierOneThrows() {
        AtomicInteger closed = new AtomicInteger();
        IOException ioFailure = new IOException("close failed");
        SpeculativeError error = new SpeculativeError("prefetch failed");
        List<Closeable> closeables = List.of(
                failing(closed, error),
                failing(closed, ioFailure),
                closed::incrementAndGet);

        Throwable failure = InputFileCloser.closeAll(closeables, null);

        assertThat(closed).hasValue(3);
        assertThat(failure).isSameAs(error);
        assertThat(failure.getSuppressed()).containsExactly(ioFailure);
    }

    @Test
    void closeFailuresAreSuppressedBeneathAnEarlierFailure() {
        IllegalStateException earlier = new IllegalStateException("child close failed");
        SpeculativeError error = new SpeculativeError("prefetch failed");

        Throwable failure = InputFileCloser.closeAll(List.of(failing(new AtomicInteger(), error)), earlier);

        assertThat(failure).isSameAs(earlier);
        assertThat(earlier.getSuppressed()).containsExactly(error);
    }

    @Test
    void rethrowThrowsAnErrorAsItIs() {
        SpeculativeError error = new SpeculativeError("prefetch failed");

        assertThatThrownBy(() -> InputFileCloser.rethrow(error))
                .isSameAs(error)
                .hasMessage("prefetch failed");
    }

    private static Closeable failing(AtomicInteger closed, Throwable failure) {
        return () -> {
            closed.incrementAndGet();
            switch (failure) {
                case IOException e -> throw e;
                case RuntimeException e -> throw e;
                case Error e -> throw e;
                default -> throw new IllegalArgumentException(failure);
            }
        };
    }

    private static final class SpeculativeError extends Error {

        SpeculativeError(String message) {
            super(message);
        }
    }
}
