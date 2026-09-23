/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Worker transport pass-through and exchange delivery for already-classified failures.
class ReadFailureTypingTest {

    // ==================== What the worker passes through ====================

    @Test
    void transportFailuresPassThroughUnchanged() {
        // Neither is the file's fault, and an UncheckedIOException is a RuntimeException only
        // because a fetch failure has to cross a task boundary.
        IOException io = new IOException("connection reset");
        UncheckedIOException unchecked = new UncheckedIOException(io);

        assertThat(ColumnWorker.asReadFailure(io)).isSameAs(io);
        assertThat(ColumnWorker.asReadFailure(unchecked)).isSameAs(unchecked);
    }

    /// An `Error` is not the file's fault and nothing here can act on it, so it is neither
    /// retyped nor placed — but it is recorded, because a consumer waiting on work the
    /// failed thread will never finish would otherwise wait for ever.
    @Test
    void anErrorReachesTheConsumerAsItWasRaised() throws Exception {
        BatchExchange<Object> exchange = BatchExchange.<Object>recycling("amount", Object::new);
        OutOfMemoryError raised = new OutOfMemoryError("Java heap space");

        exchange.signalError(raised);

        assertThatThrownBy(exchange::checkError).isSameAs(raised);
    }

    // ==================== What the exchange does with it ====================

    @Test
    void checkErrorRaisesAReadFailureAsItStands() {
        BatchExchange<Object> exchange = BatchExchange.<Object>recycling("amount", Object::new);
        ParquetReadException signalled = new ParquetReadException("[f.parquet] CRC mismatch");
        exchange.signalError(signalled);

        assertThatThrownBy(exchange::checkError).isSameAs(signalled);
    }

    @Test
    void checkErrorRaisesAnErrorAsItStands() {
        // Relabelling this one would make an OutOfMemoryError catchable by a handler written
        // for a read failure, one frame below the reader.
        BatchExchange<Object> exchange = BatchExchange.<Object>recycling("amount", Object::new);
        OutOfMemoryError oome = new OutOfMemoryError("Java heap space");
        exchange.signalError(oome);

        assertThatThrownBy(exchange::checkError).isSameAs(oome);
    }
}
