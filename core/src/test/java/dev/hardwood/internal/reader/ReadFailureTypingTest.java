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
import dev.hardwood.reader.SchemaIncompatibleException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What the decode pipeline says a failure means, asserted where the decision is made.
///
/// The reader's exception model turns on two frames: [ColumnWorker#asReadFailure] decides what a
/// throwable escaping a decoder means, and [BatchExchange#checkError] carries that decision to
/// the consumer. A corrupt file exercises one arm of the first at a time and none of the second,
/// so both are asserted directly.
class ReadFailureTypingTest {

    // ==================== What a decoder threw, said as what it means ====================

    @Test
    void decoderRuntimeExceptionBecomesAReadFailureKeepingItsCause() {
        // The shapes a corrupt file actually produces: a dictionary index out of range, an RLE
        // run header that cannot be, a length that will not fit.
        for (RuntimeException raised : new RuntimeException[] {
                new ArrayIndexOutOfBoundsException("Index 7 out of bounds for length 4"),
                new IllegalStateException("Invalid RLE run header"),
                new ArithmeticException("integer overflow"),
                new IllegalArgumentException("negative page length"),
                new NullPointerException() }) {

            Throwable typed = ColumnWorker.asReadFailure(raised);

            assertThat(typed)
                    .as("%s from a decoder is the file being wrong", raised.getClass().getSimpleName())
                    .isInstanceOf(ParquetReadException.class)
                    .hasCause(raised);
        }
    }

    @Test
    void readFailureMessageFallsBackToTheTypeWhenTheOriginalHasNone() {
        Throwable typed = ColumnWorker.asReadFailure(new ArrayIndexOutOfBoundsException());

        assertThat(typed).hasMessage("ArrayIndexOutOfBoundsException");
    }

    @Test
    void transportFailuresPassThroughUnchanged() {
        // Neither is the file's fault, and an UncheckedIOException is a RuntimeException only
        // because a fetch failure has to cross a task boundary.
        IOException io = new IOException("connection reset");
        UncheckedIOException unchecked = new UncheckedIOException(io);

        assertThat(ColumnWorker.asReadFailure(io)).isSameAs(io);
        assertThat(ColumnWorker.asReadFailure(unchecked)).isSameAs(unchecked);
    }

    @Test
    void alreadyTypedFailuresPassThroughUnchanged() {
        // Re-wrapping would bury the message that already says what is wrong.
        ParquetReadException read = new ParquetReadException("bad magic");
        SchemaIncompatibleException schema = new SchemaIncompatibleException("column type differs");
        UnsupportedOperationException unsupported =
                new UnsupportedOperationException("BROTLI requires com.aayushatharva.brotli4j:brotli4j");

        assertThat(ColumnWorker.asReadFailure(read)).isSameAs(read);
        assertThat(ColumnWorker.asReadFailure(schema)).isSameAs(schema);
        assertThat(ColumnWorker.asReadFailure(unsupported)).isSameAs(unsupported);
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
