/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import dev.hardwood.internal.reader.engine.micro.runtime.Driver;

/// The read as a consumer sees it: the [Engine] run by a generic [Driver], and batches taken from
/// it in row order.
final class Reader implements AutoCloseable {

    private final Driver<Fact, Command, Engine> driver;

    /// @param threads the pool's threads; `0` runs the decodes on the consumer's thread
    /// @param trace whether to print every fact, the thread posting it, and what the poll returned
    Reader(int threads, boolean trace) {
        Engine engine = new Engine(SimulatedFile.layout(), SimulatedFile.TOTAL_ROWS, SimulatedFile.ROW_GROUP_ROWS,
                Engine.MAX_FETCH_BYTES, Engine.MAX_REQUEST_BYTES, Math.max(1, 2 * threads));
        this.driver = new Driver<>(engine, new ReadInterpreter(), threads, trace);
    }

    /// The next batch in row order, `null` at the end. Waits for one; on zero threads, runs the
    /// read's decodes meanwhile.
    Batch next() {
        return driver.await(engine -> engine.hasBatch() || engine.done() || engine.failure() != null, engine -> {
            if (engine.failure() != null) {
                throw new IllegalStateException("read failed", engine.failure());
            }
            return engine.take();
        });
    }

    @Override
    public void close() {
        driver.close();
    }
}
