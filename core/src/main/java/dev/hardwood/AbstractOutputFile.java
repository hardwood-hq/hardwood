/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/// A base for implementing [OutputFile] that enforces its lifecycle.
///
/// The public methods are final: each checks the state the lifecycle table in [OutputFile]
/// allows it in, throws `IllegalStateException` otherwise, and calls the protected `do` method
/// that does the work. [#position()] counts the bytes [#doWrite(ByteBuffer)] accepted. A
/// subclass implements `doWrite` and overrides the others it needs; they do nothing by default:
///
/// | Method | Called by, while the file is open | Does |
/// |---|---|---|
/// | [#doCreate()] | `create()` | acquires the destination's resources |
/// | [#doWrite(ByteBuffer)] | `write` | appends the bytes |
/// | [#doClose()] | `close()` | publishes the file at the destination |
/// | [#doDiscard()] | `discard()`, or `close()` when `doClose()` throws | releases the resources and throws away what was written |
///
/// Each `do` method other than `doWrite` is called at most once per file. When `doCreate()`
/// throws, the file ends discarded without a call to `doDiscard()`, so `doCreate()` must leave
/// nothing held when it fails. When `doClose()` throws, the file ends discarded: `doDiscard()`
/// runs, a failure of it is attached to the `doClose()` failure as suppressed, and the
/// `doClose()` failure is rethrown. The file is discarded before `doDiscard()` runs and is not
/// discarded again, so `doDiscard()` must release what it can before it throws.
///
/// **This API is [Experimental]:** the shape may change in future releases.
@Experimental
public abstract class AbstractOutputFile implements OutputFile {

    private enum State {
        NEW("has not been created"),
        OPEN("is open"),
        CLOSED("is closed"),
        DISCARDED("was discarded");

        private final String description;

        State(String description) {
            this.description = description;
        }
    }

    private final String destination;
    private State state = State.NEW;
    private long position;

    /// @param destination the destination, as the `IllegalStateException`s name it, such as a
    ///        path or a URI
    protected AbstractOutputFile(String destination) {
        this.destination = Objects.requireNonNull(destination, "destination");
    }

    @Override
    public final void create() throws IOException {
        if (state != State.NEW) {
            throw rejection("cannot be created");
        }
        state = State.OPEN;
        try {
            doCreate();
        }
        catch (Throwable t) {
            state = State.DISCARDED;
            throw t;
        }
    }

    @Override
    public final void write(ByteBuffer data) throws IOException {
        requireOpen();
        int length = data.remaining();
        doWrite(data);
        if (data.hasRemaining()) {
            throw new IllegalStateException("Output for " + destination + " left " + data.remaining() + " of "
                    + length + " bytes unwritten");
        }
        position += length;
    }

    @Override
    public final long position() {
        requireOpen();
        return position;
    }

    /// Publishes an open file. Does nothing on a file that is published or discarded, and ends
    /// an uncreated file as discarded without publishing anything.
    @Override
    public final void close() throws IOException {
        if (state == State.NEW) {
            state = State.DISCARDED;
        }
        if (state != State.OPEN) {
            return;
        }
        try {
            doClose();
        }
        catch (Throwable t) {
            state = State.DISCARDED;
            try {
                doDiscard();
            }
            catch (Throwable discardFailure) {
                t.addSuppressed(discardFailure);
            }
            throw t;
        }
        state = State.CLOSED;
    }

    /// Discards an open file. Does nothing on a file that is published or discarded, and ends
    /// an uncreated file as discarded.
    @Override
    public final void discard() throws IOException {
        if (state == State.NEW) {
            state = State.DISCARDED;
        }
        if (state != State.OPEN) {
            return;
        }
        state = State.DISCARDED;
        doDiscard();
    }

    /// Throws unless the file is published, for a subclass that hands out the finished file.
    ///
    /// @throws IllegalStateException if the file is not published
    protected final void requireClosed() {
        if (state != State.CLOSED) {
            throw rejection("is not closed");
        }
    }

    /// Acquires the destination's resources, such as opening a file channel. Does nothing by
    /// default.
    ///
    /// @throws IOException if the resources cannot be acquired; the file then ends discarded
    protected void doCreate() throws IOException {
    }

    /// Appends the remaining bytes of `data`, consuming the buffer. Returning with bytes left in
    /// `data` makes `write` throw `IllegalStateException`, since `position()` would no longer
    /// match the file.
    ///
    /// @param data the bytes to append
    /// @throws IOException if the write fails
    protected abstract void doWrite(ByteBuffer data) throws IOException;

    /// Publishes the file at the destination. May leave resources held when it fails, for
    /// [#doDiscard()] to release. Does nothing by default.
    ///
    /// @throws IOException if the file cannot be published; the file then ends discarded
    protected void doClose() throws IOException {
    }

    /// Releases the resources and throws away what was written, leaving the destination as if
    /// nothing was written. Releases what it can before it throws, since it is not called again.
    /// Does nothing by default.
    ///
    /// @throws IOException if the resources cannot be released
    protected void doDiscard() throws IOException {
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw rejection("is not open");
        }
    }

    private IllegalStateException rejection(String problem) {
        return new IllegalStateException("Output for " + destination + " " + problem + "; it " + state.description);
    }
}
