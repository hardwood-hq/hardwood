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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The lifecycle [AbstractOutputFile] enforces: which `do` method each call runs in each state,
/// and the state it leaves the file in.
class AbstractOutputFileTest {

    /// Records each `do` call; the ones named in `failing` throw after recording.
    private static final class Recording extends AbstractOutputFile {

        final List<String> calls = new ArrayList<>();
        final Set<String> failing = new HashSet<>();

        Recording(String... failing) {
            super("out.parquet");
            this.failing.addAll(List.of(failing));
        }

        private void call(String name) throws IOException {
            calls.add(name);
            if (failing.contains(name)) {
                throw new IOException(name + " failed");
            }
        }

        @Override
        protected void doCreate() throws IOException {
            call("create");
        }

        @Override
        protected void doWrite(ByteBuffer data) throws IOException {
            call("write");
            data.position(data.limit());
        }

        @Override
        protected void doClose() throws IOException {
            call("close");
        }

        @Override
        protected void doDiscard() throws IOException {
            call("discard");
        }

        /// Calls `close()` and `discard()` again, as a caller retrying or cleaning up does.
        void endAgain() throws IOException {
            close();
            discard();
        }
    }

    private static Recording open(String... failing) throws IOException {
        Recording out = new Recording(failing);
        out.create();
        return out;
    }

    private static void assertRejected(Recording out, String state) {
        assertThatThrownBy(out::create)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for out.parquet cannot be created; it " + state);
        if (!state.equals("is open")) {
            assertThatThrownBy(() -> out.write(ByteBuffer.allocate(1)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Output for out.parquet is not open; it " + state);
            assertThatThrownBy(out::position)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Output for out.parquet is not open; it " + state);
        }
    }

    /// `position()` counts the bytes `doWrite` accepted, leaving out a write that failed.
    @Test
    void anOpenFileWritesAndCountsTheBytes() throws IOException {
        Recording out = open();
        out.write(ByteBuffer.allocate(3));
        out.failing.add("write");

        assertThatThrownBy(() -> out.write(ByteBuffer.allocate(5)))
                .isInstanceOf(IOException.class)
                .hasMessage("write failed");
        assertThat(out.position()).isEqualTo(3);
        assertThat(out.calls).containsExactly("create", "write", "write");
        assertRejected(out, "is open");
    }

    @Test
    void anUncreatedFileRejectsWrites() {
        Recording out = new Recording();

        assertThatThrownBy(() -> out.write(ByteBuffer.allocate(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for out.parquet is not open; it has not been created");
        assertThat(out.calls).isEmpty();
    }

    /// A published file stays published: neither a second close nor a discard reaches it.
    @Test
    void closePublishesAnOpenFileOnce() throws IOException {
        Recording out = open();
        out.close();
        out.endAgain();

        assertThat(out.calls).containsExactly("create", "close");
        assertRejected(out, "is closed");
    }

    @Test
    void discardThrowsAwayAnOpenFileOnce() throws IOException {
        Recording out = open();
        out.discard();
        out.endAgain();

        assertThat(out.calls).containsExactly("create", "discard");
        assertRejected(out, "was discarded");
    }

    /// try-with-resources around a file whose writer failed before creating it closes it.
    @Test
    void closeOrDiscardOfAnUncreatedFileEndsItWithoutWork() throws IOException {
        Recording closed = new Recording();
        closed.close();
        Recording discarded = new Recording();
        discarded.discard();

        assertThat(closed.calls).isEmpty();
        assertThat(discarded.calls).isEmpty();
        assertRejected(closed, "was discarded");
        assertRejected(discarded, "was discarded");
    }

    /// A failed create leaves nothing to discard, so `doDiscard` does not run.
    @Test
    void aFailedCreateEndsTheFile() throws IOException {
        Recording out = new Recording("create");

        assertThatThrownBy(out::create)
                .isInstanceOf(IOException.class)
                .hasMessage("create failed");
        out.endAgain();

        assertThat(out.calls).containsExactly("create");
        assertRejected(out, "was discarded");
    }

    /// A close whose publishing fails discards the file, so a retried close has nothing to do.
    @Test
    void aFailedCloseDiscardsTheFile() throws IOException {
        Recording out = open("close");

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("close failed");
        out.endAgain();

        assertThat(out.calls).containsExactly("create", "close", "discard");
        assertRejected(out, "was discarded");
    }

    @Test
    void aFailedDiscardIsSuppressedOnTheFailedClose() throws IOException {
        Recording out = open("close", "discard");

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("close failed")
                .satisfies(e -> assertThat(e.getSuppressed())
                        .singleElement()
                        .satisfies(s -> assertThat(s).hasMessage("discard failed")));
        assertRejected(out, "was discarded");
    }

    /// A discard that fails is not retried: the file is discarded before `doDiscard` runs.
    @Test
    void aFailedDiscardEndsTheFile() throws IOException {
        Recording out = open("discard");

        assertThatThrownBy(out::discard)
                .isInstanceOf(IOException.class)
                .hasMessage("discard failed");
        out.endAgain();

        assertThat(out.calls).containsExactly("create", "discard");
        assertRejected(out, "was discarded");
    }

    /// A `doWrite` that stops short would leave `position()` past the end of the file, so the
    /// footer offsets would be wrong.
    @Test
    void aWriteThatLeavesBytesBehindFails() throws IOException {
        AbstractOutputFile out = new AbstractOutputFile("sink") {
            @Override
            protected void doWrite(ByteBuffer data) {
                data.get();
            }
        };
        out.create();

        assertThatThrownBy(() -> out.write(ByteBuffer.allocate(4)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for sink left 3 of 4 bytes unwritten");
        assertThat(out.position()).isZero();
    }

    /// An `Error`, such as an `OutOfMemoryError` allocating a buffer, ends the file as an
    /// exception does, so a partly published file is still discarded.
    @Test
    void anErrorEndsTheFileAsAnExceptionDoes() throws IOException {
        List<String> calls = new ArrayList<>();
        AbstractOutputFile failingCreate = new AbstractOutputFile("sink") {
            @Override
            protected void doCreate() {
                throw new OutOfMemoryError("create failed");
            }

            @Override
            protected void doWrite(ByteBuffer data) {
            }
        };
        AbstractOutputFile failingClose = new AbstractOutputFile("sink") {
            @Override
            protected void doWrite(ByteBuffer data) {
            }

            @Override
            protected void doClose() {
                throw new OutOfMemoryError("close failed");
            }

            @Override
            protected void doDiscard() {
                calls.add("discard");
            }
        };

        assertThatThrownBy(failingCreate::create)
                .isInstanceOf(OutOfMemoryError.class)
                .hasMessage("create failed");
        assertThatThrownBy(failingCreate::create)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for sink cannot be created; it was discarded");

        failingClose.create();
        assertThatThrownBy(failingClose::close)
                .isInstanceOf(OutOfMemoryError.class)
                .hasMessage("close failed");
        assertThat(calls).containsExactly("discard");
        assertThatThrownBy(failingClose::create)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for sink cannot be created; it was discarded");
    }

    /// The hooks other than `doWrite` do nothing by default.
    @Test
    void aSubclassOverridesOnlyDoWrite() throws IOException {
        AbstractOutputFile out = new AbstractOutputFile("sink") {
            @Override
            protected void doWrite(ByteBuffer data) {
                data.position(data.limit());
            }
        };
        out.create();
        out.write(ByteBuffer.allocate(4));
        assertThat(out.position()).isEqualTo(4);
        out.close();

        assertThatThrownBy(out::create)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for sink cannot be created; it is closed");
    }
}
