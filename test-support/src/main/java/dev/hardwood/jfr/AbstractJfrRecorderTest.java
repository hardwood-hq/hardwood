/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.jfr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;

/// Base class for tests that capture JFR events. Each test gets its own
/// `RecordingStream`, started in `@BeforeEach` and stopped synchronously by
/// [#awaitEvents]. Hardwood `@Event` classes are default-enabled at the JVM
/// level, so tests that only care about them can skip [#enable] entirely and
/// just query via [#events]. Default-disabled JDK events (e.g. `jdk.SocketRead`)
/// must be turned on per test via [#enable].
///
/// ### Usage
///
/// ```java
/// class MyTest extends AbstractJfrRecorderTest {
///     @Test
///     void example() throws Exception {
///         // enable(...) only needed for default-disabled events like jdk.SocketRead
///
///         // ... run the code under test ...
///
///         awaitEvents();
///         long count = events("dev.hardwood.RowGroupScanned").count();
///         assertThat(count).isEqualTo(expected);
///     }
/// }
/// ```
///
/// ### Asymmetric assertions
///
/// When writing assertions against event counts or event field sums, include
/// a lower bound (e.g. `isGreaterThan(0)`) — an upper-bound-only assertion
/// trivially passes if no events were captured (for example, if the test
/// forgets [#enable] for a default-disabled event like `jdk.SocketRead`,
/// or if the code under test never runs the relevant path).
public abstract class AbstractJfrRecorderTest {

    private RecordingStream recording;
    private List<RecordedEvent> capturedEvents;
    private boolean stopped;

    @BeforeEach
    final void startJfrRecording() {
        capturedEvents = Collections.synchronizedList(new ArrayList<>());
        recording = new RecordingStream();
        recording.onEvent(capturedEvents::add);
        recording.startAsync();
        stopped = false;
    }

    @AfterEach
    final void closeJfrRecording() {
        try {
            if (!stopped) {
                recording.stop();
            }
        } finally {
            recording.close();
        }
    }

    /// Enables the given default-disabled event types for this test's
    /// recording. Call before the code under test runs. Default-enabled events
    /// (such as Hardwood's `@Event` classes) do not require this.
    protected final void enable(String... eventNames) {
        for (String name : eventNames) {
            recording.enable(name);
        }
    }

    /// Synchronously stops the recording and drains pending events before
    /// returning. After this call, [#events] reflects the full set of captured
    /// events. Safe to call multiple times; only the first call stops.
    protected final void awaitEvents() {
        if (!stopped) {
            recording.stop();
            stopped = true;
        }
    }

    /// Returns the captured events whose event-type name matches `name`.
    /// Should be called after [#awaitEvents] to ensure all events have been
    /// drained.
    protected final Stream<RecordedEvent> events(String name) {
        return capturedEvents.stream()
                .filter(e -> name.equals(e.getEventType().getName()));
    }
}
