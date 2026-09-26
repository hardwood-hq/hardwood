/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.jfr;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import jdk.jfr.Event;
import jdk.jfr.Name;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that the events [AbstractJfrRecorderTest] captures keep their values when
/// later events of the same type arrive in a later flush of the recording stream.
public class RecordedEventRetentionTest extends AbstractJfrRecorderTest {

    /// Longer than the recording stream's one-second flush period, so that the two
    /// events are delivered in different flushes.
    private static final Duration GAP = Duration.ofMillis(2500);

    @Name("dev.hardwood.test.RetentionMarker")
    static final class MarkerEvent extends Event {
        String label;
    }

    @Test
    void retainsEventsDeliveredInEarlierFlushes() throws Exception {
        commitMarker("first");
        Thread.sleep(GAP);
        commitMarker("second");

        awaitEvents();

        assertThat(events("dev.hardwood.test.RetentionMarker").map(event -> event.getString("label")))
                .containsExactly("first", "second");
    }

    private static void commitMarker(String label) {
        MarkerEvent event = new MarkerEvent();
        event.label = label;
        event.commit();
    }
}
