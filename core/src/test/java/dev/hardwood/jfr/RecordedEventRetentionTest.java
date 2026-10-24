/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.jfr;

import org.junit.jupiter.api.Test;

import jdk.jfr.Event;
import jdk.jfr.Name;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that the events [AbstractJfrRecorderTest] captures keep their values when
/// later events of the same type arrive in a later flush of the recording stream.
public class RecordedEventRetentionTest extends AbstractJfrRecorderTest {

    @Name("dev.hardwood.test.RetentionMarker")
    static final class MarkerEvent extends Event {
        String label;
    }

    @Test
    void retainsEventsDeliveredInEarlierFlushes() throws Exception {
        commitMarker("first");
        // Committed once the first is delivered, so the two arrive in different flushes
        awaitDelivered("dev.hardwood.test.RetentionMarker");
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
