/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputTest {

    private final List<String> woken = new ArrayList<>();

    /// Each waiter is woken once, in the order it registered, as the output ends; one registering
    /// after is woken at once.
    @Test
    void wakesEachWaiterOnceAsItEnds() {
        Output<String> output = new Output<>();
        output.whenEnded(() -> woken.add("first " + output.value()));
        output.whenEnded(() -> woken.add("second " + output.value()));
        assertThat(woken).isEmpty();

        output.arrived("bytes");
        output.whenEnded(() -> woken.add("late " + output.value()));

        assertThat(woken).containsExactly("first bytes", "second bytes", "late bytes");
    }

    /// A failure wakes the waiters as a value does.
    @Test
    void wakesItsWaitersWithAFailure() {
        Output<String> output = new Output<>();
        output.whenEnded(() -> woken.add("failed: " + output.failure().getMessage()));

        output.failed(new IllegalStateException("lost"));

        assertThat(woken).containsExactly("failed: lost");
        assertThat(output.hasValue()).isFalse();
    }

    /// An output ends once.
    @Test
    void endsOnce() {
        Output<String> output = Output.of("bytes");

        assertThatThrownBy(() -> output.failed(new IllegalStateException("lost")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("An output ended twice");
    }
}
