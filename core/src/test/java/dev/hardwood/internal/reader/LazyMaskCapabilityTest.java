/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LazyMaskCapabilityTest {

    @Test
    void probesOnFirstRequestOnly() throws IOException {
        AtomicInteger probes = new AtomicInteger();
        LazyMaskCapability capability = new LazyMaskCapability(() -> {
            probes.incrementAndGet();
            return MaskCapability.NO;
        });

        assertThat(probes).hasValue(0);
        assertThat(capability.get()).isEqualTo(MaskCapability.NO);
        assertThat(capability.get()).isEqualTo(MaskCapability.NO);
        assertThat(probes).hasValue(1);
    }

    @Test
    void failedProbeIsRetriedOnNextRequest() throws IOException {
        AtomicInteger probes = new AtomicInteger();
        LazyMaskCapability capability = new LazyMaskCapability(() -> {
            if (probes.incrementAndGet() == 1) {
                throw new IOException("probe failed");
            }
            return MaskCapability.YES;
        });

        assertThatThrownBy(capability::get)
                .isInstanceOf(IOException.class)
                .hasMessage("probe failed");
        assertThat(capability.get()).isEqualTo(MaskCapability.YES);
        assertThat(capability.get()).isEqualTo(MaskCapability.YES);
        assertThat(probes).hasValue(2);
    }

    @Test
    void knownCapabilityNeedsNoProbe() throws IOException {
        assertThat(LazyMaskCapability.of(MaskCapability.YES).get()).isEqualTo(MaskCapability.YES);
    }
}
