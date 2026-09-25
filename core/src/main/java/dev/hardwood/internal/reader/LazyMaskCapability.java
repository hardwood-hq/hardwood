/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;

/// A row group's [MaskCapability], determined on first request and cached.
///
/// Determining it can read a page header per nested column without an OffsetIndex, and that read
/// is only worth issuing when a page mask would apply to the row group. The shared metadata of a
/// row group therefore carries this holder instead of the value, and a caller that needs the gate
/// asks for it. Several column threads can read the same row group's metadata, so the first
/// request probes under the holder's lock and later ones reuse the result. A failed probe caches
/// nothing.
public final class LazyMaskCapability {

    /// Determines the capability, possibly by reading from the file.
    @FunctionalInterface
    interface Probe {
        MaskCapability run() throws IOException;
    }

    private final Probe probe;
    private volatile MaskCapability value;

    LazyMaskCapability(Probe probe) {
        this.probe = probe;
    }

    /// A holder whose capability is known without probing.
    static LazyMaskCapability of(MaskCapability capability) {
        LazyMaskCapability known = new LazyMaskCapability(() -> capability);
        known.value = capability;
        return known;
    }

    /// Returns the capability, running the probe on the first call.
    MaskCapability get() throws IOException {
        MaskCapability result = value;
        if (result != null) {
            return result;
        }
        synchronized (this) {
            result = value;
            if (result == null) {
                result = probe.run();
                value = result;
            }
            return result;
        }
    }
}
