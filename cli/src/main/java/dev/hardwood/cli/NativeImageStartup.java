/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

@ApplicationScoped
public class NativeImageStartup {

    void onStart(@Observes StartupEvent event) {
        NativeLibraryLoader.loadZstd();
        NativeLibraryLoader.loadLz4();
        NativeLibraryLoader.loadSnappy();
    }
}
