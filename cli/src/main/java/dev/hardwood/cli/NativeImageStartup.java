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

/**
 * Eagerly loads JNI-based compression native libraries (zstd-jni, lz4-java, snappy-java) when
 * running as a GraalVM native image. In a standard JVM these libraries self-extract their
 * platform-specific {@code .so}/{@code .dylib}/{@code .dll} from their JAR at runtime, but that
 * mechanism does not work in a native image. Instead, the build unpacks the native libraries into
 * {@code lib/} alongside the binary (via the {@code native} Maven profile), and this startup bean
 * loads them explicitly before any Parquet reading occurs.
 */
@ApplicationScoped
public class NativeImageStartup {

    void onStart(@Observes StartupEvent event) {
        NativeLibraryLoader.loadZstd();
        NativeLibraryLoader.loadLz4();
        NativeLibraryLoader.loadSnappy();
    }
}
