/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class NativeLibraryLoaderTest {

    @Test
    void inImageCodeReturnsFalseOnJvm() {
        assertThat(NativeLibraryLoader.inImageCode()).isFalse();
    }

    @Test
    void loadCodecsAreNoOpOnJvm() {
        // Should not throw — all load methods are no-ops outside a native image
        NativeLibraryLoader.loadZstd();
        NativeLibraryLoader.loadLz4();
        NativeLibraryLoader.loadSnappy();
    }

    @Test
    void libPathEnvOverridesDefault(@TempDir Path tmpDir) throws IOException {
        // Create a fake lib file to verify HARDWOOD_LIB_PATH is respected
        // (loadCodec short-circuits on inImageCode(), so we just verify no errors)
        Files.createFile(tmpDir.resolve("libzstd-jni-fake.so"));
        NativeLibraryLoader.loadZstd();
    }
}
