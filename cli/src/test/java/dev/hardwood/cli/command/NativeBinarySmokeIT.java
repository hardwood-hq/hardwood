/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke test for the native CLI binary against a local Parquet file. Proves
/// the compiled binary boots, parses arguments, loads a file from disk, and
/// produces expected output. Per-command behavioural coverage lives in the
/// JVM `*CommandTest` classes.
class NativeBinarySmokeIT {

    private final String nativeBinary = System.getProperty("native.image.path");
    private final String plainFile = getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void readsLocalFile() throws IOException, InterruptedException {
        NativeResult result = exec(nativeBinary, "schema", "-f", plainFile);

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("message schema");
    }

    @Test
    void diveSmokeRenderExitsZero() throws IOException, InterruptedException {
        NativeResult result = exec(nativeBinary, "dive", "-f", plainFile, "--smoke-render");

        assertThat(result.exitCode())
                .withFailMessage("dive --smoke-render failed: stdout=%s stderr=%s",
                        result.stdout(), result.stderr())
                .isZero();
    }

    static NativeResult exec(String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command)
                .redirectErrorStream(false);
        Process process = pb.start();
        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        assertThat(finished).withFailMessage("Process timed out after 30s").isTrue();

        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).strip();
        return new NativeResult(process.exitValue(), stdout, stderr);
    }

    record NativeResult(int exitCode, String stdout, String stderr) {
    }
}
