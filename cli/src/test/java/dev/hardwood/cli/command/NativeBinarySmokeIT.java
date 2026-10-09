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
import java.util.regex.Pattern;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import dev.hardwood.cli.internal.Version;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke test for the native CLI binary against a local Parquet file. Proves
/// the compiled binary boots, parses arguments, loads a file from disk, and
/// produces expected output. Per-command behavioural coverage lives in the
/// JVM `*CommandTest` classes.
@Tag("native")
class NativeBinarySmokeIT {

    /// `hardwood <project-version> (<revision>)`, the same grammar `HardwoodCommandTest` pins
    /// for the JVM. The project version must be semver-shaped, since Maven always supplies it;
    /// the revision is left open because it comes from `git` and legitimately reads `unknown`
    /// in a build outside a checkout.
    private static final Pattern VERSION_LINE = Pattern.compile("hardwood \\d+\\.\\d+\\.\\d+\\S* \\(\\S+\\)");

    private final String nativeBinary = System.getProperty("native.image.path");
    private final String plainFile = Cli.resourcePath("/plain_uncompressed.parquet");

    @Test
    void readsLocalFile() throws IOException, InterruptedException {
        NativeResult result = exec(nativeBinary, "schema", "-f", plainFile);

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("message schema");
    }

    /// The build identity lives in a resource, which is the one part of the JVM build that a
    /// native image can silently drop: unless it is registered in `resource-config.json`, the
    /// binary starts fine and reports `unknown` instead. Comparing the binary against the JVM
    /// it was compiled from catches that, where a smoke test that only checks the exit code or
    /// the `hardwood` prefix does not. Covers the TUI too — its help overlay renders this same
    /// [Version] value.
    ///
    /// The comparison alone would not be enough. If the resource never reached the core
    /// artifact at all, both sides read the same degraded value and agree, and the test would
    /// pass on a build that has no identity. The binary's own output is therefore pinned to a
    /// resolved shape first, which holds regardless of what the JVM side resolved; the
    /// comparison then adds that it is the *same* build.
    @Test
    void reportsTheSameBuildVersionAsTheJvm() throws IOException, InterruptedException {
        NativeResult result = exec(nativeBinary, "--version");

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).matches(VERSION_LINE);
        assertThat(result.stdout()).isEqualTo("hardwood " + Version.getVersion());
    }

    /// Hardwood's records reach `java.util.logging` through the JDK's own `System.LoggerFinder`
    /// provider, which the JDK looks up through `ServiceLoader`. Should the binary not find it,
    /// they would print in the JDK's two-line fallback format instead of the CLI's.
    @Test
    void printsAHardwoodWarningAsOneLine() throws IOException, InterruptedException {
        String annotated = Cli.resourcePath("/annotated_repeated_group_test.parquet");

        NativeResult result = exec(nativeBinary, "schema", "-f", annotated);

        assertThat(result.exitCode()).isZero();
        assertThat(result.stderr()).isEqualTo("WARNING: Ignoring 6 annotation(s) on repeated groups outside"
                + " a LIST or MAP group; those groups are read as though unannotated: foo_mkv (MAP_KEY_VALUE);"
                + " foo_list (LIST); foo_list_lt (LIST); foo_map (MAP); foo_map_ct (MAP); s.bar (LIST)");
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
