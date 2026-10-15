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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/// Runs the CLI as a separate process, for tests that configure it through environment
/// variables. The process sees only the given variables, plus `PATH`, so the AWS variables
/// of the machine running the tests do not reach it; its profile files do unless the given
/// variables point `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` elsewhere.
final class CliProcess {

    private CliProcess() {
    }

    /// Runs the native binary at `binary`.
    static Cli.Result runNative(String binary, Map<String, String> environment, String... args)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(binary);
        command.addAll(List.of(args));
        return run(command, environment);
    }

    /// Runs the CLI on a JVM with the test classpath.
    static Cli.Result runJvm(Map<String, String> environment, String... args)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-native-access=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                "dev.hardwood.cli.Main"));
        command.addAll(List.of(args));
        return run(command, environment);
    }

    private static Cli.Result run(List<String> command, Map<String, String> environment)
            throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(command);
        String path = builder.environment().get("PATH");
        builder.environment().clear();
        if (path != null) {
            builder.environment().put("PATH", path);
        }
        builder.environment().putAll(environment);

        Process process = builder.start();
        boolean finished = process.waitFor(60, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        assertThat(finished).withFailMessage("Process timed out after 60s").isTrue();

        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).strip();
        return new Cli.Result(process.exitValue(), stdout, stderr);
    }
}
