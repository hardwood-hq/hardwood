/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.PrintWriter;
import java.io.StringWriter;

import picocli.CommandLine;

/// In-process launcher for CLI command tests. Executes the top-level `hardwood`
/// command directly via picocli with stdout and stderr captured into strings,
/// avoiding the Quarkus bootstrap that `@QuarkusMainTest` pays per test class.
///
/// Mirrors the configuration applied to the production `CommandLine`
/// produced by [HardwoodCommand.getCommandLineInstance] so tests exercise the
/// same parser behaviour.
final class Cli {

    private Cli() {
    }

    static Result launch(String... args) {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();

        CommandLine commandLine = new CommandLine(new HardwoodCommand())
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setOut(new PrintWriter(out, true))
                .setErr(new PrintWriter(err, true));

        int exitCode = commandLine.execute(args);
        return new Result(exitCode, stripTrailingNewlines(out.toString()), stripTrailingNewlines(err.toString()));
    }

    /// `QuarkusMainLauncher` strips the trailing line separator from captured
    /// output. Match that so assertions using text blocks (which have no
    /// trailing newline) behave identically whether launched directly or via
    /// Quarkus.
    private static String stripTrailingNewlines(String s) {
        int end = s.length();
        while (end > 0) {
            char c = s.charAt(end - 1);
            if (c == '\n' || c == '\r') {
                end--;
            }
            else {
                break;
            }
        }
        return s.substring(0, end);
    }

    record Result(int exitCode, String output, String errorOutput) {
    }
}
