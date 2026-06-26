/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.aesh.command.CommandResult;

import dev.hardwood.cli.AeshCli;

/// In-process launcher for CLI command tests. Executes the top-level `hardwood`
/// command directly via Aesh with stdout and stderr captured into strings,
/// avoiding the Quarkus bootstrap that `@QuarkusMainTest` pays per test class.
final class Cli {

    private Cli() {
    }

    static Result launch(String... args) {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        try {
            System.setOut(new PrintStream(out, true));
            System.setErr(new PrintStream(err, true));

            CommandResult result = AeshCli.execute(args);
            int exitCode = result.isSuccess() ? 0 : (result.getResultValue() != 0 ? result.getResultValue() : 1);
            return new Result(exitCode, stripTrailingNewlines(out.toString()), stripTrailingNewlines(err.toString()));
        } catch (Exception e) {
            String errMessage = e.getMessage() != null ? e.getMessage() : e.toString();
            // Since System.err was redirected to err, let's write to it directly:
            System.err.println(errMessage);
            return new Result(1, stripTrailingNewlines(out.toString()), stripTrailingNewlines(err.toString()));
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    /// `QuarkusMainLauncher` strips the trailing line separator from captured
    /// output. Match that so assertions using text blocks (which have no
    /// trailing newline) behave identically whether launched directly or via
    /// Quarkus.
    private static String stripTrailingNewlines(String s) {
        s = s.replace("\r\n", "\n").replace('\r', '\n');
        int end = s.length();
        while (end > 0) {
            char c = s.charAt(end - 1);
            if (c == '\n') {
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
