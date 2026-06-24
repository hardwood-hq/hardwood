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
import java.nio.charset.StandardCharsets;

import org.aesh.AeshRuntimeRunner;

/// In-process launcher for CLI command tests. Executes the top-level `hardwood`
/// command directly via aesh with stdout and stderr captured into strings.
final class Cli {

    private Cli() {
    }

    static Result launch(String... args) {
        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        PrintStream origOut = System.out;
        PrintStream origErr = System.err;

        int exitCode;
        try {
            System.setOut(new PrintStream(outBuf, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(errBuf, true, StandardCharsets.UTF_8));

            org.aesh.command.CommandResult result = AeshRuntimeRunner.builder()
                    .command(HardwoodCommand.class)
                    .args(args)
                    .execute();
            exitCode = result == org.aesh.command.CommandResult.SUCCESS ? 0 : 1;
        }
        catch (Exception e) {
            exitCode = 1;
            new PrintStream(errBuf, true, StandardCharsets.UTF_8).println(e.getMessage());
        }
        finally {
            System.setOut(origOut);
            System.setErr(origErr);
        }

        return new Result(exitCode,
                stripTrailingNewlines(outBuf.toString(StandardCharsets.UTF_8)),
                stripTrailingNewlines(errBuf.toString(StandardCharsets.UTF_8)));
    }

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
