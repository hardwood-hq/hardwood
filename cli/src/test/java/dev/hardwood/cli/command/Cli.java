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

import dev.hardwood.cli.Main;

/// In-process launcher for CLI command tests. Drives the same [Main.run] entry
/// point the binary uses — including its exit-code mapping — with stdout and
/// stderr captured into strings.
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
            exitCode = Main.run(args);
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
