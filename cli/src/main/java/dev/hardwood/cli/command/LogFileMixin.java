/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;

import dev.hardwood.cli.internal.CliLogging;

/// The `--log-file` option, shared by every command that reads a file. See
/// `_designs/CLI_LOGGING.md`.
public class LogFileMixin {

    @Option(
            name = "log-file",
            description = "Write the command's log records to the given path: Hardwood's from FINE, including "
                    + "one line per S3 fetch, and other libraries' warnings. The file is truncated on each "
                    + "invocation. Off by default.")
    Path logFile;

    /// Runs a non-interactive command with its records copied to the log file, if one is given.
    /// Fails without running the command when the log file cannot be opened.
    CommandResult run(Supplier<CommandResult> command) {
        CliLogging.Session logging;
        try {
            logging = CliLogging.forCommand(logFile);
        }
        catch (IOException e) {
            reportFailure(e);
            return CommandResult.FAILURE;
        }
        try (logging) {
            return command.get();
        }
    }

    /// Takes the records off the terminal for a TUI session, or reports why the log file cannot
    /// be opened and returns `null`.
    CliLogging.Session openForTui() {
        try {
            return CliLogging.forTui(logFile);
        }
        catch (IOException e) {
            reportFailure(e);
            return null;
        }
    }

    private void reportFailure(IOException e) {
        System.err.println("Error: cannot write log file " + logFile + ": " + failure(e));
    }

    /// Why the log file could not be opened. The two common causes carry only the path as their
    /// message, which the caller already prints.
    private static String failure(IOException e) {
        return switch (e) {
            case NoSuchFileException ignored -> "its directory does not exist";
            case AccessDeniedException ignored -> "permission denied";
            default -> e.getMessage();
        };
    }
}
