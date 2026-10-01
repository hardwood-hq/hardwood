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

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionVisibility;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.DiveApp;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.internal.CliLogging;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.s3.RangeBacking;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;

@CommandDefinition(
        name = "dive",
        description = "Interactively explore a Parquet file's structure.",
        generateHelp = true)
public class DiveCommand implements Command<CommandInvocation> {

    @Mixin
    FileMixin fileMixin;

    @Option(
            name = "max-dict-bytes",
            description = "Maximum compressed dictionary-page size (in bytes) to auto-load on the "
                    + "Dictionary screen; larger pages require a confirm prompt. "
                    + "Default: ${DEFAULT-VALUE} (16 MiB).",
            defaultValue = "16777216")
    int maxDictBytes;

    @Option(
            name = "smoke-render",
            hasValue = false,
            description = "Render one frame to a 120x40 buffer and exit 0. Used by the native-image smoke test; not intended for interactive use.",
            visibility = OptionVisibility.HIDDEN)
    boolean smokeRender;

    @Option(
            name = "log-file",
            description = "Write the session's log records to the given path: Hardwood's from FINE, including "
                    + "per-fetch entries from S3InputFile, and other libraries' warnings. The file is truncated "
                    + "on each invocation. Off by default.")
    Path logFile;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        if (!smokeRender && interactiveTerminalUnavailable()) {
            System.err.println(
                    "Error: 'dive' requires an interactive terminal. "
                            + "Re-run attached to a TTY (with Docker: docker run -it ...).");
            return CommandResult.FAILURE;
        }

        InputFile inputFile = fileMixin.toInputFile(RangeBacking.SPARSE_TEMPFILE);
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        CliLogging.TuiSession logging;
        try {
            logging = CliLogging.forTui(logFile);
        }
        catch (IOException e) {
            System.err.println("Error: cannot write log file " + logFile + ": " + logFileFailure(e));
            return CommandResult.FAILURE;
        }
        try (logging; ParquetModel model = ParquetModel.open(inputFile, fileMixin.file)) {
            model.setDictionaryReadCapBytes(maxDictBytes);
            DiveApp app = new DiveApp(model);
            if (smokeRender) {
                Buffer buffer = Buffer.empty(new Rect(0, 0, 120, 40));
                app.renderOnce(buffer);
                return CommandResult.SUCCESS;
            }
            app.run();
            return CommandResult.SUCCESS;
        }
        catch (IOException | ParquetReadException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }
        catch (Exception e) {
            System.err.println("Error running dive TUI: " + e.getMessage());
            return CommandResult.FAILURE;
        }
    }

    /// Why the log file could not be opened. The two common causes carry only the path as their
    /// message, which the caller already prints.
    private static String logFileFailure(IOException e) {
        return switch (e) {
            case NoSuchFileException ignored -> "its directory does not exist";
            case AccessDeniedException ignored -> "permission denied";
            default -> e.getMessage();
        };
    }

    /// Reports whether stdin or stdout is not attached to a terminal.
    /// `System.console()` is `null` when either is redirected, including in
    /// the native image.
    private static boolean interactiveTerminalUnavailable() {
        return System.console() == null;
    }
}
