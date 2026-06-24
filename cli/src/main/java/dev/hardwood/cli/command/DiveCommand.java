/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
import dev.hardwood.cli.internal.Fmt;
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
            description = "Maximum chunk size (in bytes) to auto-load on the Dictionary screen; "
                    + "larger chunks require a confirm prompt. Default: ${DEFAULT-VALUE} (16 MiB).",
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
            description = "Write FINE-level dev.hardwood logs (including per-fetch entries from S3InputFile) "
                    + "to the given path. The file is truncated on each invocation. Off by default.")
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

        FileHandler logHandler = installLogFileHandler();
        try (ParquetModel model = ParquetModel.open(inputFile, fileMixin.file)) {
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
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }
        catch (Exception e) {
            System.err.println("Error running dive TUI: " + e.getMessage());
            return CommandResult.FAILURE;
        }
        finally {
            if (logHandler != null) {
                logHandler.close();
            }
        }
    }

    private static boolean interactiveTerminalUnavailable() {
        return System.console() == null;
    }

    private FileHandler installLogFileHandler() {
        Logger logger = Logger.getLogger("dev.hardwood");
        logger.setUseParentHandlers(false);
        if (logFile == null) {
            return null;
        }
        try {
            FileHandler handler = new FileHandler(logFile.toString(), false);
            handler.setLevel(Level.FINE);
            handler.setFormatter(new SimpleFormatter() {
                @Override
                public String format(LogRecord record) {
                    return Fmt.fmt("%1$tFT%1$tT.%1$tL %2$s [%3$s] %4$s%n",
                            record.getMillis(), record.getLevel(), record.getLoggerName(),
                            formatMessage(record));
                }
            });
            logger.setLevel(Level.FINE);
            logger.addHandler(handler);
            return handler;
        }
        catch (IOException e) {
            System.err.println("Failed to open log file " + logFile + ": " + e.getMessage());
            return null;
        }
    }
}
