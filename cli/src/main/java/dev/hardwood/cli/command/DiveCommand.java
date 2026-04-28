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
import java.util.concurrent.Callable;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.DiveApp;
import dev.hardwood.cli.dive.ParquetModel;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/// Launches the interactive `hardwood dive` TUI for exploring a Parquet file's
/// structure. See `_designs/INTERACTIVE_DIVE_TUI.md`.
///
/// **Logging.** Quarkus pins JBoss LogManager as the runtime
/// `System.LoggerFinder` (via the transitive `jboss-logmanager`
/// dependency, which Quarkus's bootstrap references directly and cannot
/// be excluded). JBoss LogManager is JUL-API-compatible, so this command
/// configures dive's optional `--log-file` via `java.util.logging` —
/// records emitted via `System.Logger` from `dev.hardwood.*` flow through
/// JBoss LogManager and into the [FileHandler] attached here.
@CommandLine.Command(
        name = "dive",
        description = "Interactively explore a Parquet file's structure.")
public class DiveCommand implements Callable<Integer> {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;

    @Spec
    CommandSpec spec;

    @CommandLine.Option(
            names = "--max-dict-bytes",
            description = "Maximum chunk size (in bytes) to auto-load on the Dictionary screen; "
                    + "larger chunks require a confirm prompt. Default: ${DEFAULT-VALUE} (16 MiB).",
            defaultValue = "16777216")
    int maxDictBytes;

    @CommandLine.Option(
            names = "--smoke-render",
            description = "Render one frame to a 120x40 buffer and exit 0. Used by the native-image smoke test; not intended for interactive use.",
            hidden = true)
    boolean smokeRender;

    @CommandLine.Option(
            names = "--log-file",
            description = "Write FINE-level dev.hardwood logs (including per-fetch entries from S3InputFile) "
                    + "to the given path. The file is truncated on each invocation. Off by default.",
            paramLabel = "<path>")
    Path logFile;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        FileHandler logHandler = installLogFileHandler();
        try (ParquetModel model = ParquetModel.open(inputFile, fileMixin.file)) {
            model.setDictionaryReadCapBytes(maxDictBytes);
            DiveApp app = new DiveApp(model);
            if (smokeRender) {
                Buffer buffer = Buffer.empty(new Rect(0, 0, 120, 40));
                app.renderOnce(buffer);
                return CommandLine.ExitCode.OK;
            }
            app.run();
            return CommandLine.ExitCode.OK;
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
        catch (Exception e) {
            spec.commandLine().getErr().println("Error running dive TUI: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
        finally {
            if (logHandler != null) {
                logHandler.close();
            }
        }
    }

    /// Configures JUL logging for an interactive dive session.
    ///
    /// Always detaches the `dev.hardwood` logger from parent handlers so
    /// nothing leaks to stdout/stderr while the TUI owns the terminal —
    /// otherwise log records would garble the rendered frames.
    ///
    /// When `--log-file` is set, also attaches a [FileHandler] writing
    /// one record per line to the given path, truncated per session.
    /// Returns the handler so it can be closed at shutdown, or `null`
    /// when no log file is requested.
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
                    return String.format("%1$tFT%1$tT.%1$tL %2$s [%3$s] %4$s%n",
                            record.getMillis(), record.getLevel(), record.getLoggerName(),
                            formatMessage(record));
                }
            });
            logger.setLevel(Level.FINE);
            logger.addHandler(handler);
            return handler;
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Failed to open log file " + logFile + ": " + e.getMessage());
            return null;
        }
    }
}
