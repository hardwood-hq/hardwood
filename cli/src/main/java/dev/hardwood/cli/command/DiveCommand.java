/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.DiveApp;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.s3.RangeBacking;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;

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
@CommandDefinition(
        name = "dive",
        description = "Interactively explore a Parquet file's structure.")
public class DiveCommand extends FileCommandBase implements Command<CommandInvocation> {

    @Option(
            name = "max-dict-bytes",
            description = "Maximum chunk size (in bytes) to auto-load on the Dictionary screen; "
                    + "larger chunks require a confirm prompt. Default: 16777216 (16 MiB).",
            defaultValue = "16777216")
    int maxDictBytes;

    @Option(
            name = "smoke-render",
            description = "Render one frame to a 120x40 buffer and exit 0. Used by the native-image smoke test; not intended for interactive use.",
            defaultValue = "false",
            hasValue = false)
    boolean smokeRender;

    @Option(
            name = "log-file",
            description = "Write FINE-level dev.hardwood logs (including per-fetch entries from S3InputFile) "
                    + "to the given path. The file is truncated on each invocation. Off by default.")
    String logFile;

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        // Without a TTY the JLine TUI can't run; checked before any file I/O.
        // Smoke-render never enters raw mode, so it is exempt.
        if (!smokeRender && interactiveTerminalUnavailable()) {
            System.err.println(
                    "Error: 'dive' requires an interactive terminal. "
                            + "Re-run attached to a TTY (with Docker: docker run -it ...).");
            return CommandResult.FAILURE;
        }

        // Dive re-reads the same byte ranges constantly (page-up/page-down
        // navigation, jump-to-end-then-back, the `t` toggle, etc.). Opt
        // into the sparse-tempfile range cache so those repeats hit a
        // local mmap instead of S3. See #373.
        InputFile inputFile = toInputFile(invocation, RangeBacking.SPARSE_TEMPFILE);
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        FileHandler logHandler = installLogFileHandler(invocation);
        try (ParquetModel model = ParquetModel.open(inputFile, getCleanedFile())) {
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
            invocation.print("Error reading file: " + e.getMessage() + "\n");
            return CommandResult.FAILURE;
        }
        catch (Exception e) {
            invocation.print("Error running dive TUI: " + e.getMessage() + "\n");
            return CommandResult.FAILURE;
        }
        finally {
            if (logHandler != null) {
                logHandler.close();
            }
        }
    }

    /// Reports whether stdin or stdout is not attached to a terminal.
    /// `System.console()` is `null` when either is redirected, including in
    /// the native image.
    private static boolean interactiveTerminalUnavailable() {
        return System.console() == null;
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
    private FileHandler installLogFileHandler(CommandInvocation invocation) {
        Logger logger = Logger.getLogger("dev.hardwood");
        logger.setUseParentHandlers(false);
        if (logFile == null) {
            return null;
        }
        try {
            FileHandler handler = new FileHandler(logFile, false);
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
            invocation.print("Failed to open log file " + logFile + ": " + e.getMessage() + "\n");
            return null;
        }
    }
}
