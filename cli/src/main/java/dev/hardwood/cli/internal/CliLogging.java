/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

/// Configures `java.util.logging`, which every log record in the CLI reaches: Hardwood's through
/// `System.Logger`, the AWS SDK's through `slf4j-jdk14`, Aesh's and JLine's directly. See
/// `_designs/CLI_LOGGING.md`.
public final class CliLogging {

    private static final String HARDWOOD = "dev.hardwood";

    private CliLogging() {
    }

    /// Prints every record of level `WARNING` and above as one `LEVEL: message` line on stderr,
    /// replacing the root logger's handlers. Each call leaves a single handler.
    ///
    /// An explicit `java.util.logging.config.file` or `java.util.logging.config.class` takes
    /// precedence, and leaves the configuration untouched.
    public static void configure() {
        if (System.getProperty("java.util.logging.config.file") != null
                || System.getProperty("java.util.logging.config.class") != null) {
            return;
        }
        Logger root = Logger.getLogger("");
        for (Handler handler : root.getHandlers()) {
            root.removeHandler(handler);
        }
        root.addHandler(new StderrHandler());
        root.setLevel(Level.WARNING);
    }

    /// Takes every record off the terminal while the TUI runs, sending them to `logFile` when one
    /// is given and discarding them otherwise. The file receives Hardwood's records from `FINE`
    /// and every other source's from the root level, and is truncated when opened.
    ///
    /// `logFile` is a path, not a `FileHandler` pattern: `%` sequences are not expanded, no lock
    /// file is created, and a second session given the same file writes to it rather than to a
    /// numbered sibling.
    ///
    /// @param logFile the file to write the session's records to, or `null`
    /// @return the session, whose `close` restores the handlers it took off
    /// @throws IOException if `logFile` cannot be opened, in which case nothing has changed
    public static TuiSession forTui(Path logFile) throws IOException {
        Handler fileHandler = null;
        if (logFile != null) {
            fileHandler = new FlushingStreamHandler(Files.newOutputStream(logFile,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
                    new TimestampedFormatter());
            fileHandler.setLevel(Level.FINE);
        }
        return new TuiSession(fileHandler);
    }

    /// The logging of one TUI session. Closing it closes the log file and puts the terminal's
    /// handlers back.
    public static final class TuiSession implements AutoCloseable {

        private final Logger root = Logger.getLogger("");
        private final Logger hardwood = Logger.getLogger(HARDWOOD);
        private final Handler[] terminalHandlers;
        private final Level hardwoodLevel;
        private final Handler fileHandler;

        private TuiSession(Handler fileHandler) {
            this.fileHandler = fileHandler;
            this.terminalHandlers = root.getHandlers();
            this.hardwoodLevel = hardwood.getLevel();
            for (Handler handler : terminalHandlers) {
                root.removeHandler(handler);
            }
            if (fileHandler != null) {
                root.addHandler(fileHandler);
                hardwood.setLevel(Level.FINE);
            }
        }

        @Override
        public void close() {
            if (fileHandler != null) {
                root.removeHandler(fileHandler);
                fileHandler.close();
            }
            hardwood.setLevel(hardwoodLevel);
            for (Handler handler : terminalHandlers) {
                root.addHandler(handler);
            }
        }
    }

    /// Writes to the `System.err` current at each record, where `ConsoleHandler` keeps the one
    /// current when it was created: `Main` replaces the stream to force UTF-8, and the command
    /// tests replace it per run to capture it.
    private static final class StderrHandler extends Handler {

        StderrHandler() {
            setFormatter(new OneLineFormatter());
        }

        @Override
        public void publish(LogRecord record) {
            if (!isLoggable(record)) {
                return;
            }
            System.err.print(getFormatter().format(record));
            System.err.flush();
        }

        @Override
        public void flush() {
            System.err.flush();
        }

        @Override
        public void close() {
            flush();
        }
    }

    /// Flushes each record, so the log file holds every record of a session that ends abruptly.
    private static final class FlushingStreamHandler extends StreamHandler {

        FlushingStreamHandler(OutputStream out, Formatter formatter) {
            super(out, formatter);
        }

        @Override
        public synchronized void publish(LogRecord record) {
            super.publish(record);
            flush();
        }
    }

    /// `LEVEL: message`, then `: ` and the exception if one is attached.
    private static final class OneLineFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            StringBuilder line = new StringBuilder(record.getLevel().getName())
                    .append(": ")
                    .append(formatMessage(record));
            if (record.getThrown() != null) {
                line.append(": ").append(record.getThrown());
            }
            return line.append(System.lineSeparator()).toString();
        }
    }

    /// `time LEVEL [logger] message`, with the time to the millisecond.
    private static final class TimestampedFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            String line = Fmt.fmt("%1$tFT%1$tT.%1$tL %2$s [%3$s] %4$s",
                    record.getMillis(), record.getLevel(), record.getLoggerName(), formatMessage(record));
            return record.getThrown() != null
                    ? line + ": " + record.getThrown() + System.lineSeparator()
                    : line + System.lineSeparator();
        }
    }
}
