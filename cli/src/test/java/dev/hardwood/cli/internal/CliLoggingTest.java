/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class CliLoggingTest {

    private static final System.Logger LOG = System.getLogger("dev.hardwood.cli.internal.CliLoggingTest");
    private static final System.Logger OTHER = System.getLogger("org.example.Library");

    /// Records reach the stderr current when they are logged, one line each, and a record below
    /// `WARNING` is dropped.
    @Test
    void printsWarningsAndAboveToTheCurrentStderr() {
        CliLogging.configure();

        assertThat(captureStderr(() -> {
            LOG.log(Level.INFO, "not shown");
            LOG.log(Level.WARNING, "shown {0}", "once");
        })).isEqualTo("WARNING: shown once" + System.lineSeparator());
        assertThat(captureStderr(() -> LOG.log(Level.ERROR, "failed", new IOException("disk full"))))
                .isEqualTo("SEVERE: failed: java.io.IOException: disk full" + System.lineSeparator());
    }

    /// Configuring again replaces the handler rather than adding a second one.
    @Test
    void configuringTwicePrintsEachRecordOnce() {
        CliLogging.configure();
        CliLogging.configure();

        assertThat(captureStderr(() -> LOG.log(Level.WARNING, "once")))
                .isEqualTo("WARNING: once" + System.lineSeparator());
    }

    /// The AWS SDK logs through SLF4J, which `slf4j-jdk14` hands to the same handler.
    @Test
    void slf4jRecordsTakeTheSameRoute() {
        CliLogging.configure();

        assertThat(captureStderr(() -> {
            LoggerFactory.getLogger("software.amazon.awssdk.Example").info("not shown");
            LoggerFactory.getLogger("software.amazon.awssdk.Example").warn("retrying {}", "request");
        })).isEqualTo("WARNING: retrying request" + System.lineSeparator());
    }

    /// While the TUI runs nothing reaches stderr; the log file receives Hardwood's records from
    /// `FINE` and other sources' from `WARNING`, and stderr is back once the session closes.
    @Test
    void tuiSessionSendsRecordsToTheLogFileOnly(@TempDir Path tempDir) throws IOException {
        CliLogging.configure();
        Path logFile = tempDir.resolve("dive.log");

        String duringSession = captureStderr(() -> {
            try (CliLogging.TuiSession ignored = CliLogging.forTui(logFile)) {
                LOG.log(Level.DEBUG, "hardwood debug");
                OTHER.log(Level.INFO, "library info");
                OTHER.log(Level.WARNING, "library warning");
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        assertThat(duringSession).isEmpty();
        assertThat(Files.readAllLines(logFile))
                .hasSize(2)
                .satisfiesExactly(
                        line -> assertThat(line).endsWith(" FINE [dev.hardwood.cli.internal.CliLoggingTest] hardwood debug"),
                        line -> assertThat(line).endsWith(" WARNING [org.example.Library] library warning"));
        assertThat(captureStderr(() -> OTHER.log(Level.WARNING, "after")))
                .isEqualTo("WARNING: after" + System.lineSeparator());
        assertThat(captureStderr(() -> LOG.log(Level.DEBUG, "not shown after"))).isEmpty();
    }

    /// The log file is the path given: no `%` expansion and no lock file beside it.
    @Test
    void logFileIsAPathNotAPattern(@TempDir Path tempDir) throws IOException {
        Path logFile = tempDir.resolve("dive-%g-%u.log");

        try (CliLogging.TuiSession ignored = CliLogging.forTui(logFile)) {
            OTHER.log(Level.WARNING, "recorded");
        }

        try (Stream<Path> files = Files.list(tempDir)) {
            assertThat(files.map(file -> file.getFileName().toString())).containsExactly("dive-%g-%u.log");
        }
        assertThat(Files.readAllLines(logFile)).singleElement().asString()
                .endsWith(" WARNING [org.example.Library] recorded");
    }

    /// Without a log file, the session's records are discarded.
    @Test
    void tuiSessionWithoutALogFileDiscardsRecords() throws IOException {
        CliLogging.configure();

        assertThat(captureStderr(() -> {
            try (CliLogging.TuiSession ignored = CliLogging.forTui(null)) {
                OTHER.log(Level.ERROR, "not shown");
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        })).isEmpty();
    }

    private static String captureStderr(Runnable action) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            action.run();
        }
        finally {
            System.setErr(original);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }
}
