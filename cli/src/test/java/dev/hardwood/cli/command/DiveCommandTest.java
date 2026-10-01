/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke test for the `hardwood dive` subcommand: verifies that it wires up end
/// to end by rendering one frame into a memory buffer and exiting 0. Does not
/// exercise interactive keyboard input — that's layer-1 state tests.
class DiveCommandTest {

    private static final String BARE_GROUPS_WARNING = "Ignoring 6 annotation(s) on repeated groups outside"
            + " a LIST or MAP group; those groups are read as though unannotated: foo_mkv (MAP_KEY_VALUE);"
            + " foo_list (LIST); foo_list_lt (LIST); foo_map (MAP); foo_map_ct (MAP); s.bar (LIST)";

    @Test
    void smokeRenderExitsZero() {
        Path fixture = Path.of(getClass().getResource("/compat_plain_int64.parquet").getPath());

        Cli.Result result = Cli.launch("dive", "-f", fixture.toString(), "--smoke-render");

        assertThat(result.exitCode())
                .withFailMessage("smoke-render failed: stdout=%s | stderr=%s", result.output(), result.errorOutput())
                .isZero();
    }

    @Test
    void rejectsMissingFileFlag() {
        Cli.Result result = Cli.launch("dive");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).containsIgnoringCase("file");
    }

    /// The Surefire fork has no console (stdout/stdin are piped), so the
    /// fail-fast guard fires for real — the same path a `docker run` without
    /// `-it` hits.
    @Test
    void failsFastWithoutTty() {
        Path fixture = Path.of(getClass().getResource("/compat_plain_int64.parquet").getPath());

        Cli.Result result = Cli.launch("dive", "-f", fixture.toString());

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).containsIgnoringCase("interactive terminal");
    }

    /// A log file that cannot be opened fails `dive` before the session starts, naming the file.
    @Test
    void failsWhenTheLogFileCannotBeOpened(@TempDir Path tempDir) {
        Path fixture = Path.of(getClass().getResource("/compat_plain_int64.parquet").getPath());
        Path logFile = tempDir.resolve("missing").resolve("dive.log");

        Cli.Result result = Cli.launch("dive", "-f", fixture.toString(), "--smoke-render",
                "--log-file", logFile.toString());

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput())
                .isEqualTo("Error: cannot write log file " + logFile + ": its directory does not exist");
    }

    /// The session's records go to the log file and not to stderr; once `dive` exits, a later
    /// command prints its warnings to stderr again.
    @Test
    void logFileTakesTheSessionsRecordsAndStderrResumesAfter(@TempDir Path tempDir) throws IOException {
        String fixture = getClass().getResource("/annotated_repeated_group_test.parquet").getPath();
        Path logFile = tempDir.resolve("dive.log");

        Cli.Result dive = Cli.launch("dive", "-f", fixture, "--smoke-render", "--log-file", logFile.toString());

        assertThat(dive.exitCode()).isZero();
        assertThat(dive.errorOutput()).isEmpty();
        assertThat(Files.readAllLines(logFile)).anySatisfy(line -> assertThat(line).endsWith(
                " WARNING [dev.hardwood.internal.schema.BareRepeatedGroups] " + BARE_GROUPS_WARNING));

        Cli.Result schema = Cli.launch("schema", "-f", fixture);

        assertThat(schema.errorOutput()).isEqualTo("WARNING: " + BARE_GROUPS_WARNING);
    }
}
