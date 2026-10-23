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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/// `--log-file` on the non-interactive commands: the file receives the command's records, and
/// stdout, stderr and the exit code stay what they are without it.
class LogFileOptionTest {

    private static final String FIXTURE = Cli.file("/annotated_repeated_group_test.parquet");

    private static final String BARE_GROUPS_WARNING = "Ignoring 6 annotation(s) on repeated groups outside"
            + " a LIST or MAP group; those groups are read as though unannotated: foo_mkv (MAP_KEY_VALUE);"
            + " foo_list (LIST); foo_list_lt (LIST); foo_map (MAP); foo_map_ct (MAP); s.bar (LIST)";

    static String[] commands() {
        return new String[] {
            "info",
            "schema",
            "print",
            "convert --format csv",
            "inspect columns",
            "inspect rowgroups",
            "inspect pages",
            "inspect dictionary -c foo.a",
        };
    }

    @ParameterizedTest
    @MethodSource("commands")
    void writesTheCommandsRecordsToTheLogFile(String command, @TempDir Path tempDir) throws IOException {
        Path logFile = tempDir.resolve("hardwood.log");

        Cli.Result without = Cli.launch(args(command, FIXTURE));
        Cli.Result with = Cli.launch(args(command, FIXTURE, "--log-file", logFile.toString()));

        assertThat(with.exitCode()).isZero();
        assertThat(with.output()).isEqualTo(without.output());
        assertThat(with.errorOutput()).isEqualTo(without.errorOutput()).isEqualTo("WARNING: " + BARE_GROUPS_WARNING);
        assertThat(Files.readAllLines(logFile)).anySatisfy(line -> assertThat(line).endsWith(
                " WARNING [dev.hardwood.internal.schema.BareRepeatedGroups] " + BARE_GROUPS_WARNING));
    }

    /// `footer` reads the trailer without parsing the schema, so a local file logs nothing; the
    /// file is still created, empty.
    @Test
    void footerCreatesTheLogFile(@TempDir Path tempDir) throws IOException {
        Path logFile = tempDir.resolve("hardwood.log");

        Cli.Result result = Cli.launch(args("footer", FIXTURE, "--log-file", logFile.toString()));

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("Trailing Magic:");
        assertThat(logFile).exists().isEmptyFile();
    }

    /// Each invocation starts the file afresh rather than appending to the previous one.
    @Test
    void truncatesAnExistingLogFile(@TempDir Path tempDir) throws IOException {
        Path logFile = tempDir.resolve("hardwood.log");
        Files.writeString(logFile, "from an earlier run\n");

        Cli.Result result = Cli.launch(args("schema", FIXTURE, "--log-file", logFile.toString()));

        assertThat(result.exitCode()).isZero();
        assertThat(Files.readAllLines(logFile))
                .doesNotContain("from an earlier run")
                .singleElement().asString().endsWith(BARE_GROUPS_WARNING);
    }

    /// A log file that cannot be opened fails the command before it reads the file, naming the
    /// log file.
    @Test
    void failsWhenTheLogFileCannotBeOpened(@TempDir Path tempDir) {
        Path logFile = tempDir.resolve("missing").resolve("hardwood.log");

        Cli.Result result = Cli.launch(args("info", FIXTURE, "--log-file", logFile.toString()));

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.output()).isEmpty();
        assertThat(result.errorOutput())
                .isEqualTo("Error: cannot write log file " + logFile + ": its directory does not exist");
    }

    /// Once a command with a log file ends, the next one logs to stderr only, as before.
    @Test
    void laterCommandsAreUnaffected(@TempDir Path tempDir) throws IOException {
        Path logFile = tempDir.resolve("hardwood.log");
        Cli.launch(args("schema", FIXTURE, "--log-file", logFile.toString()));
        List<String> logged = Files.readAllLines(logFile);

        Cli.Result after = Cli.launch(args("schema", FIXTURE));

        assertThat(after.errorOutput()).isEqualTo("WARNING: " + BARE_GROUPS_WARNING);
        assertThat(Files.readAllLines(logFile)).isEqualTo(logged);
    }

    private static String[] args(String command, String file, String... extra) {
        List<String> args = new ArrayList<>(List.of(command.split(" ")));
        args.add("-f");
        args.add(file);
        args.addAll(List.of(extra));
        return args.toArray(new String[0]);
    }
}
