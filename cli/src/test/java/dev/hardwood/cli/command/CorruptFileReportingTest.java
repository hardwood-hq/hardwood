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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/// Every command that opens a Parquet file reports a corrupt one as a message, not a stack
/// trace.
///
/// A file whose bytes are wrong raises `ParquetReadException`, which is unchecked — so unlike
/// the `IOException` it replaced, nothing makes a command handle it. This asserts each command
/// does, because a `hardwood` invocation that prints a stack trace and no message is the worst
/// way to be told a file is bad.
class CorruptFileReportingTest {

    @TempDir
    static Path tempDir;

    private static String corruptFile;

    @BeforeAll
    static void writeCorruptFile() throws IOException {
        // A valid file with its trailing magic overwritten: the smallest corruption every
        // command meets at the same place, when it opens the file.
        Path source = Paths.get(
                CorruptFileReportingTest.class.getResource("/plain_uncompressed.parquet").getPath());
        byte[] bytes = Files.readAllBytes(source);
        for (int i = bytes.length - 4; i < bytes.length; i++) {
            bytes[i] = 'X';
        }
        Path target = tempDir.resolve("corrupt.parquet");
        Files.write(target, bytes);
        corruptFile = target.toString();
    }

    static String[] commands() {
        return new String[] {
            "info",
            "schema",
            "print",
            "convert --format csv",
            "inspect columns",
            "inspect rowgroups",
            "inspect pages",
        };
    }

    @ParameterizedTest
    @MethodSource("commands")
    void reportsACorruptFileAsAMessage(String command) {
        List<String> args = new ArrayList<>(List.of(command.split(" ")));
        args.add("-f");
        args.add(corruptFile);

        Cli.Result result = Cli.launch(args.toArray(new String[0]));

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput())
                .contains("Error reading file:")
                .contains("Not a Parquet file (invalid magic number at end)");
    }
}
