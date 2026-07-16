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
import java.util.List;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.writer.ParquetFileStitcher;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "merge",
        description = "Merge same-schema Parquet files without re-encoding their pages.")
public final class MergeCommand implements Callable<Integer> {
    @CommandLine.Mixin HelpMixin help;
    @Spec CommandSpec spec;

    @CommandLine.Option(names = {"-o", "--output"}, required = true, paramLabel = "FILE",
            description = "Output Parquet file.")
    Path output;

    @CommandLine.Parameters(arity = "1..*", paramLabel = "INPUT",
            description = "Input Parquet files in output row order.")
    List<Path> inputs;

    @Override
    public Integer call() {
        try {
            Path normalizedOutput = output.toAbsolutePath().normalize();
            for (Path input : inputs) {
                Path normalizedInput = input.toAbsolutePath().normalize();
                if (normalizedInput.equals(normalizedOutput)
                        || (Files.exists(input) && Files.exists(output) && Files.isSameFile(input, output))) {
                    throw new IllegalArgumentException("Output must not also be an input: " + input);
                }
            }
            ParquetFileStitcher.stitch(InputFile.ofPaths(inputs), OutputFile.of(output));
            return CommandLine.ExitCode.OK;
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            spec.commandLine().getErr().println(e.getMessage());
            return CommandLine.ExitCode.USAGE;
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error merging files: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
    }
}
