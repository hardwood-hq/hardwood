/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.aesh.command.parser.CommandLineParserException;
import org.aesh.util.completer.ShellCompletionGenerator;
import org.aesh.util.completer.ShellCompletionGenerator.ShellType;

import dev.hardwood.cli.command.HardwoodCommand;

/// Build-time utility that generates shell completion scripts for the
/// `hardwood` CLI using aesh's [ShellCompletionGenerator].
///
/// Usage: `java -cp ... dev.hardwood.cli.internal.GenerateCompletion <outputDir>`
///
/// Generates:
/// - `<outputDir>/hardwood_completion` (bash)
/// - `<outputDir>/hardwood_completion.zsh` (zsh)
/// - `<outputDir>/hardwood_completion.fish` (fish)
public final class GenerateCompletion {

    private GenerateCompletion() {
    }

    public static void main(String[] args) throws CommandLineParserException, IOException {
        if (args.length < 1) {
            System.err.println("Usage: GenerateCompletion <outputDir>");
            System.exit(1);
        }
        Path outputDir = Path.of(args[0]);
        Files.createDirectories(outputDir);

        String commandName = "hardwood";

        String bash = ShellCompletionGenerator.generate(ShellType.BASH, HardwoodCommand.class, commandName);
        Files.writeString(outputDir.resolve("hardwood_completion"), bash);

        String zsh = ShellCompletionGenerator.generate(ShellType.ZSH, HardwoodCommand.class, commandName);
        Files.writeString(outputDir.resolve("hardwood_completion.zsh"), zsh);

        String fish = ShellCompletionGenerator.generate(ShellType.FISH, HardwoodCommand.class, commandName);
        Files.writeString(outputDir.resolve("hardwood_completion.fish"), fish);

        System.out.println("Generated completion scripts in " + outputDir);
    }
}
