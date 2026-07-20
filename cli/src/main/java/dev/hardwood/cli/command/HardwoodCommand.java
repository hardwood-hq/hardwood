/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;

import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Version;

@CommandDefinition(name = "hardwood", description = "A command-line interface for inspecting Parquet files", generateHelp = true, groupCommands = {
        InfoCommand.class,
        SchemaCommand.class,
        ConvertCommand.class,
        FooterCommand.class,
        InspectCommand.class,
        PrintCommand.class,
        DiveCommand.class
})
public class HardwoodCommand implements Command<CommandInvocation> {

    @Option(shortName = 'V', name = "version", hasValue = false, description = "Print version information and exit.")
    boolean version;

    @Override
    public CommandResult execute(CommandInvocation commandInvocation) {
        if (version) {
            System.out.println(Fmt.fmt("hardwood %s", Version.getVersion()));
            return CommandResult.SUCCESS;
        }
        System.out.println(commandInvocation.getHelpInfo());
        return CommandResult.SUCCESS;
    }
}
