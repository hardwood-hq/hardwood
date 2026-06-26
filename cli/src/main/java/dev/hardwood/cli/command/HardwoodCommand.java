/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommand;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.invocation.CommandInvocation;

@GroupCommandDefinition(
    name = "hardwood",
    description = "A command-line interface for hardwood",
    groupCommands = {
        HelpCommand.class,
        InfoCommand.class,
        SchemaCommand.class,
        ConvertCommand.class,
        FooterCommand.class,
        InspectCommand.class,
        PrintCommand.class,
        DiveCommand.class
    }
)
public class HardwoodCommand implements GroupCommand<CommandInvocation> {

    @Override
    public List<Command<CommandInvocation>> getCommands() {
        return List.of(
            new HelpCommand(),
            new InfoCommand(),
            new SchemaCommand(),
            new ConvertCommand(),
            new FooterCommand(),
            new InspectCommand(),
            new PrintCommand(),
            new DiveCommand()
        );
    }

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        invocation.println("Usage: hardwood [command] [options]");
        invocation.println("Use 'hardwood help' to see all available commands.");
        return CommandResult.SUCCESS;
    }
}
