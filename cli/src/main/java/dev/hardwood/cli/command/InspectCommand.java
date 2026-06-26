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
    name = "inspect",
    description = "Low-level introspection commands.",
    groupCommands = {
        InspectPagesCommand.class,
        InspectDictionaryCommand.class,
        InspectColumnsCommand.class,
        InspectRowGroupsCommand.class
    }
)
public class InspectCommand implements GroupCommand<CommandInvocation> {

    @Override
    public List<Command<CommandInvocation>> getCommands() {
        return List.of(
            new InspectPagesCommand(),
            new InspectDictionaryCommand(),
            new InspectColumnsCommand(),
            new InspectRowGroupsCommand()
        );
    }

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        invocation.println("inspect requires a subcommand.");
        return CommandResult.FAILURE;
    }
}
