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

@CommandDefinition(name = "inspect", description = "Low-level introspection commands.", generateHelp = true, groupCommands = {
        InspectPagesCommand.class,
        InspectDictionaryCommand.class,
        InspectColumnsCommand.class,
        InspectRowGroupsCommand.class
})
public class InspectCommand implements Command<CommandInvocation> {

    @Override
    public CommandResult execute(CommandInvocation ci) {
        System.out.println(ci.getHelpInfo());
        return CommandResult.SUCCESS;
    }
}
