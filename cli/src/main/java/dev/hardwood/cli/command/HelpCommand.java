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
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Arguments;

@CommandDefinition(name = "help", description = "Display help information about the specified command.")
public class HelpCommand implements Command<CommandInvocation> {

    private static final List<String> TOP_LEVEL_COMMANDS = List.of(
            "help", "info", "schema", "convert", "footer", "inspect", "print", "dive");
    private static final List<String> INSPECT_COMMANDS = List.of(
            "pages", "dictionary", "columns", "rowgroups");

    @Arguments(description = "The command to display the usage help message for.")
    private List<String> commands;

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        if (commands == null || commands.isEmpty()) {
            invocation.print(invocation.getHelpInfo("hardwood") + "\n");
        } else {
            String fullCommand = String.join(" ", commands);
            if (!knownCommand(commands)) {
                invocation.print("Unknown command: '" + fullCommand + "'.\n");
                return CommandResult.FAILURE;
            }
            try {
                String helpInfo = invocation.getHelpInfo(fullCommand);
                if (helpInfo == null || helpInfo.isEmpty()) {
                    invocation.print("Unknown command: '" + fullCommand + "'.\n");
                    return CommandResult.FAILURE;
                }
                invocation.print(helpInfo + "\n");
            } catch (Exception e) {
                invocation.print("Unknown command: '" + fullCommand + "'.\n");
                return CommandResult.FAILURE;
            }
        }
        return CommandResult.SUCCESS;
    }

    private static boolean knownCommand(List<String> commands) {
        if (commands.size() == 1) {
            return TOP_LEVEL_COMMANDS.contains(commands.getFirst());
        }
        return commands.size() == 2
                && "inspect".equals(commands.getFirst())
                && INSPECT_COMMANDS.contains(commands.get(1));
    }
}
