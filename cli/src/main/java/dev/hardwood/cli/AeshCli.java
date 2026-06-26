/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import java.util.List;

import org.aesh.command.AeshCommandRuntimeBuilder;
import org.aesh.command.CommandResult;
import org.aesh.command.CommandRuntime;
import org.aesh.command.impl.registry.AeshCommandRegistryBuilder;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.settings.Settings;
import org.aesh.command.settings.SettingsBuilder;

import dev.hardwood.cli.command.HardwoodCommand;
import dev.hardwood.cli.internal.Version;

public final class AeshCli {

    private static final List<String> TOP_LEVEL_COMMANDS = List.of(
            "help", "info", "schema", "convert", "footer", "inspect", "print", "dive");
    private static final List<String> INSPECT_COMMANDS = List.of(
            "pages", "dictionary", "columns", "rowgroups");

    private AeshCli() {
    }

    public static CommandRuntime<CommandInvocation> runtime() throws Exception {
        CommandRegistry<CommandInvocation> registry = AeshCommandRegistryBuilder.<CommandInvocation>builder()
                .command(HardwoodCommand.class)
                .create();

        Settings settings = SettingsBuilder.builder()
                .commandRegistry(registry)
                .logging(false)
                .build();

        return AeshCommandRuntimeBuilder.<CommandInvocation>builder()
                .settings(settings)
                .build();
    }

    public static CommandResult execute(String... args) throws Exception {
        if (args.length > 0 && ("-h".equals(args[0]) || "--help".equals(args[0]))) {
            System.out.print(runtime().commandInfo("hardwood"));
            return CommandResult.SUCCESS;
        }
        if (args.length > 0 && ("-v".equals(args[0]) || "--version".equals(args[0]))) {
            System.out.println("hardwood version " + Version.getVersion());
            return CommandResult.SUCCESS;
        }
        if (args.length > 1 && "help".equals(args[0]) && !knownCommand(args, 1)) {
            String fullCommand = String.join(" ", List.of(args).subList(1, args.length));
            System.out.println("Unknown command: '" + fullCommand + "'.");
            return CommandResult.valueOf(1);
        }
        return runtime().executeCommand(commandLine(args));
    }

    public static String commandLine(String... args) {
        StringBuilder sb = new StringBuilder("hardwood");
        for (String arg : args) {
            sb.append(' ').append(quote(arg));
        }
        return sb.toString();
    }

    private static String quote(String arg) {
        if (arg == null || arg.isEmpty()) {
            return "''";
        }
        boolean needsQuotes = false;
        for (int i = 0; i < arg.length(); i++) {
            char c = arg.charAt(i);
            if (Character.isWhitespace(c) || c == '\'' || c == '"' || c == '\\') {
                needsQuotes = true;
                break;
            }
        }
        if (!needsQuotes) {
            return arg;
        }
        return "'" + arg.replace("'", "'\\''") + "'";
    }

    private static boolean knownCommand(String[] args, int offset) {
        int size = args.length - offset;
        if (size == 1) {
            return TOP_LEVEL_COMMANDS.contains(args[offset]);
        }
        return size == 2
                && "inspect".equals(args[offset])
                && INSPECT_COMMANDS.contains(args[offset + 1]);
    }
}
