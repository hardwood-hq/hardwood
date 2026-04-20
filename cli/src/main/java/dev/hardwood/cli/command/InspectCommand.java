/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import picocli.CommandLine;

@CommandLine.Command(name = "inspect", description = "Low-level introspection commands.", subcommands = {
        InspectPagesCommand.class,
        InspectDictionaryCommand.class,
        InspectColumnsCommand.class,
        InspectRowGroupsCommand.class
})
public class InspectCommand implements Runnable {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
