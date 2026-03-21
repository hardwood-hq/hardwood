/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "column-index", description = "Print min/max statistics per page for a column.")
public class InspectColumnIndexCommand implements Callable<Integer> {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;
    @Spec
    CommandSpec spec;
    @CommandLine.Option(names = "--column", required = true, paramLabel = "COLUMN", description = "Column name to inspect.")
    String column;

    @Override
    public Integer call() {
        spec.commandLine().getOut().println("Column index statistics (min/max per page) are not yet available in hardwood.");
        spec.commandLine().getOut().println("The Parquet ColumnIndex structure is not exposed by the current API.");
        return CommandLine.ExitCode.OK;
    }
}
