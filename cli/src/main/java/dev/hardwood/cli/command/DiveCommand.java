/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.DiveApp;
import dev.hardwood.cli.dive.ParquetModel;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/// Launches the interactive `hardwood dive` TUI for exploring a Parquet file's
/// structure. See `_designs/INTERACTIVE_DIVE_TUI.md`.
@CommandLine.Command(
        name = "dive",
        description = "Interactively explore a Parquet file's structure.")
public class DiveCommand implements Callable<Integer> {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;

    @Spec
    CommandSpec spec;

    @CommandLine.Option(
            names = "--rows",
            description = "Page size for the Data preview screen. Default: ${DEFAULT-VALUE}.",
            defaultValue = "20")
    int rowsPerPage;

    @CommandLine.Option(
            names = "--smoke-render",
            description = "Render one frame to a 120x40 buffer and exit 0. Used by the native-image smoke test; not intended for interactive use.",
            hidden = true)
    boolean smokeRender;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        try (ParquetModel model = ParquetModel.open(inputFile, fileMixin.file)) {
            model.setPreviewPageSize(rowsPerPage);
            DiveApp app = new DiveApp(model);
            if (smokeRender) {
                Buffer buffer = Buffer.empty(new Rect(0, 0, 120, 40));
                app.renderOnce(buffer);
                return CommandLine.ExitCode.OK;
            }
            app.run();
            return CommandLine.ExitCode.OK;
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
        catch (Exception e) {
            spec.commandLine().getErr().println("Error running dive TUI: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
    }
}
