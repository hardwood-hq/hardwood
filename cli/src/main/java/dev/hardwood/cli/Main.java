/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import org.aesh.AeshRuntimeRunner;
import org.aesh.command.CommandResult;

import dev.hardwood.cli.command.HardwoodCommand;
import dev.hardwood.cli.internal.NativeLibraryLoader;

public class Main {

    public static void main(String[] args) {
        System.exit(run(args));
    }

    /// Runs the `hardwood` CLI and returns the process exit code:
    /// `0` on success, `1` for a failed command, `2` for a usage error,
    /// `127` for an unknown command. `AeshRuntimeRunner` does not exit the
    /// JVM itself, so [main] maps the result here. Split out from [main] so
    /// tests can assert the exit code without terminating the test JVM.
    public static int run(String[] args) {
        NativeLibraryLoader.loadZstd();
        NativeLibraryLoader.loadLz4();
        NativeLibraryLoader.loadSnappy();

        CommandResult result = AeshRuntimeRunner.builder()
                .command(HardwoodCommand.class)
                .args(args)
                .execute();
        return result.getExitCode();
    }
}
