/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

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
        forceUtf8Output();
        NativeLibraryLoader.loadZstd();
        NativeLibraryLoader.loadLz4();
        NativeLibraryLoader.loadSnappy();
        NativeLibraryLoader.loadBrotli();

        CommandResult result = AeshRuntimeRunner.builder()
                .command(HardwoodCommand.class)
                .args(args)
                .execute();
        return result.getExitCode();
    }

    /// Writes output as UTF-8 whatever the platform encoding says. Several
    /// commands emit non-ASCII — the eighth-block bars of the level
    /// histograms, the `—` placeholders, the `⚠` of the consistency check —
    /// and on a host with no UTF-8 locale the default stream encodes them as
    /// `?`. A native image is the worse case: its default charset is fixed
    /// when the image is built, so setting `LANG` at runtime does not reach
    /// it. Both are avoided by naming the charset here rather than inheriting
    /// one.
    private static void forceUtf8Output() {
        if (StandardCharsets.UTF_8.equals(System.out.charset())) {
            return;
        }
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out),
                true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err),
                true, StandardCharsets.UTF_8));
    }
}
