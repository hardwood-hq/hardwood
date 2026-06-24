/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import org.aesh.AeshRuntimeRunner;

import dev.hardwood.cli.command.HardwoodCommand;
import dev.hardwood.cli.internal.NativeLibraryLoader;

public class Main {

    public static void main(String[] args) {
        NativeLibraryLoader.loadZstd();
        NativeLibraryLoader.loadLz4();
        NativeLibraryLoader.loadSnappy();

        AeshRuntimeRunner.builder()
                .command(HardwoodCommand.class)
                .args(args)
                .execute();
    }
}
