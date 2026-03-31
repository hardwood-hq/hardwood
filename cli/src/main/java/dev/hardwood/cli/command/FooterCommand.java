/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "footer", description = "Print decoded footer length, offset, and file structure.")
public class FooterCommand implements Callable<Integer> {

    private static final byte[] PARQUET_MAGIC = { 'P', 'A', 'R', '1' };
    // Parquet trailer layout (last 8 bytes): [4-byte footer length LE][4-byte magic PAR1]
    private static final int TRAILER_SIZE = 8;
    private static final int MAGIC_SIZE = 4;

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;

    @Spec
    CommandSpec spec;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }
        try (inputFile) {
            inputFile.open();
            long fileSize = inputFile.length();

            if (fileSize < TRAILER_SIZE + MAGIC_SIZE) {
                spec.commandLine().getErr().println("File is too small to be a valid Parquet file.");
                return CommandLine.ExitCode.SOFTWARE;
            }

            ByteBuffer trailer = inputFile.readRange(fileSize - TRAILER_SIZE, TRAILER_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);

            int footerLength = trailer.getInt();
            byte[] trailingMagic = new byte[MAGIC_SIZE];
            trailer.get(trailingMagic);

            if (!isMagic(trailingMagic)) {
                spec.commandLine().getErr().println("Invalid Parquet file: trailing magic bytes not found.");
                return CommandLine.ExitCode.SOFTWARE;
            }

            byte[] leadingMagic = new byte[MAGIC_SIZE];
            inputFile.readRange(0, MAGIC_SIZE).get(leadingMagic);

            long footerOffset = fileSize - TRAILER_SIZE - footerLength;

            spec.commandLine().getOut().println("File Size:     " + fileSize + " bytes");
            spec.commandLine().getOut().println("Footer Offset: " + footerOffset + " bytes");
            spec.commandLine().getOut().println("Footer Length: " + footerLength + " bytes");
            spec.commandLine().getOut().println("Leading Magic:  " + new String(leadingMagic, StandardCharsets.US_ASCII));
            spec.commandLine().getOut().println("Trailing Magic: " + new String(trailingMagic, StandardCharsets.US_ASCII));
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private static boolean isMagic(byte[] bytes) {
        for (int i = 0; i < MAGIC_SIZE; i++) {
            if (bytes[i] != PARQUET_MAGIC[i]) {
                return false;
            }
        }
        return true;
    }
}
