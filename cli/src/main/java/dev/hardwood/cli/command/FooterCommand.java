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

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;

import dev.hardwood.InputFile;

@CommandDefinition(name = "footer", description = "Print decoded footer length, offset, and file structure.", generateHelp = true)
public class FooterCommand implements Command<CommandInvocation> {

    private static final byte[] PARQUET_MAGIC = { 'P', 'A', 'R', '1' };
    private static final byte[] ENCRYPTED_MAGIC = { 'P', 'A', 'R', 'E' };
    private static final int TRAILER_SIZE = 8;
    private static final int MAGIC_SIZE = 4;

    @Mixin
    FileMixin fileMixin;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }
        try (inputFile) {
            inputFile.open();
            long fileSize = inputFile.length();

            if (fileSize < TRAILER_SIZE + MAGIC_SIZE) {
                System.err.println("File is too small to be a valid Parquet file.");
                return CommandResult.FAILURE;
            }

            ByteBuffer trailer = inputFile.readRange(fileSize - TRAILER_SIZE, TRAILER_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);

            int footerLength = trailer.getInt();
            byte[] trailingMagic = new byte[MAGIC_SIZE];
            trailer.get(trailingMagic);

            if (isMagic(trailingMagic, ENCRYPTED_MAGIC)) {
                System.err.println(
                        "Encrypted Parquet files are not supported (Parquet Modular Encryption).");
                return CommandResult.FAILURE;
            }

            if (!isMagic(trailingMagic, PARQUET_MAGIC)) {
                System.err.println("Invalid Parquet file: trailing magic bytes not found.");
                return CommandResult.FAILURE;
            }

            byte[] leadingMagic = new byte[MAGIC_SIZE];
            inputFile.readRange(0, MAGIC_SIZE).get(leadingMagic);

            long footerOffset = fileSize - TRAILER_SIZE - footerLength;

            System.out.println("File Size:     " + fileSize + " bytes");
            System.out.println("Footer Offset: " + footerOffset + " bytes");
            System.out.println("Footer Length: " + footerLength + " bytes");
            System.out.println("Leading Magic:  " + new String(leadingMagic, StandardCharsets.US_ASCII));
            System.out.println("Trailing Magic: " + new String(trailingMagic, StandardCharsets.US_ASCII));
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    private static boolean isMagic(byte[] bytes, byte[] magic) {
        for (int i = 0; i < MAGIC_SIZE; i++) {
            if (bytes[i] != magic[i]) {
                return false;
            }
        }
        return true;
    }
}
