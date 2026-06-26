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

import dev.hardwood.InputFile;

@CommandDefinition(name = "footer", description = "Print decoded footer length, offset, and file structure.")
public class FooterCommand extends FileCommandBase implements Command<CommandInvocation> {

    private static final byte[] PARQUET_MAGIC = { 'P', 'A', 'R', '1' };
    // Written in place of PARQUET_MAGIC when the footer is encrypted (Parquet
    // Modular Encryption, encrypted-footer mode).
    private static final byte[] ENCRYPTED_MAGIC = { 'P', 'A', 'R', 'E' };
    // Parquet trailer layout (last 8 bytes): [4-byte footer length LE][4-byte magic PAR1]
    private static final int TRAILER_SIZE = 8;
    private static final int MAGIC_SIZE = 4;

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        InputFile inputFile = toInputFile(invocation);
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

            invocation.println("File Size:     " + fileSize + " bytes");
            invocation.println("Footer Offset: " + footerOffset + " bytes");
            invocation.println("Footer Length: " + footerLength + " bytes");
            invocation.println("Leading Magic:  " + new String(leadingMagic, StandardCharsets.US_ASCII));
            invocation.println("Trailing Magic: " + new String(trailingMagic, StandardCharsets.US_ASCII));
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
