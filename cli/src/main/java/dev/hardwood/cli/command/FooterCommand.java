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
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.FooterStats;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

@CommandDefinition(name = "footer", description = "Print decoded footer length, offset, and file structure.", generateHelp = true)
public class FooterCommand implements Command<CommandInvocation> {

    private static final byte[] PARQUET_MAGIC = { 'P', 'A', 'R', '1' };
    private static final byte[] ENCRYPTED_MAGIC = { 'P', 'A', 'R', 'E' };
    private static final int TRAILER_SIZE = 8;
    private static final int MAGIC_SIZE = 4;

    /// Labels are padded to this width before their value so all five values line
    /// up in one column; `Trailing Magic:` is the longest label, so it sets it.
    private static final int LABEL_WIDTH = 15;
    private static final String LABEL = "%-" + LABEL_WIDTH + "s ";

    @Mixin
    FileMixin fileMixin;

    @Mixin
    LogFileMixin logFileMixin;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        return logFileMixin.run(this::run);
    }

    private CommandResult run() {
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

            String leadingMagicText = new String(leadingMagic, StandardCharsets.US_ASCII);
            String trailingMagicText = new String(trailingMagic, StandardCharsets.US_ASCII);

            System.out.println(Fmt.fmt(LABEL + "%d bytes", "File Size:", fileSize));
            System.out.println(Fmt.fmt(LABEL + "%d bytes", "Footer Offset:", footerOffset));
            System.out.println(Fmt.fmt(LABEL + "%d bytes", "Footer Length:", footerLength));
            System.out.println(Fmt.fmt(LABEL + "%s", "Leading Magic:", leadingMagicText));
            System.out.println(Fmt.fmt(LABEL + "%s", "Trailing Magic:", trailingMagicText));

            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                printFooterStats(FooterStats.of(reader.getFileMetaData(), fileSize), fileSize);
            }
            catch (IOException | ParquetReadException e) {
                System.err.println("Error reading file: " + e.getMessage());
                return CommandResult.FAILURE;
            }
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    /// The footer aggregates dive's Footer screen shows, in plain text: the
    /// data region vs footer+indexes split, the encoding and codec
    /// histograms, page-index / bloom-filter / dictionary coverage, and the
    /// aggregate byte sizes. The figures come from the shared [FooterStats],
    /// so the command and the screen cannot drift apart.
    private static void printFooterStats(FooterStats stats, long fileSize) {
        System.out.println();
        System.out.println("Layout");
        if (stats.totalChunks() > 0) {
            System.out.println(sectionRow("Data region", layoutValue(stats.dataRegionBytes(), fileSize), 16));
        }
        System.out.println(sectionRow("Footer + indexes",
                layoutValue(stats.footerAndIndexBytes(), fileSize), 16));

        System.out.println();
        System.out.println("Encodings");
        int encodingWidth = stats.encodingHistogram().keySet().stream()
                .mapToInt(name -> name.name().length()).max().orElse(0);
        stats.encodingHistogram().forEach((encoding, count) -> System.out.println(
                sectionRow(encoding.name(), String.valueOf(count), encodingWidth)));

        System.out.println();
        System.out.println("Codecs");
        int codecWidth = stats.codecHistogram().keySet().stream()
                .mapToInt(name -> name.name().length()).max().orElse(0);
        stats.codecHistogram().forEach((codec, count) -> System.out.println(
                sectionRow(codec.name(), String.valueOf(count), codecWidth)));

        System.out.println();
        System.out.println("Coverage");
        System.out.println(sectionRow("Column indexes",
                stats.columnIndexCount() + " / " + stats.totalChunks() + " chunks", 14));
        System.out.println(sectionRow("Offset indexes",
                stats.offsetIndexCount() + " / " + stats.totalChunks() + " chunks", 14));
        System.out.println(sectionRow("Bloom filters",
                stats.bloomFilterCount() + " / " + stats.totalChunks() + " chunks", 14));
        System.out.println(sectionRow("Dictionaries",
                stats.dictionaryCount() + " / " + stats.totalChunks() + " chunks", 14));

        System.out.println();
        System.out.println("Aggregates");
        System.out.println(sectionRow("Compressed", Sizes.format(stats.compressedBytes()), 12));
        System.out.println(sectionRow("Uncompressed", Sizes.format(stats.uncompressedBytes()), 12));
        System.out.println(sectionRow("Compression",
                Sizes.compression(stats.compressedBytes(), stats.uncompressedBytes()), 12));
    }

    private static String layoutValue(long bytes, long fileSize) {
        return Sizes.format(bytes) + "  (" + Fmt.fmt("%.1f%%", 100.0 * bytes / fileSize) + ")";
    }

    private static String sectionRow(String label, String value, int width) {
        return "  " + Fmt.fmt("%-" + width + "s %s", label, value);
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
