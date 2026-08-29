/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;
import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

@CommandDefinition(name = "info", description = "Display high-level file information.", generateHelp = true)
public class InfoCommand implements Command<CommandInvocation> {

    /// Values wider than this are cut short with a trailing `…` in the key/value
    /// metadata section — these routinely carry kilobytes of embedded JSON or a
    /// base64-encoded Arrow schema, which would otherwise bury the rest of the
    /// command's output.
    private static final int MAX_VALUE_WIDTH = 60;

    @Mixin
    FileMixin fileMixin;

    @Option(name = "kv-key", description = "Print only the full, untruncated value of this key-value metadata entry.")
    String kvKey;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileMetaData metadata = reader.getFileMetaData();
            return kvKey != null ? printSingleKeyValue(metadata, kvKey) : printSummary(metadata);
        }
        catch (IOException | ParquetReadException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }
    }

    private static CommandResult printSummary(FileMetaData metadata) {
        long totalCompressed = 0;
        long totalUncompressed = 0;
        for (RowGroup rg : metadata.rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                totalCompressed += cc.metaData().totalCompressedSize();
                totalUncompressed += cc.metaData().totalUncompressedSize();
            }
        }

        System.out.println("Format Version:    " + metadata.version());
        System.out.println("Created By:        " + (metadata.createdBy() != null ? metadata.createdBy() : "unknown"));
        System.out.println("Row Groups:        " + metadata.rowGroups().size());
        System.out.println("Total Rows:        " + metadata.numRows());
        System.out.println("Uncompressed Size: " + Sizes.format(totalUncompressed));
        System.out.println("Compressed Size:   " + Sizes.format(totalCompressed));

        Map<String, String> keyValueMetadata = metadata.keyValueMetadata();
        if (!keyValueMetadata.isEmpty()) {
            System.out.println();
            System.out.println("Key/Value Metadata (" + keyValueMetadata.size() + "):");
            printKeyValueMetadata(keyValueMetadata);
        }
        return CommandResult.SUCCESS;
    }

    /// Handles `--kv-key`: prints exactly the named entry's value, untruncated and
    /// with no substitutions, and no summary block around it, so the output can be
    /// piped straight into another tool (e.g.
    /// `hardwood info -f x.parquet --kv-key ARROW:schema | base64 -d`).
    ///
    /// An entry that carries no value at all is reported as an error rather than
    /// printed as an empty line: the caller asked for content that isn't there, and
    /// a consumer down the pipe cannot tell the difference otherwise.
    private static CommandResult printSingleKeyValue(FileMetaData metadata, String key) {
        Map<String, String> keyValueMetadata = metadata.keyValueMetadata();
        if (!keyValueMetadata.containsKey(key)) {
            System.err.println("No key-value metadata entry named '" + key + "'.");
            return CommandResult.FAILURE;
        }
        String value = keyValueMetadata.get(key);
        if (value == null) {
            System.err.println("Key-value metadata entry '" + key + "' has no value.");
            return CommandResult.FAILURE;
        }
        System.out.println(value);
        return CommandResult.SUCCESS;
    }

    /// Prints one aligned line per entry: key left-padded to the widest key in this
    /// file, byte-length right-aligned to the widest size, then the value truncated
    /// to [MAX_VALUE_WIDTH]. A value that renders to nothing — the empty string, or
    /// no value at all — is omitted rather than leaving a trailing gutter of blank
    /// spaces; the size column still tells the two apart.
    private static void printKeyValueMetadata(Map<String, String> keyValueMetadata) {
        int keyWidth = 0;
        int sizeWidth = 0;
        for (Map.Entry<String, String> entry : keyValueMetadata.entrySet()) {
            keyWidth = Math.max(keyWidth, Strings.width(entry.getKey()));
            sizeWidth = Math.max(sizeWidth, size(entry.getValue()).length());
        }

        for (Map.Entry<String, String> entry : keyValueMetadata.entrySet()) {
            String line = "  " + Strings.padRight(entry.getKey(), keyWidth)
                    + "  " + Fmt.fmt("%" + sizeWidth + "s", size(entry.getValue()));
            String rendered = render(entry.getValue());
            System.out.println(rendered.isEmpty() ? line : line + "  " + rendered);
        }
    }

    /// The size column for one entry: the value's length in bytes as written to the
    /// file, or [Strings#ABSENT_VALUE] if it carries no value. `KeyValue.value` is
    /// optional in `parquet.thrift`, so an absent value is a different thing from a
    /// present-but-empty one, which shows `0 B`.
    private static String size(String value) {
        return value == null ? Strings.ABSENT_VALUE : Sizes.format(byteLength(value));
    }

    /// The value column for one entry: control characters replaced so a writer's
    /// content cannot break the layout or drive the terminal, then truncated to
    /// [MAX_VALUE_WIDTH] cells. The sanitiser is the shared
    /// [Strings#sanitizeControls] — the same rule column values follow.
    private static String render(String value) {
        if (value == null) {
            return "";
        }
        return Strings.truncateRight(Strings.sanitizeControls(value), MAX_VALUE_WIDTH);
    }

    private static int byteLength(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
