/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.RowReaderBuilder;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

/// Shared `-n / --rows` parsing and application for CLI commands that
/// support head / tail / ALL row limiting (`print`, `convert`, ...).
final class RowLimits {

    static final String ALL = "ALL";

    private RowLimits() {
    }

    /// Parses the raw `-n` argument into a row-limit integer:
    /// positive for head, negative for tail, `0` for no limit (`ALL`).
    /// Throws [CommandLine.ParameterException] for invalid input, including
    /// an explicit `0` (which would otherwise collide with the no-limit sentinel).
    static int parse(String value, CommandSpec spec) {
        if (ALL.equalsIgnoreCase(value)) {
            return 0;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "Invalid value for option '-n': expected a non-zero integer or 'ALL', got '" + value + "'");
        }
        if (parsed == 0) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "Invalid value for option '-n': expected a non-zero integer or 'ALL', got '0'");
        }
        return parsed;
    }

    /// Builds a [RowReader] applying the parsed row limit:
    /// positive → `head(limit)`, negative → `tail(-limit)`, zero → no limit.
    static RowReader buildRowReader(ParquetFileReader reader, ColumnProjection projection, int rowLimit) {
        RowReaderBuilder builder = reader.buildRowReader().projection(projection);
        if (rowLimit > 0) {
            builder.head(rowLimit);
        }
        else if (rowLimit < 0) {
            builder.tail(-rowLimit);
        }
        return builder.build();
    }
}
