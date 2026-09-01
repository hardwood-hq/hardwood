/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.util.Set;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;
import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

@CommandDefinition(name = "schema", description = "Print the file schema.", generateHelp = true)
public class SchemaCommand implements Command<CommandInvocation> {

    enum Format {
        NATIVE,
        AVRO,
        PROTO
    }

    @Mixin
    FileMixin fileMixin;

    @Option(shortName = 'F', name = "format", defaultValue = "NATIVE", description = "Output format: NATIVE (default), AVRO, PROTO.")
    Format format;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileSchema schema = reader.getFileSchema();

            try {
                String output = switch (format) {
                    case NATIVE -> schema.toString();
                    case AVRO -> AvroSchemaEmitter.toAvroSchema(schema);
                    case PROTO -> ProtoSchemaEmitter.toProtoSchema(schema);
                };
                System.out.println(output);
            }
            catch (IllegalArgumentException e) {
                System.err.println("Error rendering schema: " + e.getMessage());
                return CommandResult.FAILURE;
            }
        }
        catch (IOException | ParquetReadException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    /// Sanitizing is not injective, so two Parquet names can map onto the same name.
    /// Neither Avro nor proto allows a record or message to repeat a field name,
    /// hence the numeric suffix.
    static String disambiguate(String name, Set<String> usedNames) {
        String candidate = name;
        for (int suffix = 2; !usedNames.add(candidate); suffix++) {
            candidate = name + "_" + suffix;
        }
        return candidate;
    }


    static String describeKeyType(SchemaNode key) {
        if (key instanceof SchemaNode.PrimitiveNode prim) {
            return prim.logicalType() == null ? prim.type().toString() : prim.type() + " (" + prim.logicalType() + ")";
        }
        return "group '" + key.name() + "'";
    }

    static String capitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
