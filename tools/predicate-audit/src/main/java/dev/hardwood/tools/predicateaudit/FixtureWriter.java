/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import dev.hardwood.tools.predicateaudit.Columns.Col;

/// Writes the audit fixtures with parquet-java 1.17.1, a writer independent of Hardwood, which
/// records statistics, the page index, dictionaries and Bloom filters in each column's own order.
final class FixtureWriter {

    /// A file layout: rows per row group and per page, whether dictionaries and Bloom filters are
    /// written, and the writer version, which decides whether a `FIXED_LEN_BYTE_ARRAY` can be
    /// dictionary-encoded.
    record Layout(String name, int rowGroupRows, int pageRows, boolean dictionary, boolean bloom, WriterVersion version) {
    }

    static final List<Layout> LAYOUTS = List.of(
            new Layout("single", Columns.ROWS, 50, false, false, WriterVersion.PARQUET_1_0),
            new Layout("multi", 100, 25, false, false, WriterVersion.PARQUET_1_0),
            new Layout("dict", 200, 40, true, false, WriterVersion.PARQUET_2_0),
            new Layout("bloom", 100, 50, false, true, WriterVersion.PARQUET_1_0));

    private FixtureWriter() {
    }

    /// Writes the flat, exotic, low-cardinality, nested and shape fixtures into `directory`. The
    /// footer-rewritten variants are derived from them afterwards by `derive_fixtures.py`.
    static void writeAll(Path directory) throws IOException {
        Files.createDirectories(directory);
        for (Layout layout : LAYOUTS) {
            write(directory.resolve("flat_" + layout.name() + ".parquet"), Columns.flat(), layout);
            write(directory.resolve("exotic_" + layout.name() + ".parquet"), Columns.exotic(), layout);
            write(directory.resolve("ts12_" + layout.name() + ".parquet"), Columns.ts12(), layout);
            Layout dictionaryEverywhere = layout.name().equals("dict") || layout.name().equals("bloom")
                    ? new Layout(layout.name(), layout.rowGroupRows(), layout.pageRows(), true, layout.bloom(),
                            WriterVersion.PARQUET_2_0)
                    : layout;
            write(directory.resolve("lowcard_" + layout.name() + ".parquet"), Columns.lowCardinality(),
                    dictionaryEverywhere);
        }
        writeNested(directory.resolve("nested_single.parquet"), Columns.ROWS, false);
        writeNested(directory.resolve("nested_multi.parquet"), 100, false);
        writeNested(directory.resolve("nested_dict.parquet"), 200, true);
        writeShapes(directory.resolve("shapes.parquet"));
    }

    static void write(Path file, List<Col> columns, Layout layout) throws IOException {
        List<Type> fields = new ArrayList<>();
        for (Col column : columns) {
            fields.add(parquetType(column));
        }
        MessageType schema = new MessageType("m", fields);
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupRowCountLimit(layout.rowGroupRows())
                .withPageRowCountLimit(layout.pageRows())
                .withDictionaryEncoding(layout.dictionary())
                .withBloomFilterEnabled(layout.bloom())
                .withMaxBloomFilterBytes(4096)
                .withWriterVersion(layout.version())
                .build()) {
            List<Column> writable = Column.of(columns);
            for (int row = 0; row < Columns.ROWS; row++) {
                Group group = groups.newGroup();
                for (Column column : writable) {
                    column.put(group, row);
                }
                writer.write(group);
            }
        }
    }

    /// A column paired with how its values go into a parquet-java [Group].
    private record Column(Col col) {

        static List<Column> of(List<Col> columns) {
            return columns.stream().map(Column::new).toList();
        }

        void put(Group group, int row) {
            Object value = col.at(row);
            switch (value) {
                case null -> {
                }
                case Boolean b -> group.add(col.name(), b);
                case Integer i -> group.add(col.name(), i);
                case Long l -> group.add(col.name(), l);
                case Float f -> group.add(col.name(), f);
                case Double d -> group.add(col.name(), d);
                case byte[] b -> group.add(col.name(), Binary.fromConstantByteArray(b));
                default -> throw new IllegalStateException("No parquet-java value for " + col.name() + ": " + value);
            }
        }
    }

    private static Type parquetType(Col column) {
        Repetition repetition = column.nullable() ? Repetition.OPTIONAL : Repetition.REQUIRED;
        String name = column.name();
        return switch (column.sem()) {
            case BOOL -> Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name);
            case I32 -> Types.primitive(PrimitiveTypeName.INT32, repetition).named(name);
            case I64 -> Types.primitive(PrimitiveTypeName.INT64, repetition).named(name);
            case INT8S -> int32(repetition, LogicalTypeAnnotation.intType(8, true), name);
            case UINT8 -> int32(repetition, LogicalTypeAnnotation.intType(8, false), name);
            case UINT32 -> int32(repetition, LogicalTypeAnnotation.intType(32, false), name);
            case UINT64 -> int64(repetition, LogicalTypeAnnotation.intType(64, false), name);
            case F32 -> Types.primitive(PrimitiveTypeName.FLOAT, repetition).named(name);
            case F64 -> Types.primitive(PrimitiveTypeName.DOUBLE, repetition).named(name);
            case F16 -> fixed(repetition, 2, LogicalTypeAnnotation.float16Type(), name);
            case DATE -> int32(repetition, LogicalTypeAnnotation.dateType(), name);
            case TIME_MS -> int32(repetition, LogicalTypeAnnotation.timeType(true, TimeUnit.MILLIS), name);
            case TIME_US -> int64(repetition, LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS), name);
            case TIME_NS -> int64(repetition, LogicalTypeAnnotation.timeType(true, TimeUnit.NANOS), name);
            case TS_MS_UTC -> int64(repetition, LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS), name);
            case TS_US_UTC -> int64(repetition, LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS), name);
            case TS_NS_UTC -> int64(repetition, LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS), name);
            case TS_MS_LOCAL -> int64(repetition, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS), name);
            case TS_US_LOCAL -> int64(repetition, LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS), name);
            case INT96 -> Types.primitive(PrimitiveTypeName.INT96, repetition).named(name);
            case DEC_I32 -> int32(repetition, LogicalTypeAnnotation.decimalType(column.scale(), 9), name);
            case DEC_I64 -> int64(repetition, LogicalTypeAnnotation.decimalType(column.scale(), 18), name);
            case DEC_FLBA -> fixed(repetition, column.width(), LogicalTypeAnnotation.decimalType(column.scale(), 20), name);
            case DEC_BA -> binary(repetition, LogicalTypeAnnotation.decimalType(column.scale(), 30), name);
            case STRING -> binary(repetition, LogicalTypeAnnotation.stringType(), name);
            case ENUM -> binary(repetition, LogicalTypeAnnotation.enumType(), name);
            case JSON -> binary(repetition, LogicalTypeAnnotation.jsonType(), name);
            case BSON -> binary(repetition, LogicalTypeAnnotation.bsonType(), name);
            case BA, ZZ -> Types.primitive(PrimitiveTypeName.BINARY, repetition).named(name);
            case FLBA -> Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition).length(column.width()).named(name);
            case UUID -> fixed(repetition, 16, LogicalTypeAnnotation.uuidType(), name);
            case INTERVAL -> fixed(repetition, 12, LogicalTypeAnnotation.intervalType(), name);
            // No parquet-java release up to 1.18.1 accepts TIMESTAMP on a FIXED_LEN_BYTE_ARRAY, so
            // derive_fixtures.py annotates it.
            case TS12_MS_UTC, TS12_NS_UTC, TS12_US_LOCAL ->
                    Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition).length(12).named(name);
            case NULL_I32 -> int32(repetition, LogicalTypeAnnotation.unknownType(), name);
            case GEOM -> binary(repetition, LogicalTypeAnnotation.geometryType(null), name);
        };
    }

    private static Type int32(Repetition repetition, LogicalTypeAnnotation annotation, String name) {
        return Types.primitive(PrimitiveTypeName.INT32, repetition).as(annotation).named(name);
    }

    private static Type int64(Repetition repetition, LogicalTypeAnnotation annotation, String name) {
        return Types.primitive(PrimitiveTypeName.INT64, repetition).as(annotation).named(name);
    }

    private static Type binary(Repetition repetition, LogicalTypeAnnotation annotation, String name) {
        return Types.primitive(PrimitiveTypeName.BINARY, repetition).as(annotation).named(name);
    }

    private static Type fixed(Repetition repetition, int width, LogicalTypeAnnotation annotation, String name) {
        return Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition).length(width).as(annotation).named(name);
    }

    /// A struct with a leaf null under a present struct, a struct nested in it, and a `LIST`.
    private static void writeNested(Path file, int rowGroupRows, boolean dictionary) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message m {
                  required int64 __row__;
                  required binary zz;
                  optional group s {
                    optional int32 x;
                    optional binary name (STRING);
                    optional group t {
                      optional int64 y;
                    }
                  }
                  optional group l (LIST) {
                    repeated group list {
                      optional int32 element;
                    }
                  }
                }
                """);
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withRowGroupRowCountLimit(rowGroupRows)
                .withPageRowCountLimit(30)
                .withDictionaryEncoding(dictionary)
                .build()) {
            for (int row = 0; row < Columns.ROWS; row++) {
                Group group = groups.newGroup();
                group.add("__row__", (long) row);
                group.add("zz", Binary.fromString("z"));
                if (!Columns.structNull(row)) {
                    Group s = group.addGroup("s");
                    if (Columns.nestedX(row) != null) {
                        s.add("x", Columns.nestedX(row));
                    }
                    if (Columns.nestedName(row) != null) {
                        s.add("name", Columns.nestedName(row));
                    }
                    if (!Columns.innerStructNull(row)) {
                        Group t = s.addGroup("t");
                        if (Columns.nestedY(row) != null) {
                            t.add("y", Columns.nestedY(row));
                        }
                    }
                }
                if (!Columns.listNull(row)) {
                    Group list = group.addGroup("l");
                    for (int i = 0; i < row % 3; i++) {
                        list.addGroup("list").add("element", i);
                    }
                }
                writer.write(group);
            }
        }
    }

    /// Group shapes for the resolver matrix: a plain and a shredded `VARIANT` (annotated afterwards),
    /// a `MAP`, a doubly nested struct and a repeated primitive.
    private static void writeShapes(Path file) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message m {
                  required int64 __row__;
                  optional group v { required binary metadata; required binary value; }
                  optional group sv { required binary metadata; optional binary value; optional int64 typed_value; }
                  optional group m (MAP) { repeated group key_value { required binary key (STRING); optional int32 value; } }
                  optional group st { optional group inner { optional int32 leaf; } }
                  repeated int32 rep;
                }
                """);
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (int row = 0; row < 10; row++) {
                Group group = groups.newGroup();
                group.add("__row__", (long) row);
                writer.write(group);
            }
        }
    }
}
