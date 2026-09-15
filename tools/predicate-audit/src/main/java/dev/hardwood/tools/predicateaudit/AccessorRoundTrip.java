/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Rule 1 read from the other side: a value an accessor returns for a row, passed back as an `eq`
/// literal, matches that row. The one exception the rule states is a `String` read from bytes that
/// are not well-formed UTF-8, which `getString` decodes lossily and which matches no row.
///
/// This is the one step that reads values through Hardwood's accessors; the oracle never does.
final class AccessorRoundTrip {

    private static final List<String> FILES = List.of("flat_single", "legacy_single", "dropped_single", "exotic_single",
            "lowcard_single");

    /// The rows around each column's edge cases: `NaN` payloads, signed zeros, padded and empty
    /// decimals, the non-canonical `INT96`, the non-ASCII strings and the extremes.
    private static final int[] ROWS = { 0, 1, 2, 5, 6, 10, 20, 30, 298, 300, 301, 302, 303, 310, 597, 598, 599 };

    record Result(int checks, List<String> disagreements, List<String> thrown) {
    }

    /// One value an accessor returned for a row.
    private record Read(long row, String column, String accessor, Object value, boolean matchesItsRow, String thrown) {
    }

    private AccessorRoundTrip() {
    }

    static Result run(Path fixtures, Path report) throws Exception {
        int checks = 0;
        List<String> disagreements = new ArrayList<>();
        List<String> thrown = new ArrayList<>();
        try (HardwoodContext context = HardwoodContext.create();
                PrintWriter out = new PrintWriter(Files.newBufferedWriter(report.resolve("roundtrip.tsv")))) {
            out.println("file\trow\tcolumn\taccessor\tliteral\texpected\thardwood");
            for (String name : FILES) {
                try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixtures.resolve(name + ".parquet")),
                        context, ReaderConfig.defaults())) {
                    for (Read read : reads(reader)) {
                        if (read.thrown() != null) {
                            String line = name + "\t" + read.row() + "\t" + read.column() + "\t" + read.accessor()
                                    + "\t-\t-\tACCESSOR THROW " + read.thrown();
                            out.println(line);
                            thrown.add(line);
                            continue;
                        }
                        checks++;
                        String matched = matches(reader, read);
                        String expected = read.matchesItsRow() ? "matches its row" : "misses its row";
                        String line = name + "\t" + read.row() + "\t" + read.column() + "\t" + read.accessor() + "\t"
                                + Oracle.show(read.value()) + "\t" + expected + "\t" + matched;
                        out.println(line);
                        if (!matched.equals(expected)) {
                            disagreements.add(line);
                        }
                    }
                }
            }
        }
        return new Result(checks, disagreements, thrown);
    }

    private static List<Read> reads(ParquetFileReader reader) throws Exception {
        FileSchema schema = reader.getFileSchema();
        List<Read> reads = new ArrayList<>();
        try (RowReader rows = reader.buildRowReader().build()) {
            long row = 0;
            int next = 0;
            while (rows.hasNext() && next < ROWS.length) {
                rows.next();
                if (row++ != ROWS[next]) {
                    continue;
                }
                next++;
                for (int i = 0; i < schema.getColumnCount(); i++) {
                    ColumnSchema column = schema.getColumn(i);
                    if (!column.name().equals("__row__") && !rows.isNull(column.name())) {
                        read(rows, row - 1, column, reads);
                    }
                }
            }
        }
        return reads;
    }

    /// Every value the column's physical and logical accessors return for the current row.
    private static void read(RowReader rows, long row, ColumnSchema column, List<Read> reads) {
        String name = column.name();
        switch (column.type()) {
            case BOOLEAN -> reads.add(new Read(row, name, "getBoolean", rows.getBoolean(name), true, null));
            case INT32 -> reads.add(new Read(row, name, "getInt", rows.getInt(name), true, null));
            case INT64 -> reads.add(new Read(row, name, "getLong", rows.getLong(name), true, null));
            case FLOAT -> reads.add(new Read(row, name, "getFloat", rows.getFloat(name), true, null));
            case DOUBLE -> reads.add(new Read(row, name, "getDouble", rows.getDouble(name), true, null));
            case INT96 -> {
                reads.add(new Read(row, name, "getBinary", rows.getBinary(name), true, null));
                reads.add(new Read(row, name, "getTimestamp", rows.getTimestamp(name), true, null));
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> reads.add(new Read(row, name, "getBinary", rows.getBinary(name), true, null));
        }
        LogicalType logicalType = column.logicalType();
        if (logicalType == null) {
            if (column.type() == PhysicalType.BYTE_ARRAY) {
                reads.add(string(rows, row, name));
            }
            return;
        }
        switch (logicalType) {
            case LogicalType.StringType ignored -> reads.add(string(rows, row, name));
            case LogicalType.EnumType ignored -> reads.add(string(rows, row, name));
            case LogicalType.JsonType ignored -> reads.add(string(rows, row, name));
            case LogicalType.DateType ignored -> reads.add(logical(row, name, "getDate", () -> rows.getDate(name)));
            case LogicalType.TimeType ignored -> reads.add(logical(row, name, "getTime", () -> rows.getTime(name)));
            case LogicalType.TimestampType t -> reads.add(t.isAdjustedToUTC()
                    ? logical(row, name, "getTimestamp", () -> rows.getTimestamp(name))
                    : logical(row, name, "getLocalTimestamp", () -> rows.getLocalTimestamp(name)));
            case LogicalType.DecimalType ignored -> reads.add(logical(row, name, "getDecimal", () -> rows.getDecimal(name)));
            case LogicalType.UuidType ignored -> reads.add(logical(row, name, "getUuid", () -> rows.getUuid(name)));
            case LogicalType.IntervalType ignored -> reads.add(logical(row, name, "getInterval", () -> rows.getInterval(name)));
            case LogicalType.Float16Type ignored -> reads.add(logical(row, name, "getFloat", () -> rows.getFloat(name)));
            // Read through the physical accessor only.
            case LogicalType.IntType ignored -> {
            }
            case LogicalType.BsonType ignored -> {
            }
            case LogicalType.GeometryType ignored -> {
            }
            case LogicalType.GeographyType ignored -> {
            }
            case LogicalType.NullType ignored -> {
            }
            // Not leaf annotations.
            case LogicalType.VariantType ignored -> {
            }
            case LogicalType.ListType ignored -> {
            }
            case LogicalType.MapType ignored -> {
            }
        }
    }

    /// A logical accessor's value, or what it threw. A stored value past what the logical type
    /// represents, such as a `TIME` of 25 hours, has no logical value to pass back; the throw is
    /// reported rather than skipped, so an accessor that breaks on a valid value does not silently
    /// drop its checks.
    private static Read logical(long row, String name, String accessor, Supplier<Object> value) {
        try {
            return new Read(row, name, accessor, value.get(), true, null);
        }
        catch (RuntimeException e) {
            return new Read(row, name, accessor, null, false, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static Read string(RowReader rows, long row, String name) {
        return new Read(row, name, "getString", rows.getString(name), wellFormedUtf8(rows.getBinary(name)), null);
    }

    private static boolean wellFormedUtf8(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
            return true;
        }
        catch (CharacterCodingException e) {
            return false;
        }
    }

    private static String matches(ParquetFileReader reader, Read read) {
        List<Long> rows = new ArrayList<>();
        try (RowReader filtered = reader.buildRowReader().filter(eq(read.column(), read.value())).build()) {
            while (filtered.hasNext()) {
                filtered.next();
                rows.add(filtered.getLong("__row__"));
            }
        }
        catch (Exception e) {
            return "THROW " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        return rows.contains(read.row()) ? "matches its row" : "misses its row";
    }

    private static FilterPredicate eq(String column, Object value) {
        return switch (value) {
            case Boolean v -> FilterPredicate.eq(column, v);
            case Integer v -> FilterPredicate.eq(column, v);
            case Long v -> FilterPredicate.eq(column, v);
            case Float v -> FilterPredicate.eq(column, v);
            case Double v -> FilterPredicate.eq(column, v);
            case byte[] v -> FilterPredicate.eq(column, v);
            case String v -> FilterPredicate.eq(column, v);
            case LocalDate v -> FilterPredicate.eq(column, v);
            case LocalTime v -> FilterPredicate.eq(column, v);
            case Instant v -> FilterPredicate.eq(column, v);
            case LocalDateTime v -> FilterPredicate.eq(column, v);
            case BigDecimal v -> FilterPredicate.eq(column, v);
            case UUID v -> FilterPredicate.eq(column, v);
            case PqInterval v -> FilterPredicate.eq(column, v);
            default -> throw new IllegalStateException("No literal for a " + value.getClass().getName());
        };
    }
}
