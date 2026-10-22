/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// The predicate corpus: the layouts `tools/simple-datagen.py` writes under
/// `src/test/resources/predicate`, the values each holds, and one `HardwoodContext` per test
/// class to read them through.
///
/// **Layouts.** `predicate_single`, `predicate_multi`, `predicate_dict` and `predicate_bloom` hold
/// the same 400 rows in one row group, in four, dictionary-encoded, and in four with a Bloom filter
/// on every column that takes one, so the row-group bounds, the page index, the dictionary and the
/// Bloom filter each decide a predicate that the record-level comparison decides again.
/// `predicate_nested` adds a struct whose leaf is null under a present struct, and a leaf below a
/// repeated path. `predicate_opaque` carries, in the single, dictionary and Bloom layouts, the
/// columns that stay out of the corpus the differential tests read: `BSON`, which DuckDB cannot
/// open, `INTERVAL`, a `DECIMAL` over `BYTE_ARRAY` holding a padded and an empty encoding, a `NULL`
/// column and `GEOMETRY`.
/// `predicate_int96` carries `INT96` columns in the same three layouts, one value stored under a
/// non-canonical encoding, and bounds recorded in byte order rather than in time order.
/// `predicate_int96_ordered` holds the same values, every one canonical, in ten-row row groups
/// under `INT96_TIMESTAMP_ORDER`, with bounds and null counts on both leaves, so a row group can be
/// dropped or decided in full by them.
/// `predicate_ts12` carries `TIMESTAMP` columns over `FIXED_LEN_BYTE_ARRAY(12)` of every unit, in the
/// single, multi, dictionary and Bloom layouts, with values past the `INT64` nanosecond range on both
/// sides of the epoch and bounds recorded in the order of the values. Its `pages` layout holds one
/// value per page under a page index, so the page index decides the predicates too.
///
/// **Values.** Every column is read once per class, unfiltered, and kept: [PredicateCorpusTest]
/// holds those values to what the generator wrote, each corpus having a `<corpus>.values.tsv.gz`
/// beside its files with every row's logical and physical value, and [PredicatePathAgreementTest]
/// applies its oracle to them.
abstract class PredicateCorpus {

    private static final Path RES = Paths.get("src/test/resources/predicate");

    static HardwoodContext context;

    /// Values of one column of one file, read unfiltered: the logical value an accessor returns
    /// and the physical one, indexed by row.
    private static final Map<String, List<Object[]>> VALUES = new HashMap<>();

    /// The values `tools/simple-datagen.py` wrote to each corpus, read once.
    private static final Map<String, Map<String, List<String[]>>> WRITTEN = new HashMap<>();

    /// The schema of each layout, read once: every case of every path asks it for the column it
    /// filters.
    private static final Map<String, FileSchema> SCHEMAS = new HashMap<>();

    @BeforeAll
    static void openContext() {
        context = HardwoodContext.create();
    }

    @AfterAll
    static void closeContext() {
        context.close();
        VALUES.clear();
        WRITTEN.clear();
        SCHEMAS.clear();
    }

    // ==================== Layouts ====================

    record Layout(String name, Path path) {
        @Override public String toString() {
            return name;
        }
    }

    static final Layout SINGLE = new Layout("single-rg", RES.resolve("predicate_single.parquet"));
    static final Layout MULTI = new Layout("multi-rg", RES.resolve("predicate_multi.parquet"));
    static final Layout DICTIONARY = new Layout("dictionary", RES.resolve("predicate_dict.parquet"));
    static final Layout BLOOM = new Layout("bloom", RES.resolve("predicate_bloom.parquet"));
    private static final Layout NESTED = new Layout("nested", RES.resolve("predicate_nested_single.parquet"));
    private static final Layout NESTED_MULTI = new Layout("nested-multi-rg",
            RES.resolve("predicate_nested_multi.parquet"));
    private static final Layout OPAQUE = new Layout("opaque", RES.resolve("predicate_opaque_single.parquet"));
    private static final Layout OPAQUE_DICTIONARY = new Layout("opaque-dictionary",
            RES.resolve("predicate_opaque_dict.parquet"));
    private static final Layout OPAQUE_BLOOM = new Layout("opaque-bloom",
            RES.resolve("predicate_opaque_bloom.parquet"));

    private static final Layout INT96_SINGLE = new Layout("int96", RES.resolve("predicate_int96_single.parquet"));
    private static final Layout INT96_MULTI = new Layout("int96-multi-rg",
            RES.resolve("predicate_int96_multi.parquet"));
    private static final Layout INT96_DICTIONARY = new Layout("int96-dictionary",
            RES.resolve("predicate_int96_dict.parquet"));
    private static final Layout INT96_ORDERED = new Layout("int96-ordered",
            RES.resolve("predicate_int96_ordered_multi.parquet"));

    private static final Layout TS12_SINGLE = new Layout("ts12", RES.resolve("predicate_ts12_single.parquet"));
    private static final Layout TS12_MULTI = new Layout("ts12-multi-rg", RES.resolve("predicate_ts12_multi.parquet"));
    private static final Layout TS12_DICTIONARY = new Layout("ts12-dictionary",
            RES.resolve("predicate_ts12_dict.parquet"));
    private static final Layout TS12_BLOOM = new Layout("ts12-bloom", RES.resolve("predicate_ts12_bloom.parquet"));
    private static final Layout TS12_PAGES = new Layout("ts12-pages", RES.resolve("predicate_ts12_pages.parquet"));

    static final List<Layout> LAYOUTS = List.of(SINGLE, MULTI, DICTIONARY, BLOOM, NESTED, NESTED_MULTI,
            OPAQUE, OPAQUE_DICTIONARY, OPAQUE_BLOOM, INT96_SINGLE, INT96_MULTI, INT96_DICTIONARY,
            INT96_ORDERED, TS12_SINGLE, TS12_MULTI, TS12_DICTIONARY, TS12_BLOOM, TS12_PAGES);

    static Stream<Layout> layouts() {
        return LAYOUTS.stream();
    }

    /// The values `tools/simple-datagen.py` wrote to the layout's corpus, by column, each as its
    /// logical and physical rendering, indexed by row.
    static Map<String, List<String[]>> writtenValues(Layout layout) {
        return WRITTEN.computeIfAbsent(corpus(layout), corpus -> {
            try {
                return writtenValues(corpus);
            }
            catch (IOException e) {
                throw new IllegalStateException("Cannot read " + corpus + ".values.tsv.gz", e);
            }
        });
    }

    private static Map<String, List<String[]>> writtenValues(String corpus) throws IOException {
        Map<String, List<String[]>> columns = new LinkedHashMap<>();
        for (String line : writtenLines(RES.resolve(corpus + ".values.tsv.gz"))) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] fields = line.split("\t", -1);
            List<String[]> rows = columns.computeIfAbsent(fields[1], key -> new ArrayList<>());
            if (Integer.parseInt(fields[0]) != rows.size()) {
                throw new IllegalStateException(corpus + ".values.tsv.gz: row " + fields[0] + " of '" + fields[1]
                        + "' out of order");
            }
            rows.add(new String[] { fields[2], fields[3] });
        }
        return columns;
    }

    private static String corpus(Layout layout) {
        return layout.path().getFileName().toString().replaceFirst("_(single|multi|dict|bloom|pages)\\.parquet$", "");
    }

    private static List<String> writtenLines(Path sidecar) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(Files.newInputStream(sidecar)), StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }

    /// A value in the form `tools/simple-datagen.py` writes it: tagged with its type, and reduced to
    /// integers, raw bits or hex without decoding anything.
    static String rendered(Object value) {
        return switch (value) {
            case null -> "null";
            case Boolean v -> "boolean:" + v;
            case Byte v -> "byte:" + v;
            case Integer v -> "int:" + v;
            case Long v -> "long:" + v;
            case Float v -> "float:" + HexFormat.of().toHexDigits(Float.floatToRawIntBits(v));
            case Double v -> "double:" + HexFormat.of().toHexDigits(Double.doubleToRawLongBits(v));
            case byte[] v -> "bytes:" + HexFormat.of().formatHex(v);
            case String v -> "string:" + HexFormat.of().formatHex(v.getBytes(StandardCharsets.UTF_8));
            case LocalDate v -> "date:" + v.toEpochDay();
            case LocalTime v -> "time:" + v.toNanoOfDay();
            case Instant v -> "instant:" + v.getEpochSecond() + ":" + v.getNano();
            case LocalDateTime v -> "datetime:" + v.toEpochSecond(ZoneOffset.UTC) + ":" + v.getNano();
            case BigDecimal v -> "decimal:" + v.unscaledValue() + ":" + v.scale();
            case UUID v -> "uuid:" + v;
            case PqInterval v -> "interval:" + v.months() + ":" + v.days() + ":" + v.milliseconds();
            default -> throw new IllegalArgumentException("No rendering for a " + value.getClass().getName());
        };
    }

    /// The values of `column`, which must be among those [PredicateCorpusTest#readsTheValuesTheFixtureHolds] checks.
    static List<Object[]> values(Layout layout, String column) {
        if (!writtenValues(layout).containsKey(column)) {
            throw new IllegalStateException(corpus(layout) + ".values.tsv.gz holds no values of '" + column
                    + "'; add the column to the corpus's _pv_write call in tools/simple-datagen.py");
        }
        return VALUES.computeIfAbsent(layout.name() + "#" + column, key -> {
            List<Object[]> values = new ArrayList<>();
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(layout.path()), context,
                    ReaderConfig.defaults());
                 RowReader rows = reader.buildRowReader().build()) {
                while (rows.hasNext()) {
                    rows.next();
                    values.add(valueOf(rows, column));
                }
            }
            catch (IOException e) {
                throw new IllegalStateException("Cannot read column '" + column + "' of " + layout.path(), e);
            }
            return values;
        });
    }

    /// The logical and physical value of `column` in the row the reader stands on, or two nulls
    /// where the leaf or an enclosing group is absent.
    private static Object[] valueOf(RowReader rows, String column) {
        int dot = column.indexOf('.');
        if (dot < 0) {
            return rows.isNull(column)
                    ? new Object[2]
                    : new Object[] { rows.getValue(column), rows.getRawValue(column) };
        }
        String group = column.substring(0, dot);
        String field = column.substring(dot + 1);
        if (rows.isNull(group) || !(rows.getValue(group) instanceof PqStruct struct) || struct.isNull(field)) {
            return new Object[2];
        }
        return new Object[] { struct.getValue(field), struct.getRawValue(field) };
    }

    static ColumnSchema schemaOf(Layout layout, String column) {
        return schemaOf(layout).getColumn(column);
    }

    static FileSchema schemaOf(Layout layout) {
        return SCHEMAS.computeIfAbsent(layout.name(), key -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(layout.path()), context,
                    ReaderConfig.defaults())) {
                return reader.getFileSchema();
            }
            catch (IOException e) {
                throw new IllegalStateException("Cannot open " + layout.path(), e);
            }
        });
    }

    static List<String> columnsOf(Layout layout) {
        return schemaOf(layout).getColumns().stream()
                .map(c -> c.fieldPath().toString())
                .toList();
    }
}
