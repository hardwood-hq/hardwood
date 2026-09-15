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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.hardwood.tools.predicateaudit.Oracle.In;
import dev.hardwood.tools.predicateaudit.Oracle.Intersects;
import dev.hardwood.tools.predicateaudit.Oracle.IsNull;
import dev.hardwood.tools.predicateaudit.Oracle.Leaf;
import dev.hardwood.tools.predicateaudit.Oracle.Not;
import dev.hardwood.tools.predicateaudit.Oracle.Op;
import dev.hardwood.tools.predicateaudit.Oracle.P;
import dev.hardwood.tools.predicateaudit.Oracle.Rows;

/// Resolves one literal of every kind, under every operator, against every column and group of
/// the fixtures, and compares acceptance with the oracle. It also records the build-time checks.
final class ResolverMatrix {

    private static final List<Object> SAMPLES = List.of(true, 1, 1L, 1.0f, 1.0, new byte[] { 1 }, "a", LocalDate.EPOCH,
            Instant.EPOCH, LocalDateTime.of(1970, 1, 1, 0, 0), LocalTime.NOON, BigDecimal.ONE, new UUID(0, 1),
            new PqInterval(1, 1, 1));

    record Result(int cells, int accepted, List<String> disagreements) {
    }

    private ResolverMatrix() {
    }

    static Result run(Path fixtures, Path report) throws Exception {
        Map<String, Rows> files = new LinkedHashMap<>();
        files.put("flat_single", Matrix.flatRows(Columns.flat()));
        files.put("exotic_single", Matrix.flatRows(Columns.exotic()));
        files.put("legacy_single", Matrix.flatRows(Columns.legacy()));
        files.put("dropped_single", Matrix.flatRows(Columns.dropped()));
        files.put("nested_single", null);
        files.put("shapes", null);
        int cells = 0;
        int accepted = 0;
        List<String> disagreements = new ArrayList<>();
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(report.resolve("resolver.tsv")))) {
            out.println("file\tcolumn\tpredicate\trule\thardwood");
            for (Map.Entry<String, Rows> file : files.entrySet()) {
                FileSchema schema = schema(fixtures.resolve(file.getKey() + ".parquet"));
                for (String column : nodeNames(schema)) {
                    for (P predicate : predicates(column)) {
                        cells++;
                        String hardwood = outcome(schema, () -> Predicates.of(predicate));
                        String rule = expectation(file.getValue(), file.getKey(), column, predicate);
                        if (hardwood.startsWith("ACCEPT")) {
                            accepted++;
                        }
                        String line = file.getKey() + "\t" + column + "\t" + Oracle.show(predicate) + "\t" + rule + "\t" + hardwood;
                        out.println(line);
                        if (!rule.equals("?") && !rule.regionMatches(0, hardwood, 0, 6)) {
                            disagreements.add(line);
                        }
                    }
                }
            }
            FileSchema flat = schema(fixtures.resolve("flat_single.parquet"));
            for (Map.Entry<String, Supplier<FilterPredicate>> special : buildTimeCases().entrySet()) {
                out.println("build-time\t-\t" + special.getKey() + "\t?\t" + outcome(flat, special.getValue()));
            }
        }
        return new Result(cells, accepted, disagreements);
    }

    private static FileSchema schema(Path file) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return reader.getFileSchema();
        }
    }

    private static List<String> nodeNames(FileSchema schema) {
        List<String> names = new ArrayList<>();
        for (SchemaNode child : schema.getRootNode().children()) {
            collect(child, "", names);
        }
        return names;
    }

    private static void collect(SchemaNode node, String prefix, List<String> names) {
        String name = prefix.isEmpty() ? node.name() : prefix + "." + node.name();
        names.add(name);
        if (node instanceof SchemaNode.GroupNode group) {
            for (SchemaNode child : group.children()) {
                collect(child, name, names);
            }
        }
    }

    private static List<P> predicates(String column) {
        List<P> predicates = new ArrayList<>();
        for (Object literal : SAMPLES) {
            for (Op op : Op.values()) {
                predicates.add(new Leaf(column, op, literal));
            }
            if (!(literal instanceof Boolean)) {
                predicates.add(new In(column, List.of(literal)));
                predicates.add(new Not(new In(column, List.of(literal))));
            }
        }
        predicates.add(new IsNull(column));
        predicates.add(new Intersects(column, 0, 0, 1, 1));
        predicates.add(new Not(new Intersects(column, 0, 0, 1, 1)));
        return predicates;
    }

    private static String outcome(FileSchema schema, Supplier<FilterPredicate> build) {
        FilterPredicate predicate;
        try {
            predicate = build.get();
        }
        catch (RuntimeException e) {
            return "BUILD " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        try {
            return "ACCEPT " + FilterPredicateResolver.resolve(predicate, schema);
        }
        catch (IllegalArgumentException e) {
            return "REFUSE " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        catch (RuntimeException e) {
            // Not a refusal: the resolver refuses a predicate with an IllegalArgumentException only.
            return "CRASH " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    /// What the rule says, `ACCEPT` or `REFUSE (reason)`, or `?` for a column the oracle does not model.
    private static String expectation(Rows rows, String file, String column, P predicate) {
        boolean nullTest = predicate instanceof IsNull;
        if (rows != null && rows.column(column) != null) {
            String refusal = nullTest ? null : Oracle.refusal(rows, predicate);
            return refusal == null ? "ACCEPT" : "REFUSE (" + refusal + ")";
        }
        return switch (file + ":" + column) {
            case "nested_single:s", "nested_single:s.t", "nested_single:l", "shapes:v", "shapes:sv", "shapes:m", "shapes:st",
                 "shapes:st.inner" -> nullTest ? "ACCEPT" : "REFUSE (group)";
            case "nested_single:l.list", "nested_single:l.list.element", "shapes:m.key_value", "shapes:m.key_value.key",
                 "shapes:m.key_value.value", "shapes:rep" -> "REFUSE (below a repeated path)";
            case "shapes:v.metadata", "shapes:v.value", "shapes:sv.metadata", "shapes:sv.value", "shapes:sv.typed_value" ->
                nullTest ? "ACCEPT" : "REFUSE (VARIANT leaf)";
            default -> "?";
        };
    }

    private static Map<String, Supplier<FilterPredicate>> buildTimeCases() {
        Map<String, Supplier<FilterPredicate>> cases = new LinkedHashMap<>();
        cases.put("eq(str, (String) null)", () -> FilterPredicate.eq("str", (String) null));
        cases.put("eq(ba, (byte[]) null)", () -> FilterPredicate.eq("ba", (byte[]) null));
        cases.put("eq(dec_i32, (BigDecimal) null)", () -> FilterPredicate.eq("dec_i32", (BigDecimal) null));
        cases.put("in(str, \"a\", null)", () -> FilterPredicate.in("str", "a", null));
        cases.put("in(date, LocalDate.EPOCH, null)", () -> FilterPredicate.in("date", LocalDate.EPOCH, null));
        cases.put("in(str, new String[0])", () -> FilterPredicate.in("str", new String[0]));
        cases.put("eq(str, \"\\uD800\")", () -> FilterPredicate.eq("str", "\uD800"));
        cases.put("eq(null, 1)", () -> FilterPredicate.eq(null, 1));
        cases.put("not(null)", () -> FilterPredicate.not(null));
        cases.put("and(eq(i32, 1), null)", () -> FilterPredicate.and(FilterPredicate.eq("i32", 1), null));
        cases.put("eq(f16, NaN with payload 0x7fc00001)", () -> FilterPredicate.eq("f16", Float.intBitsToFloat(0x7fc00001)));
        cases.put("eq(f16, 1.4E-45f)", () -> FilterPredicate.eq("f16", 1.4e-45f));
        cases.put("lt(f16, 1.4E-45f)", () -> FilterPredicate.lt("f16", 1.4e-45f));
        cases.put("eq(time_ms, LocalTime.MAX)", () -> FilterPredicate.eq("time_ms", LocalTime.MAX));
        cases.put("lt(time_ms, LocalTime.MAX)", () -> FilterPredicate.lt("time_ms", LocalTime.MAX));
        cases.put("not(gt(bool, true))", () -> FilterPredicate.not(FilterPredicate.gt("bool", true)));
        cases.put("not(lt(date, LocalDate.MIN))", () -> FilterPredicate.not(FilterPredicate.lt("date", LocalDate.MIN)));
        return cases;
    }
}
