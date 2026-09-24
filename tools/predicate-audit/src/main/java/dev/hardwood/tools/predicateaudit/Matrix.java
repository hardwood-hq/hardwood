/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.tools.predicateaudit.Columns.Col;
import dev.hardwood.tools.predicateaudit.Oracle.P;
import dev.hardwood.tools.predicateaudit.Oracle.Rows;

/// Puts every case to every read path over every layout of a fixture group, and compares each
/// answer with the oracle.
final class Matrix {

    /// One fixture group: the file prefix, its columns and the cases put to it.
    record Group(String name, Rows rows, List<P> cases, List<String> layouts) {
    }

    /// The outcome of one group over one layout.
    record Tally(String group, String layout, int cells, int rows, int empty, int refused, int coarse, List<String> disagreements) {
    }

    private Matrix() {
    }

    static List<Group> groups() {
        List<String> flatLayouts = FixtureWriter.LAYOUTS.stream().map(FixtureWriter.Layout::name).toList();
        List<Group> groups = new ArrayList<>();
        List<P> flatCases = new ArrayList<>(Cases.forColumns(Columns.flat()));
        flatCases.addAll(Cases.compositions());
        groups.add(new Group("flat", flatRows(Columns.flat()), flatCases, flatLayouts));
        List<P> exoticCases = new ArrayList<>(Cases.forColumns(Columns.exotic()));
        exoticCases.addAll(Cases.intersects());
        groups.add(new Group("exotic", flatRows(Columns.exotic()), exoticCases, flatLayouts));
        groups.add(new Group("ts12", flatRows(Columns.ts12()), Cases.forColumns(Columns.ts12()), flatLayouts));
        groups.add(new Group("legacy", flatRows(Columns.legacy()), Cases.forColumns(Columns.legacy()), flatLayouts));
        groups.add(new Group("dropped", flatRows(Columns.dropped()), Cases.forColumns(Columns.dropped()), flatLayouts));
        groups.add(new Group("lowcard", flatRows(Columns.lowCardinality()), Cases.forColumns(Columns.lowCardinality()),
                flatLayouts));
        groups.add(new Group("nested", nestedRows(), Cases.nested(), List.of("single", "multi", "dict")));
        return groups;
    }

    static Rows flatRows(List<Col> columns) {
        Map<String, Col> byName = new HashMap<>();
        for (Col column : columns) {
            byName.put(column.name(), column);
        }
        return new Rows() {
            @Override
            public Col column(String name) {
                return byName.get(name);
            }

            @Override
            public Object stored(String name, int row) {
                return byName.get(name).at(row);
            }

            @Override
            public boolean nodeNull(String name, int row) {
                return byName.get(name).at(row) == null;
            }
        };
    }

    static Rows nestedRows() {
        Map<String, Col> leaves = Columns.nestedLeaves();
        return new Rows() {
            @Override
            public Col column(String name) {
                return leaves.get(name);
            }

            @Override
            public Object stored(String name, int row) {
                return leaves.get(name).at(row);
            }

            @Override
            public boolean nodeNull(String name, int row) {
                return switch (name) {
                    case "s" -> Columns.structNull(row);
                    case "s.t" -> Columns.innerStructNull(row);
                    case "l" -> Columns.listNull(row);
                    case "r" -> false;
                    default -> leaves.get(name).at(row) == null;
                };
            }
        };
    }

    /// Runs every group over every layout, writing `matrix.tsv`, and returns the tallies.
    static List<Tally> run(Path fixtures, Path report) throws Exception {
        List<Tally> tallies = new ArrayList<>();
        try (HardwoodContext context = HardwoodContext.create();
             ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
             PrintWriter matrix = new PrintWriter(Files.newBufferedWriter(report.resolve("matrix.tsv")))) {
            matrix.println("group\tlayout\tpredicate\texpected\tverdict\tanswers");
            Map<String, Future<List<String>>> lines = new LinkedHashMap<>();
            for (Group group : groups()) {
                for (String layout : group.layouts()) {
                    Path file = fixtures.resolve(group.name() + "_" + layout + ".parquet");
                    lines.put(group.name() + "\t" + layout, executor.submit(() -> runLayout(context, group, layout, file)));
                }
            }
            for (Map.Entry<String, Future<List<String>>> entry : lines.entrySet()) {
                String[] key = entry.getKey().split("\t");
                tallies.add(tally(key[0], key[1], entry.getValue().get(), matrix));
            }
        }
        return tallies;
    }

    private static Tally tally(String group, String layout, List<String> lines, PrintWriter matrix) {
        int rows = 0;
        int empty = 0;
        int refused = 0;
        int coarse = 0;
        List<String> disagreements = new ArrayList<>();
        for (String line : lines) {
            matrix.println(line);
            String[] fields = line.split("\t", 6);
            String expected = fields[3];
            switch (fields[4]) {
                case "DISAGREE" -> disagreements.add(line);
                case "ROW-GROUPS" -> coarse++;
                default -> {
                }
            }
            if (expected.startsWith("REFUSE")) {
                refused++;
            }
            else if (expected.startsWith("0 ")) {
                empty++;
            }
            else if (!fields[4].equals("ROW-GROUPS")) {
                rows++;
            }
        }
        return new Tally(group, layout, lines.size(), rows, empty, refused, coarse, disagreements);
    }

    private static List<String> runLayout(HardwoodContext context, Group group, String layout, Path file) throws IOException {
        List<String> lines = new ArrayList<>();
        try (ParquetFileReader withMetadata = ParquetFileReader.open(InputFile.of(file), context, ReaderConfig.defaults());
             ParquetFileReader withoutMetadata = ParquetFileReader.open(InputFile.of(file), context, ReadPath.NO_METADATA)) {
            for (P predicate : group.cases()) {
                lines.add(group.name() + "\t" + layout + "\t" + cell(group.rows(), predicate, withMetadata, withoutMetadata));
            }
        }
        return lines;
    }

    /// One cell: the predicate, what the rule expects, the verdict and the answers of every path.
    private static String cell(Rows rows, P predicate, ParquetFileReader withMetadata, ParquetFileReader withoutMetadata) {
        String refusal = Oracle.refusal(rows, predicate);
        boolean coarse = refusal == null && Oracle.containsIntersects(predicate);
        String expected = refusal != null ? "REFUSE (" + refusal + ")" : coarse ? "row groups" : expectedRows(rows, predicate);

        FilterPredicate filter;
        String buildFailure = null;
        try {
            filter = Predicates.of(predicate);
        }
        catch (RuntimeException e) {
            filter = null;
            buildFailure = "BUILD " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        Map<String, String> answers = new LinkedHashMap<>();
        boolean agrees = true;
        for (ReadPath path : ReadPath.values()) {
            String answer = buildFailure != null
                    ? buildFailure
                    : path.read(path.metadataFiltering ? withMetadata : withoutMetadata, filter);
            answers.put(path.label, answer);
            if (refusal != null) {
                agrees &= answer.startsWith("THROW IllegalArgumentException") || answer.startsWith("BUILD IllegalArgumentException");
            }
            else if (!coarse) {
                agrees &= answer.equals(expected);
            }
        }
        String verdict = coarse ? "ROW-GROUPS" : agrees ? "AGREE" : "DISAGREE";
        return Oracle.show(predicate) + "\t" + expected + "\t" + verdict + "\t" + render(answers);
    }

    private static String expectedRows(Rows rows, P predicate) {
        List<Long> matching = new ArrayList<>();
        for (int row = 0; row < Columns.ROWS; row++) {
            if (Oracle.eval(rows, predicate, row) == Oracle.Tri.T) {
                matching.add((long) row);
            }
        }
        return ReadPath.show(matching);
    }

    /// The paths' answers, collapsed to one where they all agree.
    private static String render(Map<String, String> answers) {
        if (answers.values().stream().distinct().count() == 1) {
            return answers.values().iterator().next();
        }
        StringBuilder rendered = new StringBuilder();
        answers.forEach((path, answer) -> rendered.append(path).append('=').append(answer).append("; "));
        return rendered.toString();
    }
}
