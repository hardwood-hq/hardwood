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
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;

/// Shows that the matrix's agreement is not an accident of layouts the reader never consults.
///
/// - **Bloom filters:** the same equality against `flat_bloom` and against a copy whose bitsets are
///   zeroed. A row that disappears in the copy was kept by the Bloom filter, so the filter was read.
///   A comparison that is not byte-exact must leave the filter unread.
/// - **Dictionaries:** row-group decisions recorded through the `dev.hardwood.RowGroupFilter` and
///   `dev.hardwood.RowGroupDictionaryFilter` JFR events on `lowcard_dict`. An absent literal inside the bounds can only be dropped by the dictionary.
///
/// Every check states the outcome the rule expects, and a check whose outcome differs is counted.
final class ConsultationChecks {

    /// One check: its description, the predicate, and the outcome the rule expects.
    private record Check(String name, FilterPredicate filter, String expected) {
    }

    record Result(int checks, List<String> unexpected, List<String> lines) {
    }

    private static final String CONSULTED = "consulted";
    private static final String NOT_CONSULTED = "not consulted";

    /// `lowcard_dict` holds three row groups, each with every one of the 40 distinct values.
    private static final String ALL_KEPT = "kept 3 of 3";
    private static final String NONE_KEPT = "kept 0 of 3";

    private ConsultationChecks() {
    }

    static Result run(Path fixtures, Path report) throws Exception {
        List<String> lines = new ArrayList<>();
        List<String> unexpected = new ArrayList<>();
        lines.add("check\tpredicate\texpected\tresult\tverdict");
        List<Check> bloomChecks = bloomChecks();
        List<Check> dictionaryChecks = dictionaryChecks();
        for (Check check : bloomChecks) {
            long intact = count(fixtures.resolve("flat_bloom.parquet"), check.filter());
            long zeroed = count(fixtures.resolve("zerobloom_bloom.parquet"), check.filter());
            String outcome = intact > zeroed ? CONSULTED : NOT_CONSULTED;
            record(lines, unexpected, "bloom", check, outcome,
                    "rows " + intact + " with the filter intact, " + zeroed + " with it zeroed: " + outcome);
        }
        for (Check check : dictionaryChecks) {
            RowGroupTally tally = new RowGroupTally();
            long rows = rowGroupDecisions(fixtures.resolve("lowcard_dict.parquet"), check.filter(), tally);
            String outcome = tally.toString();
            record(lines, unexpected, "dictionary", check, outcome, "rows " + rows + ", row groups " + outcome);
        }
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(report.resolve("consultation.tsv")))) {
            lines.forEach(out::println);
        }
        return new Result(bloomChecks.size() + dictionaryChecks.size(), unexpected, lines);
    }

    private static void record(List<String> lines, List<String> unexpected, String kind, Check check, String outcome, String result) {
        boolean asExpected = outcome.equals(check.expected());
        String line = kind + "\t" + check.name() + "\t" + check.expected() + "\t" + result + "\t" + (asExpected ? "AS EXPECTED" : "UNEXPECTED");
        lines.add(line);
        if (!asExpected) {
            unexpected.add(line);
        }
    }

    private static List<Check> bloomChecks() {
        return List.of(
                new Check("eq(i32, 20)", FilterPredicate.eq("i32", 20), CONSULTED),
                new Check("eq(str, \"k0310\")", FilterPredicate.eq("str", "k0310"), CONSULTED),
                new Check("eq(f32, 5.0f)", FilterPredicate.eq("f32", 5.0f), CONSULTED),
                new Check("eq(dec_ba, bytes 14)", FilterPredicate.eq("dec_ba", new byte[] { 0x14 }), CONSULTED),
                new Check("eq(dec_ba, BigDecimal 0.020), not byte-exact", FilterPredicate.eq("dec_ba", new BigDecimal("0.020")),
                        NOT_CONSULTED),
                new Check("eq(ts96, the 12 bytes of row 310)",
                        FilterPredicate.eq("ts96", Columns.int96(Columns.int96Nanos(Columns.PROBE_ROW), true)), CONSULTED),
                new Check("eq(ts96, Instant of row 310), not byte-exact",
                        FilterPredicate.eq("ts96", Instant.EPOCH.plusNanos(Columns.int96Nanos(Columns.PROBE_ROW))), NOT_CONSULTED),
                new Check("eq(f16, 2.5f), not byte-exact", FilterPredicate.eq("f16", 2.5f), NOT_CONSULTED),
                new Check("eq(f16, bytes of row 310)", FilterPredicate.eq("f16", Columns.float16At(Columns.PROBE_ROW)), CONSULTED),
                new Check("eq(dec_flba, BigDecimal 0.20)", FilterPredicate.eq("dec_flba", new BigDecimal("0.20")), CONSULTED),
                new Check("eq(uuid, bytes of row 310)", FilterPredicate.eq("uuid", Columns.uuidAt(Columns.PROBE_ROW)), CONSULTED),
                new Check("in(i64, 200000000000)", FilterPredicate.in("i64", 200_000_000_000L), CONSULTED),
                new Check("eq(u32, row 310)", FilterPredicate.eq("u32", (int) (2_147_483_648L - 300_000_000L + 310 * 1_000_000L)),
                        CONSULTED));
    }

    private static List<Check> dictionaryChecks() {
        return List.of(
                new Check("eq(str, \"k0100\"), absent, in range", FilterPredicate.eq("str", "k0100"), NONE_KEPT),
                new Check("eq(str, \"k0300\"), present", FilterPredicate.eq("str", "k0300"), ALL_KEPT),
                new Check("eq(f32, 0.75f), absent", FilterPredicate.eq("f32", 0.75f), NONE_KEPT),
                new Check("eq(f32, NaN), present under three payloads", FilterPredicate.eq("f32", Float.NaN), ALL_KEPT),
                new Check("in(f64, 0.75, 1.25), absent", FilterPredicate.in("f64", 0.75, 1.25), NONE_KEPT),
                new Check("eq(f16, 0.125f), absent", FilterPredicate.eq("f16", 0.125f), NONE_KEPT),
                new Check("eq(dec_ba, bytes 02), absent: the row stores 00 02", FilterPredicate.eq("dec_ba", new byte[] { 0x02 }),
                        NONE_KEPT),
                new Check("eq(dec_ba, bytes 00 02), present", FilterPredicate.eq("dec_ba", new byte[] { 0x00, 0x02 }), ALL_KEPT),
                new Check("eq(dec_flba, bytes of 0.40), present",
                        FilterPredicate.eq("dec_flba", Columns.fixedDecimal(BigInteger.valueOf(40), 9)), ALL_KEPT));
    }

    private static long count(Path file, FilterPredicate filter) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = reader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }

    /// The row groups a read kept: those `dev.hardwood.RowGroupFilter` reports kept by statistics
    /// and bloom filters, less each one a `dev.hardwood.RowGroupDictionaryFilter` event reports
    /// dropped by its dictionaries.
    private static final class RowGroupTally {

        private int total;
        private int kept;

        synchronized void add(RecordedEvent event) {
            if (event.getEventType().getName().equals("dev.hardwood.RowGroupFilter")) {
                total += event.getInt("totalRowGroups");
                kept += event.getInt("rowGroupsKept");
            }
            else {
                kept--;
            }
        }

        @Override
        public synchronized String toString() {
            return "kept " + kept + " of " + total;
        }
    }

    /// Reads `file` under `filter`, adding each row-group decision to `tally`, and returns the row count.
    private static long rowGroupDecisions(Path file, FilterPredicate filter, RowGroupTally tally) throws Exception {
        long rows;
        try (RecordingStream stream = new RecordingStream()) {
            stream.enable("dev.hardwood.RowGroupFilter");
            stream.enable("dev.hardwood.RowGroupDictionaryFilter");
            stream.onEvent("dev.hardwood.RowGroupFilter", tally::add);
            stream.onEvent("dev.hardwood.RowGroupDictionaryFilter", tally::add);
            stream.startAsync();
            rows = count(file, filter);
            stream.stop();
            stream.awaitTermination(Duration.ofSeconds(5));
        }
        return rows;
    }
}
