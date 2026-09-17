/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/// Entry point of the predicate rule audit; see `tools/predicate-audit/README.md`.
///
/// - `fixtures <dir>` writes the parquet-java fixtures.
/// - `audit <fixtures> <report> <baseline>` runs the matrix, the resolver matrix, the consultation
///   checks, the accessor round-trip and the engine comparison over fixtures and their derived
///   variants, writes `summary.md` beside the full results, and exits with status 3 when the
///   findings differ from the baseline.
public final class PredicateAudit {

    /// The reader's logger for statistics it will not prune by. Named as a string because the
    /// class it belongs to is package-private.
    private static final String DISCARDED_STATS_LOGGER = "dev.hardwood.internal.predicate.MinMaxStats";

    /// Held for the lifetime of the run: a logger keeps the level set on it only while something
    /// keeps the logger.
    private static final Logger DISCARDED_STATS = Logger.getLogger(DISCARDED_STATS_LOGGER);

    static {
        // Fixtures carry NaN bounds and sort orders the reader does not read, by design, and every
        // read of a cell meets them again. Left on, one run writes over a hundred thousand records
        // through one synchronized handler, which every audit thread then queues behind.
        DISCARDED_STATS.setLevel(Level.SEVERE);
        // The logger is named by string, and `System.Logger` only routes to `java.util.logging`
        // while no `System.LoggerFinder` provider is on the classpath. Either a rename of the
        // class or an added provider would turn the line above into a no-op, which shows up as
        // an audit that is slow again rather than as a failure.
        if (DISCARDED_STATS.isLoggable(Level.WARNING)) {
            throw new IllegalStateException(
                    "Failed to silence " + DISCARDED_STATS_LOGGER + ": it still logs at WARNING");
        }
    }

    private PredicateAudit() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("fixtures")) {
            FixtureWriter.writeAll(Path.of(args[1]));
            return;
        }
        if (args.length == 4 && args[0].equals("audit")) {
            if (!audit(Path.of(args[1]), Path.of(args[2]), Path.of(args[3]))) {
                System.exit(3);
            }
            return;
        }
        System.err.println("usage: PredicateAudit fixtures <dir> | audit <fixtures> <report> <baseline>");
        System.exit(2);
    }

    /// Returns whether the run's findings match the baseline.
    private static boolean audit(Path fixtures, Path report, Path baseline) throws Exception {
        Files.createDirectories(report);
        long start = System.nanoTime();
        List<Matrix.Tally> tallies = Matrix.run(fixtures, report);
        log("matrix", start);
        ResolverMatrix.Result resolver = ResolverMatrix.run(fixtures, report);
        log("resolver", start);
        ConsultationChecks.Result consultation = ConsultationChecks.run(fixtures, report);
        log("consultation checks", start);
        AccessorRoundTrip.Result roundTrip = AccessorRoundTrip.run(fixtures, report);
        log("accessor round-trip", start);
        EngineComparison.Result engines = EngineComparison.run(fixtures, report);
        log("engines", start);
        List<String> findings = Baseline.findings(tallies, resolver, consultation, roundTrip, engines);
        Baseline.write(report, findings);
        Baseline.Comparison comparison = Baseline.compare(baseline, findings);
        writeSummary(report, tallies, resolver, consultation, roundTrip, engines, comparison);
        System.out.println("report: " + report.resolve("summary.md"));
        if (!comparison.matches()) {
            System.out.println("findings differ from " + baseline + ": " + comparison.unexpected().size() + " unexpected, "
                    + comparison.missing().size() + " missing");
            comparison.unexpected().forEach(finding -> System.out.println("+ " + finding));
            comparison.missing().forEach(finding -> System.out.println("- " + finding));
        }
        return comparison.matches();
    }

    private static void log(String step, long start) {
        System.out.println(step + " done after " + Duration.ofNanos(System.nanoTime() - start).toSeconds() + " s");
    }

    private static void writeSummary(Path report, List<Matrix.Tally> tallies, ResolverMatrix.Result resolver,
            ConsultationChecks.Result consultation, AccessorRoundTrip.Result roundTrip, EngineComparison.Result engines,
            Baseline.Comparison comparison)
            throws Exception {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(report.resolve("summary.md")))) {
            out.println("# Predicate rule audit");
            out.println();
            int cells = tallies.stream().mapToInt(Matrix.Tally::cells).sum();
            int disagreements = tallies.stream().mapToInt(t -> t.disagreements().size()).sum();
            out.println("- **Matrix:** " + cells + " cells x " + ReadPath.values().length + " read paths, " + disagreements
                    + " disagreeing with the rule (`matrix.tsv`)");
            out.println("- **Resolver:** " + resolver.cells() + " cells, " + resolver.accepted() + " accepted, "
                    + resolver.disagreements().size() + " disagreeing with the rule (`resolver.tsv`)");
            out.println("- **Consultation:** " + consultation.checks() + " checks, " + consultation.unexpected().size()
                    + " not as expected (`consultation.tsv`)");
            out.println("- **Accessor round-trip:** " + roundTrip.checks() + " values passed back as literals, "
                    + roundTrip.disagreements().size() + " disagreeing with the rule, " + roundTrip.thrown().size()
                    + " logical reads that threw (`roundtrip.tsv`)");
            out.println("- **Baseline:** " + (comparison.matches() ? "matches" : comparison.unexpected().size()
                    + " findings not in it, " + comparison.missing().size() + " of its findings missing")
                    + " (`findings.tsv`)");
            out.println("- **Engines:** " + engines.predicates() + " predicates, " + engines.differing().size()
                    + " where an engine or Hardwood departs from the rule, or on the PyArrow file the engines from each other"
                    + " (`engines.tsv`)");
            out.println();
            if (!comparison.matches()) {
                out.println("## Baseline drift");
                out.println();
                out.println("```");
                comparison.unexpected().forEach(finding -> out.println("+ " + finding));
                comparison.missing().forEach(finding -> out.println("- " + finding));
                out.println("```");
                out.println();
            }
            out.println("## Matrix");
            out.println();
            out.println("| group | layout | cells | rows | empty | refused | row groups (`intersects`) | disagree |");
            out.println("|---|---|---|---|---|---|---|---|");
            for (Matrix.Tally t : tallies) {
                out.println("| " + t.group() + " | " + t.layout() + " | " + t.cells() + " | " + t.rows() + " | " + t.empty() + " | "
                        + t.refused() + " | " + t.coarse() + " | " + t.disagreements().size() + " |");
            }
            out.println();
            out.println("## Disagreements");
            out.println();
            out.println("```");
            tallies.stream().flatMap(t -> t.disagreements().stream()).forEach(out::println);
            resolver.disagreements().forEach(out::println);
            consultation.unexpected().forEach(out::println);
            roundTrip.disagreements().forEach(out::println);
            out.println("```");
            out.println();
            out.println("## Logical reads that threw");
            out.println();
            out.println("Expected only for a stored value past what its logical type represents, such as a `TIME` of 25 hours.");
            out.println();
            out.println("```");
            roundTrip.thrown().forEach(out::println);
            out.println("```");
            out.println();
            out.println("## Bloom filter and dictionary consultation");
            out.println();
            out.println("```");
            consultation.lines().forEach(out::println);
            out.println("```");
        }
    }
}
