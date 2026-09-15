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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/// The findings an audit run is expected to produce, and the comparison of a run against them.
///
/// A finding is one line: its step, then the step's own columns. Matrix, resolver, consultation and
/// round-trip lines are disagreements with the rule, of which a clean tree has none. A
/// `roundtrip-throw` names a logical read that threw, without the message. An `engine` line names a
/// predicate where an engine or Hardwood departs from the rule, with every engine's answer, so a
/// changed answer is drift even while the predicate still differs.
///
/// `baseline.tsv` holds the expected findings. Blank lines and lines starting with `#` are ignored;
/// the comments say why each finding is expected.
final class Baseline {

    record Comparison(List<String> unexpected, List<String> missing) {

        boolean matches() {
            return unexpected.isEmpty() && missing.isEmpty();
        }
    }

    private Baseline() {
    }

    static List<String> findings(List<Matrix.Tally> tallies, ResolverMatrix.Result resolver,
            ConsultationChecks.Result consultation, AccessorRoundTrip.Result roundTrip, EngineComparison.Result engines) {
        List<String> findings = new ArrayList<>();
        tallies.stream().flatMap(t -> t.disagreements().stream()).forEach(line -> findings.add("matrix\t" + line));
        resolver.disagreements().forEach(line -> findings.add("resolver\t" + line));
        consultation.unexpected().forEach(line -> findings.add("consultation\t" + line));
        roundTrip.disagreements().forEach(line -> findings.add("roundtrip\t" + line));
        // file, row, column and accessor; the message carries nothing the key does not.
        roundTrip.thrown().forEach(line -> findings.add("roundtrip-throw\t"
                + String.join("\t", Arrays.copyOf(line.split("\t", -1), 4))));
        engines.differing().forEach(line -> findings.add("engine\t" + line));
        return findings;
    }

    static Comparison compare(Path baseline, List<String> findings) throws IOException {
        // Trailing whitespace is dropped on both sides, so an editor trimming it keeps an entry intact.
        Set<String> expected = new LinkedHashSet<>();
        for (String line : Files.readAllLines(baseline)) {
            if (!line.isBlank() && !line.startsWith("#")) {
                expected.add(line.stripTrailing());
            }
        }
        Set<String> actual = new LinkedHashSet<>();
        findings.forEach(finding -> actual.add(finding.stripTrailing()));
        List<String> unexpected = actual.stream().filter(finding -> !expected.contains(finding)).toList();
        List<String> missing = expected.stream().filter(finding -> !actual.contains(finding)).toList();
        return new Comparison(unexpected, missing);
    }

    /// Writes every finding of the run, in baseline form, so the baseline can be updated from it.
    static void write(Path report, List<String> findings) throws IOException {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(report.resolve("findings.tsv")))) {
            findings.forEach(out::println);
        }
    }
}
